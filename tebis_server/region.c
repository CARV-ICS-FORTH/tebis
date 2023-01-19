// Copyright [2020] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "region.h"
#include "../tebis_rdma/rdma.h"
#include "../tebis_server/send_index/send_index_callbacks.h"
#include "configurables.h"
#include "metadata.h"
#include "server_communication.h"
#include <btree/conf.h>
#include <infiniband/verbs.h>
#include <pthread.h>

struct region_desc {
	pthread_mutex_t region_mgmnt_lock;
	pthread_rwlock_t kreon_lock;

	struct krm_region *region;
	/*for replica_role deserializing the index*/
	pthread_rwlock_t replica_log_map_lock;
	struct krm_segment_entry *replica_log_map;
	//struct krm_segment_entry *replica_index_map[MAX_LEVELS];
	//RDMA related staff for sending the index
	struct ibv_mr remote_mem_buf[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct ibv_mr *local_buffer[MAX_HEIGHT];
	struct sc_msg_pair rpc[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct rdma_message_context rpc_ctx[KRM_MAX_BACKUPS][MAX_LEVELS];
	uint8_t rpc_in_use[KRM_MAX_BACKUPS][MAX_LEVELS];
	//Staff for deserializing the index at the replicas
	struct di_buffer *index_buffer[MAX_LEVELS][MAX_HEIGHT];
	// send_index_reply_checker_t send_index_reply_checker;
	enum krm_region_role role;
	par_handle *db;
	union {
		struct ru_master_state *m_state;
		struct ru_replica_state *r_state;
	};
	volatile int64_t pending_region_tasks;
	int64_t next_lsn_to_be_replicated;
	enum region_replica_buf_status replica_buf_status;
	enum krm_region_status status;
};

static void region_initialize_rdma_buf_metadata(struct ru_master_log_buffer *rdma_buf, uint32_t backup_id,
						struct s2s_msg_get_rdma_buffer_rep *rep,
						enum tb_rdma_buf_category rdma_buf_cat)
{
	rdma_buf->segment_size = SEGMENT_SIZE;
	rdma_buf->segment.start = 0;
	rdma_buf->segment.end = SEGMENT_SIZE;
	rdma_buf->segment.curr_end = 0;
	rdma_buf->segment.mr[backup_id] = rep->l0_recovery_mr;
	if (rdma_buf_cat == TEBIS_BIG_RECOVERY_RDMA_BUF)
		rdma_buf->segment.mr[backup_id] = rep->big_recovery_mr;
	rdma_buf->segment.replicated_bytes = 0;
	assert(rdma_buf->segment.mr[backup_id].length == SEGMENT_SIZE);
}

uint32_t region_got_all_get_rdma_buffers_replies(region_desc_t r_desc)
{
	/*check replies*/
	uint32_t ready_buffers = 0;
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		if (r_desc->m_state->primary_to_backup[i].stat == RU_BUFFER_REQUESTED) {
			/*check reply and process
			 *wait first for the header and then the payload
			*/
			if (r_desc->m_state->primary_to_backup[i].msg_pair.reply->receive != TU_RDMA_REGULAR_MSG)
				continue;
			/*Check arrival of payload*/
			uint8_t tail = get_receive_field(r_desc->m_state->primary_to_backup[i].msg_pair.reply);
			if (tail != TU_RDMA_REGULAR_MSG)
				continue;

			struct s2s_msg_get_rdma_buffer_rep *rep =
				(struct s2s_msg_get_rdma_buffer_rep *)(((char *)r_desc->m_state->primary_to_backup[i]
										.msg_pair.reply) +
								       sizeof(struct msg_header));
			assert(rep->status == TEBIS_SUCCESS);

			region_initialize_rdma_buf_metadata(&r_desc->m_state->l0_recovery_rdma_buf, i, rep,
							    TEBIS_L0_RECOVERY_RDMA_BUF);
			region_initialize_rdma_buf_metadata(&r_desc->m_state->big_recovery_rdma_buf, i, rep,
							    TEBIS_BIG_RECOVERY_RDMA_BUF);

			r_desc->m_state->primary_to_backup[i].stat = RU_BUFFER_OK;
			/*finally free the message*/
			sc_free_rpc_pair(&r_desc->m_state->primary_to_backup[i].msg_pair);
		}
		if (r_desc->m_state->primary_to_backup[i].stat == RU_BUFFER_OK)
			++ready_buffers;
	}

	if (ready_buffers != r_desc->region->num_of_backup) {
		// log_info("Not all replicas ready waiting status %d",
		// task->kreon_operation_status);
		return 0;
	}
	return 1;
}
