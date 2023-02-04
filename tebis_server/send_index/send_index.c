// Copyright [2023] [FORTH-ICS]
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
#include "send_index.h"
#include "../metadata.h"
#include "../region_desc.h"
#include "../tebis_rdma/rdma.h"
#include "btree/conf.h"
#include "btree/level_write_appender.h"
#include "send_index_callbacks.h"
#include "send_index_rewriter.h"
#include <assert.h>
#include <infiniband/verbs.h>
#include <log.h>
#include <parallax/parallax.h>
#include <parallax/structures.h>
#include <rdma/rdma_verbs.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
// IWYU pragma: no_forward_declare region_desc
#define SEGMENT_START(x) (x - (x % SEGMENT_SIZE))

uint64_t send_index_flush_rdma_buffer(struct region_desc *r_desc, enum log_category log_type)
{
	struct ru_replica_state *replica_state = region_desc_get_replica_state(r_desc);
	uint32_t rdma_buffer_size = replica_state->l0_recovery_rdma_buf.rdma_buf_size;
	char *rdma_buffer = (char *)replica_state->l0_recovery_rdma_buf.mr->addr;
	if (log_type == BIG)
		rdma_buffer = (char *)replica_state->big_recovery_rdma_buf.mr->addr;

	const char *error_message = NULL;
	/*persist the buffer*/
	uint64_t replica_new_segment_offt = par_flush_segment_in_log(region_desc_get_db(r_desc), rdma_buffer,
								     rdma_buffer_size, log_type, &error_message);
	if (error_message) {
		log_fatal("the flushing of the segment failed");
		_exit(EXIT_FAILURE);
	}

	return replica_new_segment_offt;
}

void send_index_create_compactions_rdma_buffer(struct send_index_create_compactions_rdma_buffer_params params)
{
	struct ru_replica_state *r_state = region_desc_get_replica_state(params.r_desc);

	if (r_state->index_buffer[params.level_id] != NULL) {
		log_fatal("Remote compaction for regions %s still pending", region_desc_get_id(params.r_desc));
		assert(0);
		_exit(EXIT_FAILURE);
	}
	// acquire a transacation ID (if level > 0) for parallax and initialize a level write appender for the upcoming compaction */
	par_init_compaction_id(region_desc_get_db(params.r_desc), params.level_id, 1);
	r_state->wappender[params.level_id] =
		wappender_init((struct db_handle *)region_desc_get_db(params.r_desc), params.level_id);
	// assign index buffer aswell
	char *backup_segment_index = NULL;
	uint32_t backup_segment_index_size = params.number_of_rows * params.number_of_columns * params.size_of_entry;

	if (posix_memalign((void **)&backup_segment_index, 4096, backup_segment_index_size)) {
		log_fatal("Failed to allocate a index buffer for the backup");
		_exit(EXIT_FAILURE);
	}

	r_state->index_buffer[params.level_id] =
		rdma_reg_write(params.conn->rdma_cm_id, backup_segment_index, backup_segment_index_size);
	if (r_state->index_buffer[params.level_id] == NULL) {
		log_fatal("Failed to reg memory");
		_exit(EXIT_FAILURE);
	}
	//log_debug("Index buffers bounds are [%lu,%lu]", (uint64_t)backup_segment_index,
	//	  (uint64_t)backup_segment_index + backup_segment_index_size);
}

void send_index_create_mr_for_segment_replies(struct send_index_create_mr_for_segment_replies_params params)
{
	struct ru_replica_state *r_state = region_desc_get_replica_state(params.r_desc);
	if (r_state->index_segment_flush_replies[params.level_id] != NULL) {
		log_fatal("The segment flush reply memory region is in use when a compaction started for level %u",
			  params.level_id);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	char *backup_segment_flush_replies_mr = NULL;
	uint32_t backup_segment_flush_replies_mr_size = WCURSOR_ALIGNMNENT;

	if (posix_memalign((void **)&backup_segment_flush_replies_mr, WCURSOR_ALIGNMNENT,
			   backup_segment_flush_replies_mr_size)) {
		log_fatal("Faield to allocate a 4k reply mr");
		assert(0);
		_exit(EXIT_FAILURE);
	}
	r_state->index_segment_flush_replies[params.level_id] = rdma_reg_write(
		params.conn->rdma_cm_id, backup_segment_flush_replies_mr, backup_segment_flush_replies_mr_size);
}

void send_index_close_compactions_rdma_buffer(struct region_desc *r_desc, uint32_t level_id)
{
	struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
	//free index buffer that was allocated for sending the index for the specific level
	if (rdma_dereg_mr(r_state->index_buffer[level_id])) {
		log_fatal("Failed to deregister rdma buffer");
		_exit(EXIT_FAILURE);
	}
	r_state->index_buffer[level_id] = NULL;
	wappender_close(r_state->wappender[level_id]);
	r_state->wappender[level_id] = NULL;
}

void send_index_close_mr_for_segment_replies(region_desc_t r_desc, uint32_t level_id)
{
	struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
	if (rdma_dereg_mr(r_state->index_segment_flush_replies[level_id])) {
		log_fatal("Failed to deregister rdma buffer");
		_exit(EXIT_FAILURE);
	}
	r_state->index_segment_flush_replies[level_id] = NULL;
	free(r_state->index_segment_flush_replies[level_id]);
}

void send_index_translate_primary_metadata(region_desc_t r_desc, uint32_t level_id, uint64_t primary_last_segment_offt,
					   uint64_t primary_first_segment_offt, uint64_t primary_new_root_offt)
{
	assert(r_desc);

	/*translate primary's fist and last compaction segment offts, root, and update replicas db_descriptor*/
	uint64_t replica_compaction_first_segment_offt =
		region_desc_get_indexmap_seg(r_desc, primary_first_segment_offt, level_id);
	if (!replica_compaction_first_segment_offt) {
		log_fatal(
			"Primary's first compaction segment offt is not present in the index map, this should never happen...");
		_exit(EXIT_FAILURE);
	}
	uint64_t replica_compaction_last_segment_offt =
		region_desc_get_indexmap_seg(r_desc, primary_last_segment_offt, level_id);
	if (!replica_compaction_first_segment_offt) {
		log_fatal(
			"Primary's last compaction segment offt is not present in the index map, this should never happen...");
		_exit(EXIT_FAILURE);
	}

	uint64_t primary_root_segment = SEGMENT_START(primary_new_root_offt);
	uint64_t root_shift = primary_new_root_offt - primary_root_segment;

	uint64_t replica_seg_offt = region_desc_get_indexmap_seg(r_desc, primary_root_segment, level_id);
	if (!replica_seg_offt) {
		log_fatal("There is no chance we dont have a segment for root already");
		_exit(EXIT_FAILURE);
	}
	uint64_t root_offt = replica_seg_offt + root_shift;
	struct db_handle *db = (struct db_handle *)region_desc_get_db(r_desc);
	db->db_desc->levels[level_id].first_segment[1] = REAL_ADDRESS(replica_compaction_first_segment_offt);
	db->db_desc->levels[level_id].last_segment[1] = REAL_ADDRESS(replica_compaction_last_segment_offt);
	db->db_desc->levels[level_id].root[1] = REAL_ADDRESS(root_offt);
}
