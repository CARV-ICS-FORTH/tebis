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

#ifndef WORK_TASK_H
#define WORK_TASK_H
#include "../tebis_rdma/rdma.h"
#include "configurables.h"
#include "messages.h"
#include "metadata.h"
#include "region_desc.h"
#include "server_communication.h"
#include <include/parallax/parallax.h>
#include <stdint.h>

enum work_task_type { KRM_CLIENT_TASK, KRM_SERVER_TASK };

enum work_task_status {
	/*overall_status*/
	TASK_START = 1,
	TASK_COMPLETE,
	/*mutation operations related*/
	GET_RSTATE,
	INIT_LOG_BUFFERS,
	INS_TO_KREON,
	REPLICATE,
	WAIT_FOR_REPLICATION_TURN,
	WAIT_FOR_REPLICATION_COMPLETION,
	ALL_REPLICAS_ACKED,
	SEND_FLUSH_COMMANDS,
	WAIT_FOR_FLUSH_REPLIES,
	TASK_GET_KEY,
	TASK_MULTIGET,
	TASK_DELETE_KEY,
	TASK_NO_OP,
	TASK_FLUSH_L0,
	TASK_CLOSE_COMPACTION,
	TASK_FLUSH_MEDIUM_LOG,
};

struct work_task {
	/*from client*/
	struct rdma_message_context msg_ctx[RU_MAX_NUM_REPLICAS];
	volatile uint64_t *replicated_bytes;
	struct par_put_metadata insert_metadata;
	uint32_t last_replica_to_ack;
	uint32_t msg_payload_size;
	/*possible messages to other server generated from this task*/
	struct sc_msg_pair communication_buf;
	struct channel_rdma *channel;
	struct connection_rdma *conn;
	struct msg_header *msg;
	region_desc_t r_desc;
	struct kv_splice *kv;
	enum tb_kv_category kv_category; /*XXX TODO make these a struct XXX*/
	uint32_t triggering_msg_offset;
	msg_header *reply_msg;
	msg_header *flush_segment_request;
	struct krm_replica_index_state *index;
	int server_id;
	int thread_id;
	int error_code;
	//int suspended;
	int seg_id_to_flush;
	uint64_t rescheduling_counter;
	enum work_task_type task_type;
	enum work_task_status kreon_operation_status;
};
#endif
