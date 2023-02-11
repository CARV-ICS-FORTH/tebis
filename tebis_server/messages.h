// Copyright [2021] [FORTH-ICS]
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
#ifndef TEBIS_MESSAGES_H
#define TEBIS_MESSAGES_H
#include <include/parallax/structures.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <stdbool.h>

#define MSG_MAX_REGION_KEY_SIZE 64
#define MAX_REPLICA_INDEX_BUFFERS 8
#define NUMBER_OF_TASKS 15

enum message_type {
	REPLICA_INDEX_GET_BUFFER_REQ = 0,
	REPLICA_INDEX_FLUSH_REQ,
	GET_RDMA_BUFFER_REQ,
	FLUSH_COMMAND_REQ,
	PUT_REQUEST,
	DELETE_REQUEST,
	GET_REQUEST,
	MULTI_GET_REQUEST,
	TEST_REQUEST,
	TEST_REQUEST_FETCH_PAYLOAD,
	NO_OP,
	FLUSH_L0_REQUEST,
	CLOSE_COMPACTION_REQUEST,
	REPLICA_INDEX_SWAP_LEVELS_REQUEST,
	REPLICA_FLUSH_MEDIUM_LOG_REQUEST,
	PUT_IF_EXISTS_REQUEST,
	PUT_REPLY,
	GET_REPLY,
	MULTI_GET_REPLY,
	DELETE_REPLY,
	NO_OP_ACK,
	/*server2server data replication*/
	FLUSH_COMMAND_REP,
	FLUSH_L0_REPLY,
	CLOSE_COMPACTION_REPLY,
	REPLICA_INDEX_SWAP_LEVELS_REPLY,
	GET_RDMA_BUFFER_REP,
	/*server2server index transfer*/
	REPLICA_INDEX_GET_BUFFER_REP,
	REPLICA_INDEX_FLUSH_REP,
	REPLICA_FLUSH_MEDIUM_LOG_REP,
	/*control stuff*/
	DISCONNECT,
	/*test messages*/
	TEST_REPLY,
	TEST_REPLY_FETCH_PAYLOAD,
	/*pseudo-messages*/
	//RECOVER_LOG_CONTEXT
};

/* XXX TODO XXX remove this */
typedef enum receive_options { SYNC_REQUEST, ASYNC_REQUEST, BUSY_WAIT } receive_options;

typedef struct msg_key {
	uint32_t size;
	char key[];
} msg_key;

typedef struct msg_value {
	uint32_t size;
	char value[];
} msg_value;

typedef struct msg_header {
	/**
	 * Groups messages that must be processed in FIFO order from the receiving
	 * side. Set to 0 if you do not need FIFO ordering
	*/
#if VALIDATE_CHECKSUMS
	uint64_t session_id;
#else
	uint32_t session_id;
#endif
	/*Inform server where we expect the reply*/
	uint32_t offset_reply_in_recv_buffer;
	uint32_t reply_length_in_recv_buffer;

	/*size of the payload*/
	uint32_t payload_length;
	/*padding so as MESSAGE total size is a multiple of MESSAGE_SEGMENT_SIZE*/
	uint32_t padding_and_tail_size;
	/*Offset in send buffer which is the same in the target's recv buffer*/
	uint32_t offset_in_send_and_target_recv_buffers;
	/**
	*  This fields contains the offset in the send buffer(if any) of the message
	*  that generated this message. Typically send in the reply messages of the
	*  protocol. Otherwise is set to 0
	*/
	uint32_t triggering_msg_offset_in_send_buffer;
	/*Type of the message: PUT_REQUEST, PUT_REPLY, GET_QUERY, GET_REPLY, etc.*/
	uint16_t msg_type;
	uint8_t op_status;
#if VALIDATE_CHECKSUMS
	char pad[28];
#endif
	uint8_t receive;
} __attribute__((packed)) msg_header;

#define MESSAGE_SEGMENT_SIZE sizeof(struct msg_header)

typedef struct msg_put_rep {
	uint32_t status;
} msg_put_rep;

typedef struct msg_delete_req {
	uint32_t key_size;
	char key[];
} msg_delete_req;

typedef struct msg_delete_rep {
	uint32_t status;
} msg_delete_rep;

typedef struct msg_multi_get_req {
	uint32_t max_num_entries;
	uint32_t fetch_keys_only;
	uint32_t seek_mode;
	uint32_t seek_key_size;
	char seek_key[];
} msg_multi_get_req;

typedef struct msg_multi_get_rep {
	uint32_t num_entries;
	uint32_t curr_entry;
	uint32_t end_of_region : 16;
	uint32_t buffer_overflow : 16;
	uint32_t pos;
	uint32_t remaining;
	uint32_t capacity;
	char kv_buffer[];
} msg_multi_get_rep;

/*server2server used for replication*/
/*msg pair for initializing remote log buffers*/
struct s2s_msg_get_rdma_buffer_req {
	// char region_key[MSG_MAX_REGION_KEY_SIZE];
	// int buffer_size;
	// int region_key_size;
	int buffer_size;
	int mregion_buffer_size;
	int backup_id;
	char mregion_buffer[];
};

struct s2s_msg_get_rdma_buffer_rep {
	struct ibv_mr l0_recovery_mr;
	struct ibv_mr medium_recovery_mr;
	struct ibv_mr big_recovery_mr;
	uint32_t status;
	int num_buffers;
};

/*flush command pair*/
struct s2s_msg_flush_cmd_req {
	/*where primary has stored its segment*/
	uint64_t primary_segment_offt;
	uint64_t last_flushed_offt;
	uint64_t uuid;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint32_t region_key_size;
	enum log_category log_type;
};

struct s2s_msg_flush_cmd_rep {
	uint64_t uuid;
	uint32_t status;
};

/*flush L0 pair*/
struct s2s_msg_flush_L0_req {
	uint64_t uuid;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint64_t small_log_tail_dev_offt;
	uint64_t small_rdma_buffer_curr_end;
	uint64_t big_log_tail_dev_offt;
	uint64_t big_rdma_buffer_curr_end;
	uint32_t region_key_size;
};

struct s2s_msg_flush_L0_rep {
	uint64_t uuid;
};

/*server2server index transfers*/
struct s2s_msg_replica_index_get_buffer_req {
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint64_t uuid;
	uint32_t region_key_size;
	uint32_t level_id;
	uint32_t num_rows;
	uint32_t num_cols;
	uint32_t entry_size;
	uint8_t tree_id;
};

struct s2s_msg_replica_index_get_buffer_rep {
	struct ibv_mr mr;
	uint64_t uuid;
};

struct s2s_msg_replica_index_flush_req {
	uint64_t primary_segment_offt;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint32_t region_key_size;
	uint32_t number_of_columns;
	uint32_t entry_size;
	uint32_t height;
	uint32_t clock;
	uint32_t level_id;
	bool is_last_segment;
	volatile char *reply_offt;
	uint32_t reply_size;
	struct ibv_mr mr_of_primary;
};

struct s2s_msg_replica_index_flush_rep {
	int64_t uuid; //not used for now
};

struct s2s_msg_replica_flush_medium_log_req {
	uint64_t primary_segment_offt;
	uint64_t IO_starting_offt;
	uint64_t uuid;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint32_t region_key_size;
	uint32_t chunk_id;
	uint32_t IO_size;
};

struct s2s_msg_replica_flush_medium_log_rep {
	int64_t uuid;
};

struct s2s_msg_close_compaction_request {
	uint64_t compaction_first_segment_offt;
	uint64_t compaction_last_segment_offt;
	uint64_t new_root_offt;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint64_t uuid;
	uint32_t region_key_size;
	uint32_t level_id;
};

struct s2s_msg_close_compaction_reply {
	uint64_t uuid;
};

struct s2s_msg_swap_levels_request {
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint64_t uuid;
	uint32_t region_key_size;
	uint32_t level_id;
	uint32_t src_tree_id;
};

struct s2s_msg_swap_levels_reply {
	uint64_t uuid;
};

int msg_push_to_multiget_buf(msg_key *key, msg_value *val, msg_multi_get_rep *buf);

#endif // TEBIS_MESSAGES_H
