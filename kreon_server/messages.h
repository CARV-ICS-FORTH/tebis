/*
* Created by Pilar Gonzalez-Ferez on 28/07/16.
 * Copyright (c) 2016 Pilar Gonzalez Ferez <pilar@ics.forth.gr>.
*/

#pragma once
#include "conf.h"
#include <assert.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <semaphore.h>
#include <time.h>
#define MSG_MAX_REGION_KEY_SIZE 64
#define MAX_REPLICA_INDEX_BUFFERS 8

#define NUMBER_OF_TASKS 11

enum message_type {
	REPLICA_INDEX_GET_BUFFER_REQ = 0,
	REPLICA_INDEX_FLUSH_REQ,
	GET_LOG_BUFFER_REQ,
	FLUSH_COMMAND_REQ,
	PUT_REQUEST,
	DELETE_REQUEST,
	GET_REQUEST,
	MULTI_GET_REQUEST,
	TEST_REQUEST,
	TEST_REQUEST_FETCH_PAYLOAD,
	NO_OP,
	PUT_IF_EXISTS_REQUEST,
	PUT_REPLY,
	GET_REPLY,
	MULTI_GET_REPLY,
	DELETE_REPLY,
	NO_OP_ACK,
	/*server2server data replication*/
	FLUSH_COMMAND_REP,
	GET_LOG_BUFFER_REP,
	/*server2server index transfer*/
	REPLICA_INDEX_GET_BUFFER_REP,
	REPLICA_INDEX_FLUSH_REP,
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

/*put related*/
typedef struct msg_put_key {
	uint32_t key_size;
	char key[];
} msg_put_key;

typedef struct msg_put_value {
	uint32_t value_size;
	char value[];
} msg_put_value;

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

typedef struct msg_get_req {
	uint32_t offset;
	uint32_t bytes_to_read;
	uint32_t fetch_value;
	uint32_t key_size;
	char key[];
} msg_get_req;

typedef struct msg_get_rep {
	uint32_t bytes_remaining;
	uint8_t key_found : 4;
	uint8_t offset_too_large : 4;
	uint32_t value_size;
	char value[];
} msg_get_rep;

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
struct s2s_msg_get_log_buffer_req {
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	int num_buffers;
	int buffer_size;
	int region_key_size;
};

struct s2s_msg_get_log_buffer_rep {
	uint32_t status;
	struct ibv_mr mr[MAX_REPLICA_INDEX_BUFFERS];
	int num_buffers;
};

/*flush command pair*/
struct s2s_msg_flush_cmd_req {
	/*where primary has stored its segment*/
	uint64_t master_segment;
	uint64_t segment_id;
	uint64_t end_of_log;
	uint64_t log_padding;
	uint64_t tail;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint32_t is_partial;
	uint32_t log_buffer_id;
	uint32_t region_key_size;
};

struct s2s_msg_flush_cmd_rep {
	uint32_t status;
};

/*server2server index transfers*/
struct s2s_msg_replica_index_get_buffer_req {
	uint64_t index_offset;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint32_t region_key_size;
	int level_id;
	int buffer_size;
	int num_buffers;
};

struct s2s_msg_replica_index_get_buffer_rep {
	uint32_t status;
	struct ibv_mr mr[MAX_REPLICA_INDEX_BUFFERS];
	int num_buffers;
};

struct s2s_msg_replica_index_flush_req {
	uint64_t primary_segment_offt;
	uint64_t seg_hash;
	uint64_t root_w;
	uint64_t root_r;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
	uint32_t seg_id;
	uint32_t region_key_size;
	int level_id;
	int tree_id;
	int is_last;
};

struct s2s_msg_replica_index_flush_rep {
	int seg_id;
	int status;
};

int msg_push_to_multiget_buf(msg_key *key, msg_value *val, msg_multi_get_rep *buf);
