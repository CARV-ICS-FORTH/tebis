/*
* Created by Pilar Gonzalez-Ferez on 28/07/16.
 * Copyright (c) 2016 Pilar Gonzalez Ferez <pilar@ics.forth.gr>.
*/

#pragma once
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <semaphore.h>
#include <time.h>
#include "conf.h"
#define MSG_MAX_REGION_KEY_SIZE 256
#define MAX_REPLICA_INDEX_BUFFERS 8
enum message_type {

	PUT_REQUEST = 1,
	PUT_REPLY,
	PUT_OFFT_REQUEST,
	PUT_OFFT_REPLY,
	GET_REQUEST,
	GET_REPLY,
	MULTI_GET_REQUEST,
	MULTI_GET_REPLY,
	DELETE_REQUEST,
	DELETE_REPLY,
	/*server2server data replication*/
	FLUSH_COMMAND_REP,
	FLUSH_COMMAND_REQ,
	GET_LOG_BUFFER_REQ,
	GET_LOG_BUFFER_REP,
	/*server2server index transfer*/
	REPLICA_INDEX_GET_BUFFER_REQ,
	REPLICA_INDEX_GET_BUFFER_REP,
	REPLICA_INDEX_FLUSH_REQ,
	REPLICA_INDEX_FLUSH_REP,
	/*control stuff*/
	RESET_BUFFER,
	RESET_BUFFER_ACK,
	RESET_RENDEZVOUS,
	I_AM_CLIENT,
	DISCONNECT,
	CHANGE_CONNECTION_PROPERTIES_REQUEST,
	CHANGE_CONNECTION_PROPERTIES_REPLY,
	/*test messages*/
	TEST_REQUEST,
	TEST_REPLY,
	TEST_REQUEST_FETCH_PAYLOAD,
	TEST_REPLY_FETCH_PAYLOAD,
	CLIENT_STOP_NOW,
	SERVER_I_AM_READY,
	CLIENT_RECEIVED_READY,
	/*pseudo-messages*/
	//RECOVER_LOG_CONTEXT
};

typedef enum receive_options { SYNC_REQUEST = 2, ASYNC_REQUEST, BUSY_WAIT } receive_options;

typedef struct msg_key {
	uint32_t size;
	char key[];
} msg_key;

typedef struct msg_value {
	uint32_t size;
	char value[];
} msg_value;

typedef struct msg_header {
	/*Inform server where we expect the reply*/
	void *reply;
	volatile uint32_t reply_length;

	uint32_t pay_len; //Size of the payload
	uint32_t padding_and_tail; //padding so as MESSAGE total size is a multiple of MESSAGE_SEGMENT_SIZE
	uint16_t type; // Type of the message: PUT_REQUEST, PUT_REPLY, GET_QUERY, GET_REPLY, etc.
	uint8_t error_code;

	volatile uint64_t local_offset; //Offset regarding the local Memory region
	volatile uint64_t remote_offset; //Offset regarding the remote Memory region

	/* This field contains the local memory address of this message.
	  It is piggybacked by the remote side to its
	  corresponding reply message. In this way
	incoming messages can be associated with
																			 *  the initial request messages*/
	volatile void *request_message_local_addr;
	/*gesalous staff also*/
	volatile int32_t ack_arrived;
	/*from most significant byte to less: <FUTURE_EXTENSION>, <FUTURE_EXTENSION>,
	 * <FUTUTE_EXTENSION>, SYNC/ASYNC(indicates if this is  a synchronous or asynchronous request*/
	volatile uint32_t got_send_completion;
	volatile uint8_t receive_options;
#ifdef CHECKSUM_DATA_MESSAGES
	unsigned long hash; // [mvard] hash of data buffer
#endif
	/*Pointer to the first element of the Payload*/
	void *data;
	/*Pointer to the "current" element of the payload. Initially equal to data*/
	void *next;
	/*groups related messages. 0 for independent*/
	uint64_t session_id;
	uint32_t receive;
} msg_header;

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
	volatile uint32_t status;
} msg_put_rep;

/*update related*/
typedef struct msg_put_offt_req {
	uint64_t offset;
	char kv[];
} msg_put_offt_req;

typedef struct msg_put_offt_rep {
	uint32_t status;
	uint64_t new_value_size;
} msg_put_offt_rep;

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

/*somehow dead*/
typedef struct set_connection_property_req {
	int desired_priority_level;
	int desired_RDMA_memory_size;
} set_connection_property_req;

typedef struct set_connection_property_reply {
	int assigned_ppriority_level;
	int assigned_RDMA_memory_size;
} set_connection_property_reply;

/*server2server used for replication*/
/*msg pair for initializing remote log buffers*/
struct msg_get_log_buffer_req {
	int num_buffers;
	int buffer_size;
	int region_key_size;
	char region_key[];
};

struct msg_get_log_buffer_rep {
	uint32_t status;
	int num_buffers;
	struct ibv_mr mr[];
};

/*flush command pair*/
struct msg_flush_cmd_req {
	/*where primary has stored its segment*/
	uint64_t master_segment;
	uint64_t segment_id;
	uint64_t end_of_log;
	uint64_t log_padding;
	uint64_t tail;
	uint32_t is_partial;
	uint32_t log_buffer_id;
	uint32_t region_key_size;
	char region_key[];
};

struct msg_flush_cmd_rep {
	uint32_t status;
};

/*server2server index transfers*/
struct msg_replica_index_get_buffer_req {
	uint64_t index_offset;
	int level_id;
	int buffer_size;
	int num_buffers;
	uint32_t region_key_size;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
};

struct msg_replica_index_get_buffer_rep {
	uint32_t status;
	int num_buffers;
	struct ibv_mr mr[MAX_REPLICA_INDEX_BUFFERS];
};

struct msg_replica_index_flush_req {
	uint64_t primary_segment_offt;
	uint64_t seg_hash;
	uint64_t root_w;
	uint64_t root_r;
	int level_id;
	int tree_id;
	uint32_t seg_id;
	int is_last;

	uint32_t region_key_size;
	char region_key[MSG_MAX_REGION_KEY_SIZE];
};

struct msg_replica_index_flush_rep {
	int seg_id;
	int status;
};

int push_buffer_in_msg_header(struct msg_header *data_message, char *buffer, uint32_t buffer_length);
int msg_push_to_multiget_buf(msg_key *key, msg_value *val, msg_multi_get_rep *buf);
