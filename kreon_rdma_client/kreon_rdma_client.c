#define _GNU_SOURCE
#include "kreon_rdma_client.h"
#include "client_utils.h"
#include <assert.h>
#include <immintrin.h>
#include <infiniband/verbs.h>
#include <pthread.h>
#include <rdma/rdma_verbs.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
//#include "../kreon_server/client_regions.h"
#include "../kreon_lib/scanner/scanner.h"
#include "../kreon_rdma/rdma.h"
#include "../kreon_server/djb2.h"
#include "../kreon_server/globals.h"
#include "../kreon_server/messages.h"
#include "../utilities/spin_loop.h"
#include <log.h>
#define KRC_GET_SIZE (16 * 1024)

static volatile uint32_t reply_checker_exit = 0;
enum reply_checker_status {
	KRC_REPLY_CHECKER_NOT_RUNNING,
	KRC_REPLY_CHECKER_RUNNING,
};
static volatile enum reply_checker_status rep_checker_stat = KRC_REPLY_CHECKER_NOT_RUNNING;

struct krc_scanner {
	krc_key *prefix_key;
	krc_key *start_key;
	krc_key *stop_key;
	krc_key *curr_key;
	krc_value *curr_value;
	uint32_t prefetch_num_entries;
	uint32_t prefetch_mem_size;
	uint32_t actual_mem_size;
	uint32_t pos;
	krc_seek_mode seek_mode;
	krc_seek_mode stop_key_seek_mode;
	uint8_t start_infinite : 1;
	uint8_t stop_infinite : 1;
	uint8_t prefix_filter_enable : 1;
	uint8_t is_valid : 1;
	uint8_t fetch_keys_only : 1;

	krc_scan_state state;
	/*copy of the server's reply*/
	msg_multi_get_rep *multi_kv_buf;
	struct cu_region_desc *curr_region;
};

static char neg_infinity[1] = { 0 };
static char *pos_infinity = "+oo";

/*extern ZooLogLevel logLevel;*/
/*logLevel = ZOO_LOG_LEVEL_ERROR;*/

static int krc_lib_init = 0;
static pthread_mutex_t lib_lock = PTHREAD_MUTEX_INITIALIZER;
/*async staff*/

struct krc_async_req {
	struct connection_rdma *conn;
	struct msg_header *request;
	struct msg_header *reply;
	uint32_t *buf_size;
	char *buf;
	void *context;
	void (*callback)(void *);
	/*struct rdma_message_context msg_ctx;*/
	uint8_t wc_checked;
	struct timespec start_time;
};

struct krc_spinner {
	utils_queue_s communication_queue;
	struct krc_async_req *private_array_of_oustanding_reqs;
	uint32_t outstanding_requests;
	uint32_t num_requests;
};
static struct krc_spinner *spinner = NULL;
pthread_t spinner_cnxt;

extern void on_completion_client(struct rdma_message_context *);
static int _krc_send_heartbeat(struct rdma_cm_id *rdma_cm_id)
{
	struct rdma_message_context send_ctx;
	//log_info("Sending heartbeat!");
	client_rdma_init_message_context(&send_ctx, NULL);
	send_ctx.on_completion_callback = on_completion_client;
	if (rdma_post_write(rdma_cm_id, &send_ctx, NULL, 0, NULL, IBV_SEND_SIGNALED, 0, 0)) {
		log_warn("Failed to send heartbeat: %s", strerror(errno));
		return KREON_FAILURE;
	}

	if (!client_rdma_send_message_success(&send_ctx)) {
		log_warn("Remote side is down!");
		return KREON_FAILURE;
	}
	return KREON_SUCCESS;
}

/* Spin until an incoming message has been detected. For our RDMA network protocol implementation, this is
 * detected by reading the value TU_RDMA_REGULAR_MSG in the last byte of the buffer where we expect the
 * message.
 *
 * \return KREON_SUCCESS if a message is successfully received
 * \return KREON_FAILURE if the remote side is down
 */
static int krc_rdma_write_spin_wait_for_msg_tail(struct rdma_cm_id *rdma_cm_id, volatile struct msg_header *msg)
{
	const unsigned timeout = 1700000; // 10 sec
	int ret = KREON_SUCCESS;

	while (1) {
		for (uint32_t i = 0; get_receive_field(msg) != TU_RDMA_REGULAR_MSG && i < timeout; ++i)
			;

		if (get_receive_field(msg) == TU_RDMA_REGULAR_MSG)
			break;

		if (_krc_send_heartbeat(rdma_cm_id) == KREON_FAILURE)
			return KREON_FAILURE;
	}
	return ret;
}

static int krc_rdma_write_spin_wait_for_header_tail(struct rdma_cm_id *rdma_cm_id, volatile struct msg_header *msg)
{
	const unsigned timeout = 1700000; // 10 sec
	int ret = KREON_SUCCESS;

	while (1) {
		for (uint32_t i = 0; msg->receive != TU_RDMA_REGULAR_MSG && i < timeout; ++i)
			;

		if (msg->receive == TU_RDMA_REGULAR_MSG)
			break;

		if (_krc_send_heartbeat(rdma_cm_id) == KREON_FAILURE)
			return KREON_FAILURE;
	}
	return ret;
}

static int krc_wait_for_message_reply(struct msg_header *req, struct connection_rdma *conn)
{
	volatile struct msg_header *rep_header =
		(struct msg_header *)&conn->recv_circular_buf->memory_region[req->offset_reply_in_recv_buffer];
	//Spin until header arrives
	if (krc_rdma_write_spin_wait_for_header_tail(conn->rdma_cm_id, rep_header) == KREON_FAILURE)
		return KREON_FAILURE;

	assert(rep_header->receive == TU_RDMA_REGULAR_MSG);
	if (!rep_header->payload_length)
		return KREON_SUCCESS;

	//Spin until payload arrives
	if (krc_rdma_write_spin_wait_for_msg_tail(conn->rdma_cm_id, rep_header) == KREON_FAILURE)
		return KREON_FAILURE;

	return KREON_SUCCESS;
}

/*allocate a message for reseting the rendezvous(recv circular buffer of the server).
 *return this msg for later use*/
static void send_no_op_operation(connection_rdma *conn)
{
	uint32_t no_op_payload_length = conn->send_circular_buf->remaining_space - MESSAGE_SEGMENT_SIZE;
	if (no_op_payload_length)
		--no_op_payload_length;

	struct msg_header *no_op_request = client_allocate_rdma_message(conn, no_op_payload_length, NO_OP);
	volatile struct msg_header *no_op_reply = client_allocate_rdma_message(conn, 0, NO_OP_ACK);

	no_op_request->receive = TU_RDMA_REGULAR_MSG;
	no_op_request->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, no_op_request);
	no_op_reply->op_status = 1;

	no_op_request->offset_reply_in_recv_buffer =
		(uint64_t)no_op_reply - (uint64_t)conn->recv_circular_buf->memory_region;
	no_op_request->reply_length_in_recv_buffer =
		sizeof(msg_header) + no_op_reply->payload_length + no_op_reply->padding_and_tail_size;

	if (__send_rdma_message(conn, no_op_request, NULL) != KREON_SUCCESS) {
		log_fatal("failed to send message");
		_exit(EXIT_FAILURE);
	}

	if (krc_wait_for_message_reply(no_op_request, conn) != KREON_SUCCESS) {
		log_fatal("Kreon dataserver is down!");
		_exit(EXIT_FAILURE);
	}

#if VALIDATE_CHECKSUMS
	uint32_t msg_len = no_op_request->payload_length + no_op_request->padding_and_tail_size + MESSAGE_SEGMENT_SIZE;
	uint64_t hash =
		djb2_hash((unsigned char *)(&no_op_request->offset_reply_in_recv_buffer), msg_len - sizeof(uint64_t));
	assert(hash == no_op_reply->session_id);
#endif

	/*zero and free the reply and req*/
	zero_rendezvous_locations(no_op_reply);
	client_free_rpc_pair(conn, no_op_reply);
}

/** Allocate a pair of buffers, one for the request and one for the reply. All messages are multiples of MESSAGE_SEGMENT_SIZE unit
 *  In case where server's recv circular buffer does no have enough space a NO_OP message is send, allocating
 *  the remaining space of the buffer. Client spins for a NO_OP_ACK.
 *  Function returns rep and req allocated in their corresponding circular buffers */
static void _krc_get_rpc_pair(connection_rdma *conn, msg_header **req, int req_msg_type, int req_size, msg_header **rep,
			      int rep_msg_type, int rep_size)
{
	pthread_mutex_lock(&conn->buffer_lock);
	pthread_mutex_lock(&conn->allocation_lock);
	*req = client_allocate_rdma_message(conn, req_size, req_msg_type);

	while (*req == NULL) {
		send_no_op_operation(conn);

		/*allocate new req*/
		*req = client_allocate_rdma_message(conn, req_size, req_msg_type);
	}

	*rep = client_allocate_rdma_message(conn, rep_size, rep_msg_type);
	pthread_mutex_unlock(&conn->allocation_lock);
	pthread_mutex_unlock(&conn->buffer_lock);
}

static int64_t krc_compare_keys(krc_key *key1, krc_key *key2)
{
	uint32_t size;

	if (key1->key_size > key2->key_size)
		size = key2->key_size;
	else
		size = key1->key_size;

	int64_t ret = memcmp(key2->key_buf, key1->key_buf, size);
	if (ret != 0)
		return ret;

	/*keys are equal check sizes*/
	if (key1->key_size == key2->key_size)
		return 0;

	/*larger key wins*/
	if (key2->key_size > key1->key_size)
		return 1;

	return -1;
}

static int64_t krc_prefix_match(krc_key *prefix, krc_key *key)
{
	if (key->key_size < prefix->key_size)
		return 0;
	if (memcmp(prefix->key_buf, key->key_buf, prefix->key_size) == 0)
		return 1;
	else
		return 0;
}

krc_ret_code krc_init(char *zookeeper_host)
{
	if (!krc_lib_init) {
		pthread_mutex_lock(&lib_lock);
		cu_init(zookeeper_host);
		krc_lib_init = 1;
		pthread_mutex_unlock(&lib_lock);
	}
	return KRC_SUCCESS;
}
static krc_ret_code krc_internal_put(uint32_t key_size, void *key, uint32_t val_size, void *value,
				     int is_update_if_exists)
{
	msg_header *req_header;
	msg_put_key *put_key;
	msg_put_value *put_value;
	msg_header *rep_header;
	msg_put_rep *put_rep;

	if (key_size + val_size + (2 * sizeof(uint32_t)) > SEGMENT_SIZE - sizeof(segment_header)) {
		log_fatal("KV size too large currently for Kreon, current max value size supported = %u bytes",
			  SEGMENT_SIZE - sizeof(segment_header));
		log_fatal("Contact gesalous@ics.forth.gr");
		exit(EXIT_FAILURE);
	}
	//old school
	//client_region *region = client_find_region(key, key_size);
	//connection_rdma *conn = get_connection_from_region(region, (uint64_t)key);
	struct cu_region_desc *r_desc = cu_get_region(key, key_size);
	connection_rdma *conn = cu_get_conn_for_region(r_desc, djb2_hash((unsigned char *)key, key_size));

	if (is_update_if_exists)
		_krc_get_rpc_pair(conn, &req_header, PUT_IF_EXISTS_REQUEST,
				  key_size + val_size + (2 * sizeof(uint32_t)), &rep_header, PUT_REPLY,
				  sizeof(msg_put_rep));
	else
		_krc_get_rpc_pair(conn, &req_header, PUT_REQUEST, key_size + val_size + (2 * sizeof(uint32_t)),
				  &rep_header, PUT_REPLY, sizeof(msg_put_rep));

	put_key = (msg_put_key *)((uint64_t)req_header + sizeof(msg_header));
	/*fill in the key payload part the data, caution we are 100% sure that it fits :-)*/
	put_key->key_size = key_size;
	memcpy(put_key->key, key, key_size);
	put_value = (msg_put_value *)((uint64_t)put_key + sizeof(msg_put_key) + put_key->key_size);
	put_value->value_size = val_size;
	memcpy(put_value->value, value, val_size);

	/*Now the reply part*/
	//rep_header = allocate_rdma_message(conn, sizeof(msg_put_rep), PUT_REPLY);
	rep_header->receive = 0;
	put_rep = (msg_put_rep *)((uint64_t)rep_header + sizeof(msg_header));
	put_rep->status = KR_REP_PENDING;

	/*inform the req about its buddy*/
	req_header->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, req_header);
	/*location where server should put the reply*/
	req_header->offset_reply_in_recv_buffer =
		(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
	req_header->reply_length_in_recv_buffer =
		sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;
	//log_info("put rep length %lu", req_header->reply_length);
	/*send the actual put*/
	if (client_send_rdma_message(conn, req_header) != KREON_SUCCESS) {
		log_warn("failed to send message");
		_exit(EXIT_FAILURE);
	}

	if (krc_wait_for_message_reply(req_header, conn) != KREON_SUCCESS) {
		log_fatal("Kreon dataserver is down!");
		_exit(EXIT_FAILURE);
	}

	put_rep = (msg_put_rep *)((uint64_t)rep_header + sizeof(msg_header));
	/*check ret code*/
	if (put_rep->status != KREON_SUCCESS) {
		log_fatal("put operation failed for key %s", key);
		_exit(EXIT_FAILURE);
	}
	zero_rendezvous_locations_l(rep_header, req_header->reply_length_in_recv_buffer);
	client_free_rpc_pair(conn, rep_header);
	return KRC_SUCCESS;
}

krc_ret_code krc_put_if_exists(uint32_t key_size, void *key, uint32_t val_size, void *value)
{
	return krc_internal_put(key_size, key, val_size, value, 1);
}

krc_ret_code krc_put(uint32_t key_size, void *key, uint32_t val_size, void *value)
{
	return krc_internal_put(key_size, key, val_size, value, 0);
}

#if 0
krc_value *krc_get(uint32_t key_size, void *key, uint32_t reply_length, uint32_t *error_code)
{
	krc_value *val = NULL;
	client_region *region = client_find_region(key, key_size);
	connection_rdma *conn = get_connection_from_region(region, (uint64_t)key);
	/*the request part*/
	msg_header *req_header = allocate_rdma_message(conn, sizeof(msg_get_req) + key_size, GET_REQUEST);

	msg_get_req *m_get = (msg_get_req *)((uint64_t)req_header + sizeof(msg_header));
	m_get->key_size = key_size;
	memcpy(m_get->key, key, key_size);
	/*the reply part*/
	msg_header *rep_header = allocate_rdma_message(conn, sizeof(msg_get_rep) + reply_length, GET_REPLY);
	req_header->reply = (char *)((uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region);
	req_header->reply_length = sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;

	req_header->request_message_local_addr = req_header;
	rep_header->receive = 0;
	/*sent the request*/
	if (send_rdma_message_busy_wait(conn, req_header) != KREON_SUCCESS) {
		log_warn("failed to send message");
		exit(EXIT_FAILURE);
	}
	/*Spin until header arrives*/
	field_spin_for_value(&rep_header->receive, TU_RDMA_REGULAR_MSG);

	/*Spin until payload arrives*/
	uint32_t *tail = (uint32_t *)(((uint64_t)rep_header + sizeof(msg_header) + rep_header->payload_length +
				       rep_header->padding_and_tail_size) -
				      TU_TAIL_SIZE);

	field_spin_for_value(tail, TU_RDMA_REGULAR_MSG);

	msg_get_rep *get_rep = (msg_get_rep *)((uint64_t)rep_header + sizeof(msg_header));

	if (get_rep->buffer_overflow) {
		log_warn("Receive buffer is smaller than the actual reply :-(");
		*error_code = KRC_BUFFER_OVERFLOW;
		goto exit;
	}

	if (!get_rep->key_found) {
		log_warn("Key %s not found!", key);
		*error_code = KRC_KEY_NOT_FOUND;
		goto exit;
	}
	val = (krc_value *)malloc(sizeof(krc_value) + get_rep->value_size);
	val->val_size = get_rep->value_size;
	memcpy(val->val_buf, get_rep->value, val->val_size);
	*error_code = KRC_SUCCESS;
exit:
	_zero_rendezvous_locations_l(rep_header, req_header->reply_length);
	client_free_rpc_pair(conn, rep_header);
	return val;
}


krc_value *krc_get_with_offset(uint32_t key_size, void *key, uint32_t offset, uint32_t size, uint32_t *error_code)
{
	msg_header *rep_header;
	/*if size is 0 it will try to read the remaining value*/
	krc_value *val = NULL;
	client_region *region = client_find_region(key, key_size);
	connection_rdma *conn = get_connection_from_region(region, (uint64_t)key);
	/*the request part*/
	msg_header *req_header = allocate_rdma_message(conn, sizeof(msg_get_offt_req) + key_size, GET_OFFT_REQUEST);

	msg_get_offt_req *get_offt_req = (msg_get_offt_req *)((uint64_t)req_header + sizeof(msg_header));
	get_offt_req->offset = offset;
	get_offt_req->size = size;
	get_offt_req->key_size = key_size;
	memcpy(get_offt_req->key_buf, key, key_size);
	/*the reply part*/
	if (size == UINT_MAX)
		rep_header =
			allocate_rdma_message(conn, sizeof(msg_get_req) + KRC_GET_OFFT_DEFAULT_SIZE, GET_OFFT_REPLY);
	else if (size == 0)
		rep_header = allocate_rdma_message(conn, sizeof(msg_get_req), GET_OFFT_REPLY);
	else
		rep_header = allocate_rdma_message(conn, sizeof(msg_get_req) + size, GET_OFFT_REPLY);

	req_header->reply = (char *)((uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region);
	req_header->reply_length = sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;

	req_header->request_message_local_addr = req_header;
	rep_header->receive = 0;
	/*sent the request*/
	if (send_rdma_message_busy_wait(conn, req_header) != KREON_SUCCESS) {
		log_warn("failed to send message");
		exit(EXIT_FAILURE);
	}
	/*Spin until header arrives*/
	field_spin_for_value(&rep_header->receive, TU_RDMA_REGULAR_MSG);

	/*Spin until payload arrives*/
	uint32_t *tail = (uint32_t *)(((uint64_t)rep_header + sizeof(msg_header) + rep_header->payload_length +
				       rep_header->padding_and_tail_size) -
				      TU_TAIL_SIZE);

	field_spin_for_value(tail, TU_RDMA_REGULAR_MSG);

	msg_get_offt_rep *get_offt_rep = (msg_get_offt_rep *)((uint64_t)rep_header + sizeof(msg_header));

	if (!get_offt_rep->key_found) {
		//log_warn("Key %s not found!", key);
		*error_code = KRC_KEY_NOT_FOUND;
		goto exit;
	}

	if (size > 0) {
		val = (krc_value *)malloc(sizeof(krc_value) + get_offt_rep->value_bytes_read);
		val->val_size = get_offt_rep->value_bytes_read;
		memcpy(val->val_buf, get_offt_rep->value, val->val_size);
	}
	*error_code = KRC_SUCCESS;
exit:
	_zero_rendezvous_locations_l(rep_header, req_header->reply_length);
	client_free_rpc_pair(conn, rep_header);
	return val;
}
#endif

krc_ret_code krc_get(uint32_t key_size, char *key, char **buffer, uint32_t *size, uint32_t offset)
{
	msg_header *req_header = NULL;
	msg_header *rep_header = NULL;
	msg_get_req *get_req = NULL;
	msg_get_rep *get_rep = NULL;
	uint32_t reply_size;
	uint32_t local_offset = offset;
	uint32_t local_buf_offset = 0;
	//old school
	//client_region *region = client_find_region(key, key_size);
	//connection_rdma *conn = get_connection_from_region(region, (uint64_t)key);
	struct cu_region_desc *r_desc = cu_get_region(key, key_size);
	connection_rdma *conn = cu_get_conn_for_region(r_desc, djb2_hash((unsigned char *)key, key_size));
	krc_ret_code code = KRC_FAILURE;
	uint8_t read_whole_value;

	if (*buffer == NULL) {
		/*app wants us to fetch the whole thing from offset, allocate, and return a buffer*/
		read_whole_value = 1;
		reply_size = KRC_GET_SIZE;
	} else {
		read_whole_value = 0;
		reply_size = *size;
	}

	while (1) {
		_krc_get_rpc_pair(conn, &req_header, GET_REQUEST, sizeof(msg_get_req) + key_size, &rep_header,
				  GET_REPLY, sizeof(msg_get_rep) + reply_size);
		//req_header = allocate_rdma_message(conn, sizeof(msg_get_req) + key_size, TU_GET_QUERY);
		get_req = (msg_get_req *)((uint64_t)req_header + sizeof(msg_header));
		get_req->key_size = key_size;
		memcpy(get_req->key, key, key_size);
		get_req->offset = local_offset;
		get_req->fetch_value = 1;
		get_req->bytes_to_read = reply_size;
		/*the reply part*/
		//rep_header = allocate_rdma_message(conn, sizeof(msg_get_rep) + reply_size, TU_GET_REPLY);
		req_header->offset_reply_in_recv_buffer =
			(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
		req_header->reply_length_in_recv_buffer =
			sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;

		req_header->triggering_msg_offset_in_send_buffer =
			real_address_to_triggering_msg_offt(conn, req_header);
		rep_header->receive = 0;

		/*send the request*/
		if (client_send_rdma_message(conn, req_header) != KREON_SUCCESS) {
			log_warn("failed to send message");
			_exit(EXIT_FAILURE);
		}
		// Wait for the reply
		if (krc_wait_for_message_reply(req_header, conn) != KREON_SUCCESS) {
			log_fatal("Kreon dataserver is down!");
			_exit(EXIT_FAILURE);
		}

		get_rep = (msg_get_rep *)((uint64_t)rep_header + sizeof(msg_header));
		/*various reply checks*/
		if (!get_rep->key_found) {
			//log_warn("Key %s not found!", key);
			code = KRC_KEY_NOT_FOUND;
			goto exit;
		}
		if (get_rep->offset_too_large) {
			code = KRC_OFFSET_TOO_LARGE;
			goto exit;
		}

		if (*buffer == NULL) {
			*size = get_rep->value_size + get_rep->bytes_remaining;
			(*buffer) = malloc(*size);
		}

		memcpy((*buffer) + local_buf_offset, get_rep->value, get_rep->value_size);
		if (!read_whole_value) {
			//log_info("actual value from server %u", get_rep->value_size);
			*size = get_rep->value_size;
			code = KRC_SUCCESS;
			goto exit;
		}
		local_offset += get_rep->value_size;
		local_buf_offset += get_rep->value_size;

		if (get_rep->bytes_remaining == 0) {
			code = KRC_SUCCESS;
			goto exit;
		} else {
			zero_rendezvous_locations_l(rep_header, req_header->reply_length_in_recv_buffer);
			client_free_rpc_pair(conn, rep_header);
		}
	}

exit:
	zero_rendezvous_locations_l(rep_header, req_header->reply_length_in_recv_buffer);
	client_free_rpc_pair(conn, rep_header);
	return code;
}

uint8_t krc_exists(uint32_t key_size, void *key)
{
	msg_header *req_header = NULL;
	msg_header *rep_header = NULL;
	msg_get_req *get_req = NULL;
	msg_get_rep *get_rep = NULL;

	struct cu_region_desc *r_desc = cu_get_region(key, key_size);
	connection_rdma *conn = cu_get_conn_for_region(r_desc, (uint64_t)key);

	_krc_get_rpc_pair(conn, &req_header, GET_REQUEST, sizeof(msg_get_req) + key_size, &rep_header, GET_REPLY,
			  sizeof(msg_get_rep));
	//req_header = allocate_rdma_message(conn, sizeof(msg_get_req) + key_size, TU_GET_QUERY);
	get_req = (msg_get_req *)((uint64_t)req_header + sizeof(msg_header));
	get_req->key_size = key_size;
	memcpy(get_req->key, key, key_size);
	get_req->offset = 0;
	get_req->fetch_value = 0;
	/*the reply part*/
	//rep_header = allocate_rdma_message(conn, sizeof(msg_get_rep), TU_GET_REPLY);
	req_header->offset_reply_in_recv_buffer =
		(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
	req_header->reply_length_in_recv_buffer =
		sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;

	req_header->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, req_header);
	rep_header->receive = 0;
	/*send the request*/
	if (client_send_rdma_message(conn, req_header) != KREON_SUCCESS) {
		log_warn("failed to send message");
		_exit(EXIT_FAILURE);
	}
	// Wait for the reply to arrive
	if (krc_wait_for_message_reply(req_header, conn) != KREON_SUCCESS) {
		log_fatal("Kreon dataserver is down!");
		_exit(EXIT_FAILURE);
	}

	get_rep = (msg_get_rep *)((uint64_t)rep_header + sizeof(msg_header));

	zero_rendezvous_locations_l(rep_header, req_header->reply_length_in_recv_buffer);
	client_free_rpc_pair(conn, rep_header);
	return get_rep->key_found;
}

krc_ret_code krc_delete(uint32_t key_size, void *key)
{
	msg_header *req_header;
	msg_header *rep_header;
	uint32_t error_code;

	struct cu_region_desc *r_desc = cu_get_region(key, key_size);
	connection_rdma *conn = cu_get_conn_for_region(r_desc, (uint64_t)key);

	_krc_get_rpc_pair(conn, &req_header, DELETE_REQUEST, sizeof(msg_delete_req) + key_size, &rep_header,
			  DELETE_REPLY, sizeof(msg_delete_rep));
	/*the request part*/
	//	msg_header *req_header = allocate_rdma_message(conn, sizeof(msg_delete_req) + key_size, DELETE_REQUEST);

	msg_delete_req *m_del = (msg_delete_req *)((uint64_t)req_header + sizeof(msg_header));
	m_del->key_size = key_size;
	memcpy(m_del->key, key, key_size);
	/*the reply part*/
	//msg_header *rep_header = allocate_rdma_message(conn, sizeof(msg_delete_rep), DELETE_REPLY);
	req_header->offset_reply_in_recv_buffer =
		(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
	req_header->reply_length_in_recv_buffer =
		sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;

	req_header->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, req_header);
	rep_header->receive = 0;
	/*sent the request*/
	if (send_rdma_message_busy_wait(conn, req_header) != KREON_SUCCESS) {
		log_warn("failed to send message");
		_exit(EXIT_FAILURE);
	}
	/*Spin until header arrives*/
	field_spin_for_value(&rep_header->receive, TU_RDMA_REGULAR_MSG);

	/*Spin until payload arrives*/
	uint8_t *tail = (uint8_t *)(((uint64_t)rep_header + sizeof(msg_header) + rep_header->payload_length +
				     rep_header->padding_and_tail_size) -
				    TU_TAIL_SIZE);

	field_spin_for_value(tail, TU_RDMA_REGULAR_MSG);

	msg_delete_rep *del_rep = (msg_delete_rep *)((uint64_t)rep_header + sizeof(msg_header));

	if (del_rep->status != KREON_SUCCESS) {
		log_warn("Key %s not found!", key);
		error_code = KRC_KEY_NOT_FOUND;
	} else
		error_code = KRC_SUCCESS;
	zero_rendezvous_locations_l(rep_header, req_header->reply_length_in_recv_buffer);
	client_free_rpc_pair(conn, rep_header);
	return error_code;
}

/*scanner staff*/
krc_scannerp krc_scan_init(uint32_t prefetch_num_entries, uint32_t prefetch_mem_size_hint)
{
	uint32_t padding;
	uint32_t actual_size = sizeof(msg_header) + sizeof(msg_multi_get_rep) + prefetch_mem_size_hint + TU_TAIL_SIZE;
	/*round it as the rdma allocator will*/
	if (actual_size % MESSAGE_SEGMENT_SIZE != 0)
		padding = MESSAGE_SEGMENT_SIZE - (actual_size % MESSAGE_SEGMENT_SIZE);
	else
		padding = 0;
	struct krc_scanner *scanner = (struct krc_scanner *)malloc(sizeof(struct krc_scanner) + actual_size + padding);
	scanner->actual_mem_size = actual_size + padding;
	scanner->prefetch_mem_size = prefetch_mem_size_hint;
	scanner->prefix_key = NULL;
	scanner->start_key = NULL;
	scanner->stop_key = NULL;
	scanner->stop_key_seek_mode = KRC_GREATER;
	scanner->prefetch_num_entries = prefetch_num_entries;
	scanner->pos = 0;
	scanner->start_infinite = 1;
	scanner->stop_infinite = 1;
	scanner->is_valid = 1;
	scanner->prefix_filter_enable = 0;
	scanner->state = KRC_UNITIALIZED;
	scanner->fetch_keys_only = 0;
	scanner->multi_kv_buf = (msg_multi_get_rep *)((uint64_t)scanner + sizeof(struct krc_scanner));
	return (krc_scannerp)scanner;
}

uint8_t krc_scan_is_valid(krc_scannerp sp)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	return sc->is_valid;
}

void krc_scan_fetch_keys_only(krc_scannerp sp)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	sc->fetch_keys_only = 1;
}

uint8_t krc_scan_get_next(krc_scannerp sp, char **key, size_t *keySize, char **value, size_t *valueSize)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	msg_header *req_header;
	msg_multi_get_req *m_get;
	msg_header *rep_header;
	msg_multi_get_rep *m_get_rep;
	char *seek_key = NULL;
	//old school
	//client_region *curr_region = (client_region *)sc->curr_region;
	struct cu_region_desc *r_desc = (struct cu_region_desc *)sc->curr_region;

	msg_multi_get_rep *multi_kv_buf = (msg_multi_get_rep *)sc->multi_kv_buf;
	connection_rdma *conn;

	uint32_t seek_key_size = 0;
	uint32_t seek_mode = sc->seek_mode == KRC_GREATER_OR_EQUAL ? GREATER_OR_EQUAL : GREATER;

	while (1) {
		switch (sc->state) {
		case KRC_UNITIALIZED:
			if (!sc->start_infinite) {
				seek_key = sc->start_key->key_buf;
				seek_key_size = sc->start_key->key_size;
				if (sc->seek_mode == KRC_GREATER_OR_EQUAL)
					seek_mode = GREATER_OR_EQUAL;
				else
					seek_mode = GREATER;

			} else {
				seek_key = neg_infinity;
				seek_key_size = 1;
				seek_mode = GREATER_OR_EQUAL;
			}

			sc->state = KRC_ISSUE_MGET_REQ;
			break;

		case KRC_FETCH_NEXT_BATCH:
			/*seek key will be the last of the batch*/
			seek_key = sc->curr_key->key_buf;
			seek_key_size = sc->curr_key->key_size;
			seek_mode = GREATER;
			sc->state = KRC_ISSUE_MGET_REQ;
			break;
		case KRC_STOP_FILTER: {
			int ret;
			if (sc->stop_key != NULL) {
				ret = krc_compare_keys(sc->curr_key, sc->stop_key);
				if (ret < 0 || (ret == 0 && sc->stop_key_seek_mode == KRC_GREATER)) {
					log_debug("stop key reached curr key %s stop key %s", sc->curr_key->key_buf,
						  sc->stop_key->key_buf);
					sc->is_valid = 0;
					sc->state = KRC_INVALID;
					sc->curr_key = NULL;
					sc->curr_value = NULL;
					goto exit;
				}
			}
			sc->state = KRC_PREFIX_FILTER;
			break;
		}
		case KRC_PREFIX_FILTER:

			if (sc->prefix_key == NULL) {
				sc->state = KRC_ADVANCE;
				goto exit;
			} else if (krc_prefix_match(sc->prefix_key, sc->curr_key)) {
				sc->state = KRC_ADVANCE;
				goto exit;
			} else {
				sc->state = KRC_INVALID;
				sc->is_valid = 0;
				goto exit;
			}
		case KRC_ISSUE_MGET_REQ: {
			r_desc = cu_get_region(seek_key, seek_key_size);
			sc->curr_region = (void *)r_desc;
			conn = cu_get_conn_for_region(r_desc, (uint64_t)key);

			_krc_get_rpc_pair(conn, &req_header, MULTI_GET_REQUEST,
					  sizeof(msg_multi_get_req) + seek_key_size, &rep_header, MULTI_GET_REPLY,
					  sc->prefetch_mem_size);
			/*the request part*/
			//req_header = allocate_rdma_message(conn, sizeof(msg_multi_get_req) + seek_key_size,
			//				   MULTI_GET_REQUEST);
			m_get = (msg_multi_get_req *)((uint64_t)req_header + sizeof(msg_header));
			m_get->max_num_entries = sc->prefetch_num_entries;
			m_get->seek_mode = seek_mode;
			m_get->fetch_keys_only = sc->fetch_keys_only;
			m_get->seek_key_size = seek_key_size;
			memcpy(m_get->seek_key, seek_key, seek_key_size);
			/*the reply part*/
			//rep_header = allocate_rdma_message(conn, sc->prefetch_mem_size, MULTI_GET_REPLY);
			req_header->offset_reply_in_recv_buffer =
				(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
			req_header->reply_length_in_recv_buffer =
				sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;
			//log_info("Client allocated for my replu %u bytes", req_header->reply_length);
			req_header->triggering_msg_offset_in_send_buffer =
				real_address_to_triggering_msg_offt(conn, req_header);
			rep_header->receive = 0;

			/*send the request*/
			if (client_send_rdma_message(conn, req_header) != KREON_SUCCESS) {
				log_warn("failed to send message");
				_exit(EXIT_FAILURE);
			}
			// Wait for the reply to arrive
			if (krc_wait_for_message_reply(req_header, conn) != KREON_SUCCESS) {
				log_fatal("Kreon dataserver is down!");
				_exit(EXIT_FAILURE);
			}

			//log_info("pay len %u padding_and_tail_size %u", rep_header->payload_length, rep_header->padding_and_tail_size);
			m_get_rep = (msg_multi_get_rep *)((uint64_t)rep_header + sizeof(msg_header));

			if (m_get_rep->buffer_overflow) {
				sc->state = KRC_BUFFER_OVERFLOW;
				break;
			}
			/*copy to local buffer to free rdma communication buffer*/
			//assert(rep_header->payload_length <= sc->actual_mem_size);

			memcpy(sc->multi_kv_buf, m_get_rep, rep_header->payload_length);
			zero_rendezvous_locations_l(rep_header, req_header->reply_length_in_recv_buffer);

			client_free_rpc_pair(conn, rep_header);
			multi_kv_buf = (msg_multi_get_rep *)sc->multi_kv_buf;
			multi_kv_buf->pos = 0;
			multi_kv_buf->remaining = multi_kv_buf->capacity;
			multi_kv_buf->curr_entry = 0;
			sc->state = KRC_ADVANCE;
			break;
		}
		case KRC_ADVANCE:
			/*point to the next element*/

			if (multi_kv_buf->curr_entry < multi_kv_buf->num_entries) {
				//log_info("sc curr %u num entries %u", multi_kv_buf->curr_entry, multi_kv_buf->num_entries);
				sc->curr_key = (krc_key *)((uint64_t)multi_kv_buf->kv_buffer + multi_kv_buf->pos);
				multi_kv_buf->pos += (sizeof(krc_key) + sc->curr_key->key_size);
				sc->curr_value = (krc_value *)((uint64_t)multi_kv_buf->kv_buffer + multi_kv_buf->pos);
				multi_kv_buf->pos += (sizeof(krc_value) + sc->curr_value->val_size);
				++multi_kv_buf->curr_entry;
				sc->state = KRC_STOP_FILTER;
				break;
			} else {
				if (!multi_kv_buf->end_of_region) {
					seek_key = sc->curr_key->key_buf;
					seek_key_size = sc->curr_key->key_size;
					seek_mode = GREATER;
					sc->state = KRC_ISSUE_MGET_REQ;
					//log_info("Time for next batch, within region, seek key %s", seek_key);
				} else if (multi_kv_buf->end_of_region &&
					   strncmp(r_desc->region.max_key, "+oo", 3) != 0) {
					seek_key = r_desc->region.max_key;
					seek_key_size = r_desc->region.max_key_size;
					sc->state = KRC_ISSUE_MGET_REQ;
					seek_mode = GREATER_OR_EQUAL;
					//log_info("Time for next batch, crossing regions, seek key %s", seek_key);
				} else {
					sc->state = KRC_END_OF_DB;
					//log_info("sorry end of db end of region = %d maximum_range %s minimum range %s",
					//	 multi_kv_buf->end_of_region, r_desc->region.max_key,
					//		 r_desc->region.min_key);
				}
			}
			break;
		case KRC_BUFFER_OVERFLOW:
		case KRC_END_OF_DB:
			sc->curr_key = NULL;
			sc->curr_value = NULL;
			sc->is_valid = 0;
			goto exit;
		default:
			log_fatal("faulty scanner state");
			exit(EXIT_FAILURE);
		}
	}
exit:
	if (sc->is_valid) {
		*keySize = sc->curr_key->key_size;
		*key = sc->curr_key->key_buf;
		*valueSize = sc->curr_value->val_size;
		*value = sc->curr_value->val_buf;
	} else {
		*keySize = 0;
		*key = NULL;
		*valueSize = 0;
		*value = NULL;
	}
	return sc->is_valid;
}

void krc_scan_set_start(krc_scannerp sp, uint32_t start_key_size, void *start_key, krc_seek_mode seek_mode)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	if (!sc->start_infinite) {
		log_warn("Nothing to do already set start key for this scanner");
		return;
	}
	switch (seek_mode) {
	case KRC_GREATER_OR_EQUAL:
	case KRC_GREATER:
		break;
	default:
		log_fatal("unknown seek_mode");
		exit(EXIT_FAILURE);
	}
	sc->seek_mode = seek_mode;
	sc->start_infinite = 0;
	sc->start_key = (krc_key *)malloc(sizeof(krc_key) + start_key_size);
	sc->start_key->key_size = start_key_size;
	memcpy(sc->start_key->key_buf, start_key, start_key_size);
	//log_info("start key set to %s", sc->start_key->key_buf);
	return;
}

void krc_scan_set_stop(krc_scannerp sp, uint32_t stop_key_size, void *stop_key, krc_seek_mode seek_mode)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	if (stop_key_size >= 3 && memcmp(stop_key, pos_infinity, stop_key_size) == 0) {
		sc->stop_infinite = 1;
		return;
	}

	if (!sc->stop_infinite) {
		log_warn("Nothing to do already set stop key for this scanner");
		return;
	}
	sc->stop_infinite = 0;
	sc->seek_mode = seek_mode;
	sc->stop_key = (krc_key *)malloc(sizeof(krc_key) + stop_key_size);
	sc->stop_key->key_size = stop_key_size;
	memcpy(sc->stop_key->key_buf, stop_key, stop_key_size);
	log_debug("stop key set to %s", sc->stop_key->key_buf);
	return;
}

void krc_scan_set_prefix_filter(krc_scannerp sp, uint32_t prefix_size, void *prefix)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	if (sc->prefix_filter_enable) {
		log_warn("Nothing to do already set prefix key for this scanner");
		return;
	}
	sc->seek_mode = KRC_GREATER_OR_EQUAL;
	sc->start_infinite = 0;
	sc->start_key = (krc_key *)malloc(sizeof(krc_key) + prefix_size);
	sc->start_key->key_size = prefix_size;
	memcpy(sc->start_key->key_buf, prefix, prefix_size);
	sc->prefix_filter_enable = 1;
	sc->prefix_key = (krc_key *)malloc(sizeof(krc_key) + prefix_size);
	sc->prefix_key->key_size = prefix_size;
	memcpy(sc->prefix_key->key_buf, prefix, prefix_size);
	return;
}

void krc_scan_close(krc_scannerp sp)
{
	struct krc_scanner *sc = (struct krc_scanner *)sp;
	if (sc->prefix_filter_enable)
		free(sc->prefix_key);
	if (!sc->start_infinite)
		free(sc->start_key);
	if (!sc->stop_infinite)
		free(sc->stop_key);
	free(sc);
	return;
}

static unsigned operation_count = 0, replies_arrived = 0;
krc_ret_code krc_close()
{
	if (rep_checker_stat == KRC_REPLY_CHECKER_RUNNING) {
		/*wait to flush outstanding requests from all queues*/
		log_info("Waiting for outstanding requests...");
		bool print = true;
		struct timespec start, current;
		clock_gettime(CLOCK_MONOTONIC, &start);
		while (spinner->outstanding_requests != 0) {
			clock_gettime(CLOCK_MONOTONIC, &current);
			if (print && current.tv_sec - start.tv_sec > 5) {
				log_info("%d: Operation count = %u, replies = %u, outstanding = %u", getpid(),
					 operation_count, replies_arrived);
				print = false;
			}
		}

		log_info("All outstanding requests done!, instructing reply checker to exit");
		reply_checker_exit = 1;
		int a = 0;
		while (rep_checker_stat == KRC_REPLY_CHECKER_RUNNING) {
			if (++a % 10000) {
				/*log_info("Waiting reply_checker to exit");*/
			}
		}
		log_info("Reply checker exited!");
		reply_checker_exit = 0;
	}
	//cu_close_open_connections();
	return KRC_SUCCESS;
}

/*new functions for asynchronous client related to #57*/
static inline void krc_send_async_request(struct connection_rdma *conn, struct msg_header *req_header,
					  struct msg_header *rep_header, callback t, void *context, uint32_t *buf_size,
					  char *buf)
{
	struct krc_async_req *req = calloc(1, sizeof(struct krc_async_req));
	req->conn = conn;
	req->request = req_header;
	req->reply = rep_header;
	req->request->receive = TU_RDMA_REGULAR_MSG;
	req->callback = t;
	req->context = context;
	req->buf_size = buf_size;
	req->buf = buf;
	req->start_time.tv_sec = 0;
	req->start_time.tv_nsec = 0;
	req->wc_checked = false;
	/*client_rdma_init_message_context(&req->msg_ctx, req->request);
	req->msg_ctx.on_completion_callback = on_completion_client;
	*/

	__sync_fetch_and_add(&spinner->outstanding_requests, 1);
	__sync_fetch_and_add(&operation_count, 1);
	req->reply->receive = UINT8_MAX;
	if (__send_rdma_message(req->conn, req->request, NULL) != KREON_SUCCESS) {
		log_fatal("failed to send message");
		_exit(EXIT_FAILURE);
	}

	while (!(utils_queue_push(&spinner->communication_queue, req))) { /*spin*/
		;
	}
}

static krc_ret_code krc_internal_aput(uint32_t key_size, void *key, uint32_t val_size, void *value, callback t,
				      void *context, int is_update_if_exists)
{
	msg_header *req_header = NULL;
	msg_put_key *put_key = NULL;
	msg_put_value *put_value = NULL;
	msg_header *rep_header = NULL;
	msg_put_rep *put_rep = NULL;

	if (key_size + val_size + (2 * sizeof(uint32_t)) > SEGMENT_SIZE - sizeof(segment_header)) {
		log_fatal("KV size too large currently for Kreon, current max value size supported = %u bytes",
			  SEGMENT_SIZE - sizeof(segment_header));
		log_fatal("Contact gesalous@ics.forth.gr");
		_exit(EXIT_FAILURE);
	}

	struct cu_region_desc *r_desc = cu_get_region(key, key_size);
	connection_rdma *conn = cu_get_conn_for_region(r_desc, djb2_hash((unsigned char *)key, key_size));

	enum message_type req_type = (is_update_if_exists) ? PUT_IF_EXISTS_REQUEST : PUT_REQUEST;
	_krc_get_rpc_pair(conn, &req_header, req_type, key_size + val_size + (2 * sizeof(uint32_t)), &rep_header,
			  PUT_REPLY, sizeof(msg_put_rep));

	put_key = (msg_put_key *)((uint64_t)req_header + sizeof(msg_header));
	/*fill in the key payload part the data, caution we are 100% sure that it fits :-)*/
	put_key->key_size = key_size;
	memcpy(put_key->key, key, key_size);
	put_value = (msg_put_value *)((uint64_t)put_key + sizeof(msg_put_key) + put_key->key_size);
	put_value->value_size = val_size;
	memcpy(put_value->value, value, val_size);

	/*Now the reply part*/
	put_rep = (msg_put_rep *)((uint64_t)rep_header + sizeof(msg_header));
	put_rep->status = KR_REP_PENDING;

	/*inform the req about its buddy*/
	req_header->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, req_header);
	/*location where server should put the reply*/
	req_header->offset_reply_in_recv_buffer =
		(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
	req_header->reply_length_in_recv_buffer =
		sizeof(struct msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;

	krc_send_async_request(conn, req_header, rep_header, t, context, NULL, NULL);
	return KRC_SUCCESS;
}

krc_ret_code krc_aput(uint32_t key_size, void *key, uint32_t val_size, void *value, callback t, void *context)
{
	return krc_internal_aput(key_size, key, val_size, value, t, context, 0);
}

krc_ret_code krc_aput_if_exists(uint32_t key_size, void *key, uint32_t val_size, void *value, callback t, void *context)
{
	return krc_internal_aput(key_size, key, val_size, value, t, context, 1);
}

static uint8_t krc_has_reply_arrived(struct krc_async_req *req)
{
	assert(req);
	assert(req->reply);
	// Check WC
	/*if (!req->wc_checked) {
		if (!sem_trywait(&req->msg_ctx.wait_for_completion)) {
			req->wc_checked = true;
			if (req->msg_ctx.wc.status != IBV_WC_SUCCESS) {
				log_fatal("RDMA write operation failed!");
				_exit(EXIT_FAILURE);
			}
		} else
			return false;
	}*/

	// Check header
	if (req->reply->receive == TU_RDMA_REGULAR_MSG) {
		// Header has arrived
		if (!req->reply->payload_length)
			return true;
		if (get_receive_field(req->reply) == TU_RDMA_REGULAR_MSG)
			return true;
	}

	struct timespec now;
	int ret = clock_gettime(CLOCK_MONOTONIC, &now);
	assert(!ret);
	if (!req->start_time.tv_sec) {
		req->start_time = now;
	}
	size_t elapsed_sec = now.tv_sec - req->start_time.tv_sec;
	if (elapsed_sec > 1000000L && _krc_send_heartbeat(req->conn->rdma_cm_id) != KREON_SUCCESS) {
		log_fatal("Kreon dataserver has failed!");
		_exit(EXIT_FAILURE);
	}
	return false;
}

static void reply_checker_handle_reply(struct krc_async_req *req)
{
	assert(req->request->session_id == req->reply->session_id);
	switch (req->reply->msg_type) {
	case PUT_REPLY: {
#if VALIDATE_CHECKSUMS
		uint32_t msg_len =
			req->request->payload_length + req->request->padding_and_tail_size + MESSAGE_SEGMENT_SIZE;
		uint64_t hash = djb2_hash((unsigned char *)(&req->request->offset_reply_in_recv_buffer),
					  msg_len - sizeof(uint64_t));
		assert(hash == req->reply->session_id);
#endif
		//you should check ret code
		break;
	}
	case GET_REPLY: {
		/*uint32_t size = TU_HEADER_SIZE + req->reply->payload_length +*/
		/*req->reply->padding_and_tail_size;*/
		struct msg_get_rep *msg_rep = (struct msg_get_rep *)((uint64_t)req->reply + sizeof(struct msg_header));
		//log_info(
		//	"Value size is %lu offset_too_large? %lu bytes_remaining %lu",
		//	msg_rep->value_size, msg_rep->offset_too_large,
		//	msg_rep->bytes_remaining);
		if (!msg_rep->key_found) {
			log_fatal("Key not found!");
			/*exit(EXIT_FAILURE);*/
		}

		if (msg_rep->value_size > *req->buf_size) {
			log_fatal("Reply larger than buffer!");
			_exit(EXIT_FAILURE);
		}
		memcpy(req->buf, msg_rep->value, msg_rep->value_size);
		*req->buf_size = msg_rep->value_size;
		break;
	}
	default:
		log_debug("unhandled msg type is %d", req->reply->msg_type);
		log_fatal("Unhandled reply type");
		_exit(EXIT_FAILURE);
	}

	if (req->callback) {
		//log_info("Calling callback for req");
		req->callback(req->context);
	}
	zero_rendezvous_locations(req->reply);
	pthread_mutex_lock(&req->conn->allocation_lock);
	/*FIXME*/
	client_free_rpc_pair(req->conn, req->reply);
	pthread_mutex_unlock(&req->conn->allocation_lock);

	memset(req, 0, sizeof(struct krc_async_req));
	__sync_fetch_and_sub(&spinner->outstanding_requests, 1);
	__sync_fetch_and_add(&replies_arrived, 1);
}

static void *krc_reply_checker(void *args)
{
	(void)args; /*nullify warning of unused variable since args are not used*/
	pthread_setname_np(pthread_self(), "reply_checker");

	spinner = (struct krc_spinner *)calloc(1, sizeof(struct krc_spinner));
	utils_queue_init(&spinner->communication_queue);
	/*allocate the private aray of size UTILS_QUEUE_CAPACITY*/
	spinner->private_array_of_oustanding_reqs =
		(struct krc_async_req *)calloc(UTILS_QUEUE_CAPACITY, sizeof(struct krc_async_req));

	log_debug("reply_checker done initialization starting spinning for possible replies");
	rep_checker_stat = KRC_REPLY_CHECKER_RUNNING;

	struct krc_async_req *curr_req = NULL;
	while (!reply_checker_exit) {
		/* find an empty place to put a request, if spinner does not find free space on a run
		 * he will wrap around */
		for (int i = 0; i < UTILS_QUEUE_CAPACITY; ++i) {
			/*possible req to be handled*/
			if (spinner->private_array_of_oustanding_reqs[i].conn) {
				curr_req = &spinner->private_array_of_oustanding_reqs[i];
				if (!krc_has_reply_arrived(curr_req))
					continue; /*reply has not arrived yet*/

				reply_checker_handle_reply(curr_req);

			} else {
				/*see if you can insert a request in this free space*/
				struct krc_async_req *req = NULL;
				if ((req = utils_queue_pop(&spinner->communication_queue))) {
					spinner->private_array_of_oustanding_reqs[i] = *req;
					free(req);
				}
			}
		}
	}
	//log_info("reply_checker exiting");
	free(spinner);
	rep_checker_stat = KRC_REPLY_CHECKER_NOT_RUNNING;
	return NULL;
}

krc_ret_code krc_aget(uint32_t key_size, char *key, uint32_t *buf_size, char *buf, callback t, void *context)
{
	uint32_t reply_size = *buf_size;
	struct cu_region_desc *r_desc = cu_get_region(key, key_size);
	struct connection_rdma *conn = cu_get_conn_for_region(r_desc, djb2_hash((unsigned char *)key, key_size));
	/*get the rdma communication buffers*/
	struct msg_header *req_header = NULL;
	struct msg_header *rep_header = NULL;
	_krc_get_rpc_pair(conn, &req_header, GET_REQUEST, sizeof(msg_get_req) + key_size, &rep_header, GET_REPLY,
			  sizeof(msg_get_rep) + reply_size);

	struct msg_get_req *m_get = (struct msg_get_req *)((uint64_t)req_header + sizeof(struct msg_header));

	m_get->key_size = key_size;
	memcpy(m_get->key, key, key_size);
	m_get->offset = 0;
	m_get->fetch_value = 1;
	m_get->bytes_to_read = reply_size;

	/*inform the req about its buddy*/
	req_header->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, req_header);
	/*location where server should put the reply*/
	req_header->offset_reply_in_recv_buffer =
		(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
	req_header->reply_length_in_recv_buffer =
		sizeof(struct msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;
	krc_send_async_request(conn, req_header, rep_header, t, context, buf_size, buf);
	return KRC_SUCCESS;
}

uint8_t krc_start_async_thread(void)
{
	if (pthread_create(&spinner_cnxt, NULL, krc_reply_checker, NULL) != 0) {
		log_fatal("Failed to spawn async reply checker");
		_exit(EXIT_FAILURE);
	}

	int a = 0;
	while (rep_checker_stat == KRC_REPLY_CHECKER_NOT_RUNNING) {
		if (++a % 10000) {
			/*log_info("Waiting reply_checker to start");*/
		}
	}
	log_info("Successfully spawned async reply checker");
	return 1;
}
