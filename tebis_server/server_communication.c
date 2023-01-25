#include "../utilities/circular_buffer.h"
#include "conf.h"
#include "djb2.h"
#include "globals.h"
#include "messages.h"
#include "metadata.h"
#include "region_server.h"
#include "uthash.h"
#include <infiniband/verbs.h>
#include <log.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdint.h>
#include <stdlib.h>

const uint32_t S2S_MSG_SIZE = S2S_MSG_SIZE_VALUE;

struct fill_request_msg_info {
	struct msg_header *request;
	uint32_t request_size;
	uint32_t request_padding;
	uint32_t req_type;
	struct msg_header *reply;
};

static void fill_request_msg(connection_rdma *conn, struct fill_request_msg_info msg_info)
{
	msg_info.request->payload_length = 0;
	msg_info.request->padding_and_tail_size = 0;
	if (msg_info.request_size > 0) {
		msg_info.request->payload_length = msg_info.request_size;
		msg_info.request->padding_and_tail_size = msg_info.request_padding + TU_TAIL_SIZE;
	}
	msg_info.request->msg_type = msg_info.req_type;
	msg_info.request->offset_in_send_and_target_recv_buffers =
		calc_offset_in_send_and_target_recv_buffer(msg_info.request, conn->send_circular_buf);
	msg_info.request->triggering_msg_offset_in_send_buffer =
		real_address_to_triggering_msg_offt(conn, msg_info.request);
	msg_info.request->offset_reply_in_recv_buffer =
		((uint64_t)msg_info.reply - (uint64_t)conn->recv_circular_buf->memory_region);
	msg_info.request->reply_length_in_recv_buffer =
		sizeof(msg_header) + msg_info.reply->payload_length + msg_info.reply->padding_and_tail_size;
	set_receive_field(msg_info.request, TU_RDMA_REGULAR_MSG);
}

static void fill_reply_msg(connection_rdma *conn, struct msg_header *reply, uint32_t reply_size, uint32_t reply_padding,
			   uint32_t reply_type)
{
	reply->payload_length = 0;
	reply->padding_and_tail_size = 0;
	if (reply_size > 0) {
		reply->payload_length = reply_size;
		reply->padding_and_tail_size = reply_padding + TU_TAIL_SIZE;
	}
	reply->msg_type = reply_type;
	reply->offset_in_send_and_target_recv_buffers =
		calc_offset_in_send_and_target_recv_buffer(reply, conn->send_circular_buf);
	reply->offset_reply_in_recv_buffer = UINT32_MAX;
	reply->reply_length_in_recv_buffer = UINT32_MAX;
	reply->triggering_msg_offset_in_send_buffer = UINT32_MAX;
	set_receive_field(reply, 0);
}

struct sc_msg_pair sc_allocate_rpc_pair(struct connection_rdma *conn, uint32_t request_size, uint32_t reply_size,
					enum message_type type)
{
	struct sc_msg_pair rep = { .request = NULL, .reply = NULL, .stat = 0 };
	char *addr = NULL;
	enum message_type req_type = type;
	enum message_type rep_type;
	rep.conn = conn;
	/*calculate the sizes for both request and reply*/
	/*request part*/
	uint32_t actual_request_size = S2S_MSG_SIZE;
	uint32_t request_padding = 0;
	if (request_size > 0) {
		actual_request_size = TU_HEADER_SIZE + request_size + TU_TAIL_SIZE;
		if (actual_request_size % S2S_MSG_SIZE != 0) {
			request_padding = (S2S_MSG_SIZE - (actual_request_size % S2S_MSG_SIZE));
			actual_request_size += request_padding;
		}
	}
	/*reply part*/
	uint32_t actual_reply_size = S2S_MSG_SIZE;
	uint32_t reply_padding = 0;
	if (reply_size > 0) {
		actual_reply_size = TU_HEADER_SIZE + reply_size + TU_TAIL_SIZE;
		if (actual_reply_size % S2S_MSG_SIZE != 0) {
			reply_padding = (S2S_MSG_SIZE - (actual_reply_size % S2S_MSG_SIZE));
			actual_reply_size += reply_padding;
		}
	}

	if (actual_reply_size != S2S_MSG_SIZE || actual_request_size != S2S_MSG_SIZE) {
		log_fatal("Cant allocate a msg for s2s communication larger thant S2S_MSG_size = %u", S2S_MSG_SIZE);
		assert(0);
		_exit(EXIT_FAILURE);
	}

	pthread_mutex_lock(&conn->buffer_lock);
	pthread_mutex_lock(&conn->allocation_lock);
	switch (req_type) {
	case GET_RDMA_BUFFER_REQ:
		rep_type = GET_RDMA_BUFFER_REP;
		break;
	case FLUSH_COMMAND_REQ:
		rep_type = FLUSH_COMMAND_REP;
		break;
	case REPLICA_INDEX_GET_BUFFER_REQ:
		rep_type = REPLICA_INDEX_GET_BUFFER_REP;
		break;
	case REPLICA_INDEX_FLUSH_REQ:
		rep_type = REPLICA_INDEX_FLUSH_REP;
		break;
	case FLUSH_L0_REQUEST:
		rep_type = FLUSH_L0_REPLY;
		break;
	case CLOSE_COMPACTION_REQUEST:
		rep_type = CLOSE_COMPACTION_REPLY;
		break;
	case REPLICA_INDEX_SWAP_LEVELS_REQUEST:
		rep_type = REPLICA_INDEX_SWAP_LEVELS_REPLY;
		break;
	default:
		log_fatal("Unsupported s2s message type %d", type);
		_exit(EXIT_FAILURE);
	}

	/* The idea is the following, if we are not able to allocate both buffers while acquiring the lock we should rollback.
     * Also we need to allocate receive buffer first and then send buffer.
     * First we allocate the receive buffer, aka where we expect the reply
     */
retry_allocate_reply:
	/*reply allocation*/
	rep.stat = allocate_space_from_circular_buffer(conn->recv_circular_buf, actual_reply_size, &addr);
	switch (rep.stat) {
	case ALLOCATION_IS_SUCCESSFULL:
	case BITMAP_RESET:
		break;
	case NOT_ENOUGH_SPACE_AT_THE_END:
		reset_circular_buffer(conn->recv_circular_buf);
		goto retry_allocate_reply;
		break;
	case SPACE_NOT_READY_YET:
		goto exit;
	}
	rep.reply = (struct msg_header *)addr;

	/*request allocation*/
	rep.stat = allocate_space_from_circular_buffer(conn->send_circular_buf, actual_request_size, &addr);
	switch (rep.stat) {
	case NOT_ENOUGH_SPACE_AT_THE_END:
		log_fatal("Server 2 Server communication should not include no_op msg");
		_exit(EXIT_FAILURE);
	case SPACE_NOT_READY_YET:
		/*rollback previous allocation*/
		free_space_from_circular_buffer(conn->recv_circular_buf, (char *)rep.reply, actual_reply_size);
		conn->recv_circular_buf->last_addr =
			(char *)((uint64_t)conn->recv_circular_buf->last_addr - actual_reply_size);
		conn->recv_circular_buf->remaining_space += actual_reply_size;
		goto exit;
	case ALLOCATION_IS_SUCCESSFULL:
	case BITMAP_RESET:
		break;
	}
	rep.request = (struct msg_header *)addr;

	/*init the headers*/
	fill_reply_msg(conn, rep.reply, reply_size, reply_padding, rep_type);
	struct fill_request_msg_info msg_info;
	msg_info.request = rep.request;
	msg_info.request_size = request_size;
	msg_info.request_padding = request_padding;
	msg_info.req_type = req_type;
	msg_info.reply = rep.reply;
	fill_request_msg(conn, msg_info);

exit:
	pthread_mutex_unlock(&conn->allocation_lock);
	pthread_mutex_unlock(&conn->buffer_lock);
	return rep;
}

void sc_free_rpc_pair(struct sc_msg_pair *p)
{
	msg_header *request = p->request;
	msg_header *reply = p->reply;
	assert(request->reply_length_in_recv_buffer != 0);
	pthread_mutex_lock(&p->conn->buffer_lock);
	pthread_mutex_lock(&p->conn->allocation_lock);

	zero_rendezvous_locations_l(reply, request->reply_length_in_recv_buffer);
	free_space_from_circular_buffer(p->conn->recv_circular_buf, (char *)reply,
					request->reply_length_in_recv_buffer);

	uint32_t size = MESSAGE_SEGMENT_SIZE;
	if (request->payload_length)
		size = TU_HEADER_SIZE + request->payload_length + request->padding_and_tail_size;

	assert(size % MESSAGE_SEGMENT_SIZE == 0);
	free_space_from_circular_buffer(p->conn->send_circular_buf, (char *)request, size);
	pthread_mutex_unlock(&p->conn->allocation_lock);
	pthread_mutex_unlock(&p->conn->buffer_lock);
}
