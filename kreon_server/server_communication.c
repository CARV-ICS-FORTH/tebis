#include "../utilities/circular_buffer.h"
#include "conf.h"
#include "djb2.h"
#include "globals.h"
#include "messages.h"
#include "metadata.h"
#include "uthash.h"
#include <infiniband/verbs.h>
#include <log.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdint.h>
#include <stdlib.h>
const uint32_t S2S_MSG_SIZE = 256;

struct sc_conn_per_server {
	uint64_t hash_key;
	struct krm_server_name server;
	struct connection_rdma *conn;
	UT_hash_handle hh;
};
struct sc_conn_per_server *sc_root_data_cps = NULL;
struct sc_conn_per_server *sc_root_compaction_cps = NULL;
static pthread_mutex_t conn_map_lock = PTHREAD_MUTEX_INITIALIZER;
static void fill_request_msg(connection_rdma *conn, struct msg_header *request, uint32_t request_size,
			     uint32_t request_padding, uint32_t req_type, struct msg_header *reply)
{
	request->payload_length = 0;
	request->padding_and_tail_size = 0;
	if (request_size > 0) {
		request->payload_length = request_size;
		request->padding_and_tail_size = request_padding + TU_TAIL_SIZE;
	}
	request->msg_type = req_type;
	request->offset_in_send_and_target_recv_buffers =
		(uint64_t)request - (uint64_t)conn->send_circular_buf->memory_region;
	request->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(conn, request);
	request->offset_reply_in_recv_buffer = ((uint64_t)reply - (uint64_t)conn->recv_circular_buf->memory_region);
	request->reply_length_in_recv_buffer =
		sizeof(msg_header) + reply->payload_length + reply->padding_and_tail_size;
	set_receive_field(request, TU_RDMA_REGULAR_MSG);
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
		(uint64_t)reply - (uint64_t)conn->send_circular_buf->memory_region;
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
		log_fatal("Cant allocate a msg for s2s communication larger thant S2S_MSG_size = %lu", S2S_MSG_SIZE);
		assert(0);
		_exit(EXIT_FAILURE);
	}

	pthread_mutex_lock(&conn->buffer_lock);
	switch (req_type) {
	case GET_LOG_BUFFER_REQ:
		rep_type = GET_LOG_BUFFER_REP;
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
	fill_request_msg(conn, rep.request, request_size, request_padding, req_type, rep.reply);

exit:
	pthread_mutex_unlock(&conn->buffer_lock);
	return rep;
}

void sc_free_rpc_pair(struct sc_msg_pair *p)
{
	msg_header *request = p->request;
	msg_header *reply = p->reply;
	assert(request->reply_length_in_recv_buffer != 0);
	zero_rendezvous_locations_l(reply, request->reply_length_in_recv_buffer);
	free_space_from_circular_buffer(p->conn->recv_circular_buf, (char *)reply,
					request->reply_length_in_recv_buffer);

	uint32_t size = MESSAGE_SEGMENT_SIZE;
	if (request->payload_length)
		size = TU_HEADER_SIZE + request->payload_length + request->padding_and_tail_size;

	assert(size % MESSAGE_SEGMENT_SIZE == 0);
	free_space_from_circular_buffer(p->conn->send_circular_buf, (char *)request, size);
}

extern int krm_zk_get_server_name(char *dataserver_name, struct krm_server_desc const *my_desc,
				  struct krm_server_name *dst, int *zk_rc);

static struct connection_rdma *sc_get_conn(struct krm_server_desc const *mydesc, char *hostname,
					   struct sc_conn_per_server **sc_root_cps)
{
	struct sc_conn_per_server *cps = NULL;
	uint64_t key;

	key = djb2_hash((unsigned char *)hostname, strlen(hostname));
	HASH_FIND_PTR(*sc_root_cps, &key, cps);
	if (cps == NULL) {
		pthread_mutex_lock(&conn_map_lock);
		HASH_FIND_PTR(*sc_root_cps, &key, cps);
		if (cps == NULL) {
			/*ok update server info from zookeeper*/
			cps = (struct sc_conn_per_server *)malloc(sizeof(struct sc_conn_per_server));
			int rc = krm_zk_get_server_name(hostname, mydesc, &cps->server, NULL);
			if (rc) {
				log_fatal("Failed to refresh info for server %s", hostname);
				_exit(EXIT_FAILURE);
			}
			char *IP = cps->server.RDMA_IP_addr;
			cps->conn = crdma_client_create_connection_list_hosts(ds_get_channel(mydesc), &IP, 1,
									      MASTER_TO_REPLICA_CONNECTION);

			/*init list here*/
			cps->hash_key = key;
			HASH_ADD_PTR(*sc_root_cps, hash_key, cps);
		}
		pthread_mutex_unlock(&conn_map_lock);
	}

	return cps->conn;
}

struct connection_rdma *sc_get_data_conn(struct krm_server_desc const *mydesc, char *hostname)
{
	return sc_get_conn(mydesc, hostname, &sc_root_data_cps);
}

struct connection_rdma *sc_get_compaction_conn(struct krm_server_desc *mydesc, char *hostname)
{
	return sc_get_conn(mydesc, hostname, &sc_root_compaction_cps);
}
