#include "send_index_callbacks.h"
#include "../../common/common.h"
#include "../metadata.h"
#include "btree/index_node.h"
#include "btree/level_write_cursor.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include "send_index_uuid_checker.h"
#include <infiniband/verbs.h>
#include <log.h>
#include <pthread.h>
#include <rdma/rdma_verbs.h>
#include <stdbool.h>
#include <stdint.h>

static void send_index_fill_flush_L0_request(msg_header *request_header, struct krm_region_desc *r_desc)
{
	struct s2s_msg_flush_L0_req *req =
		(struct s2s_msg_flush_L0_req *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = r_desc->region->min_key_size;
	strcpy(req->region_key, r_desc->region->min_key);
	req->uuid = (uint64_t)req;
}

static void send_index_fill_get_rdma_buffer_request(msg_header *request_header, struct krm_region_desc *r_desc,
						    uint32_t level_id, uint8_t tree_id,
						    struct wcursor_level_write_cursor *new_level_cursor)
{
	struct s2s_msg_replica_index_get_buffer_req *req =
		(struct s2s_msg_replica_index_get_buffer_req *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = r_desc->region->min_key_size;
	strcpy(req->region_key, r_desc->region->min_key);
	req->level_id = level_id;
	req->tree_id = tree_id;
	req->num_rows = wcursor_get_number_of_rows(new_level_cursor);
	req->num_cols = wcursor_get_number_of_cols(new_level_cursor);
	req->entry_size = wcursor_get_compaction_index_entry_size(new_level_cursor);
	req->uuid = (uint64_t)req;
}

static void send_index_fill_close_compaction_request(msg_header *request_header, struct krm_region_desc *r_desc,
						     uint32_t level_id)
{
	struct s2s_msg_close_compaction_request *req =
		(struct s2s_msg_close_compaction_request *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = r_desc->region->min_key_size;
	strcpy(req->region_key, r_desc->region->min_key);
	req->level_id = level_id;
	req->uuid = (uint64_t)req;
}

static void send_index_fill_swap_levels_request(msg_header *request_header, struct krm_region_desc *r_desc,
						uint32_t level_id)
{
	struct s2s_msg_swap_levels_request *req =
		(struct s2s_msg_swap_levels_request *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = r_desc->region->min_key_size;
	strcpy(req->region_key, r_desc->region->min_key);
	req->level_id = level_id;
	req->uuid = (uint64_t)req;
}

static void send_index_fill_flush_index_segment(msg_header *request_header, struct krm_region_desc *r_desc,
						struct wcursor_level_write_cursor *wcursor, uint32_t height,
						uint32_t level_id, uint32_t clock, uint32_t replica_id)
{
	struct s2s_msg_replica_index_flush_req *f_req =
		(struct s2s_msg_replica_index_flush_req *)((char *)request_header + sizeof(struct msg_header));
	f_req->height = height;
	f_req->level_id = level_id;
	f_req->clock = clock;
	f_req->region_key_size = r_desc->region->min_key_size;
	strcpy(f_req->region_key, r_desc->region->min_key);
	/*related for the reply part*/
	f_req->mr_of_primary = *r_desc->local_buffer[level_id];
	f_req->reply_size = wcursor_segment_buffer_status_size(wcursor);
	f_req->reply_offt = wcursor_segment_buffer_get_status_addr(wcursor, height, clock, replica_id);
}

static void send_index_fill_reply_fields(struct sc_msg_pair *msg_pair, struct connection_rdma *r_conn)
{
	struct msg_header *request = msg_pair->request;
	struct msg_header *reply = msg_pair->reply;

	request->receive = TU_RDMA_REGULAR_MSG;
	request->triggering_msg_offset_in_send_buffer = real_address_to_triggering_msg_offt(r_conn, request);
	reply->op_status = 1;
	request->offset_reply_in_recv_buffer = (uint64_t)reply - (uint64_t)r_conn->recv_circular_buf->memory_region;
	request->reply_length_in_recv_buffer =
		sizeof(msg_header) + reply->payload_length + reply->padding_and_tail_size;
}

struct send_index_allocate_msg_pair_info {
	struct connection_rdma *conn;
	uint32_t request_size;
	uint32_t reply_size;
	enum message_type request_type;
};
static struct sc_msg_pair
send_index_allocate_msg_pair(struct send_index_allocate_msg_pair_info send_index_allocation_info)
{
	struct sc_msg_pair msg_pair =
		sc_allocate_rpc_pair(send_index_allocation_info.conn, send_index_allocation_info.request_size,
				     send_index_allocation_info.reply_size, send_index_allocation_info.request_type);
	while (msg_pair.stat != ALLOCATION_IS_SUCCESSFULL) {
		msg_pair = sc_allocate_rpc_pair(send_index_allocation_info.conn,
						send_index_allocation_info.request_size,
						send_index_allocation_info.reply_size,
						send_index_allocation_info.request_type);
	}
	return msg_pair;
}

static void send_index_flush_L0(struct send_index_context *context)
{
	struct krm_region_desc *r_desc = context->r_desc;
	struct krm_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
		uint32_t flush_L0_request_size = sizeof(struct s2s_msg_flush_L0_req);
		uint32_t flush_L0_reply_size = sizeof(struct s2s_msg_flush_L0_rep);

		//allocate msg
		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = flush_L0_request_size,
			.reply_size = flush_L0_reply_size,
			.request_type = FLUSH_L0_REQUEST
		};
		r_desc->rpc[i][0] = send_index_allocate_msg_pair(send_index_allocation_info);
		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][0];

		// send the msg
		msg_header *req_header = msg_pair->request;
		send_index_fill_flush_L0_request(req_header, r_desc);
		req_header->session_id = (uint64_t)r_desc->region;

		if (__send_rdma_message(r_conn, req_header, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][0];
		// spin for reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debugging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, FLUSH_L0_REQUEST);
		// free msg space
		sc_free_rpc_pair(msg_pair);
	}
}

static void send_index_allocate_rdma_buffer_in_replicas(struct send_index_context *context, uint32_t level_id,
							uint8_t tree_id,
							struct wcursor_level_write_cursor *new_level_cursor)
{
	struct krm_region_desc *r_desc = context->r_desc;
	struct krm_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
		uint32_t get_buffer_request_size = sizeof(struct s2s_msg_replica_index_get_buffer_req);
		uint32_t get_buffer_reply_size = sizeof(struct s2s_msg_replica_index_get_buffer_rep);

		log_debug("Sending get rdma buffer for region %s", r_desc->region->id);
		// allocate msg
		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = get_buffer_request_size,
			.reply_size = get_buffer_reply_size,
			.request_type = REPLICA_INDEX_GET_BUFFER_REQ
		};
		r_desc->rpc[i][level_id] = send_index_allocate_msg_pair(send_index_allocation_info);
		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];

		// send the msg
		send_index_fill_get_rdma_buffer_request(msg_pair->request, r_desc, level_id, tree_id, new_level_cursor);
		send_index_fill_reply_fields(msg_pair, r_conn);
		msg_pair->request->session_id = (uint64_t)r_desc->region;

		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, REPLICA_INDEX_GET_BUFFER_REQ);
		struct s2s_msg_replica_index_get_buffer_rep *get_buffer_reply =
			(struct s2s_msg_replica_index_get_buffer_rep *)((uint64_t)msg_pair->reply +
									sizeof(struct msg_header));
		r_desc->remote_mem_buf[i][level_id] = get_buffer_reply->mr;
		log_debug("Newly registered memory region is at %lu", (uint64_t)get_buffer_reply->mr.addr);
		// free msg space
		sc_free_rpc_pair(msg_pair);
	}
}

static void send_index_reg_write_primary_compaction_buffer(struct send_index_context *context,
							   struct wcursor_level_write_cursor *wcursor,
							   uint32_t level_id)
{
	assert(context && wcursor);

	// TODO: develop a better way to find the protection domain
	struct connection_rdma *r_conn =
		sc_get_compaction_conn(context->server, context->r_desc->region->backups[0].kreon_ds_hostname);

	char *wcursor_segment_buffer_offt = wcursor_get_cursor_buffer(wcursor, 0, 0);
	uint32_t wcursor_segment_buffer_size = wcursor_get_segment_buffer_size(wcursor);
	context->r_desc->local_buffer[level_id] =
		rdma_reg_write(r_conn->rdma_cm_id, wcursor_segment_buffer_offt, wcursor_segment_buffer_size);
	if (context->r_desc->local_buffer[level_id] == NULL) {
		log_fatal("Failed to reg memory");
		_exit(EXIT_FAILURE);
	}
}

static void send_index_close_compaction(struct send_index_context *context, uint32_t level_id)
{
	struct krm_region_desc *r_desc = context->r_desc;
	struct krm_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		assert(r_desc->send_index_flush_index_segment_rpc_in_use[i][level_id][0] == false);
		assert(r_desc->send_index_flush_index_segment_rpc_in_use[i][level_id][1] == false);
		r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
		uint32_t send_index_close_compaction_request_size = sizeof(struct s2s_msg_close_compaction_request);
		uint32_t send_index_close_compaction_reply_size = sizeof(struct s2s_msg_close_compaction_reply);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = send_index_close_compaction_request_size,
			.reply_size = send_index_close_compaction_reply_size,
			.request_type = CLOSE_COMPACTION_REQUEST
		};
		r_desc->rpc[i][level_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		send_index_fill_close_compaction_request(msg_pair->request, r_desc, level_id);
		send_index_fill_reply_fields(msg_pair, r_conn);
		msg_pair->request->session_id = (uint64_t)r_desc->region;

		// send the msg
		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, CLOSE_COMPACTION_REQUEST);
		// free msg space
		sc_free_rpc_pair(msg_pair);
	}
}

/* static void send_index_replicate_index_segment(struct send_index_context *context, */
/* 					       struct wcursor_level_write_cursor *wcursor, uint32_t buf_size, */
/* 					       uint32_t level_id, uint32_t height, uint32_t clock) */
/* { */
/* 	struct krm_region_desc *r_desc = context->r_desc; */
/* 	struct krm_server_desc *server = context->server; */
/* 	assert(r_desc && server); */
/* 	int ret = 0; */
/* 	char *primary_buffer = wcursor_get_cursor_buffer(wcursor, height, clock); */
/* 	uint32_t number_of_columns = wcursor_get_number_of_cols(wcursor); */
/* 	uint32_t entry_size = wcursor_get_number_of_rows(wcursor); */

/* 	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) { */
/* 		uint32_t row_size_of_backup = entry_size * number_of_columns; */
/* 		char *backup_buffer = (char *)r_desc->remote_mem_buf[i][level_id].addr + */
/* 				      ((height * row_size_of_backup) + (clock * entry_size)); */
/* 		struct connection_rdma *r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname); */
/* 		while (1) { */
/* 			ret = rdma_post_write(r_conn->rdma_cm_id, NULL, primary_buffer, buf_size, */
/* 					      r_desc->local_buffer[level_id], IBV_SEND_SIGNALED, */
/* 					      (uint64_t)backup_buffer, r_desc->remote_mem_buf[i][level_id].rkey); */
/* 			if (!ret) */
/* 				break; */
/* 		} */
/* 	} */
/* } */

static void send_index_send_flush_index_segment(struct send_index_context *context,
						struct wcursor_level_write_cursor *wcursor, uint32_t level_id,
						uint32_t clock, uint32_t height, bool is_last)
{
	struct krm_region_desc *r_desc = context->r_desc;
	struct krm_server_desc *server = context->server;
	assert(r_desc && server);
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct connection_rdma *r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
		uint32_t request_size = sizeof(struct s2s_msg_replica_index_flush_req);
		uint32_t reply_size = sizeof(struct s2s_msg_replica_index_flush_rep);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = request_size,
			.reply_size = reply_size,
			.request_type = REPLICA_INDEX_FLUSH_REQ,
		};
		r_desc->send_index_flush_index_segment_rpc[i][level_id][clock] =
			send_index_allocate_msg_pair(send_index_allocation_info);

		assert(r_desc->send_index_flush_index_segment_rpc_in_use[i][level_id][clock] == false);
		r_desc->send_index_flush_index_segment_rpc_in_use[i][level_id][clock] = true;

		struct sc_msg_pair *msg_pair = &r_desc->send_index_flush_index_segment_rpc[i][level_id][clock];
		send_index_fill_flush_index_segment(msg_pair->request, r_desc, wcursor, height, level_id, clock, i);
		send_index_fill_reply_fields(msg_pair, r_conn);
		msg_pair->request->session_id = (uint64_t)r_desc->region;

		//send the msg
		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}

		// if is_last is true then we are in the flushing stage of the wcursor compaction, and we spin for the last segments
		// to be written in the replicas
		if (is_last)
			wcursor_spin_for_buffer_status(wcursor);
	}
}

static void send_index_send_segment(struct send_index_context *context, struct wcursor_level_write_cursor *wcursor,
				    uint32_t buf_size, uint32_t height, uint32_t level_id, uint32_t clock, bool is_last)
{
	(void)buf_size;
	/* rdma_write index segment to replicas */
	//send_index_replicate_index_segment(context, wcursor, buf_size, level_id, height, clock);
	/* send control msg to flush the index segment to replicas */
	send_index_send_flush_index_segment(context, wcursor, level_id, clock, height, is_last);
}

static void send_index_swap_levels(struct send_index_context *context, uint32_t level_id)
{
	struct krm_region_desc *r_desc = context->r_desc;
	struct krm_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
		uint32_t send_index_swap_levels_request_size = sizeof(struct s2s_msg_swap_levels_request);
		uint32_t send_index_swap_levels_reply_size = sizeof(struct s2s_msg_swap_levels_reply);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = send_index_swap_levels_request_size,
			.reply_size = send_index_swap_levels_reply_size,
			.request_type = REPLICA_INDEX_SWAP_LEVELS_REQUEST
		};
		r_desc->rpc[i][level_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		send_index_fill_swap_levels_request(msg_pair->request, r_desc, level_id);
		msg_pair->request->session_id = (uint64_t)r_desc->region;

		// send the msg
		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, REPLICA_INDEX_SWAP_LEVELS_REQUEST);
		// free msg space
		sc_free_rpc_pair(msg_pair);
	}
}

void send_index_compaction_started_callback(void *context, uint32_t src_level_id, uint8_t dst_tree_id,
					    struct wcursor_level_write_cursor *new_level)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	//compaction from L0 to L1
	if (!src_level_id)
		send_index_flush_L0(send_index_cxt);

	send_index_allocate_rdma_buffer_in_replicas(send_index_cxt, src_level_id, dst_tree_id, new_level);
	send_index_reg_write_primary_compaction_buffer(send_index_cxt, new_level, src_level_id);
}

void send_index_compaction_ended_callback(void *context, uint32_t src_level_id)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;

	send_index_close_compaction(send_index_cxt, src_level_id);
}

void send_index_swap_levels_callback(void *context, uint32_t src_level_id)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;

	send_index_swap_levels(send_index_cxt, src_level_id);
}

void send_index_compaction_wcursor_flush_segment_callback(void *context, struct wcursor_level_write_cursor *wcursor,
							  uint32_t level_id, uint32_t height, uint32_t buf_size,
							  uint32_t clock, bool is_last)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;

	send_index_send_segment(send_index_cxt, wcursor, buf_size, height, level_id, clock, is_last);
}

void send_index_compaction_wcursor_got_flush_replies(void *context, uint32_t level_id, uint32_t clock)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	//log_debug("i got all the replies for level %u", level_id);
	struct krm_region_desc *r_desc = send_index_cxt->r_desc;
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		assert(r_desc->send_index_flush_index_segment_rpc_in_use[i][level_id][clock] == true);
		sc_free_rpc_pair(&r_desc->send_index_flush_index_segment_rpc[i][level_id][clock]);
		r_desc->send_index_flush_index_segment_rpc_in_use[i][level_id][clock] = false;
	}
}

void send_index_init_callbacks(struct krm_server_desc *server, struct krm_region_desc *r_desc)
{
	struct send_index_context *send_index_cxt =
		(struct send_index_context *)calloc(1, sizeof(struct send_index_context));
	send_index_cxt->r_desc = r_desc;
	send_index_cxt->server = server;
	void *cxt = (void *)send_index_cxt;

	struct parallax_callback_funcs send_index_callback_functions = { 0 };
	send_index_callback_functions.compaction_started_cb = send_index_compaction_started_callback;
	send_index_callback_functions.compaction_ended_cb = send_index_compaction_ended_callback;
	send_index_callback_functions.swap_levels_cb = send_index_swap_levels_callback;
	send_index_callback_functions.comp_write_cursor_flush_segment_cb =
		send_index_compaction_wcursor_flush_segment_callback;
	send_index_callback_functions.comp_write_cursor_got_flush_replies_cb =
		send_index_compaction_wcursor_got_flush_replies;

	parallax_init_callbacks(r_desc->db, &send_index_callback_functions, cxt);
}
