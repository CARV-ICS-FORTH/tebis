#include "send_index_callbacks.h"
#include "../../common/common.h"
#include "../metadata.h"
#include "../region_desc.h"
#include "allocator/volume_manager.h"
#include "btree/index_node.h"
#include "btree/level_write_cursor.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include "send_index_uuid_checker/send_index_uuid_checker.h"
#include <infiniband/verbs.h>
#include <log.h>
#include <pthread.h>
#include <rdma/rdma_verbs.h>
#include <stdbool.h>
#include <stdint.h>

static void send_index_fill_flush_L0_request(msg_header *request_header, region_desc_t r_desc,
					     uint64_t small_log_tail_dev_offt, uint64_t big_log_tail_dev_offt)
{
	struct s2s_msg_flush_L0_req *req =
		(struct s2s_msg_flush_L0_req *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->uuid = (uint64_t)req;
	req->small_log_tail_dev_offt = small_log_tail_dev_offt;
	req->big_log_tail_dev_offt = big_log_tail_dev_offt;
}

static void send_index_fill_get_rdma_buffer_request(msg_header *request_header, region_desc_t r_desc, uint32_t level_id,
						    uint8_t tree_id,
						    struct wcursor_level_write_cursor *new_level_cursor)
{
	struct s2s_msg_replica_index_get_buffer_req *req =
		(struct s2s_msg_replica_index_get_buffer_req *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->level_id = level_id;
	req->tree_id = tree_id;
	req->num_rows = wcursor_get_number_of_rows(new_level_cursor);
	req->num_cols = wcursor_get_number_of_cols(new_level_cursor);
	req->entry_size = wcursor_get_compaction_index_entry_size(new_level_cursor);
	req->uuid = (uint64_t)req;
}

static void send_index_fill_close_compaction_request(msg_header *request_header, struct region_desc *r_desc,
						     uint32_t level_id)
{
	struct s2s_msg_close_compaction_request *req =
		(struct s2s_msg_close_compaction_request *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->level_id = level_id;
	req->uuid = (uint64_t)req;
}

static void send_index_fill_swap_levels_request(msg_header *request_header, struct region_desc *r_desc,
						uint32_t level_id)
{
	struct s2s_msg_swap_levels_request *req =
		(struct s2s_msg_swap_levels_request *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->level_id = level_id;
	req->uuid = (uint64_t)req;
}

static void send_index_fill_flush_index_segment(msg_header *request_header, region_desc_t r_desc,
						uint64_t start_segment_offt, struct wcursor_level_write_cursor *wcursor,
						uint32_t height, uint32_t level_id, uint32_t clock, uint32_t replica_id,
						bool is_last_segment)
{
	(void)is_last_segment;
	struct s2s_msg_replica_index_flush_req *f_req =
		(struct s2s_msg_replica_index_flush_req *)((char *)request_header + sizeof(struct msg_header));
	f_req->primary_segment_offt = start_segment_offt;
	f_req->entry_size = wcursor_get_compaction_index_entry_size(wcursor);
	f_req->number_of_columns = wcursor_get_number_of_cols(wcursor);
	f_req->height = height;
	f_req->level_id = level_id;
	f_req->clock = clock;
	f_req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(f_req->region_key, region_desc_get_min_key(r_desc));
	/*related for the reply part*/

	f_req->mr_of_primary = *region_desc_get_primary_local_rdma_buffer(r_desc, level_id);
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

static void send_index_flush_L0(struct send_index_context *context, uint64_t small_log_tail_dev_offt,
				uint64_t big_log_tail_dev_offt)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		r_conn = sc_get_data_conn(server, region_desc_get_backup_hostname(r_desc, i),
					  region_desc_get_backup_IP(r_desc, i));
		uint32_t flush_L0_request_size = sizeof(struct s2s_msg_flush_L0_req);
		uint32_t flush_L0_reply_size = sizeof(struct s2s_msg_flush_L0_rep);

		//allocate msg
		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = flush_L0_request_size,
			.reply_size = flush_L0_reply_size,
			.request_type = FLUSH_L0_REQUEST
		};
		struct sc_msg_pair new_message_pair = send_index_allocate_msg_pair(send_index_allocation_info);
		region_desc_set_msg_pair(r_desc, new_message_pair, i, 0);
		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, 0);
		// struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, 0);
		// r_desc->rpc[i][0] = send_index_allocate_msg_pair(send_index_allocation_info);

		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][0];

		// send the msg
		msg_header *req_header = msg_pair->request;
		send_index_fill_flush_L0_request(req_header, r_desc, small_log_tail_dev_offt, big_log_tail_dev_offt);
		req_header->session_id = region_desc_get_uuid(r_desc);

		if (__send_rdma_message(r_conn, req_header, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][0];
		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, 0);
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
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		r_conn = sc_get_data_conn(server, region_desc_get_backup_hostname(r_desc, i),
					  region_desc_get_backup_IP(r_desc, i));

		uint32_t get_buffer_request_size = sizeof(struct s2s_msg_replica_index_get_buffer_req);
		uint32_t get_buffer_reply_size = sizeof(struct s2s_msg_replica_index_get_buffer_rep);

		log_debug("Sending get rdma buffer for region %s", region_desc_get_id(r_desc));
		// allocate msg
		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = get_buffer_request_size,
			.reply_size = get_buffer_reply_size,
			.request_type = REPLICA_INDEX_GET_BUFFER_REQ
		};
		struct sc_msg_pair new_msg_pair = send_index_allocate_msg_pair(send_index_allocation_info);
		region_desc_set_msg_pair(r_desc, new_msg_pair, i, level_id);
		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, level_id);
		// r_desc->rpc[i][level_id] = send_index_allocate_msg_pair(send_index_allocation_info);
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];

		// send the msg
		send_index_fill_get_rdma_buffer_request(msg_pair->request, r_desc, level_id, tree_id, new_level_cursor);
		send_index_fill_reply_fields(msg_pair, r_conn);
		msg_pair->request->session_id = region_desc_get_uuid(r_desc);

		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, level_id);
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, REPLICA_INDEX_GET_BUFFER_REQ);
		struct s2s_msg_replica_index_get_buffer_rep *get_buffer_reply =
			(struct s2s_msg_replica_index_get_buffer_rep *)((uint64_t)msg_pair->reply +
									sizeof(struct msg_header));

		region_desc_set_remote_buf(r_desc, i, get_buffer_reply->mr, level_id);
		// r_desc->remote_mem_buf[i][level_id] = get_buffer_reply->mr;
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
	struct connection_rdma *r_conn = sc_get_compaction_conn(context->server,
								region_desc_get_backup_hostname(context->r_desc, 0),
								region_desc_get_backup_IP(context->r_desc, 0));

	char *wcursor_segment_buffer_offt = wcursor_get_cursor_buffer(wcursor, 0, 0);
	uint32_t wcursor_segment_buffer_size = wcursor_get_segment_buffer_size(wcursor);

	struct ibv_mr *reg_mem =
		rdma_reg_write(r_conn->rdma_cm_id, wcursor_segment_buffer_offt, wcursor_segment_buffer_size);
	if (NULL == reg_mem) {
		log_fatal("rdma_reg_write failed");
		perror("Reason:");
		_exit(EXIT_FAILURE);
	}
	region_desc_set_primary_local_rdma_buffer(context->r_desc, level_id, reg_mem);
}

static void send_index_close_compaction(struct send_index_context *context, uint32_t level_id)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		r_conn = sc_get_data_conn(server, region_desc_get_backup_hostname(r_desc, i),
					  region_desc_get_backup_IP(r_desc, i));
		uint32_t send_index_close_compaction_request_size = sizeof(struct s2s_msg_close_compaction_request);
		uint32_t send_index_close_compaction_reply_size = sizeof(struct s2s_msg_close_compaction_reply);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = send_index_close_compaction_request_size,
			.reply_size = send_index_close_compaction_reply_size,
			.request_type = CLOSE_COMPACTION_REQUEST
		};

		region_desc_set_msg_pair(r_desc, send_index_allocate_msg_pair(send_index_allocation_info), i, level_id);
		// r_desc->rpc[i][level_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, level_id);
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		send_index_fill_close_compaction_request(msg_pair->request, r_desc, level_id);
		send_index_fill_reply_fields(msg_pair, r_conn);
		msg_pair->request->session_id = (uint64_t)region_desc_get_uuid(r_desc);

		// send the msg
		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, level_id);
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, CLOSE_COMPACTION_REQUEST);
		// free msg space
		sc_free_rpc_pair(msg_pair);
	}
}

static void send_index_replicate_index_segment(struct send_index_context *context,
					       struct wcursor_level_write_cursor *wcursor, uint32_t buf_size,
					       uint32_t level_id, uint32_t height, uint32_t clock)
{
	region_desc_t r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	int ret = 0;
	char *primary_buffer = wcursor_get_cursor_buffer(wcursor, height, clock);
	uint32_t number_of_columns = wcursor_get_number_of_cols(wcursor);
	uint32_t entry_size = wcursor_get_compaction_index_entry_size(wcursor);

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		uint32_t row_size_of_backup = entry_size * number_of_columns;
		struct ibv_mr remote_mem = region_desc_get_remote_buf(r_desc, backup_id, level_id);
		char *backup_buffer = (char *)remote_mem.addr + ((height * row_size_of_backup) + (clock * entry_size));
		//log_debug("Trying to write at remote offt %lu", (uint64_t)backup_buffer);
		struct connection_rdma *r_conn =
			sc_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id));
		while (1) {
			struct ibv_mr *local_mem = region_desc_get_primary_local_rdma_buffer(r_desc, level_id);

			ret = rdma_post_write(r_conn->rdma_cm_id, NULL, primary_buffer, buf_size, local_mem,
					      IBV_SEND_SIGNALED, (uint64_t)backup_buffer, remote_mem.rkey);
			if (!ret)
				break;
		}
	}
}

static void send_index_send_flush_index_segment(struct send_index_context *context, uint64_t start_segment_offt,
						struct wcursor_level_write_cursor *wcursor, uint32_t level_id,
						uint32_t clock, uint32_t height, bool is_last)
{
	region_desc_t r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		struct connection_rdma *r_conn =
			sc_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id));
		uint32_t request_size = sizeof(struct s2s_msg_replica_index_flush_req);
		uint32_t reply_size = sizeof(struct s2s_msg_replica_index_flush_rep);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = request_size,
			.reply_size = reply_size,
			.request_type = REPLICA_INDEX_FLUSH_REQ,
		};
		struct sc_msg_pair msg_pair = send_index_allocate_msg_pair(send_index_allocation_info);
		region_desc_set_flush_index_segment_msg_pair(r_desc, backup_id, level_id, clock, msg_pair);
		msg_pair = region_desc_get_flush_index_segment_msg_pair(r_desc, backup_id, level_id, clock);

		send_index_fill_flush_index_segment(msg_pair.request, r_desc, wcursor, height, level_id, clock,
						    backup_id, is_last);
		send_index_fill_reply_fields(&msg_pair, r_conn);
		msg_pair.request->session_id = (uint64_t)region_desc_get_uuid(r_desc);

		//send the msg
		if (__send_rdma_message(r_conn, msg_pair.request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}

		// if is_last is true then we are in the flushing stage of the wcursor compaction, and we spin for the last segments
		// to be written in the replicas
		if (is_last)
			wcursor_spin_for_buffer_status(wcursor);
	}
}

static void send_index_send_segment(struct send_index_context *context, uint64_t start_segment_offt,
				    struct wcursor_level_write_cursor *wcursor, uint32_t buf_size, uint32_t height,
				    uint32_t level_id, uint32_t clock, bool is_last)
{
	/* rdma_write index segment to replicas */
	send_index_replicate_index_segment(context, wcursor, buf_size, level_id, height, clock);
	/* send control msg to flush the index segment to replicas */
	send_index_send_flush_index_segment(context, start_segment_offt, wcursor, level_id, clock, height, is_last);
}

static void send_index_swap_levels(struct send_index_context *context, uint32_t level_id)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;
	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		r_conn = sc_get_data_conn(server, region_desc_get_backup_hostname(r_desc, i),
					  region_desc_get_backup_IP(r_desc, i));
		uint32_t send_index_swap_levels_request_size = sizeof(struct s2s_msg_swap_levels_request);
		uint32_t send_index_swap_levels_reply_size = sizeof(struct s2s_msg_swap_levels_reply);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = send_index_swap_levels_request_size,
			.reply_size = send_index_swap_levels_reply_size,
			.request_type = REPLICA_INDEX_SWAP_LEVELS_REQUEST
		};
		region_desc_set_msg_pair(r_desc, send_index_allocate_msg_pair(send_index_allocation_info), i, level_id);
		// r_desc->rpc[i][level_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, level_id);
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		send_index_fill_swap_levels_request(msg_pair->request, r_desc, level_id);
		msg_pair->request->session_id = (uint64_t)region_desc_get_uuid(r_desc);

		// send the msg
		if (__send_rdma_message(r_conn, msg_pair->request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t i = 0; i < region_desc_get_num_backup(r_desc); ++i) {
		struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, level_id);
		// struct sc_msg_pair *msg_pair = &r_desc->rpc[i][level_id];
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, REPLICA_INDEX_SWAP_LEVELS_REQUEST);
		// free msg space
		sc_free_rpc_pair(msg_pair);
	}
}

void send_index_compaction_started_callback(void *context, uint64_t small_log_tail_dev_offt,
					    uint64_t big_log_tail_dev_offt, uint32_t src_level_id, uint8_t dst_tree_id,
					    struct wcursor_level_write_cursor *new_level)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	//compaction from L0 to L1
	if (!src_level_id)
		send_index_flush_L0(send_index_cxt, small_log_tail_dev_offt, big_log_tail_dev_offt);

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

void send_index_compaction_wcursor_flush_segment_callback(void *context, uint64_t start_segment_offt,
							  struct wcursor_level_write_cursor *wcursor, uint32_t level_id,
							  uint32_t height, uint32_t buf_size, uint32_t clock,
							  bool is_last)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;

	send_index_send_segment(send_index_cxt, start_segment_offt, wcursor, buf_size, height, level_id, clock,
				is_last);
}

void send_index_compaction_wcursor_got_flush_replies(void *context, uint32_t level_id, uint32_t clock)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	//log_debug("i got all the replies for level %u", level_id);
	region_desc_t r_desc = send_index_cxt->r_desc;
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id)
		region_desc_free_flush_index_segment_msg_pair(r_desc, backup_id, level_id, clock);
}

void send_index_init_callbacks(struct regs_server_desc *server, struct region_desc *r_desc)
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

	parallax_init_callbacks(region_desc_get_db(r_desc), &send_index_callback_functions, cxt);
}
