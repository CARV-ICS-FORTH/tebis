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
#include "send_index_callbacks.h"
#include "../../common/common.h"
#include "../conf.h"
#include "../configurables.h"
#include "../region_desc.h"
#include "../region_server.h"
#include "../tebis_rdma/rdma.h"
#include "../tebis_server/messages.h"
#include "../tebis_server/server_communication.h"
#include "../utilities/circular_buffer.h"
#include "allocator/log_structures.h"
#include "parallax/structures.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include "send_index_uuid_checker/send_index_uuid_checker.h"
#include <assert.h>
#include <btree/level_write_cursor.h>
#include <infiniband/verbs.h>
#include <log.h>
#include <rdma/rdma_verbs.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
	struct ru_master_log_buffer *rdma_buff = region_desc_get_primary_L0_log_buf(r_desc);
	req->small_rdma_buffer_curr_end = rdma_buff->remote_buffers.curr_end;

	rdma_buff = region_desc_get_primary_big_log_buf(r_desc);
	req->big_rdma_buffer_curr_end = rdma_buff->remote_buffers.curr_end;
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
						     uint32_t level_id, uint64_t compaction_first_segment_offt,
						     uint64_t compaction_last_segment_offt, uint64_t new_root_offt)
{
	struct s2s_msg_close_compaction_request *req =
		(struct s2s_msg_close_compaction_request *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->level_id = level_id;
	req->compaction_first_segment_offt = compaction_first_segment_offt;
	req->compaction_last_segment_offt = compaction_last_segment_offt;
	req->new_root_offt = new_root_offt;
	req->uuid = (uint64_t)req;
}

static void send_index_fill_swap_levels_request(msg_header *request_header, struct region_desc *r_desc,
						uint32_t level_id, uint32_t src_tree_id)
{
	struct s2s_msg_swap_levels_request *req =
		(struct s2s_msg_swap_levels_request *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->level_id = level_id;
	req->src_tree_id = src_tree_id;
	req->uuid = (uint64_t)req;
}

static void send_index_fill_flush_index_segment(msg_header *request_header, region_desc_t r_desc,
						uint64_t start_segment_offt, struct wcursor_level_write_cursor *wcursor,
						uint32_t height, uint32_t dst_level_id, uint32_t clock,
						uint32_t replica_id, bool is_last_segment)
{
	(void)is_last_segment;
	struct s2s_msg_replica_index_flush_req *f_req =
		(struct s2s_msg_replica_index_flush_req *)((char *)request_header + sizeof(struct msg_header));
	f_req->primary_segment_offt = start_segment_offt;
	f_req->entry_size = wcursor_get_compaction_index_entry_size(wcursor);
	f_req->number_of_columns = wcursor_get_number_of_cols(wcursor);
	f_req->height = height;
	f_req->level_id = dst_level_id;
	f_req->clock = clock;
	f_req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(f_req->region_key, region_desc_get_min_key(r_desc));
	/*related for the reply part*/

	uint32_t src_level_id = dst_level_id - 1;
	f_req->mr_of_primary = *region_desc_get_primary_local_rdma_buffer(r_desc, src_level_id);
	f_req->reply_size = wcursor_segment_buffer_status_size(wcursor);
	f_req->reply_offt = wcursor_segment_buffer_get_status_addr(wcursor, height, clock, replica_id);
}

static void send_index_fill_flush_medium_log_segment(msg_header *request_header, region_desc_t r_desc,
						     uint64_t segment_offt, uint64_t IO_starting_offt, uint32_t IO_size,
						     uint32_t chunk_id)
{
	assert(request_header);
	struct s2s_msg_replica_flush_medium_log_req *req =
		(struct s2s_msg_replica_flush_medium_log_req *)((char *)request_header + sizeof(struct msg_header));
	req->primary_segment_offt = segment_offt;
	req->chunk_id = chunk_id;
	req->IO_starting_offt = IO_starting_offt;
	req->IO_size = IO_size;
	req->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(req->region_key, region_desc_get_min_key(r_desc));
	req->uuid = (uint64_t)req;
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

// TODO (geostyl): there is a similar struct/function for build index, move this functionality to the common dir
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

/**
 * @brief Primary uses this function to command backups to flush L0
 */
static void send_index_flush_L0(struct send_index_context *context, uint64_t small_log_tail_dev_offt,
				uint64_t big_log_tail_dev_offt)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	struct sc_msg_pair flush_L0_command[KRM_MAX_BACKUPS] = { 0 };
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		uint32_t flush_L0_request_size = sizeof(struct s2s_msg_flush_L0_req);
		uint32_t flush_L0_reply_size = sizeof(struct s2s_msg_flush_L0_rep);

		//allocate msg
		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = flush_L0_request_size,
			.reply_size = flush_L0_reply_size,
			.request_type = FLUSH_L0_REQUEST
		};
		flush_L0_command[backup_id] = send_index_allocate_msg_pair(send_index_allocation_info);
		// region_desc_set_msg_pair(r_desc, new_message_pair, i, 0);
		// struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, i, 0);

		// send the msg
		struct msg_header *req_header = flush_L0_command[backup_id].request;
		send_index_fill_flush_L0_request(req_header, r_desc, small_log_tail_dev_offt, big_log_tail_dev_offt);
		req_header->session_id = region_desc_get_uuid(r_desc);

		if (__send_rdma_message(r_conn, req_header, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		assert(flush_L0_command[backup_id].reply != NULL);
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		// spin for reply, are you sure?
		teb_spin_for_message_reply(flush_L0_command[backup_id].request, r_conn);
		// for debugging purposes
		send_index_uuid_checker_validate_uuid(&flush_L0_command[backup_id], FLUSH_L0_REQUEST);
		// free msg space
		sc_free_rpc_pair(&flush_L0_command[backup_id]);
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

	struct sc_msg_pair allocate_buffer_cmd[KRM_MAX_BACKUPS] = { 0 };
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));

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
		allocate_buffer_cmd[backup_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		// send the msg
		uint32_t dst_level_id = level_id + 1;
		send_index_fill_get_rdma_buffer_request(allocate_buffer_cmd[backup_id].request, r_desc, dst_level_id,
							tree_id, new_level_cursor);
		send_index_fill_reply_fields(&allocate_buffer_cmd[backup_id], r_conn);
		allocate_buffer_cmd[backup_id].request->session_id = region_desc_get_uuid(r_desc);

		if (__send_rdma_message(r_conn, allocate_buffer_cmd[backup_id].request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		// spin for the reply
		teb_spin_for_message_reply(allocate_buffer_cmd[backup_id].request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(&allocate_buffer_cmd[backup_id], REPLICA_INDEX_GET_BUFFER_REQ);
		struct s2s_msg_replica_index_get_buffer_rep *get_buffer_reply =
			(struct s2s_msg_replica_index_get_buffer_rep *)((uint64_t)allocate_buffer_cmd[backup_id].reply +
									sizeof(struct msg_header));

		region_desc_set_remote_buf(r_desc, backup_id, get_buffer_reply->mr, level_id);
		log_debug("Newly registered memory region is at %lu", (uint64_t)get_buffer_reply->mr.addr);
		// free msg space
		sc_free_rpc_pair(&allocate_buffer_cmd[backup_id]);
	}
}

static void send_index_reg_write_primary_compaction_buffer(struct send_index_context *context,
							   struct wcursor_level_write_cursor *wcursor,
							   uint32_t level_id)
{
	assert(context && wcursor);

	// TODO: develop a better way to find the protection domain
	struct connection_rdma *r_conn = regs_get_compaction_conn(context->server,
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

static void send_index_close_compaction(struct send_index_context *context, uint32_t level_id,
					uint64_t compaction_first_segment_offt, uint64_t compaction_last_segment_offt,
					uint64_t new_root_offt)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;
	struct sc_msg_pair close_compaction_cmd[KRM_MAX_BACKUPS] = { 0 };

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		uint32_t send_index_close_compaction_request_size = sizeof(struct s2s_msg_close_compaction_request);
		uint32_t send_index_close_compaction_reply_size = sizeof(struct s2s_msg_close_compaction_reply);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = send_index_close_compaction_request_size,
			.reply_size = send_index_close_compaction_reply_size,
			.request_type = CLOSE_COMPACTION_REQUEST
		};

		close_compaction_cmd[backup_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		// struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, backup_id, level_id);
		uint32_t dst_level_id = level_id + 1;
		send_index_fill_close_compaction_request(close_compaction_cmd[backup_id].request, r_desc, dst_level_id,
							 compaction_first_segment_offt, compaction_last_segment_offt,
							 new_root_offt);
		send_index_fill_reply_fields(&close_compaction_cmd[backup_id], r_conn);
		close_compaction_cmd[backup_id].request->session_id = (uint64_t)region_desc_get_uuid(r_desc);

		// send the msg
		if (__send_rdma_message(r_conn, close_compaction_cmd[backup_id].request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		// spin for the reply
		teb_spin_for_message_reply(close_compaction_cmd[backup_id].request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(&close_compaction_cmd[backup_id], CLOSE_COMPACTION_REQUEST);
		// free msg space
		sc_free_rpc_pair(&close_compaction_cmd[backup_id]);
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
		struct connection_rdma *r_conn = regs_get_data_conn(server,
								    region_desc_get_backup_hostname(r_desc, backup_id),
								    region_desc_get_backup_IP(r_desc, backup_id));
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
		struct connection_rdma *r_conn = regs_get_data_conn(server,
								    region_desc_get_backup_hostname(r_desc, backup_id),
								    region_desc_get_backup_IP(r_desc, backup_id));
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

		uint32_t dst_level_id = level_id + 1;
		send_index_fill_flush_index_segment(msg_pair.request, r_desc, start_segment_offt, wcursor, height,
						    dst_level_id, clock, backup_id, is_last);
		send_index_fill_reply_fields(&msg_pair, r_conn);
		msg_pair.request->session_id = (uint64_t)region_desc_get_uuid(r_desc);
		log_debug("Sending flush index segment message");
		//send the msg
		if (__send_rdma_message(r_conn, msg_pair.request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	// if is_last is true then we are in the flushing stage of the wcursor compaction, and we spin for the last segments
	// to be written in the replicas
	if (is_last)
		wcursor_spin_for_buffer_status(wcursor);
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

static void send_index_swap_levels(struct send_index_context *context, uint32_t level_id, uint32_t src_tree_id)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	struct connection_rdma *r_conn = NULL;

	struct sc_msg_pair swap_level_cmd[KRM_MAX_BACKUPS] = { 0 };
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		uint32_t send_index_swap_levels_request_size = sizeof(struct s2s_msg_swap_levels_request);
		uint32_t send_index_swap_levels_reply_size = sizeof(struct s2s_msg_swap_levels_reply);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = send_index_swap_levels_request_size,
			.reply_size = send_index_swap_levels_reply_size,
			.request_type = REPLICA_INDEX_SWAP_LEVELS_REQUEST
		};
		// region_desc_set_msg_pair(r_desc, send_index_allocate_msg_pair(send_index_allocation_info), i, level_id);
		swap_level_cmd[backup_id] = send_index_allocate_msg_pair(send_index_allocation_info);

		// struct sc_msg_pair *msg_pair = region_desc_get_msg_pair(r_desc, backup_id, level_id);
		uint32_t dst_level_id = level_id + 1;
		send_index_fill_swap_levels_request(swap_level_cmd[backup_id].request, r_desc, dst_level_id,
						    src_tree_id);
		swap_level_cmd[backup_id].request->session_id = (uint64_t)region_desc_get_uuid(r_desc);

		// send the msg
		if (__send_rdma_message(r_conn, swap_level_cmd[backup_id].request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		// spin for the reply
		teb_spin_for_message_reply(swap_level_cmd[backup_id].request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(&swap_level_cmd[backup_id], REPLICA_INDEX_SWAP_LEVELS_REQUEST);
		// free msg space
		sc_free_rpc_pair(&swap_level_cmd[backup_id]);
	}
}

void send_index_compaction_started_callback(void *context, uint64_t small_log_tail_dev_offt,
					    uint64_t big_log_tail_dev_offt, uint32_t src_level_id, uint8_t src_tree_id,
					    struct wcursor_level_write_cursor *new_level)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	//compaction from L0 to L1
	if (!src_level_id) {
		send_index_flush_L0(send_index_cxt, small_log_tail_dev_offt, big_log_tail_dev_offt);
		struct ru_master_log_buffer *rdma_buff = region_desc_get_primary_L0_log_buf(send_index_cxt->r_desc);
		rdma_buff->remote_buffers.flushed_until_offt = rdma_buff->remote_buffers.curr_end;
		rdma_buff = region_desc_get_primary_big_log_buf(send_index_cxt->r_desc);
		rdma_buff->remote_buffers.flushed_until_offt = rdma_buff->remote_buffers.curr_end;
	}

	send_index_allocate_rdma_buffer_in_replicas(send_index_cxt, src_level_id, src_tree_id, new_level);
	send_index_reg_write_primary_compaction_buffer(send_index_cxt, new_level, src_level_id);
}

void send_index_compaction_ended_callback(void *context, uint32_t src_level_id, uint64_t compaction_first_segment_offt,
					  uint64_t compaction_last_segment_offt, uint64_t new_root_offt)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;

	send_index_close_compaction(send_index_cxt, src_level_id, compaction_first_segment_offt,
				    compaction_last_segment_offt, new_root_offt);
}

void send_index_swap_levels_callback(void *context, uint32_t src_level_id, uint32_t src_tree_id)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	send_index_swap_levels(send_index_cxt, src_level_id, src_tree_id);
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

void send_index_replicate_medium_log_chunk(struct send_index_context *context, uint64_t IO_starting_offt,
					   uint32_t IO_size, uint32_t tail_id)
{
	region_desc_t r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		struct ru_master_log_buffer *remote_medium_log = region_desc_get_primary_medium_log_buf(r_desc);
		struct ibv_mr remote_mr = remote_medium_log->remote_buffers.mr[backup_id];
		struct connection_rdma *r_conn = regs_get_data_conn(server,
								    region_desc_get_backup_hostname(r_desc, backup_id),
								    region_desc_get_backup_IP(r_desc, backup_id));
		while (1) {
			struct ibv_mr *local_mem = region_desc_get_medium_log_buffer(r_desc, tail_id);
			char *inmem_medium_segment = (char *)local_mem->addr;
			int ret = rdma_post_write(r_conn->rdma_cm_id, NULL, &inmem_medium_segment[IO_starting_offt],
						  IO_size, local_mem, IBV_SEND_SIGNALED, (uint64_t)remote_mr.addr,
						  remote_mr.rkey);
			if (!ret)
				break;
		}
	}
}

void send_index_send_flush_medium_log_command(struct send_index_context *context, uint64_t segment_offt,
					      uint64_t IO_starting_offt, uint32_t IO_size, uint32_t chunk_id,
					      uint32_t tail_id)
{
	region_desc_t r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	assert(r_desc && server);

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		struct connection_rdma *r_conn = regs_get_data_conn(server,
								    region_desc_get_backup_hostname(r_desc, backup_id),
								    region_desc_get_backup_IP(r_desc, backup_id));
		uint32_t request_size = sizeof(struct s2s_msg_replica_flush_medium_log_req);
		uint32_t reply_size = sizeof(struct s2s_msg_replica_flush_medium_log_rep);

		struct send_index_allocate_msg_pair_info send_index_allocation_info = {
			.conn = r_conn,
			.request_size = request_size,
			.reply_size = reply_size,
			.request_type = REPLICA_FLUSH_MEDIUM_LOG_REQUEST,
		};
		struct sc_msg_pair msg_pair = send_index_allocate_msg_pair(send_index_allocation_info);
		region_desc_set_flush_medium_log_segment_msg_pair(r_desc, backup_id, tail_id, msg_pair);
		send_index_fill_flush_medium_log_segment(msg_pair.request, r_desc, segment_offt, IO_starting_offt,
							 IO_size, chunk_id);
		send_index_fill_reply_fields(&msg_pair, r_conn);
		msg_pair.request->session_id = (uint64_t)region_desc_get_uuid(r_desc);
		log_debug("Sending flush medium log segment msg bid %u tailid %u", backup_id, tail_id);

		//send the msg
		if (__send_rdma_message(r_conn, msg_pair.request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}
}

void send_index_spin_for_medium_log_replies(void *context, uint32_t tail_id)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	struct region_desc *r_desc = send_index_cxt->r_desc;
	struct regs_server_desc *server = send_index_cxt->server;
	assert(r_desc && server);

	struct connection_rdma *r_conn = NULL;
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		struct sc_msg_pair *msg_pair =
			region_desc_get_flush_medium_log_segment_msg_pair(r_desc, backup_id, tail_id);
		// spin for the reply
		teb_spin_for_message_reply(msg_pair->request, r_conn);
		log_debug("WTF %u %u", backup_id, tail_id);
		//assert(0);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(msg_pair, REPLICA_FLUSH_MEDIUM_LOG_REQUEST);
		// free msg space
		region_desc_free_flush_medium_log_segment_msg_pair(r_desc, backup_id, tail_id);
	}
}

void send_index_segment_is_full(void *context, uint64_t segment_offt, uint64_t IO_starting_offt, uint32_t IO_size,
				uint32_t chunk_id, uint32_t tail_id)
{
	assert(context);
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;

	send_index_replicate_medium_log_chunk(send_index_cxt, IO_starting_offt, IO_size, tail_id);

	send_index_send_flush_medium_log_command(send_index_cxt, segment_offt, IO_starting_offt, IO_size, chunk_id,
						 tail_id);
}

void send_index_init_callbacks(struct regs_server_desc *server, region_desc_t r_desc)
{
	struct send_index_context *send_index_cxt =
		(struct send_index_context *)calloc(1, sizeof(struct send_index_context));
	send_index_cxt->r_desc = r_desc;
	send_index_cxt->server = server;
	void *cxt = (void *)send_index_cxt;

	struct parallax_callback_funcs send_index_callback_functions = { 0 };
	send_index_callback_functions.segment_is_full_cb = send_index_segment_is_full;
	send_index_callback_functions.spin_for_medium_log_flush = send_index_spin_for_medium_log_replies;
	send_index_callback_functions.compaction_started_cb = send_index_compaction_started_callback;
	send_index_callback_functions.compaction_ended_cb = send_index_compaction_ended_callback;
	send_index_callback_functions.swap_levels_cb = send_index_swap_levels_callback;
	send_index_callback_functions.comp_write_cursor_flush_segment_cb =
		send_index_compaction_wcursor_flush_segment_callback;
	send_index_callback_functions.comp_write_cursor_got_flush_replies_cb =
		send_index_compaction_wcursor_got_flush_replies;

	parallax_init_callbacks(region_desc_get_db(r_desc), &send_index_callback_functions, cxt);
}
