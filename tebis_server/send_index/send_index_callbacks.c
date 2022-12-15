#include "send_index_callbacks.h"
#include "../../common/common.h"
#include "../metadata.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include <log.h>
#include <stdint.h>

static void send_index_fill_flush_L0_request(msg_header *request_header, struct krm_region_desc *r_desc)
{
	struct s2s_msg_flush_L0_req *req =
		(struct s2s_msg_flush_L0_req *)((char *)request_header + sizeof(struct msg_header));
	req->region_key_size = r_desc->region->min_key_size;
	strcpy(req->region_key, r_desc->region->min_key);
	req->uuid = (uint64_t)req;
}

static void send_index_flush_L0(struct send_index_context *context)
{
	struct krm_region_desc *r_desc = context->r_desc;
	struct krm_server_desc *server = context->server;
	assert(r_desc && server);

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct connection_rdma *r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
		uint32_t flush_L0_request_size = sizeof(struct s2s_msg_flush_L0_req);
		uint32_t flush_L0_reply_size = sizeof(struct s2s_msg_flush_L0_rep);

		// allocate the flush L0 msg, retry until success
		struct sc_msg_pair msg_pair =
			sc_allocate_rpc_pair(r_conn, flush_L0_request_size, flush_L0_reply_size, FLUSH_L0_REQUEST);
		while (msg_pair.stat != ALLOCATION_IS_SUCCESSFULL) {
			msg_pair = sc_allocate_rpc_pair(r_conn, flush_L0_request_size, flush_L0_reply_size,
							FLUSH_L0_REQUEST);
		}

		msg_header *req_header = msg_pair.request;
		send_index_fill_flush_L0_request(req_header, r_desc);
		req_header->session_id = (uint64_t)r_desc->region;

		// send the msg
		if (__send_rdma_message(r_conn, req_header, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}

		// spin for reply
		teb_spin_for_message_reply(req_header, r_conn);

		// for debugging purposes
		struct s2s_msg_flush_L0_req *flush_req_L0_payload =
			(struct s2s_msg_flush_L0_req *)((char *)msg_pair.request + sizeof(struct msg_header));
		struct s2s_msg_flush_L0_rep *flush_rep_L0_payload =
			(struct s2s_msg_flush_L0_rep *)((char *)msg_pair.reply + sizeof(struct msg_header));

		if (flush_req_L0_payload->uuid != flush_rep_L0_payload->uuid) {
			assert(0);
			log_fatal("Mismatch in piggybacked uuid");
			_exit(EXIT_FAILURE);
		}

		// free msg space
		sc_free_rpc_pair(&msg_pair);
	}
}

void send_index_compaction_started_callback(void *context, uint32_t src_level_id)
{
	struct send_index_context *send_index_cxt = (struct send_index_context *)context;
	//compaction from L0 to L1
	if (src_level_id)
		send_index_flush_L0(send_index_cxt);

	//allocate RDMA buffers in replicas
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

	parallax_init_callbacks(r_desc->db, &send_index_callback_functions, cxt);
}
