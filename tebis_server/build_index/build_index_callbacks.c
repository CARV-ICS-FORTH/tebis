#include "build_index_callbacks.h"
#include "../../common/common.h"
#include "../conf.h"
#include "../messages.h"
#include "../region_desc.h"
#include "../region_server.h"
#include "../send_index/send_index_uuid_checker/send_index_uuid_checker.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include <assert.h>
#include <log.h>
#include <stdlib.h>
#include <unistd.h>

struct build_index_allocate_msg_pair_info {
	struct connection_rdma *conn;
	uint32_t request_size;
	uint32_t reply_size;
	enum message_type request_type;
};
static struct sc_msg_pair
build_index_allocate_msg_pair(struct build_index_allocate_msg_pair_info build_index_allocation_info)
{
	struct sc_msg_pair msg_pair =
		sc_allocate_rpc_pair(build_index_allocation_info.conn, build_index_allocation_info.request_size,
				     build_index_allocation_info.reply_size, build_index_allocation_info.request_type);
	while (msg_pair.stat != ALLOCATION_IS_SUCCESSFULL) {
		msg_pair = sc_allocate_rpc_pair(build_index_allocation_info.conn,
						build_index_allocation_info.request_size,
						build_index_allocation_info.reply_size,
						build_index_allocation_info.request_type);
	}
	return msg_pair;
}

static void build_index_fill_compact_L0(msg_header *request_header, region_desc_t r_desc)
{
	assert(request_header);
	struct s2s_msg_compact_L0_request *request =
		(struct s2s_msg_compact_L0_request *)((char *)request_header + sizeof(struct msg_header));
	request->region_key_size = region_desc_get_min_key_size(r_desc);
	strcpy(request->region_key, region_desc_get_min_key(r_desc));
	request->uuid = (uint64_t)request;
}

static void send_flush_L0_compaction(struct build_index_context *context)
{
	struct region_desc *r_desc = context->r_desc;
	struct regs_server_desc *server = context->server;
	struct connection_rdma *r_conn = NULL;
	struct sc_msg_pair compact_L0_cmd[KRM_MAX_BACKUPS] = { 0 };

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		uint32_t compact_L0_request_size = sizeof(struct s2s_msg_compact_L0_request);
		uint32_t compact_L0_reply_size = sizeof(struct s2s_msg_compact_L0_reply);

		//allocate msg
		struct build_index_allocate_msg_pair_info build_index_allocation_info = {
			.conn = r_conn,
			.request_size = compact_L0_request_size,
			.reply_size = compact_L0_reply_size,
			.request_type = BUILD_INDEX_COMPACT_L0_REQUEST
		};
		compact_L0_cmd[backup_id] = build_index_allocate_msg_pair(build_index_allocation_info);
		build_index_fill_compact_L0(compact_L0_cmd[backup_id].request, r_desc);
		compact_L0_cmd[backup_id].request->session_id = (uint64_t)region_desc_get_uuid(r_desc);
		log_debug("Sending compact L0 request to replicas");

		if (__send_rdma_message(r_conn, compact_L0_cmd[backup_id].request, NULL) != TEBIS_SUCCESS) {
			log_fatal("failed to send message");
			_exit(EXIT_FAILURE);
		}
	}

	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(r_desc); ++backup_id) {
		r_conn = regs_get_data_conn(server, region_desc_get_backup_hostname(r_desc, backup_id),
					    region_desc_get_backup_IP(r_desc, backup_id));
		// spin for the reply
		teb_spin_for_message_reply(compact_L0_cmd[backup_id].request, r_conn);
		// for debuging purposes
		send_index_uuid_checker_validate_uuid(&compact_L0_cmd[backup_id], BUILD_INDEX_COMPACT_L0_REQUEST);
		// free msg space
		sc_free_rpc_pair(&compact_L0_cmd[backup_id]);
	}
}

void build_index_L0_compaction_started_cb(void *context)
{
	assert(context);
	struct build_index_context *build_index_cxt = (struct build_index_context *)context;

	send_flush_L0_compaction(build_index_cxt);
}

void build_index_init_callbacks(struct regs_server_desc *server, struct region_desc *r_desc)
{
	assert(server && r_desc);
	struct build_index_context *build_index_cxt =
		(struct build_index_context *)calloc(1, sizeof(struct build_index_context));
	build_index_cxt->r_desc = r_desc;
	build_index_cxt->server = server;

	struct parallax_callback_funcs build_index_callback_function = { 0 };
	build_index_callback_function.build_index_L0_compaction_started_cb = build_index_L0_compaction_started_cb;

	parallax_init_callbacks(region_desc_get_db(r_desc), &build_index_callback_function, (void *)build_index_cxt);
}
