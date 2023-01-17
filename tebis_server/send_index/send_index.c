#include "send_index.h"
#include "btree/btree.h"
#include "btree/index_node.h"
#include "btree/level_write_cursor.h"
#include "send_index_callbacks.h"
#include <log.h>
#include <rdma/rdma_verbs.h>

uint64_t send_index_flush_rdma_buffer(struct krm_region_desc *r_desc, enum log_category log_type)
{
	uint32_t rdma_buffer_size = r_desc->r_state->l0_recovery_rdma_buf.rdma_buf_size;
	char *rdma_buffer = (char *)r_desc->r_state->l0_recovery_rdma_buf.mr->addr;
	if (log_type == BIG)
		rdma_buffer = (char *)r_desc->r_state->big_recovery_rdma_buf.mr->addr;

	const char *error_message = NULL;
	/*persist the buffer*/
	uint64_t replica_new_segment_offt =
		par_flush_segment_in_log(r_desc->db, rdma_buffer, rdma_buffer_size, log_type, &error_message);
	if (error_message) {
		log_fatal("the flushing of the segment failed");
		_exit(EXIT_FAILURE);
	}

	return replica_new_segment_offt;
}

void send_index_create_compactions_rdma_buffer(struct send_index_create_compactions_rdma_buffer_params params)
{
	uint32_t dst_level_id = params.level_id + 1;
	if (params.r_desc->r_state->index_buffer[dst_level_id] != NULL) {
		log_fatal("Remote compaction for regions %s still pending", params.r_desc->region->id);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	// acquire a transacation ID (if level > 0) for parallax and initialize a write_cursor for the upcoming compaction */
	par_init_compaction_id(params.r_desc->db, dst_level_id, params.tree_id);
	params.r_desc->r_state->write_cursor_segments[dst_level_id] =
		wcursor_init_write_cursor(dst_level_id, (struct db_handle *)params.r_desc->db, params.tree_id, true);
	// assign index buffer aswell
	char *wcursor_segment_buffer_offt =
		wcursor_get_segment_buffer_offt(params.r_desc->r_state->write_cursor_segments[dst_level_id]);
	uint32_t wcursor_segment_buffer_size =
		wcursor_get_segment_buffer_size(params.r_desc->r_state->write_cursor_segments[dst_level_id]);

	params.r_desc->r_state->index_buffer[dst_level_id] =
		rdma_reg_write(params.conn->rdma_cm_id, wcursor_segment_buffer_offt, wcursor_segment_buffer_size);
}

void send_index_close_compactions_rdma_buffer(struct krm_region_desc *r_desc, uint32_t level_id)
{
	uint32_t dst_level_id = level_id + 1;
	//free index buffer that was allocated for sending the index for the specific level
	if (rdma_dereg_mr(r_desc->r_state->index_buffer[dst_level_id])) {
		log_fatal("Failed to deregister rdma buffer");
		_exit(EXIT_FAILURE);
	}
	r_desc->r_state->index_buffer[dst_level_id] = NULL;
	wcursor_close_write_cursor(r_desc->r_state->write_cursor_segments[dst_level_id]);
	r_desc->r_state->write_cursor_segments[dst_level_id] = NULL;
}
