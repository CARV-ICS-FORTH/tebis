#include "send_index.h"
#include "btree/btree.h"
#include "btree/conf.h"
#include "btree/index_node.h"
#include "btree/level_write_appender.h"
#include "btree/level_write_cursor.h"
#include "send_index_callbacks.h"
#include <log.h>
#include <rdma/rdma_verbs.h>
#include <stdlib.h>

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
	// acquire a transacation ID (if level > 0) for parallax and initialize a level write appender for the upcoming compaction */
	par_init_compaction_id(params.r_desc->db, dst_level_id, params.tree_id);
	params.r_desc->r_state->wappender[dst_level_id] =
		wappender_init((struct db_handle *)params.r_desc->db, params.tree_id, dst_level_id);
	// assign index buffer aswell
	char *backup_segment_index = NULL;
	uint32_t backup_segment_index_size = params.number_of_rows * params.number_of_columns * params.size_of_entry;

	if (posix_memalign((void **)&backup_segment_index, 4096, backup_segment_index_size)) {
		log_fatal("Failed to allocate a index buffer for the backup");
		_exit(EXIT_FAILURE);
	}

	params.r_desc->r_state->index_buffer[dst_level_id] =
		rdma_reg_write(params.conn->rdma_cm_id, backup_segment_index, backup_segment_index_size);
	if (params.r_desc->r_state->index_buffer[dst_level_id] == NULL) {
		log_fatal("Failed to reg memory");
		_exit(EXIT_FAILURE);
	}
}

void send_index_create_mr_for_segment_replies(struct send_index_create_mr_for_segment_replies_params params)
{
	uint32_t dst_level_id = params.level_id + 1;
	if (params.r_desc->r_state->index_segment_flush_replies[dst_level_id] != NULL) {
		log_fatal("The segment flush reply memory region is in use when a compaction started for level %u",
			  dst_level_id);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	char *backup_segment_flush_replies_mr = NULL;
	uint32_t backup_segment_flush_replies_mr_size = WCURSOR_ALIGNMNENT;

	if (posix_memalign((void **)&backup_segment_flush_replies_mr, WCURSOR_ALIGNMNENT,
			   backup_segment_flush_replies_mr_size)) {
		log_fatal("Faield to allocate a 4k reply mr");
		assert(0);
		_exit(EXIT_FAILURE);
	}
	params.r_desc->r_state->index_segment_flush_replies[dst_level_id] = rdma_reg_write(
		params.conn->rdma_cm_id, backup_segment_flush_replies_mr, backup_segment_flush_replies_mr_size);
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
	wappender_close(r_desc->r_state->wappender[dst_level_id]);
	r_desc->r_state->wappender[dst_level_id] = NULL;
}

void send_index_close_mr_for_segment_replies(struct krm_region_desc *r_desc, uint32_t level_id)
{
	uint32_t dst_level_id = level_id + 1;
	if (rdma_dereg_mr(r_desc->r_state->index_segment_flush_replies[dst_level_id])) {
		log_fatal("Failed to deregister rdma buffer");
		_exit(EXIT_FAILURE);
	}
	r_desc->r_state->index_segment_flush_replies[dst_level_id] = NULL;
	free(r_desc->r_state->index_segment_flush_replies[dst_level_id]);
}
