#include "send_index.h"
#include <log.h>

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
