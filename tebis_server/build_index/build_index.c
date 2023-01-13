#include "build_index.h"
#include "../rdma_buffer_iterator/rdma_buffer_iterator.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include "log.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include <stdlib.h>
#include <unistd.h>

static void insert_kv(rdma_buffer_iterator_t iterator, struct krm_region_desc *r_desc)
{
	par_handle *handle = r_desc->db;
	const char *error_message = NULL;
	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iterator);
	par_put_serialized(handle, (char *)kv, &error_message);
	if (error_message) {
		log_fatal("Error uppon inserting key %s, key_size %u", kv_splice_get_key_offset_in_kv(kv),
			  kv_splice_get_key_size(kv));
		_exit(EXIT_FAILURE);
	}
}

void build_index(struct build_index_task *task)
{
	rdma_buffer_iterator_t rdma_buf_iterator =
		rdma_buffer_iterator_init(task->rdma_buffer, task->rdma_buffers_size);

	while (rdma_buffer_iterator_is_valid(rdma_buf_iterator) == VALID) {
		insert_kv(rdma_buf_iterator, task->r_desc);
		rdma_buffer_iterator_next(rdma_buf_iterator);
	}
}

/**
 * Build index logic
 * parse the overflown RDMA buffer and insert all the kvs that reside in the buffer.
 * Inserts are being sorted in an increasing lsn wise order
*/
void build_index_procedure(struct krm_region_desc *r_desc, enum log_category log_type)
{
	struct build_index_task build_index_task;
	build_index_task.rdma_buffer = r_desc->r_state->l0_recovery_rdma_buf.mr->addr;
	if (log_type == BIG)
		build_index_task.rdma_buffer = r_desc->r_state->big_recovery_rdma_buf.mr->addr;

	build_index_task.rdma_buffers_size = r_desc->r_state->l0_recovery_rdma_buf.rdma_buf_size;
	build_index_task.r_desc = r_desc;
	build_index(&build_index_task);
}
