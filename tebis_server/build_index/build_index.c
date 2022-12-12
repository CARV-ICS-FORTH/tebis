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
		log_fatal("Error uppon inserting key %s, key_size %u", get_key_offset_in_kv(kv), get_key_size(kv));
		_exit(EXIT_FAILURE);
	}
}

void rco_build_index(struct rco_build_index_task *task)
{
	rdma_buffer_iterator_t rdma_buf_iterator =
		rdma_buffer_iterator_init(task->rdma_buffer, task->rdma_buffers_size);

	while (rdma_buffer_iterator_is_valid(rdma_buf_iterator) == VALID) {
		insert_kv(rdma_buf_iterator, task->r_desc);
		rdma_buffer_iterator_next(rdma_buf_iterator);
	}
}
