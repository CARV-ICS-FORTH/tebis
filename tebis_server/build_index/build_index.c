#include "build_index.h"
#include "../rdma_buffer_iterator/rdma_buffer_iterator.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include "build_index_callbacks.h"
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
	rdma_buffer_iterator_t l0_recovery_iterator =
		rdma_buffer_iterator_init(task->l0_recovery_rdma_buffer, task->rdma_buffers_size);
	rdma_buffer_iterator_t big_recovery_iterator =
		rdma_buffer_iterator_init(task->big_recovery_rdma_buffer, task->rdma_buffers_size);

	while (rdma_buffer_iterator_is_valid(l0_recovery_iterator) == VALID &&
	       rdma_buffer_iterator_is_valid(big_recovery_iterator) == VALID) {
		struct lsn *curr_l0_recovery_lsn = rdma_buffer_iterator_get_lsn(l0_recovery_iterator);
		struct lsn *curr_big_recovery_lsn = rdma_buffer_iterator_get_lsn(big_recovery_iterator);

		int64_t ret = compare_lsns(curr_l0_recovery_lsn, curr_big_recovery_lsn);

		if (ret > 0) {
			insert_kv(l0_recovery_iterator, task->r_desc);
			rdma_buffer_iterator_next(l0_recovery_iterator);
		} else if (ret < 0) {
			insert_kv(big_recovery_iterator, task->r_desc);
			rdma_buffer_iterator_next(big_recovery_iterator);
		} else {
			log_fatal("Found a duplicate lsn > 0 in both rdma buffers, this should never happen");
			_exit(EXIT_FAILURE);
		}
	}

	rdma_buffer_iterator_t unterminated_iterator = l0_recovery_iterator;
	if (rdma_buffer_iterator_is_valid(l0_recovery_iterator) == INVALID)
		unterminated_iterator = big_recovery_iterator;
	//proceed unterminated iterator until the end-of-buffer
	while (rdma_buffer_iterator_is_valid(unterminated_iterator) == VALID) {
		insert_kv(unterminated_iterator, task->r_desc);
		rdma_buffer_iterator_next(unterminated_iterator);
	}
}

struct parallax_callback_funcs get_build_index_callbacks(void)
{
	struct parallax_callback_funcs build_index_callbacks = { 0 };
	build_index_callbacks.segment_is_full_cb = build_index_segment_is_full_callback;
	return build_index_callbacks;
}

void *build_index_get_context(void)
{
	return NULL;
}
