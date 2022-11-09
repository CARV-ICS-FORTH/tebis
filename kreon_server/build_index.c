#include "build_index.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include "log.h"
#include "metadata.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include "rdma_buffer_iterator.h"
#include <stdlib.h>
#include <unistd.h>

static void insert_and_proceed_iterator(rdma_buffer_iterator_t iterator, struct krm_region_desc *r_desc)
{
	par_handle *db = r_desc->db;
	const char *error_message = NULL;
	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iterator);
	par_put_serialized(db, (char *)kv, &error_message);
	if (error_message) {
		log_fatal("Error uppon inserting key %s, key_size %u", get_key_offset_in_kv(kv), get_key_size(kv));
		_exit(EXIT_FAILURE);
	}

	rdma_buffer_iterator_next(iterator);
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

		if (ret > 0)
			insert_and_proceed_iterator(l0_recovery_iterator, task->r_desc);
		else if (ret < 0)
			insert_and_proceed_iterator(big_recovery_iterator, task->r_desc);
		else {
			log_fatal("Found a duplicate lsn > 0 in both rdma buffers, this should never happen");
			_exit(EXIT_FAILURE);
		}
	}

	rdma_buffer_iterator_t unterminated_iterator = l0_recovery_iterator;
	if (rdma_buffer_iterator_is_valid(l0_recovery_iterator) == INVALID)
		unterminated_iterator = big_recovery_iterator;
	//proceed unterminated iterator until the end-of-buffer
	while (rdma_buffer_iterator_is_valid(unterminated_iterator) == VALID) {
		insert_and_proceed_iterator(unterminated_iterator, task->r_desc);
	}
}
