#include "rdma_buffer_iterator.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include "log.h"
#include <assert.h>

struct rdma_buffer_iterator {
	char *start_offt;
	char *end_offt;
	char *curr_offset;
	int64_t rdma_buffer_size;
};

uint8_t iterator_is_valid(rdma_buffer_iterator_t iter)
{
	if (iter->curr_offset < iter->start_offt || iter->curr_offset > iter->end_offt)
		return 0;
	return 1;
}

rdma_buffer_iterator_t rdma_buffer_iterator_init(char *rdma_buffer_start_offt, int64_t rdma_buffer_size)
{
	assert(rdma_buffer_start_offt);
	rdma_buffer_iterator_t iterator = calloc(1, sizeof(struct rdma_buffer_iterator));
	iterator->rdma_buffer_size = rdma_buffer_size;
	iterator->start_offt = rdma_buffer_start_offt;
	iterator->end_offt = iterator->start_offt + rdma_buffer_size;
	iterator->curr_offset = iterator->start_offt;
	return iterator;
}

enum rdma_buffer_iterator_status rdma_buffer_iterator_next(rdma_buffer_iterator_t iter)
{
	if (!iterator_is_valid(iter))
		return INVALID;

	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iter);
	int32_t key_size = kv_splice_get_key_size(kv);
	int32_t value_size = kv_splice_get_value_size(kv);
	int32_t kv_size = key_size + value_size + kv_splice_get_metadata_size();

	// proceed iterator
	iter->curr_offset = iter->curr_offset + get_lsn_size() + kv_size;

	if (iterator_is_valid(iter))
		return VALID;
	return INVALID;
}

struct lsn *rdma_buffer_iterator_get_lsn(rdma_buffer_iterator_t iter)
{
	assert(iterator_is_valid(iter));
	struct lsn *curr_lsn = (struct lsn *)iter->curr_offset;
	log_debug("CHecking lsn %lu", curr_lsn->id);
	return (struct lsn *)iter->curr_offset;
}

struct kv_splice *rdma_buffer_iterator_get_kv(rdma_buffer_iterator_t iter)
{
	assert(iterator_is_valid(iter));
	struct kv_splice *kv = (struct kv_splice *)(iter->curr_offset + get_lsn_size());
	return kv;
}

uint64_t rdma_buffer_iterator_get_remaining_space(rdma_buffer_iterator_t iter)
{
	return (uint64_t)iter->end_offt - (uint64_t)iter->curr_offset;
}

enum rdma_buffer_iterator_status rdma_buffer_iterator_is_valid(rdma_buffer_iterator_t iter)
{
	if (iterator_is_valid(iter) &&
	    rdma_buffer_iterator_get_remaining_space(iter) > kv_splice_get_min_possible_kv_size() + get_lsn_size()) {
		return VALID;
	}
	return INVALID;
}
