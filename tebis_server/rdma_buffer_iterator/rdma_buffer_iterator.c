// Copyright [2023] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "rdma_buffer_iterator.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include <assert.h>
#include <stdlib.h>

struct rdma_buffer_iterator {
	char *start_offt;
	char *end_offt;
	char *curr_offset;
	int64_t rdma_buffer_size;
};

uint8_t rdma_buffer_iterator_is_in_bounds(rdma_buffer_iterator_t iter)
{
	if (iter->curr_offset < iter->start_offt || iter->curr_offset > iter->end_offt)
		return 0;
	return 1;
}

rdma_buffer_iterator_t rdma_buffer_iterator_init(char *rdma_buffer_start_offt, int64_t rdma_buffer_size,
						 char *iterator_starting_offt)
{
	assert(rdma_buffer_start_offt);
	rdma_buffer_iterator_t iterator = calloc(1, sizeof(struct rdma_buffer_iterator));
	iterator->rdma_buffer_size = rdma_buffer_size;
	iterator->start_offt = rdma_buffer_start_offt;
	iterator->end_offt = iterator->start_offt + rdma_buffer_size;
	iterator->curr_offset = iterator_starting_offt;
	return iterator;
}

enum rdma_buffer_iterator_status rdma_buffer_iterator_next(rdma_buffer_iterator_t iter)
{
	if (!rdma_buffer_iterator_is_in_bounds(iter))
		return INVALID;

	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iter);
	int32_t key_size = kv_splice_get_key_size(kv);
	int32_t value_size = kv_splice_get_value_size(kv);
	int32_t kv_size = key_size + value_size + kv_splice_get_metadata_size();

	// proceed iterator
	iter->curr_offset = iter->curr_offset + get_lsn_size() + kv_size;

	if (rdma_buffer_iterator_is_in_bounds(iter))
		return VALID;
	return INVALID;
}

struct lsn *rdma_buffer_iterator_get_lsn(rdma_buffer_iterator_t iter)
{
	assert(rdma_buffer_iterator_is_in_bounds(iter));
	return (struct lsn *)iter->curr_offset;
}

struct kv_splice *rdma_buffer_iterator_get_kv(rdma_buffer_iterator_t iter)
{
	assert(rdma_buffer_iterator_is_in_bounds(iter));
	struct kv_splice *kv = (struct kv_splice *)(iter->curr_offset + get_lsn_size());
	return kv;
}

uint64_t rdma_buffer_iterator_get_remaining_space(rdma_buffer_iterator_t iter)
{
	return (uint64_t)iter->end_offt - (uint64_t)iter->curr_offset;
}

enum rdma_buffer_iterator_status rdma_buffer_iterator_is_valid(rdma_buffer_iterator_t iter)
{
	uint32_t minimum_possible_kv_size = kv_splice_get_min_possible_kv_size() + get_lsn_size();
	uint64_t current_lsn_id = rdma_buffer_iterator_get_lsn(iter)->id;

	if (!rdma_buffer_iterator_is_in_bounds(iter))
		return INVALID;
	if (rdma_buffer_iterator_get_remaining_space(iter) < minimum_possible_kv_size)
		return INVALID;
	if (!current_lsn_id) // found a zeroed lsn, so the rest space is zeroed, terminate the iterator
		return INVALID;

	return VALID;
}
