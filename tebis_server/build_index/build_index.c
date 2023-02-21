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
#include "build_index.h"
#include "../rdma_buffer_iterator/rdma_buffer_iterator.h"
#include "../region_desc.h"
#include "../tebis_server/metadata.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include "log.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include <assert.h>
#include <btree/btree.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
// IWYU pragma: no_forward_declare region_desc

static void insert_kv(rdma_buffer_iterator_t iterator, struct region_desc *r_desc)
{
	par_handle *handle = region_desc_get_db(r_desc);
	const char *error_message = NULL;
	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iterator);
	struct kv_splice_base splice_base = { .kv_cat = calculate_KV_category(kv_splice_get_key_size(kv),
									      kv_splice_get_value_size(kv), insertOp),
					      .kv_type = KV_FORMAT,
					      .kv_splice = kv };
	par_put_serialized(handle, (char *)&splice_base, &error_message, true, false);
	if (error_message) {
		log_fatal("Error uppon inserting key %s, key_size %u", kv_splice_get_key_offset_in_kv(kv),
			  kv_splice_get_key_size(kv));
		_exit(EXIT_FAILURE);
	}
}

void build_index(struct build_index_task *task)
{
	rdma_buffer_iterator_t small_rdma_buf_iterator =
		rdma_buffer_iterator_init(task->small_rdma_buffer, task->rdma_buffers_size,
					  task->small_rdma_buffer + sizeof(struct segment_header));
	rdma_buffer_iterator_t big_rdma_buf_iterator =
		rdma_buffer_iterator_init(task->big_rdma_buffer, task->rdma_buffers_size, task->big_rdma_buffer);

	while (rdma_buffer_iterator_is_valid(small_rdma_buf_iterator) == VALID &&
	       rdma_buffer_iterator_is_valid(big_rdma_buf_iterator) == VALID) {
		struct lsn *small_iter_curr_lsn = rdma_buffer_iterator_get_lsn(small_rdma_buf_iterator);
		struct lsn *big_iter_curr_lsn = rdma_buffer_iterator_get_lsn(big_rdma_buf_iterator);
		if (compare_lsns(small_iter_curr_lsn, big_iter_curr_lsn) < 0) {
			insert_kv(small_rdma_buf_iterator, task->r_desc);
			rdma_buffer_iterator_next(small_rdma_buf_iterator);
		} else {
			insert_kv(big_rdma_buf_iterator, task->r_desc);
			rdma_buffer_iterator_next(big_rdma_buf_iterator);
		}
	}

	rdma_buffer_iterator_t unterminated_cursor = small_rdma_buf_iterator;
	if (rdma_buffer_iterator_is_valid(big_rdma_buf_iterator) == VALID)
		unterminated_cursor = big_rdma_buf_iterator;

	assert(rdma_buffer_iterator_is_valid(unterminated_cursor) == VALID);
	while (rdma_buffer_iterator_is_valid(unterminated_cursor) == VALID) {
		insert_kv(unterminated_cursor, task->r_desc);
		rdma_buffer_iterator_next(unterminated_cursor);
	}
}

void build_index_procedure(struct region_desc *r_desc)
{
	struct build_index_task build_index_task;
	struct ru_replica_state *replica_state = region_desc_get_replica_state(r_desc);
	assert(replica_state);
	build_index_task.small_rdma_buffer = replica_state->l0_recovery_rdma_buf.mr->addr;
	build_index_task.big_rdma_buffer = replica_state->big_recovery_rdma_buf.mr->addr;
	build_index_task.rdma_buffers_size = replica_state->l0_recovery_rdma_buf.rdma_buf_size;
	build_index_task.r_desc = r_desc;
	build_index(&build_index_task);
}
