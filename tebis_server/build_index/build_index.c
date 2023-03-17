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
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
// IWYU pragma: no_forward_declare region_desc

#define BUILD_INDEX_QUEUE_SIZE 16
struct build_index_item {
	char *buffer;
	uint32_t size;
};

struct build_index_queue {
	struct build_index_item item[BUILD_INDEX_QUEUE_SIZE];
	pthread_mutex_t mutex;
	sem_t empty;
	sem_t full;
	uint32_t in;
	uint32_t out;
	uint32_t capacity;
};

struct build_index_worker {
	pthread_t context;
	struct region_desc *r_desc;
	struct build_index_queue *queue;
	uint64_t count;
};

void build_index_add_buffer(struct build_index_worker *worker, char *buffer, uint32_t size)
{
	struct build_index_queue *queue = worker->queue;
	char *copy = calloc(1UL, size);
	memcpy(copy, buffer, size);

	sem_wait(&queue->empty);

	pthread_mutex_lock(&queue->mutex);
	queue->item[queue->in].buffer = copy;
	queue->item[queue->in].size = size;
	queue->in = (queue->in + 1) % queue->capacity;
	pthread_mutex_unlock(&queue->mutex);
	sem_post(&queue->full);
	log_debug("Added a buffer");
}

struct build_index_item *build_index_consume(struct build_index_queue *queue)
{
	struct build_index_item *item = NULL;
	sem_wait(&queue->full);
	pthread_mutex_lock(&queue->mutex);
	item = &queue->item[queue->out];
	queue->out = (queue->out + 1) % queue->capacity;
	pthread_mutex_unlock(&queue->mutex);
	sem_post(&queue->empty);
	log_debug("Got a buffer for processing");
	return item;
}

static void insert_kv(rdma_buffer_iterator_t iterator, struct region_desc *r_desc)
{
	par_handle *handle = region_desc_get_db(r_desc);
	const char *error_message = NULL;
	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iterator);
	struct kv_splice_base splice_base = { .kv_cat = calculate_KV_category(kv_splice_get_key_size(kv),
									      kv_splice_get_value_size(kv), insertOp),
					      .kv_type = KV_FORMAT,
					      .kv_splice = kv };
	// log_debug("Inserting key %s", kv_splice_base_get_key_buf(&splice_base));
	par_put_serialized(handle, (char *)&splice_base, &error_message, true, false);
	if (error_message) {
		log_fatal("Error uppon inserting key %s, key_size %u", kv_splice_get_key_offset_in_kv(kv),
			  kv_splice_get_key_size(kv));
		_exit(EXIT_FAILURE);
	}
}

void build_index(struct build_index_worker *worker, struct build_index_item *item)
{
	rdma_buffer_iterator_t big_rdma_buf_iterator =
		rdma_buffer_iterator_init(item->buffer, item->size, item->buffer);

	log_info("(Before) Inserted %lu keys for region %s", worker->count, region_desc_get_id(worker->r_desc));
	while (rdma_buffer_iterator_is_valid(big_rdma_buf_iterator) == VALID) {
		// if (++worker->count % 20000 == 0)
		// 	log_info("Inserted %lu keys for region %s", worker->count, region_desc_get_id(worker->r_desc));
		++worker->count;
		insert_kv(big_rdma_buf_iterator, worker->r_desc);
		rdma_buffer_iterator_next(big_rdma_buf_iterator);
	}
	assert(worker->count - count != 0);
	log_info("(After) Inserted %lu keys for region %s", worker->count, region_desc_get_id(worker->r_desc));
}

static void *build_index_worker(void *build_worker)
{
	struct build_index_worker *build_index_worker = (struct build_index_worker *)build_worker;

	for (;;) {
		struct build_index_item *item = build_index_consume(build_index_worker->queue);
		build_index(build_index_worker, item);
		free(item->buffer);
	}
	return NULL;
}

static struct build_index_queue *build_index_create_queue(void)
{
	struct build_index_queue *queue = calloc(1UL, sizeof(struct build_index_queue));
	pthread_mutex_init(&queue->mutex, NULL);
	sem_init(&queue->empty, 0, BUILD_INDEX_QUEUE_SIZE);
	sem_init(&queue->full, 0, 0);
	queue->capacity = BUILD_INDEX_QUEUE_SIZE;
	return queue;
}

struct build_index_worker *build_index_create_worker(struct region_desc *r_desc)
{
	struct build_index_worker *worker = calloc(1UL, sizeof(struct build_index_worker));
	worker->queue = build_index_create_queue();
	worker->r_desc = r_desc;

	return worker;
}

void build_index_start_worker(struct build_index_worker *worker)
{
	pthread_create(&worker->context, NULL, build_index_worker, worker);
}
