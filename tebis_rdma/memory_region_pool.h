// Copyright [2019] [FORTH-ICS]
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
#ifndef MEMORY_REGION_POOL_H
#define MEMORY_REGION_POOL_H
#include <stddef.h>
struct ibv_pd;
struct rdma_cm_id;

typedef enum pool_type { DYNAMIC = 123, PREALLOCATED } pool_type;

typedef struct memory_region_pool {
	// FIXME LIST is not thread-safe; need to add at least a coarse-grained lock
	struct klist *free_mrs; // Free list for each priority level
	size_t max_allocated_memory; // Maximum amount of memory allocated in bytes
	size_t allocated_memory; // Currently allocated memory in bytes
	struct ibv_pd *pd; // The protection domain in which all new memory regions will be pinned
	pool_type type;
	size_t default_allocation_size; /*used only for dynamic currently*/
} memory_region_pool;

typedef struct memory_region {
	memory_region_pool *mrpool; // The memory pool where this mr was allocated
	size_t memory_region_length;
	struct ibv_mr *local_memory_region; // memory region struct as returned by ibv_reg_mr
	char *local_memory_buffer; // the malloced buffer for the above mr
	struct ibv_mr *remote_memory_region; // memory region struct as returned by ibv_reg_mr
	char *remote_memory_buffer; // the malloced buffer for the above mr
} memory_region;

memory_region_pool *mrpool_create(struct ibv_pd *pd, size_t max_allocated_memory, pool_type type,
				  size_t allocation_size);
memory_region *mrpool_allocate_memory_region(memory_region_pool *pool, struct rdma_cm_id *id);
void mrpool_free_memory_region(memory_region **mr);

/*for client connections*/
extern const size_t MEM_REGION_BASE_SIZE;
extern const size_t MR_PREALLOCATE_COUNT;
extern const size_t REPLICA_BUFFER_SIZE;

#endif //_MEMORY_REGION_POOL_H
