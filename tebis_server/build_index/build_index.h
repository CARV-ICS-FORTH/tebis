// Copyright [2020] [FORTH-ICS]
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
#ifndef BUILD_INDEX_H
#define BUILD_INDEX_H
#include <parallax/structures.h>
#include <stdint.h>

struct region_desc;
struct build_index_task {
	struct region_desc *r_desc;
	char *rdma_buffer; // address at the begining of the l0 recovery rdma buf be parsed
	int64_t rdma_buffers_size; // size of the RDMA buffers
};

void build_index(struct build_index_task *task);

/**
 * Build index logic
 * parse the overflown RDMA buffer and insert all the kvs that reside in the buffer.
 * Inserts are being sorted in an increasing lsn wise order
*/
void build_index_procedure(struct region_desc *r_desc, enum log_category log_type);
#endif // REMOTE_COMPACTION_H_
