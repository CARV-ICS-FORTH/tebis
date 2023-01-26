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
#ifndef SEND_INDEX_CALLBACKS_H
#define SEND_INDEX_CALLBACKS_H
#include <stdint.h>
struct region_desc;
struct regs_server_desc;
struct wcursor_level_write_cursor;

struct send_index_context {
	struct region_desc *r_desc;
	struct regs_server_desc *server;
};

void send_index_compaction_started_callback(void *context, uint64_t small_log_tail_dev_offt,
					    uint64_t big_log_tail_dev_offt, uint32_t src_level_id, uint8_t dst_tree_id,
					    struct wcursor_level_write_cursor *new_level);

void send_index_init_callbacks(struct regs_server_desc *server, struct region_desc *r_desc);

#endif // SEND_INDEX_CALLBACKS_H_
