#ifndef SEND_INDEX_CALLBACKS_H_
#define SEND_INDEX_CALLBACKS_H_
#include <stdint.h>

struct send_index_context {
	struct region_desc *r_desc;
	struct regs_server_desc *server;
};
struct wcursor_level_write_cursor;

void send_index_compaction_started_callback(void *context, uint64_t small_log_tail_dev_offt,
					    uint64_t big_log_tail_dev_offt, uint32_t src_level_id, uint8_t dst_tree_id,
					    struct wcursor_level_write_cursor *new_level);

void send_index_init_callbacks(struct regs_server_desc *server, struct region_desc *r_desc);

#endif // SEND_INDEX_CALLBACKS_H_
