#ifndef SEND_INDEX_CALLBACKS_H_
#define SEND_INDEX_CALLBACKS_H_
#include <stdint.h>

struct send_index_context {
	struct krm_region_desc *r_desc;
	struct krm_server_desc *server;
};

void send_index_compaction_started_callback(void *context, uint32_t src_level_id);

void send_index_init_callbacks(struct krm_server_desc *server, struct krm_region_desc *r_desc);

#endif // SEND_INDEX_CALLBACKS_H_
