#ifndef SEND_INDEX_REWRITER_H
#define SEND_INDEX_REWRITER_H
#include "../metadata.h"
#include "../region_desc.h"
#include "btree/btree.h"
#include "send_index_callbacks.h"
#include <stdbool.h>
typedef struct send_index_rewriter *send_index_rewriter_t;

send_index_rewriter_t send_index_rewriter_init(region_desc_t r_desc);

void send_index_rewriter_rewrite_index(send_index_rewriter_t rewriter, region_desc_t r_desc,
				       struct segment_header *segment, uint32_t level_id);

void send_index_rewriter_destroy(send_index_rewriter_t *rewriter);

#endif // SEND_INDEX_REWRITER_H
