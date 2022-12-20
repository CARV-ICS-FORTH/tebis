#ifndef REMOTE_COMPACTION_H_
#define REMOTE_COMPACTION_H_
#include "../metadata.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include <stdbool.h>
#include <stdint.h>

struct build_index_task {
	struct krm_region_desc *r_desc;
	char *rdma_buffer; // address at the begining of the l0 recovery rdma buf be parsed
	int64_t rdma_buffers_size; // size of the RDMA buffers
};

void build_index(struct build_index_task *task);

/**
 * Build index logic
 * parse the overflown RDMA buffer and insert all the kvs that reside in the buffer.
 * Inserts are being sorted in an increasing lsn wise order
*/
void build_index_procedure(struct krm_region_desc *r_desc, enum log_category log_type);
#endif // REMOTE_COMPACTION_H_
