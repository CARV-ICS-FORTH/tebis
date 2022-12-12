#ifndef REMOTE_COMPACTION_H_
#define REMOTE_COMPACTION_H_
#include "../metadata.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include <stdbool.h>
#include <stdint.h>

struct rco_build_index_task {
	struct krm_region_desc *r_desc;
	char *rdma_buffer; // address at the begining of the l0 recovery rdma buf be parsed
	int64_t rdma_buffers_size; // size of the RDMA buffers
};

void rco_build_index(struct rco_build_index_task *task);

#endif // REMOTE_COMPACTION_H_
