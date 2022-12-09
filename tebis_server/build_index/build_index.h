#ifndef REMOTE_COMPACTION_H_
#define REMOTE_COMPACTION_H_
#include "../metadata.h"
#include "parallax_callbacks/parallax_callbacks.h"
#include <stdbool.h>
#include <stdint.h>

struct rco_build_index_task {
	struct krm_region_desc *r_desc;
	char *l0_recovery_rdma_buffer; // address at the begining of the l0 recovery rdma buf
	char *big_recovery_rdma_buffer; // address at the begining of the l0 recovery rdma buf
	int64_t rdma_buffers_size; // size of the RDMA buffers
};

void rco_build_index(struct rco_build_index_task *task);

struct parallax_callback_funcs build_index_get_callbacks(void);

void *build_index_get_context(void);

#endif // REMOTE_COMPACTION_H_
