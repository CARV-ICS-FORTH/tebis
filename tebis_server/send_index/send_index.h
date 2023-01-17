#ifndef SEND_INDEX_H_
#define SEND_INDEX_H_
#include "../metadata.h"
#include "include/parallax/structures.h"

/*parameters for function send_index_create_compactions_rdma_buffer*/
struct send_index_create_compactions_rdma_buffer_params {
	struct krm_region_desc *r_desc;
	connection_rdma *conn;
	uint32_t tree_id;
	uint32_t level_id;
};

/**
 * Send index logic
 * flush the overflown RDMA buffer in appropriate Parallax's log, and update the HashTable that holds the segment mappings
 * @param r_desc: the region desciptor from which the rdma buffer is flushed
 * @param log_type: the type of the buffer to be flushed (L0-recovery, big)
*/
uint64_t send_index_flush_rdma_buffer(struct krm_region_desc *r_desc, enum log_category log_type);

void send_index_create_compactions_rdma_buffer(struct send_index_create_compactions_rdma_buffer_params params);

void send_index_close_compactions_rdma_buffer(struct krm_region_desc *r_desc, uint32_t level_id);
#endif // SEND_INDEX_H_
