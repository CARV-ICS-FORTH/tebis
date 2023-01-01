#ifndef SEND_INDEX_H_
#define SEND_INDEX_H_
#include "../metadata.h"
#include "include/parallax/structures.h"

/**
 * Send index logic
 * flush the overflown RDMA buffer in appropriate Parallax's log, and update the HashTable that holds the segment mappings
 * @param r_desc: the region desciptor from which the rdma buffer is flushed
 * @param log_type: the type of the buffer to be flushed (L0-recovery, big)
*/
uint64_t send_index_flush_rdma_buffer(struct krm_region_desc *r_desc, enum log_category log_type);

/**
 * creates and registers an RDMA buffer for the primary to send (incrementally) the new compaction index to the replicas.
 * each replica must have one rdma buffer.
 * @param conn: The rdma connection between primary and backup
 */
struct ibv_mr *send_index_create_compactions_rdma_buffer(connection_rdma *conn);
#endif // SEND_INDEX_H_
