#ifndef RDMA_BUFFER_ITERATOR_H
#define RDMA_BUFFER_ITERATOR_H
#include <stdint.h>

typedef struct rdma_buffer_iterator *rdma_buffer_iterator_t;
enum rdma_buffer_iterator_status { VALID = 0, INVALID };

extern rdma_buffer_iterator_t rdma_buffer_iterator_init(char *rdma_buffer_start_offt, int64_t rdma_buffer_size);
extern enum rdma_buffer_iterator_status rdma_buffer_iterator_next(rdma_buffer_iterator_t iter);
extern struct lsn *rdma_buffer_iterator_get_lsn(rdma_buffer_iterator_t iter);
extern struct kv_splice *rdma_buffer_iterator_get_kv(rdma_buffer_iterator_t iter);
extern enum rdma_buffer_iterator_status rdma_buffer_iterator_is_valid(rdma_buffer_iterator_t iter);

extern uint8_t iterator_is_valid(rdma_buffer_iterator_t iter);

#endif // RDMA_BUFFER_ITERATOR_H
