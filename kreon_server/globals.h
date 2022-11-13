#pragma once
#include "../kreon_rdma/rdma.h"
#include <pthread.h>
#include <stdint.h>

enum operation_type { TEB_PUT = 0, TEB_GET };
extern char *globals_get_RDMA_IP_filter(void);
extern void globals_set_RDMA_IP_filter(char *RDMA_IP_filter);

extern char *globals_get_zk_host(void);
extern void globals_set_zk_host(char *host);

extern int globals_get_connections_per_server(void);

extern int globals_get_job_scheduling_max_queue_depth(void);

extern int globals_get_worker_spin_time_usec(void);

extern void globals_set_dev(char *dev);
extern char *globals_get_dev(void);
extern uint64_t globals_get_dev_size(void);

extern void globals_create_rdma_channel(void);
extern struct channel_rdma *globals_get_rdma_channel(void);

extern void globals_init_volume(void);

extern void globals_set_l0_size(uint32_t l0_size);
extern uint32_t globals_get_l0_size(void);

extern void globals_set_growth_factor(uint32_t growth_factor);
extern uint32_t globals_get_growth_factor(void);

extern void globals_set_send_index(int enable);
extern int globals_get_send_index(void);

extern void globals_open_trace_file(const char *filename);
extern void globals_append_trace_file(uint32_t key_size, void *key, uint32_t value_size, void *value,
				      enum operation_type op);
extern void globals_close_trace_file(void);
