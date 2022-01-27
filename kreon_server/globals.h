#pragma once
#include <pthread.h>
#include <stdint.h>
#include "../kreon_rdma/rdma.h"

char *globals_get_RDMA_IP_filter(void);
void globals_set_RDMA_IP_filter(char *RDMA_IP_filter);

char *globals_get_zk_host(void);
void globals_set_zk_host(char *host);

int globals_get_connections_per_server(void);
void globals_set_connections_per_server(int connections_per_server);

int globals_get_job_scheduling_max_queue_depth(void);
void globals_set_job_scheduling_max_queue_depth(int job_scheduling_max_queue_depth);

int globals_get_worker_spin_time_usec(void);
void globals_set_worker_spin_time_usec(int worker_spin_time_usec);

void globals_set_dev(char *dev);
char *globals_get_dev(void);
uint64_t globals_get_dev_size(void);

void globals_create_rdma_channel(void);
void globals_set_rdma_channel(struct channel_rdma *channel);
struct channel_rdma *globals_get_rdma_channel(void);

void globals_init_volume(void);

void globals_set_l0_size(uint32_t l0_size);
uint32_t globals_get_l0_size(void);

void globals_set_growth_factor(uint32_t growth_factor);
uint32_t globals_get_growth_factor(void);
