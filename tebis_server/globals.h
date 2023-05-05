// Copyright [2020] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef GLOBALS_H
#define GLOBALS_H
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
#endif
