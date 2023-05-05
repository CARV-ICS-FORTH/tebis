// Copyright [2019] [FORTH-ICS]
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
#ifndef CLIENT_UTILS_H
#define CLIENT_UTILS_H
#include "../tebis_rdma/rdma.h"
#include "../tebis_server/configurables.h"
#include "../tebis_server/metadata.h"
#include <pthread.h>
#include <stdint.h>
#include <uthash.h>

struct cu_region_desc {
	struct krm_region region;
	/*plus future other staff*/
};

typedef struct cu_conn_per_server {
	struct krm_server_name server_id;
	uint64_t hash_key;
	connection_rdma **connections;
	UT_hash_handle hh;
} cu_conn_per_server;

struct cu_regions {
	struct cu_region_desc r_desc[KRM_MAX_REGIONS];
	pthread_mutex_t r_lock;
	uint32_t num_regions;
	cu_conn_per_server *root_cps;
	pthread_mutex_t conn_lock;
	struct channel_rdma *channel;
	/*plus future other staff*/
};

uint8_t cu_init(char *zookeeper_host);
struct cu_region_desc *cu_get_region(char *key, uint32_t key_size);
connection_rdma *cu_get_conn_for_region(struct cu_region_desc *r_desc, uint64_t seed);
void cu_close_open_connections(void);
#endif
