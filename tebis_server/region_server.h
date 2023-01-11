// Copyright [2023] [FORTH-ICS]
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
#ifndef REGION_SERVER_H
#define REGION_SERVER_H
#include "metadata.h"

struct regs_server_desc {
	struct krm_server_name name;
	sem_t wake_up;
	pthread_mutex_t msg_list_lock;
	struct tebis_klist *msg_list;
	zhandle_t *zh;
	struct rco_pool *compaction_pool;
	char *mail_path;
	uint8_t IP[IP_SIZE];
	uint8_t RDMA_IP[IP_SIZE];
	enum krm_server_role role;
	enum krm_server_state state;
	uint8_t zconn_state;
	uint32_t RDMA_port;
	/*entry in the root table of my dad (numa_server)*/
	int root_server_id;
	// /*filled only by the leader server*/
	// struct krm_leader_regions *ld_regions;
	// struct krm_leader_ds_map *dataservers_map;
	struct krm_ds_regions *ds_regions;
};

/**
 * @brief Searches the corresponding region in a region server.
 * @param region_server pointer to the region server object
 * @param key pointer to the key
 * @param key_size size of the key
 */
struct krm_region_desc *regs_get_region(struct regs_server_desc const *region_server, char *key, uint32_t key_size);
void *regs_run_region_server(void *args);
#endif
