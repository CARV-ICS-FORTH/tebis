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
#include "configurables.h"
#include "metadata.h"
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <uthash.h>
#include <zookeeper.h>
struct work_task;

struct regs_conn_per_server {
	uint64_t server_key;
	struct connection_rdma *conn;
	UT_hash_handle hh;
};

struct regs_regions {
	struct region_desc *r_desc[KRM_MAX_DS_REGIONS];
	int num_ds_regions;
};
struct regs_server_desc {
	struct krm_server_name name;
	pthread_t region_server_cnxt;
	sem_t wake_up;
	pthread_mutex_t msg_list_lock;
	struct tebis_klist *msg_list;
	zhandle_t *zh;
	struct rco_pool *compaction_pool;
	char *mail_path;
	uint8_t IP[IP_SIZE];
	uint8_t RDMA_IP[IP_SIZE];
	uint8_t zconn_state;
	int RDMA_port;
	/*entry in the root table of my dad (numa_server)*/
	int root_server_id;
	struct regs_conn_per_server *root_data_conn_map;
	struct sc_conn_per_server *root_compaction_conn_map;
	pthread_mutex_t conn_map_lock;
	// /*filled only by the leader server*/
	// struct krm_leader_regions *ld_regions;
	// struct krm_leader_ds_map *dataservers_map;
	struct regs_regions ds_regions;
};

/**
 * @brief Creates a region server descriptor with zeroed values
 */
struct regs_server_desc *regs_create_server(void);
void regs_destroy_server(struct regs_server_desc *region_server);

/**
 * @brief Searches the corresponding region in a region server.
 * @param region_server pointer to the region server object
 * @param key pointer to the key
 * @param key_size size of the key
 */
struct region_desc *regs_get_region_desc(struct regs_server_desc const *region_server, char *key, uint32_t key_size);

/**
 * @brief the main loop of the region server. Caution it should be called with pthread_create
 */
void regs_start_server(struct regs_server_desc *region_server);

/**
 * @brief sets the group id this region server belongs to
 * @param region_server pointer to the region server object
 * @param group_id the id of the group
 */
void regs_set_group_id(struct regs_server_desc *region_server, int group_id);
/**
 * @brief Sets the rdma port this servers is responsible for
 */
void regs_set_rdma_port(struct regs_server_desc *region_server, int port);

/**
 * @brief Returns a pointer to the Tebis name of the server
 * @param region_server pointer to the region server object
 */
char *regs_get_server_name(struct regs_server_desc *region_server);

/**
 * @brief Lookups (in Zookeeper) information about server with hostname
 * @param region_server_desc pointer to the region_server object
 * @param server_hostname pointer to the hostname of the server for which we
 * need information
 * @param server_info the object to be filled with the server's information
 * @returns ZOK on success otherwise the appropriate Zookeeper error code.
 * Zookeeper error code can be transformed to string with the zerror() function
 * of Zookeeper.
 */
int regs_lookup_server_info(struct regs_server_desc *region_server_desc, char *server_hostname,
			    struct krm_server_name *server_info);

void regs_execute_put_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_get_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_multi_get_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_delete_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_flush_command_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_get_rdma_buffer_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_replica_index_get_buffer_req(struct regs_server_desc const *region_server_desc,
					       struct work_task *task);
void regs_execute_flush_medium_log(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_no_op(struct regs_server_desc const *mydesc, struct work_task *task);
void regs_execute_test_req(struct regs_server_desc const *region_server_desc, struct work_task *task);

void regs_execute_replica_index_flush_req(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_test_req_fetch_payload(struct regs_server_desc const *mydesc, struct work_task *task);
void regs_execute_flush_L0_op(struct regs_server_desc const *region_server_desc, struct work_task *task);
void regs_execute_compact_L0(struct regs_server_desc const *region_server_desc, struct work_task *task);

void regs_execute_send_index_close_compaction(struct regs_server_desc const *region_server_desc,
					      struct work_task *task);

void regs_execute_replica_index_swap_levels(struct regs_server_desc const *region_server_desc, struct work_task *task);
struct connection_rdma *regs_get_data_conn(struct regs_server_desc const *region_server, char *hostname,
					   char *IP_address);
struct connection_rdma *regs_get_compaction_conn(struct regs_server_desc *region_server, char *hostname,
						 char *IP_address);
#endif
