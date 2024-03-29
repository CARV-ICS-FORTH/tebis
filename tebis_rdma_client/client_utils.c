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
#include "client_utils.h"
#include "../tebis_rdma/rdma.h"
#include "../tebis_server/conf.h"
#include "../tebis_server/configurables.h"
#include "../tebis_server/djb2.h"
#include "../tebis_server/globals.h"
#include "../tebis_server/messages.h"
#include "../tebis_server/metadata.h"
#include "../tebis_server/zk_utils.h"
#include "../utilities/spin_loop.h"
#include <cJSON.h>
#include <log.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uthash.h>
#include <zookeeper.h>
#include <zookeeper.jute.h>

static int cu_is_connected = 0;
static zhandle_t *cu_zh = NULL;
struct cu_regions client_regions;

static void _cu_zk_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	/*
 	* zookeeper_init might not have returned, so we
 	* use zkh instead.
 	*/
	(void)zkh;
	(void)path;
	(void)context;
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE)
			cu_is_connected = 1;
		else if (state == ZOO_CONNECTING_STATE) {
			log_fatal("Disconnected from zookeeper");
			exit(EXIT_FAILURE);
		}
	}
}

static uint8_t cu_insert_region(struct cu_regions *regions, struct cu_region_desc *c_region)
{
	int64_t ret;
	int start_idx = 0;
	int end_idx = regions->num_regions - 1;
	int middle = 0;
	uint8_t rc = 0;

	pthread_mutex_lock(&client_regions.r_lock);
	if (regions->num_regions == KRM_MAX_REGIONS) {
		log_warn("Warning! Adding new region failed, max_regions %d reached", KRM_MAX_REGIONS);
		rc = 0;
		goto exit;
	}

	if (regions->num_regions > 0) {
		while (start_idx <= end_idx) {
			middle = (start_idx + end_idx) / 2;
			ret = zku_key_cmp(regions->r_desc[middle].region.min_key_size,
					  regions->r_desc[middle].region.min_key, c_region->region.min_key_size,
					  c_region->region.min_key);
			//log_info("compared %s with %s got %ld", desc->ld_regions->regions[middle].min_key,
			//	 region->min_key, ret);
			if (ret == 0) {
				log_warn("Warning failed to add region, range already present\n");
				rc = 0;
				break;
			}

			if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx) {
					memmove(&regions->r_desc[middle + 1], &regions->r_desc[middle],
						(regions->num_regions - middle) * sizeof(struct cu_region_desc));
					regions->r_desc[middle] = *c_region;
					++regions->num_regions;
					rc = 1;
					goto exit;
				}
			} else {
				start_idx = middle + 1;
				if (start_idx > end_idx) {
					middle++;
					memmove(&regions->r_desc[middle + 1], &regions->r_desc[middle],
						(regions->num_regions - middle) * sizeof(struct cu_region_desc));
					regions->r_desc[middle] = *c_region;
					++regions->num_regions;
					rc = 1;
					goto exit;
				}
			}
		}
	} else {
		regions->r_desc[0] = *c_region;
		++regions->num_regions;
		rc = 1;
	}
exit:
	pthread_mutex_unlock(&client_regions.r_lock);
	return rc;
}

static int cu_fetch_zk_server_entry(char *dataserver_name, struct krm_server_name *dst)
{
	// char *server_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, dataserver_name);
	char *server_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_REGION_SERVERS_EPOCHS, KRM_SLASH, dataserver_name);
	struct Stat stat;
	char buffer[2048];
	int buffer_len = 2048;
	int rc = zoo_get(cu_zh, server_path, 0, buffer, &buffer_len, &stat);

	if (rc != ZOK) {
		log_warn("Oops where is info about this guy: %s", server_path);
		free(server_path);
		return -1;
	}
	free(server_path);

	// Parse json string with server's krm_server_name struct
	cJSON *server_json = cJSON_ParseWithLength(buffer, buffer_len);
	if (!cJSON_IsObject(server_json)) {
		cJSON_Delete(server_json);
		return -1;
	}

	cJSON *hostname = cJSON_GetObjectItem(server_json, "hostname");
	cJSON *dataserver_name_retrieved = cJSON_GetObjectItem(server_json, "dataserver_name");
	cJSON *rdma_ip = cJSON_GetObjectItem(server_json, "rdma_ip_addr");
	cJSON *epoch = cJSON_GetObjectItem(server_json, "epoch");
	cJSON *leader = cJSON_GetObjectItem(server_json, "leader");
	if (!cJSON_IsString(hostname) || !cJSON_IsString(dataserver_name_retrieved) || !cJSON_IsString(rdma_ip) ||
	    !cJSON_IsNumber(epoch) || !cJSON_IsString(leader)) {
		cJSON_Delete(server_json);
		return -1;
	}
	strncpy(dst->hostname, cJSON_GetStringValue(hostname), strlen(cJSON_GetStringValue(hostname)));
	strncpy(dst->kreon_ds_hostname, cJSON_GetStringValue(dataserver_name_retrieved),
		strlen(cJSON_GetStringValue(dataserver_name_retrieved)));
	dst->kreon_ds_hostname_length = strlen(cJSON_GetStringValue(dataserver_name_retrieved));
	strncpy(dst->RDMA_IP_addr, cJSON_GetStringValue(rdma_ip), strlen(cJSON_GetStringValue(rdma_ip)));
	dst->epoch = cJSON_GetNumberValue(epoch);
	strncpy(dst->kreon_leader, cJSON_GetStringValue(leader), strlen(cJSON_GetStringValue(leader)));

	cJSON_Delete(server_json);
	return 0;
}

static uint8_t cu_fetch_region_table(void)
{
	// FIXME rewrite to work with the new json string format
	struct cu_region_desc r_desc;
	char *region = NULL;
	struct Stat stat;
	int ret = 0;

	if (cu_zh == NULL) {
		log_warn("ZK is not initialized!");
		return 0;
	}

	memset(&client_regions, 0x00, sizeof(struct cu_regions));
	/*get regions and fix table*/
	char *regions_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_REGIONS_PATH);
	struct String_vector regions;
	int rc = zoo_get_children(cu_zh, regions_path, 0, &regions);
	if (rc != ZOK) {
		log_warn("Can't fetch regions from zookeeper path %s error code %s", regions_path, zku_op2String(rc));
		ret = 0;
		goto exit;
	}
	client_regions.num_regions = 0;
	// Iterate region entries to build the region table
	char region_json_string[2048];
	memset(region_json_string, 0, sizeof(region_json_string));
	int32_t region_json_string_size = sizeof(region_json_string);

	for (int i = 0; i < regions.count; i++) {
		char *region_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_REGIONS_PATH, KRM_SLASH, regions.data[i]);
		int region_json_string_length = sizeof(region_json_string);
		rc = zoo_get(cu_zh, region_path, 0, region_json_string, &region_json_string_length, &stat);
		if (rc != ZOK) {
			log_fatal("Failed to retrieve region %s from Zookeeper", region_path);
			_exit(EXIT_FAILURE);
		} else if (stat.dataLength > region_json_string_size) {
			log_fatal("Statically allocated buffer is not large enough to hold the json region entry."
				  "Json region entry length is %d and buffer size is %lu",
				  stat.dataLength, sizeof(region_json_string));
			_exit(EXIT_FAILURE);
		}
		cJSON *region_json = cJSON_ParseWithLength(region_json_string, region_json_string_length);
		if (cJSON_IsInvalid(region_json)) {
			log_fatal("Failed to parse json string %s of region %s", region_json_string, region_path);
			_exit(EXIT_FAILURE);
		}
		cJSON *id = cJSON_GetObjectItem(region_json, "id");
		cJSON *min_key = cJSON_GetObjectItem(region_json, "min_key");
		cJSON *max_key = cJSON_GetObjectItem(region_json, "max_key");
		cJSON *primary = cJSON_GetObjectItem(region_json, "primary");
		cJSON *backups = cJSON_GetObjectItem(region_json, "backups");
		cJSON *status = cJSON_GetObjectItem(region_json, "status");
		if (!cJSON_IsString(id) || !cJSON_IsString(min_key) || !cJSON_IsString(max_key) ||
		    !cJSON_IsString(primary) || !cJSON_IsArray(backups) || !cJSON_IsNumber(status)) {
			log_fatal("Failed to parse json string of region %s", region_path);
			_exit(EXIT_FAILURE);
		}
		struct krm_region region = { 0 };
		strncpy(region.id, cJSON_GetStringValue(id), strlen(cJSON_GetStringValue(id)));
		strncpy(region.min_key, cJSON_GetStringValue(min_key), strlen(cJSON_GetStringValue(min_key)));
		if (!strcmp(region.min_key, "-oo")) {
			memset(region.min_key, 0, KRM_MAX_KEY_SIZE);
			region.min_key_size = 1;
		} else {
			region.min_key_size = strlen(region.min_key);
		}
		strncpy(region.max_key, cJSON_GetStringValue(max_key), strlen(cJSON_GetStringValue(max_key)));
		region.max_key_size = strlen(region.max_key);
		region.stat = (enum krm_region_status)cJSON_GetNumberValue(status);
		// Find primary's krm_server_name struct
		if (cu_fetch_zk_server_entry(cJSON_GetStringValue(primary), &region.primary) != 0) {
			log_fatal("Could not fetch zookeeper entry for server %s", cJSON_GetStringValue(primary));
			assert(0);
			_exit(EXIT_FAILURE);
		}

		r_desc.region = region;
		cu_insert_region(&client_regions, &r_desc);

		cJSON_Delete(region_json);
		free(region_path);
	}
exit:

	if (!region)
		free(region);
	free(regions_path);
	ret = 1;
	return ret;
}

uint8_t cu_init(char *zookeeper_host)
{
	LIBRARY_MODE = CLIENT_MODE;
	pthread_mutex_init(&client_regions.r_lock, NULL);
	pthread_mutex_init(&client_regions.conn_lock, NULL);
	globals_create_rdma_channel();
	/*log_info("Initializing, connectiong to zookeeper at %s", zk_host_port);*/
	globals_set_zk_host(zookeeper_host);
	cu_zh = zookeeper_init(globals_get_zk_host(), _cu_zk_watcher, 15000, 0, 0, 0);
	wait_for_value((uint32_t *)&cu_is_connected, 1);
	cu_fetch_region_table();
	return 1;
}

struct cu_region_desc *cu_get_region(const char *key, uint32_t key_size)
{
	struct cu_regions *cli_regions = &client_regions;
	struct cu_region_desc *region = NULL;
	uint32_t times_retried = 0;
	uint32_t times_to_retry = 10000;
	/*retry $times_to_retry times before exiting cause of concurrent region inserts. This should now be a large number.
	 *if a region is found continue */
	while (times_retried < times_to_retry && region == NULL) {
		pthread_mutex_lock(&cli_regions->r_lock);
		uint32_t start_idx = 0;
		uint32_t end_idx = cli_regions->num_regions - 1;
		region = NULL;

		while (start_idx <= end_idx) {
			uint32_t middle = (start_idx + end_idx) / 2;
			int ret = zku_key_cmp(cli_regions->r_desc[middle].region.min_key_size,
					      cli_regions->r_desc[middle].region.min_key, key_size, key);
			//log_debug("Comparing region min %s with key %s ret %ld",
			//	 kreon_regions[middle]->ID_region.minimum_range + 4, key, ret);
			if (ret < 0 || ret == 0) {
				start_idx = middle + 1;
				if (zku_key_cmp(cli_regions->r_desc[middle].region.max_key_size,
						cli_regions->r_desc[middle].region.max_key, key_size, key) > 0) {
					region = &cli_regions->r_desc[middle];
					break;
				}
			} else
				end_idx = middle - 1;
		}
		pthread_mutex_unlock(&cli_regions->r_lock);
	}

	if (region == NULL) {
		log_fatal("NULL region for key %s of size %u\n", key, key_size);
		_exit(EXIT_FAILURE);
	}
	return region;
}

static void cu_add_conn_for_server(struct krm_server_name *server, uint64_t hash_key)
{
	char *host = server->RDMA_IP_addr;
	//log_info("Connection to RDMA IP %s", server->RDMA_IP_addr);
	cu_conn_per_server *cps = (cu_conn_per_server *)malloc(sizeof(cu_conn_per_server));
	cps->connections = (struct connection_rdma **)malloc(globals_get_connections_per_server() *
							     sizeof(struct connection_rdma *));
	for (int i = 0; i < globals_get_connections_per_server(); i++) {
		cps->connections[i] = crdma_client_create_connection_list_hosts(globals_get_rdma_channel(), &host, 1,
										CLIENT_TO_SERVER_CONNECTION);
	}
	cps->hash_key = hash_key;
	HASH_ADD_PTR(client_regions.root_cps, hash_key, cps);
}

connection_rdma *cu_get_conn_for_region(struct cu_region_desc *r_desc, uint64_t seed)
{
	cu_conn_per_server *cps = NULL;

	uint64_t hash_key = djb2_hash((unsigned char *)r_desc->region.primary.kreon_ds_hostname,
				      r_desc->region.primary.kreon_ds_hostname_length);
retry:
	cps = NULL;
	pthread_mutex_lock(&client_regions.conn_lock);
	/*Do we have any open connections with the server?*/
	HASH_FIND_PTR(client_regions.root_cps, &hash_key, cps);
	pthread_mutex_unlock(&client_regions.conn_lock);
	if (cps == NULL) {
		pthread_mutex_lock(&client_regions.conn_lock);

		HASH_FIND_PTR(client_regions.root_cps, &hash_key, cps);
		if (cps == NULL) {
			/*Refresh your knowledge about the server*/
			// FIXME fix for new json format. refactor code in cu_fetch_region_table into a function
			int rc = cu_fetch_zk_server_entry(r_desc->region.primary.kreon_ds_hostname,
							  &r_desc->region.primary);
			if (rc) {
				log_warn("Failed to refresh server info %s from zookeeper",
					 r_desc->region.primary.kreon_ds_hostname);
				_exit(EXIT_FAILURE);
				pthread_mutex_unlock(&client_regions.conn_lock);
				return NULL;
			}
			//log_info("RDMA addr = %s", r_desc->region.primary.RDMA_IP_addr);
			cu_add_conn_for_server(&r_desc->region.primary, hash_key);
		}
		pthread_mutex_unlock(&client_regions.conn_lock);
		goto retry;
	}
	return cps->connections[seed % globals_get_connections_per_server()];
}

void cu_close_open_connections(void)
{
	struct cu_conn_per_server *current;
	struct cu_conn_per_server *tmp;
	msg_header *req_header;
	int i;
	/*iterate all open connections and send the disconnect message*/
	HASH_ITER(hh, client_regions.root_cps, current, tmp)
	{
		//log_info("Closing connections with server %s", current->server_id.kreon_ds_hostname);
		for (i = 0; i < globals_get_connections_per_server(); i++) {
			/*send disconnect msg*/
			pthread_mutex_lock(&current->connections[i]->buffer_lock);
			req_header = client_allocate_rdma_message(current->connections[i], 0, DISCONNECT);
			pthread_mutex_unlock(&current->connections[i]->buffer_lock);
			req_header->offset_reply_in_recv_buffer = UINT32_MAX;
			req_header->reply_length_in_recv_buffer = 0;

			if (client_send_rdma_message(current->connections[i], req_header) != TEBIS_SUCCESS) {
				log_warn("failed to send message");
				_exit(EXIT_FAILURE);
			}

			// FIXME calling free for the connection_rdma* isn't enough. We need to free the rest
			// of the resources allocated for the connection, like the memory region buffers
			free(current->connections[i]);
			//log_info("Closing connection number %d", i);
		}
		HASH_DEL(client_regions.root_cps, current);
		free(current); /* free it */
	}
}
