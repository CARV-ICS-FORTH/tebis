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
#define _GNU_SOURCE
#include "region_server.h"
#include "../tebis_rdma/memory_region_pool.h"
#include "../tebis_rdma/rdma.h"
#include "../tebis_rdma_client/msg_factory.h"
#include "../utilities/circular_buffer.h"
#include "../utilities/spin_loop.h"
#include "allocator/log_structures.h"
#include "allocator/persistent_operations.h"
#include "btree/btree.h"
#include "build_index/build_index.h"
#include "build_index/build_index_callbacks.h"
#include "conf.h"
#include "djb2.h"
#include "globals.h"
#include "list.h"
#include "master/command.h"
#include "master/mregion.h"
#include "messages.h"
#include "metadata.h"
#include "parallax/structures.h"
#include "region_desc.h"
#include "send_index/send_index.h"
#include "send_index/send_index_callbacks.h"
#include "send_index/send_index_rewriter.h"
#include "send_index/send_index_uuid_checker/send_index_uuid_checker.h"
#include "server_communication.h"
#include "work_task.h"
#include "zk_utils.h"
#include <arpa/inet.h>
#include <assert.h>
#include <btree/compaction_daemon.h>
#include <btree/compaction_worker.h>
#include <btree/conf.h>
#include <btree/gc.h>
#include <btree/kv_pairs.h>
#include <btree/level_write_appender.h>
#include <btree/lsn.h>
#include <cJSON.h>
#include <ifaddrs.h>
#include <include/parallax/parallax.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <log.h>
#include <netinet/in.h>
#include <pthread.h>
#include <rdma/rdma_verbs.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <uthash.h>
#include <zookeeper.h>
#include <zookeeper.jute.h>
// IWYU pragma: no_forward_declare region_desc
uint64_t ds_hash_key;

char *regs_get_server_name(struct regs_server_desc *region_server)
{
	return region_server->name.kreon_ds_hostname;
}

static void *regs_run_region_server(void *args);

struct regs_server_desc *regs_create_server(void)
{
	struct regs_server_desc *region_server_desc = calloc(1UL, sizeof(struct regs_server_desc));
	pthread_mutex_init(&region_server_desc->conn_map_lock, NULL);
	return region_server_desc;
}

void regs_destroy_server(struct regs_server_desc *region_server)
{
	if (NULL == region_server)
		return;
	free(region_server);
}

zhandle_t *regs_get_zk_handle(struct regs_server_desc *region_server)
{
	return region_server->zh;
}

void regs_start_server(struct regs_server_desc *region_server)
{
	log_info("Starting Region Server");
	if (pthread_create(&region_server->region_server_cnxt, NULL, regs_run_region_server, region_server)) {
		log_fatal("Failed to start metadata_server");
		_exit(EXIT_FAILURE);
	}
}

void regs_set_group_id(struct regs_server_desc *region_server, int group_id)
{
	region_server->root_server_id = group_id;
}

int regs_get_rdma_port(struct regs_server_desc *region_server)
{
	return region_server->RDMA_port;
}

void regs_set_rdma_port(struct regs_server_desc *region_server, int port)
{
	region_server->RDMA_port = port;
}

/**
 * @brief Returns the epoch of the server
 */
static uint64_t regs_get_server_epoch(struct regs_server_desc *region_server)
{
	return region_server->name.epoch;
}

static void regs_get_IP_addresses(struct regs_server_desc *server)
{
	char addr[INET_ADDRSTRLEN] = { 0 };
	struct ifaddrs *ifaddr = { 0 };
	struct ifaddrs *ifa = { 0 };
	int family = -1;

	if (getifaddrs(&ifaddr) == -1) {
		perror("getifaddrs");
		_exit(EXIT_FAILURE);
	}
	for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr == NULL)
			continue;
		family = ifa->ifa_addr->sa_family;
		if (family != AF_INET)
			continue;
		struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
		// addr = inet_ntoa(sa->sin_addr);
		inet_ntop(AF_INET, &(sa->sin_addr), addr, INET_ADDRSTRLEN);
		char *ip_filter = globals_get_RDMA_IP_filter();
		if (strncmp(addr, ip_filter, strlen(ip_filter)) != 0)
			continue;
		log_debug("RDMA IP prefix %s match for Interface: %s Full IP Address: %s", globals_get_RDMA_IP_filter(),
			  ifa->ifa_name, addr);
		if (sprintf(server->name.RDMA_IP_addr, "%s:%u", addr, server->RDMA_port) < 0) {
			log_fatal("Failed to write IP address");
			_exit(EXIT_FAILURE);
		}
		freeifaddrs(ifaddr);
		return;
	}
	log_fatal("Failed to find an IP for RDMA in the subnet %s", globals_get_RDMA_IP_filter());
	_exit(EXIT_FAILURE);
}

/**
 * Watcher we use to process session events. In particular,
 * when it receives a ZOO_CONNECTED_STATE event, we set the
 * connected variable so that we know that the session has
 * been established.
 */
void zk_main_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	struct regs_server_desc *my_desc = (struct regs_server_desc *)context;
	/*
* zookeeper_init might not have returned, so we
* use zkh instead.
*/
	log_debug("MAIN watcher type %d state %d path %s", type, state, path);
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			my_desc->zconn_state = KRM_CONNECTED;

		} else if (state == ZOO_CONNECTING_STATE) {
			if (my_desc->zconn_state == KRM_CONNECTED) {
				log_warn("Disconnected from zookeeper %s trying to reconnect", globals_get_zk_host());
				my_desc->zh =
					zookeeper_init(globals_get_zk_host(), zk_main_watcher, 15000, 0, my_desc, 0);
				log_warn("Connected! TODO re register watchers");
			}
		}
	} else {
		log_warn("Unhandled event");
	}
}

#if 0
static void zoo_rmr_folder(zhandle_t *zh, const char *path)
{
	struct String_vector children = { 0 };
	int ret_code = zoo_get_children(zh, path, 0, &children);
	if (ret_code != ZOK) {
		log_fatal("Failed to delete zk folder %s", path);
		_exit(EXIT_FAILURE);
	}
	for (int i = 0; i < children.count; ++i) {
		char *child_path = children.data[i];
		zoo_rmr_folder(zh, child_path);
	}
	if (zoo_delete(zh, path, -1) != ZOK) {
		log_fatal("Failed to remove folder %s", path);
		_exit(EXIT_FAILURE)
	}
}
#endif

void mailbox_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	(void)state;
	(void)path;
	struct Stat stat;

	struct regs_server_desc *s_desc = (struct regs_server_desc *)watcherCtx;
	if (type == ZOO_CREATED_EVENT) {
		log_info("ZOO_CREATE_EVENT");
	} else if (type == ZOO_DELETED_EVENT) {
		log_info("ZOO_DELETED_EVENT");
	} else if (type == ZOO_CHANGED_EVENT) {
		log_info("ZOO_CHANGED_EVENT");
	} else if (type == ZOO_CHILD_EVENT) {
		log_info("ZOO_CHILD_EVENT");
	} else if (type == ZOO_SESSION_EVENT) {
		log_warn("Lost connection with zookeeper, someone else will handle it I suppose");
		return;
	} else if (type == ZOO_NOTWATCHING_EVENT) {
		log_info("ZOO_NOTWATCHING_EVENT");
	} else {
		log_info("what?");
	}

	/*get children with watcher*/
	if (type == ZOO_CHILD_EVENT) {
		struct String_vector *mails = (struct String_vector *)calloc(1, sizeof(struct String_vector));
		int ret_code = zoo_wget_children(zh, s_desc->mail_path, mailbox_watcher, (void *)s_desc, mails);
		if (ret_code != ZOK) {
			log_fatal("failed to get mails from path %s error code: %s ", s_desc->mail_path,
				  zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		for (int i = 0; i < mails->count; i++) {
			char *mail = zku_concat_strings(3, s_desc->mail_path, KRM_SLASH, mails->data[i]);
			struct krm_msg *msg = (struct krm_msg *)calloc(1, sizeof(struct krm_msg));

			int buffer_len = sizeof(struct krm_msg);
			ret_code = zoo_get(s_desc->zh, mail, 0, (char *)msg, &buffer_len, &stat);
			if (ret_code != ZOK) {
				log_fatal("Failed to fetch email %s", mail);
				_exit(EXIT_FAILURE);
			}

			//log_info("fetched mail %s for region %s", mail, msg->region.id);
			pthread_mutex_lock(&s_desc->msg_list_lock);
			tebis_klist_add_last(s_desc->msg_list, msg, NULL, NULL);
			sem_post(&s_desc->wake_up);
			pthread_mutex_unlock(&s_desc->msg_list_lock);
			//log_info("Deleting %s", mail);
			ret_code = zoo_delete(s_desc->zh, mail, -1);
			if (ret_code != ZOK) {
				log_fatal("Failed to delete mail %s", mail);
				_exit(EXIT_FAILURE);
			}
			free(mail);
		}
		free(mails);
	} else {
		log_fatal("Unhandled type of event type is %d", type);
		_exit(EXIT_FAILURE);
	}
}

/**
 * @brief Searches for a region in the region table of the Region Server. If it
 * finds it returns the position and sets found flag to true. If it does not is
 * sets found to false and returns the position in the array that shoud be
 * present. Caution in case where the region array is empty it returns -1 as
 * its position.
 */
static int regs_get_region_pos(struct regs_regions const *ds_regions, char *key, uint32_t key_size, bool *found)
{
	*found = false;

	if (0 == ds_regions->num_ds_regions)
		return -1;

	int start_idx = 0;
	int end_idx = ds_regions->num_ds_regions - 1;
	/*log_info("start %d end %d", start_idx, end_idx);*/
	int middle = 0;
	int ret_code_min = 0;

	while (start_idx <= end_idx) {
		middle = (start_idx + end_idx) / 2;
		ret_code_min = zku_key_cmp(region_desc_get_min_key_size(ds_regions->r_desc[middle]),
					   region_desc_get_min_key(ds_regions->r_desc[middle]), key_size, key);

		int ret_code_max = zku_key_cmp(region_desc_get_max_key_size(ds_regions->r_desc[middle]),
					       region_desc_get_max_key(ds_regions->r_desc[middle]), key_size, key);
		if (ret_code_min <= 0 && ret_code_max > 0) {
			*found = true;
			return middle;
		}
		if (ret_code_min > 0)
			end_idx = middle - 1;
		else
			start_idx = middle + 1;
	}

	int pos = ret_code_min > 0 ? --middle : middle;
	log_info("pos = %d", pos);
	return pos;
}

static bool regs_insert_region_desc(struct regs_regions *region_table, region_desc_t region_desc)
{
	if (region_table->num_ds_regions >= KRM_MAX_DS_REGIONS) {
		log_fatal("Cannot insert another regions array is full can host up to %u regions", KRM_MAX_DS_REGIONS);
		_exit(EXIT_FAILURE);
	}
	bool found = false;
	int pos = regs_get_region_pos(region_table, region_desc_get_min_key(region_desc),
				      region_desc_get_min_key_size(region_desc), &found);

	if (found) {
		log_fatal("Region with min key %.*s already present", region_desc_get_min_key_size(region_desc),
			  region_desc_get_min_key(region_desc));
		assert(0);
		_exit(EXIT_FAILURE);
	}
	log_debug("Region pos is %d", pos);

	memmove(&region_table->r_desc[pos + 2], &region_table->r_desc[pos + 1],
		(region_table->num_ds_regions - (pos + 1)) * sizeof(struct krm_region_desc *));
	region_table->r_desc[pos + 1] = region_desc;
	++region_table->num_ds_regions;
	return found;
}
/**
 * Returns r_desc that key should be hosted. Returns NULL if region_table is empty
 */
region_desc_t regs_get_region_desc(struct regs_server_desc const *region_server, char *key, uint32_t key_size)
{
	bool found = false;
	int pos = regs_get_region_pos(&region_server->ds_regions, key, key_size, &found);
	if (-1 == pos) {
		log_warn("Querying an empty region table returning NULL");
		return NULL;
	}

	return found ? region_server->ds_regions.r_desc[pos] : NULL;
}

region_desc_t regs_open_region(struct regs_server_desc *region_server, mregion_t mregion, enum server_role server_role)
{
	region_desc_t region_desc = region_desc_create(mregion, server_role);

	disable_gc();

	par_db_options db_options = { .volume_name = globals_get_dev(),
				      .create_flag = PAR_CREATE_DB,
				      .db_name = region_desc_get_id(region_desc),
				      .options = par_get_default_options() };
	db_options.options[LEVEL0_SIZE].value = KB(globals_get_l0_size());
	db_options.options[GROWTH_FACTOR].value = globals_get_growth_factor();

	if (server_role == BACKUP_DEAD || server_role == BACKUP_INFANT || server_role == BACKUP_NEWBIE ||
	    server_role == BACKUP) {
		// open DB as backup
		db_options.options[PRIMARY_MODE].value = 0;
		db_options.options[REPLICA_MODE].value = 1;
		db_options.options[REPLICA_BUILD_INDEX].value = 1;
		db_options.options[REPLICA_SEND_INDEX].value = 0;
	}

	if (globals_get_send_index() && region_desc_get_num_backup(region_desc) > 0) {
		db_options.options[NUMBER_OF_REPLICAS].value = region_desc_get_num_backup(region_desc);
		db_options.options[ENABLE_COMPACTION_DOUBLE_BUFFERING].value = 1;
		db_options.options[WCURSOR_SPIN_FOR_FLUSH_REPLIES].value = 1;
		db_options.options[REPLICA_SEND_INDEX].value = 1;
		db_options.options[REPLICA_BUILD_INDEX].value = 0;
	}
	const char *error_message = NULL;
	par_handle parallax_db = par_open(&db_options, &error_message);
	if (error_message) {
		log_fatal("Error uppon opening the DB, error %s", error_message);
		_exit(EXIT_FAILURE);
	}

	region_desc_set_db(region_desc, parallax_db);
	if (error_message) {
		log_fatal("Error uppon opening DB: %s, error %s", region_desc_get_id(region_desc), error_message);
		_exit(EXIT_FAILURE);
	}

	regs_insert_region_desc(&region_server->ds_regions, region_desc);
	if (globals_get_send_index() && region_desc_get_num_backup(region_desc) > 0)
		send_index_init_callbacks(region_server, region_desc);
	else
		build_index_init_callbacks(region_server, region_desc);

	region_desc_increase_lsn(region_desc);
	return region_desc;
}

static void regs_process_command(struct regs_server_desc *region_server, struct krm_msg *msg)
{
	(void)region_server;
	(void)msg;
	// 	char *zk_path;
	// 	struct krm_msg reply;
	// 	int rc;
	// 	switch (msg->type) {
	// 	case KRM_OPEN_REGION_AS_PRIMARY:
	// 	case KRM_OPEN_REGION_AS_BACKUP:
	// 		/*first check if the msg responds to the epoch I am currently in*/
	// 		if (msg->epoch != region_server->name.epoch) {
	// 			log_warn("Epochs mismatch I am at epoch %lu msg refers to epoch %lu", region_server->name.epoch,
	// 				 msg->epoch);
	// 			reply.type = (msg->type == KRM_OPEN_REGION_AS_PRIMARY) ? KRM_NACK_OPEN_PRIMARY :
	// 										 KRM_NACK_OPEN_BACKUP;
	// 			reply.error_code = KRM_BAD_EPOCH;
	// 			strcpy(reply.sender, region_server->name.kreon_ds_hostname);
	// 			reply.region = msg->region;
	// 		} else {
	// 			open_region(region_server, &msg->region,
	// 				    msg->type == KRM_OPEN_REGION_AS_PRIMARY ? KRM_PRIMARY : KRM_BACKUP);

	// 			reply.type = (msg->type == KRM_OPEN_REGION_AS_PRIMARY) ? KRM_ACK_OPEN_PRIMARY :
	// 										 KRM_ACK_OPEN_BACKUP;
	// 			reply.error_code = KRM_SUCCESS;
	// 			strcpy(reply.sender, region_server->name.kreon_ds_hostname);
	// 			reply.region = msg->region;
	// 		}
	// #define MAIL_ID_LENGTH 128
	// 		char mail_id[MAIL_ID_LENGTH] = { 0 };
	// 		int mail_id_len = MAIL_ID_LENGTH;
	// 		zk_path =
	// 			zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, msg->sender, KRM_MAIL_TITLE);
	// 		rc = zoo_create(region_server->zh, zk_path, (char *)&reply, sizeof(struct krm_msg),
	// 				&ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, mail_id, mail_id_len);

	// 		if (rc != ZOK) {
	// 			log_fatal("Failed to respond path is %s code is %s", zk_path, zku_op2String(rc));
	// 			_exit(EXIT_FAILURE);
	// 		}
	// 		log_debug("Successfully sent ACK to %s for region %s", msg->sender, msg->region.id);
	// 		free(zk_path);
	// 		break;
	// 	case KRM_CLOSE_REGION:
	// 		log_fatal("Unsupported types KRM_CLOSE_REGION");
	// 		_exit(EXIT_FAILURE);
	// 	case KRM_BUILD_PRIMARY:
	// 		log_fatal("Unsupported types KRM_BUILD_PRIMARY");
	// 		_exit(EXIT_FAILURE);
	// 	default:
	// 		log_fatal("No action for message type %d (Probably corrupted message type)", msg->type);
	// 		_exit(EXIT_FAILURE);
	// 	}
}

int regs_lookup_server_info(struct regs_server_desc *region_server_desc, char *server_hostname,
			    struct krm_server_name *server_info)
{
	/*check if you are hostname-RDMA_port belongs to the project*/
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_REGION_SERVERS_EPOCHS, KRM_SLASH, server_hostname);
	struct Stat stat;
	char buffer[2048] = { 0 };
	int buffer_len = sizeof(buffer);
	int ret_code = zoo_get(region_server_desc->zh, zk_path, 0, buffer, &buffer_len, &stat);

	if (ret_code != ZOK) {
		log_debug("Cannot find server: %s reason: %s", zk_path, zerror(ret_code));
		free(zk_path);
		return ret_code;
	}
	free(zk_path);

	// Parse json string with server's krm_server_name struct
	cJSON *json = cJSON_ParseWithLength(buffer, buffer_len);
	if (!cJSON_IsObject(json)) {
		cJSON_Delete(json);
		return -1;
	}

	cJSON *hostname = cJSON_GetObjectItem(json, "hostname");
	cJSON *region_server = cJSON_GetObjectItem(json, "dataserver_name");
	cJSON *rdma_ip = cJSON_GetObjectItem(json, "rdma_ip_addr");
	cJSON *epoch = cJSON_GetObjectItem(json, "epoch");
	cJSON *leader = cJSON_GetObjectItem(json, "leader");
	if (!cJSON_IsString(hostname) || !cJSON_IsString(region_server) || !cJSON_IsString(rdma_ip) ||
	    !cJSON_IsNumber(epoch) || !cJSON_IsString(leader)) {
		cJSON_Delete(json);
		return -1;
	}
	strncpy((char *)region_server_desc->name.hostname, cJSON_GetStringValue(hostname),
		strlen(cJSON_GetStringValue(hostname)));
	strncpy((char *)region_server_desc->name.kreon_ds_hostname, cJSON_GetStringValue(region_server),
		strlen(cJSON_GetStringValue(region_server)));
	server_info->kreon_ds_hostname_length = strlen(cJSON_GetStringValue(region_server));
	strncpy(server_info->RDMA_IP_addr, cJSON_GetStringValue(rdma_ip), KRM_MAX_RDMA_IP_SIZE - 1);
	server_info->epoch = cJSON_GetNumberValue(epoch);
	strncpy(region_server_desc->name.kreon_leader, cJSON_GetStringValue(leader), KRM_HOSTNAME_SIZE - 1);

	cJSON_Delete(json);

	return ret_code;
}

/**
 * @brief Updates the server info in zookeeper
 */
static int regs_update_server_info(struct regs_server_desc *region_server, struct krm_server_name *src)
{
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_REGION_SERVERS_EPOCHS, KRM_SLASH,
					   region_server->name.kreon_ds_hostname);
	cJSON *obj = cJSON_CreateObject();
	cJSON_AddStringToObject(obj, "hostname", src->hostname);
	cJSON_AddStringToObject(obj, "dataserver_name", src->kreon_ds_hostname);
	cJSON_AddStringToObject(obj, "leader", "Andreas Papandreou");
	cJSON_AddStringToObject(obj, "RDMA_IP_addr", src->RDMA_IP_addr);
	cJSON_AddNumberToObject(obj, "epoch", src->epoch);

	const char *json_string = cJSON_Print(obj);
	struct Stat stat = { 0 };
	int ret_code = zoo_exists(regs_get_zk_handle(region_server), zk_path, 0, &stat);

	if (ZOK != ret_code) {
		log_debug("Zookeeper path: %s does not exist creating one..", zk_path);
		ret_code = zoo_create(regs_get_zk_handle(region_server), zk_path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
				      ZOO_PERSISTENT, NULL, -1);
	}
	ret_code = zoo_set(regs_get_zk_handle(region_server), zk_path, json_string, strlen(json_string), -1);
	if (ret_code != ZOK) {
		log_fatal("Failed to update server info %s in Zookeeper for keeping info about server: %s reason: %s",
			  zk_path, regs_get_server_name(region_server), zerror(ret_code));
		_exit(EXIT_FAILURE);
	}

	cJSON_Delete(obj);
	free((void *)json_string);
	free(zk_path);
	return ret_code;
}

/**
 * Announces Region Server presence in the Tebis cluster. More precisely, it creates an ephemeral znode under /aliveservers
 */
static void regs_announce_server_presence(struct regs_server_desc *region_server)
{
	char path[KRM_HOSTNAME_SIZE] = { 0 };
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH, KRM_SLASH,
					   regs_get_server_name(region_server));
	int ret_code = zoo_create(region_server->zh, zk_path, (const char *)&region_server->name,
				  sizeof(struct krm_server_name), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, path,
				  KRM_HOSTNAME_SIZE);
	if (ret_code != ZOK) {
		log_fatal("Failed to annouce my presence code %s", zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	log_debug("Region server annoucing my presence at path: %s", regs_get_server_name(region_server));
	free(zk_path);
}

static struct ibv_mr *regs_allocate_rdma_buffer(uint32_t size, struct rdma_cm_id *rdma_cm_id)
{
	char *addr = NULL;
	if (posix_memalign((void **)&addr, 4096, size)) {
		log_fatal("Failed to allocate aligned RDMA buffer");
		perror("Reason\n");
		_exit(EXIT_FAILURE);
	}
	/*Zero RDMA buffer*/
	memset(addr, 0, size);

	struct ibv_mr *new_mr = rdma_reg_write(rdma_cm_id, addr, size);
	if (!new_mr) {
		log_fatal("Failed to reg write the buffer");
		perror("Reason\n");
		_exit(EXIT_FAILURE);
	}
	return new_mr;
}

static void regs_initialize_rdma_buf_metadata(struct ru_master_log_buffer *rdma_buf, uint32_t backup_id,
					      struct ibv_mr *mr, enum log_type log_type)
{
	rdma_buf->segment_size = rdma_buf->remote_buffers.end = SEGMENT_SIZE;
	rdma_buf->remote_buffers.start = rdma_buf->remote_buffers.curr_end = rdma_buf->primary_buffer_curr_end = 0;
	if (log_type == SMALL_LOG) {
		rdma_buf->remote_buffers.curr_end = sizeof(struct segment_header);
		rdma_buf->primary_buffer_curr_end = sizeof(struct segment_header);
	}
	rdma_buf->remote_buffers.mr[backup_id] = *mr;
	rdma_buf->remote_buffers.replicated_bytes = 0;
	assert(rdma_buf->remote_buffers.mr[backup_id].length == SEGMENT_SIZE);
}

static void regs_init_log_buffers_with_replicas(struct regs_server_desc const *server, region_desc_t region_desc)
{
	if (!region_desc_get_num_backup(region_desc))
		return;

	struct connection_rdma *conn = NULL;
	for (uint32_t backup_id = 0; backup_id < region_desc_get_num_backup(region_desc); ++backup_id) {
		conn = regs_get_data_conn(server, region_desc_get_backup_hostname(region_desc, backup_id),
					  region_desc_get_backup_IP(region_desc, backup_id));

		log_debug("Sending GET_RDMA_BUFFERs req to Server %s",
			  region_desc_get_backup_hostname(region_desc, backup_id));
		struct sc_msg_pair get_log_buffer = { 0 };
		//get space in circular buffer
		do {
			get_log_buffer = sc_allocate_rpc_pair(
				conn, sizeof(struct s2s_msg_get_rdma_buffer_req) + MREG_get_region_size(),
				sizeof(struct s2s_msg_get_rdma_buffer_rep) + (sizeof(struct ibv_mr)),
				GET_RDMA_BUFFER_REQ);
		} while (get_log_buffer.stat != ALLOCATION_IS_SUCCESSFULL);

		struct msg_header *request_header = get_log_buffer.request;
		volatile struct msg_header *reply_header = get_log_buffer.reply;
		//prepare the header
		request_header->triggering_msg_offset_in_send_buffer =
			real_address_to_triggering_msg_offt(conn, request_header);
		/*location where server should put the reply*/
		request_header->offset_reply_in_recv_buffer =
			(uint64_t)reply_header - (uint64_t)conn->recv_circular_buf->memory_region;
		request_header->reply_length_in_recv_buffer =
			sizeof(struct msg_header) + reply_header->payload_length + reply_header->padding_and_tail_size;
		request_header->session_id = (uint64_t)region_desc_get_uuid(region_desc);

		/*prepare the body*/
		struct s2s_msg_get_rdma_buffer_req *get_buffer_req =
			(struct s2s_msg_get_rdma_buffer_req *)((char *)request_header + sizeof(struct msg_header));
		get_buffer_req->buffer_size = SEGMENT_SIZE;
		get_buffer_req->mregion_buffer_size = MREG_get_region_size();
		mregion_t mregion = region_desc_get_mregion(region_desc);
		MREG_serialize_region(mregion, get_buffer_req->mregion_buffer, get_buffer_req->mregion_buffer_size);
		__send_rdma_message(conn, request_header, NULL);

		/*spin until replica replies*/
		while (reply_header->receive != TU_RDMA_REGULAR_MSG)
			;
		log_debug("Got header reply from RegionServer: %s to initialize region: %s as backup",
			  MREG_get_region_backup(mregion, backup_id), MREG_get_region_id(mregion));
		volatile uint8_t tail = get_receive_field(reply_header);
		while (tail != TU_RDMA_REGULAR_MSG)
			;
		log_debug("Got full reply from RegionServer: %s to initialize region: %s as backup",
			  MREG_get_region_backup(mregion, backup_id), MREG_get_region_id(mregion));

		struct s2s_msg_get_rdma_buffer_rep *get_buffer_rep =
			(struct s2s_msg_get_rdma_buffer_rep *)(((char *)reply_header) + sizeof(struct msg_header));
		assert(get_buffer_rep->status == TEBIS_SUCCESS);

		regs_initialize_rdma_buf_metadata(region_desc_get_primary_L0_log_buf(region_desc), backup_id,
						  &get_buffer_rep->l0_recovery_mr, SMALL_LOG);
		regs_initialize_rdma_buf_metadata(region_desc_get_primary_medium_log_buf(region_desc), backup_id,
						  &get_buffer_rep->medium_recovery_mr, MEDIUM_LOG);
		regs_initialize_rdma_buf_metadata(region_desc_get_primary_big_log_buf(region_desc), backup_id,
						  &get_buffer_rep->big_recovery_mr, BIG_LOG);

		sc_free_rpc_pair(&get_log_buffer);
	}
	struct ru_master_log_buffer *l0_rec_buf = region_desc_get_primary_L0_log_buf(region_desc);
	l0_rec_buf->primary_buffer = regs_allocate_rdma_buffer(SEGMENT_SIZE, conn->rdma_cm_id);
	struct ru_master_log_buffer *big_rec_buf = region_desc_get_primary_big_log_buf(region_desc);
	big_rec_buf->primary_buffer = regs_allocate_rdma_buffer(SEGMENT_SIZE, conn->rdma_cm_id);
}

static void regs_handle_master_command(struct regs_server_desc *region_server, MC_command_t command)
{
	log_debug("Region server: %s got the following command", regs_get_server_name(region_server));
	MC_print_command(command);

	switch (MC_get_command_code(command)) {
	case OPEN_REGION_START:
		log_debug("RegionServer: %s opening region: %s", regs_get_server_name(region_server),
			  MC_get_region_id(command));
		mregion_t mregion = MREG_deserialize_region(MC_get_buffer(command), MC_get_buffer_size(command));
		MREG_print_region_configuration(mregion);
		region_desc_t region_desc = regs_open_region(region_server, mregion, MC_get_role(command));
		regs_init_log_buffers_with_replicas(region_server, region_desc);
		if (region_desc_get_num_backup(region_desc))
			region_desc_register_medium_log_buffers(
				region_desc,
				regs_get_data_conn(region_server, region_desc_get_backup_hostname(region_desc, 0),
						   region_desc_get_backup_IP(region_desc, 0)));

		break;
	case RECONFIGURE_GROUP:
		log_fatal("Got RECONFIGURE_GROUP command XXX TODO XXX");
		mregion = MREG_deserialize_region(MC_get_buffer(command), MC_get_buffer_size(command));
		region_desc = regs_get_region_desc(region_server, MREG_get_region_id(mregion),
						   strlen(MREG_get_region_id(mregion)));
		if (NULL == region_desc) {
			log_fatal("Master commands me to reconfigure region: %s that I don't have",
				  MREG_get_region_id(mregion));
			_exit(EXIT_FAILURE);
		}
		//close region command
		//reopen
		_exit(EXIT_FAILURE);
		break;
	default:
		log_debug("Unhandled case");
		_exit(EXIT_FAILURE);
	}
}

static void command_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)type;
	(void)state;
	struct regs_server_desc *server = (struct regs_server_desc *)context;
	log_debug("New command from master %s", path);
	struct String_vector commands = { 0 };
	zoo_wget_children(zkh, path, command_watcher, context, &commands);
	for (int i = 0; i < commands.count; ++i) {
		char *command_path = zku_concat_strings(6, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
							server->name.kreon_ds_hostname, KRM_SLASH, commands.data[i]);
		char buffer[KRM_COMMAND_BUFFER_SIZE] = { 0 };
		int buf_len = sizeof(buffer);
		struct Stat stat = { 0 };
		int ret_code = zoo_get(zkh, command_path, 0, (char *)buffer, &buf_len, &stat);
		if (ret_code != ZOK) {
			log_fatal("Error could not get command from path %s reason: %s", command_path,
				  zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		MC_command_t command = MC_deserialize_command(buffer, buf_len);
		regs_handle_master_command(server, command);
		if (zoo_delete(zkh, command_path, -1) != ZOK) {
			log_fatal("Failed to delete command: %s from mailbox", command_path);
			_exit(EXIT_FAILURE);
		}
		free(command_path);
	}
}

/**
 * @brief Creates server's mailbox path in Zookeeper
 */
static void regs_create_server_mailbox(struct regs_server_desc *region_server)
{
	log_debug("Creating *NEW* mailbox and leaving a watcher");

	char *mailbox =
		zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, regs_get_server_name(region_server));
	int ret_code = zoo_create(regs_get_zk_handle(region_server), mailbox, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
				  ZOO_PERSISTENT, NULL, -1);

	if (ret_code != ZOK) {
		log_fatal("failed to query zookeeper for path %s contents with code %s", mailbox,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}

	struct String_vector stale_msgs = { 0 };
	ret_code = zoo_wget_children(region_server->zh, mailbox, command_watcher, region_server, &stale_msgs);

	if (ZOK != ret_code) {
		log_fatal("Failed to set a watcher for new master commands. Reason is: %s", zerror(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(mailbox);
}

/**
 * @brief Each time a region server enters the group it changes its epoch
 * and cleans its previous state. This functions does a cleanup of possible
 * previous messages
 */
static void regs_clean_server_mailbox(struct regs_server_desc *region_server, uint64_t previous_epoch)
{
	log_debug("Cleaning stale messages from my mailbox from previous epoch %lu", previous_epoch);

	char epoch_to_string[256] = { 0 };
	if (snprintf(epoch_to_string, sizeof(epoch_to_string), "%lu", previous_epoch) < 0) {
		log_fatal("Failed to convert epoch to string");
		_exit(EXIT_FAILURE);
	}
	char *mailbox = zku_concat_strings(6, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
					   regs_get_server_name(region_server), KRM_DASH, epoch_to_string);
	struct String_vector stale_msgs = { 0 };
	int ret_code = zoo_wget_children(region_server->zh, mailbox, command_watcher, region_server, &stale_msgs);

	if (ret_code != ZOK) {
		log_fatal("failed to query zookeeper for path %s contents with code %s", mailbox,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	for (int i = 0; i < stale_msgs.count; i++) {
		/*iterate old mails and delete them*/
		char *mail = zku_concat_strings(6, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
						region_server->name.kreon_ds_hostname, KRM_SLASH, stale_msgs.data[i]);
		log_debug("Deleting %s", mail);
		ret_code = zoo_delete(region_server->zh, mail, -1);
		if (ret_code != ZOK) {
			log_fatal("failed to delete stale mail msg %s error %s", mail, zku_op2String(ret_code));
			_exit(EXIT_FAILURE);
		}
		free(mail);
	}
	free(mailbox);
}

static void regs_register_server(struct regs_server_desc *region_server)
{
	if (gethostname(region_server->name.hostname, KRM_HOSTNAME_SIZE) != 0) {
		log_fatal("failed to get my hostname");
		_exit(EXIT_FAILURE);
	}
	/*now fill your cluster hostname*/
	if (snprintf(region_server->name.kreon_ds_hostname, KRM_HOSTNAME_SIZE, "%s:%d", region_server->name.hostname,
		     regs_get_rdma_port(region_server)) < 0) {
		log_fatal("Failed to create hostname string");
		_exit(EXIT_FAILURE);
	}
	struct krm_server_name server_info = { 0 };
	log_debug("Constructing my Tebis name: %s ...", region_server->name.kreon_ds_hostname);
	int ret_code = regs_lookup_server_info(region_server, region_server->name.kreon_ds_hostname, &server_info);
	uint64_t epoch = ret_code == ZOK ? ++server_info.epoch : 1;

	char tebis_name[KRM_HOSTNAME_SIZE] = { 0 };
	if (snprintf(tebis_name, KRM_HOSTNAME_SIZE, "%s:%lu", region_server->name.kreon_ds_hostname, epoch) < 0) {
		log_fatal("Failed to create region server Tebis name");
		_exit(EXIT_FAILURE);
	}
	memcpy(region_server->name.kreon_ds_hostname, tebis_name, KRM_HOSTNAME_SIZE);
	regs_get_IP_addresses(region_server);
	log_debug("Everything ok I decided that my Tebis name is %s", regs_get_server_name(region_server));
	regs_update_server_info(region_server, &region_server->name);
	++region_server->name.epoch;
}

static void regs_init_zookeeper(struct regs_server_desc *region_server)
{
	region_server->zh = zookeeper_init(globals_get_zk_host(), zk_main_watcher, 15000, 0, region_server, 0);
	if (!region_server->zh) {
		log_fatal("failed to connect to zk %s", globals_get_zk_host());
		_exit(EXIT_FAILURE);
	}
	field_spin_for_value(&region_server->zconn_state, KRM_CONNECTED);
}

static void regs_init_server_state(struct regs_server_desc *server)
{
	server->mail_path =
		zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, server->name.kreon_ds_hostname);

	sem_init(&server->wake_up, 0, 0);
	server->msg_list = tebis_klist_init();
}

static bool regs_am_I_part_of_the_cluster(struct regs_server_desc *region_server_desc)
{
	(void)region_server_desc;
	return true;
	// return regs_lookup_server_info(region_server_desc, region_server_desc->name.kreon_ds_hostname,
	// 			       &region_server_desc->name) == ZOK;
}

static void *regs_run_region_server(void *args)
{
	pthread_setname_np(pthread_self(), "rserver");
	zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);

	struct regs_server_desc *region_server = (struct regs_server_desc *)args;
	regs_init_server_state(region_server);
	regs_init_zookeeper(region_server);

	if (!regs_am_I_part_of_the_cluster(region_server)) {
		log_fatal("Host %s cannot determine that is part of the cluster",
			  region_server->name.kreon_ds_hostname);
		_exit(EXIT_FAILURE);
	}

	log_debug("Hello I am Region Server: %s epoch is %lu", regs_get_server_name(region_server),
		  regs_get_server_epoch(region_server));
	regs_register_server(region_server);

	if (1 == regs_get_server_epoch(region_server)) {
		log_debug("First time I join the cluster no previous cleanup needed");
		log_debug("Formatting volume to enter the system as a completely new server");
		globals_init_volume();
	} else {
		log_debug("I have rejoined the cluster current epoch is %lu cleaning up previous mailbox",
			  regs_get_server_epoch(region_server));
		regs_clean_server_mailbox(region_server, regs_get_server_epoch(region_server));
	}

	regs_create_server_mailbox(region_server);

	// regs_update_server_info(region_server, &region_server->name);

	regs_announce_server_presence(region_server);

	sem_wait(&region_server->wake_up);

	while (1) {
		struct tebis_klist_node *node = NULL;

		pthread_mutex_lock(&region_server->msg_list_lock);
		node = tebis_klist_remove_first(region_server->msg_list);
		pthread_mutex_unlock(&region_server->msg_list_lock);
		if (!node) {
			sem_wait(&region_server->wake_up);
			continue;
		}
		regs_process_command(region_server, (struct krm_msg *)node->data);
		free(node->data);
		free(node);
	}
	return NULL;
}

static uint8_t regs_key_exists(struct work_task *task)
{
	assert(task);
	par_handle par_hd = region_desc_get_db(task->r_desc);
	struct par_key pkey = { 0 };
	pkey.size = kv_splice_get_key_size(task->kv);
	pkey.data = kv_splice_get_key_offset_in_kv(task->kv);
	return par_exists(par_hd, &pkey) != PAR_KEY_NOT_FOUND;
}

/**
 * @brief Fills the replication fields of a put msg. Only put msgs need to be replicated.
 *  The space of these fields is preallocated from the client in order to have zero copy transfer
 *  from primaries to backups
 *  The replications fields are |lsn|sizes_tail|payload_tail|
 *  */
static void regs_fill_replication_fields(msg_header *msg, struct par_put_metadata metadata)
{
	assert(msg);
	struct lsn *lsn_of_put_msg = put_msg_get_lsn_offset(msg);
	lsn_of_put_msg->id = metadata.lsn + 1;
	struct kv_splice *kv = put_msg_get_kv_offset(msg);
	set_sizes_tail(kv, TU_RDMA_REPLICATION_MSG);
	set_payload_tail(kv, TU_RDMA_REPLICATION_MSG);
}

static void regs_insert_kv_to_store(struct work_task *task)
{
#if CREATE_TRACE_FILE
	log_fatal("Fix creation of trace file with the new format");
	_exit(EXIT_FAILURE);
	uint32_t key_size = *(uint32_t *)task->kv->kv_payload;
	char *key = task->kv->kv_payload + sizeof(uint32_t);
	uint32_t value_size = *(uint32_t *)(task->kv->kv_payload + sizeof(uint32_t) + key_size);
	char *value = task->kv->kv_payload + 2 * sizeof(uint32_t) + key_size;
	globals_append_trace_file(key_size, key, value_size, value, TEB_PUT);
#endif
	struct kv_splice_base splice_base = { .kv_cat = calculate_KV_category(kv_splice_get_key_size(task->kv),
									      kv_splice_get_value_size(task->kv),
									      insertOp),
					      .kv_type = KV_FORMAT,
					      .kv_splice = task->kv };
	/*insert kv to data store*/
	const char *error_message = NULL;
	struct par_put_metadata metadata =
		par_put_serialized(region_desc_get_db(task->r_desc), (char *)&splice_base, &error_message, true, false);
	if (error_message) {
		region_desc_leave_parallax(task->r_desc);
		return;
	}
	task->insert_metadata = metadata;
	/*replication path*/
	task->kreon_operation_status = TASK_COMPLETE;
	if (region_desc_get_num_backup(task->r_desc)) {
		regs_fill_replication_fields(task->msg, metadata);
		task->kv_category = TEBIS_SMALLORMEDIUM;
		if (metadata.key_value_category == BIG_INLOG)
			task->kv_category = TEBIS_BIG;

		task->kreon_operation_status = WAIT_FOR_REPLICATION_TURN;
	}
}

static void regs_fill_flush_request(struct region_desc *r_desc, struct s2s_msg_flush_cmd_req *flush_request,
				    struct work_task *task)
{
	flush_request->primary_segment_offt = task->insert_metadata.flush_segment_offt;
	flush_request->log_type = task->insert_metadata.log_type;
	flush_request->region_key_size = region_desc_get_min_key_size(r_desc),
	strcpy(flush_request->region_key, region_desc_get_min_key(r_desc));
	flush_request->uuid = (uint64_t)flush_request;
	struct ru_master_log_buffer *rdma_buffer = region_desc_get_primary_L0_log_buf(r_desc);
	if (flush_request->log_type == BIG)
		rdma_buffer = region_desc_get_primary_big_log_buf(r_desc);
	assert(flush_request->log_type != MEDIUM);
	flush_request->last_flushed_offt = rdma_buffer->remote_buffers.flushed_until_offt;
}

void regs_send_flush_commands(struct regs_server_desc const *server, struct work_task *task)
{
	//log_debug("Send flush commands");
	struct region_desc *r_desc = task->r_desc;

	for (uint32_t backup_id = task->last_replica_to_ack; backup_id < region_desc_get_num_backup(r_desc);
	     ++backup_id) {
		if (region_desc_get_flush_cmd_status(r_desc, backup_id) != RU_BUFFER_UNINITIALIZED)
			continue;
		/*allocate and send command*/
		struct connection_rdma *conn = regs_get_data_conn(server,
								  region_desc_get_backup_hostname(r_desc, backup_id),
								  region_desc_get_backup_IP(r_desc, backup_id));
		/*send flush req and piggyback it with the seg id num*/
		uint32_t req_size = sizeof(struct s2s_msg_flush_cmd_req) + region_desc_get_min_key_size(r_desc);
		uint32_t rep_size = sizeof(struct s2s_msg_flush_cmd_rep);
		region_desc_set_flush_msg_pair(r_desc, backup_id,
					       sc_allocate_rpc_pair(conn, req_size, rep_size, FLUSH_COMMAND_REQ));

		struct sc_msg_pair *flush_cmd = region_desc_get_flush_msg_pair(r_desc, backup_id);
		if (flush_cmd->stat != ALLOCATION_IS_SUCCESSFULL) {
			task->last_replica_to_ack = backup_id;
			return;
		}

		flush_cmd = region_desc_get_flush_msg_pair(r_desc, backup_id);
		msg_header *req_header = flush_cmd->request;

		//time to send the message
		req_header->session_id = region_desc_get_uuid(r_desc);
		struct s2s_msg_flush_cmd_req *f_req =
			(struct s2s_msg_flush_cmd_req *)((char *)req_header + sizeof(struct msg_header));
		regs_fill_flush_request(r_desc, f_req, task);
		__send_rdma_message(conn, req_header, NULL);
		region_desc_set_primary2backup_buffer_stat(r_desc, backup_id, RU_BUFFER_REQUESTED);
	}
	task->last_replica_to_ack = 0;
	task->kreon_operation_status = WAIT_FOR_FLUSH_REPLIES;
}

void regs_wait_for_flush_replies(struct work_task *task)
{
	//log_debug("wait for flush replies");
	struct region_desc *r_desc = task->r_desc;
	for (uint32_t i = task->last_replica_to_ack; i < region_desc_get_num_backup(task->r_desc); ++i) {
		/*check if header has arrived*/
		struct sc_msg_pair *flush_cmd = region_desc_get_flush_msg_pair(task->r_desc, i);
		msg_header *reply = flush_cmd->reply;

		if (reply->receive != TU_RDMA_REGULAR_MSG) {
			task->last_replica_to_ack = i;
			return;
		}
		/*check if payload has arrived*/
		uint8_t tail = get_receive_field(reply);
		if (tail != TU_RDMA_REGULAR_MSG) {
			task->last_replica_to_ack = i;
			return;
		}
	}
	//got all replies motherfuckers
	region_desc_lock_region_mngmt(task->r_desc);
	for (uint32_t i = 0; i < region_desc_get_num_backup(task->r_desc); ++i) {
		// re-zero the metadat of the flushed segment
		struct ru_master_log_buffer *flushed_log = task->insert_metadata.log_type == BIG ?
								   region_desc_get_primary_big_log_buf(r_desc) :
								   region_desc_get_primary_L0_log_buf(r_desc);

		if (!globals_get_send_index())
			flushed_log = region_desc_get_primary_big_log_buf(r_desc);
		// if (task->insert_metadata.log_type == BIG)
		// 	struct ru_master_log_buffer_seg *flushed_segment = flushed_log->segment;
		// if (task->insert_metadata.log_type == BIG)
		// 	flushed_segment = &r_desc->m_state->big_recovery_rdma_buf.segment;
		flushed_log->remote_buffers.curr_end = 0;
		flushed_log->primary_buffer_curr_end = 0;
		if (task->insert_metadata.log_type == L0_RECOVERY) {
			flushed_log->remote_buffers.curr_end = sizeof(struct segment_header);
			flushed_log->primary_buffer_curr_end = sizeof(struct segment_header);
		}
		//build index
		if (!globals_get_send_index()) {
			flushed_log->remote_buffers.curr_end = 0;
			flushed_log->primary_buffer_curr_end = 0;
		}
		flushed_log->remote_buffers.replicated_bytes = 0;
		//for debuging purposes
		send_index_uuid_checker_validate_uuid(region_desc_get_flush_msg_pair(r_desc, i), FLUSH_COMMAND_REQ);
		/*free the flush msg*/
		sc_free_rpc_pair(region_desc_get_flush_msg_pair(r_desc, i));
	}
	region_desc_unlock_region_mngmt(task->r_desc);

	task->last_replica_to_ack = 0;
	task->kreon_operation_status = REPLICATE;
}

inline static uint8_t regs_buffer_have_enough_space(struct ru_master_log_buffer *r_buf, struct work_task *task)
{
	/* if (task->kv_category == TEBIS_BIG) */
	/* 	log_debug("[current end of big buf %lu end %lu]", r_buf->segment.curr_end, r_buf->segment.end); */
	/* else */
	/* 	log_debug("[current end of small buf %lu end %lu]", r_buf->segment.curr_end, r_buf->segment.end); */
	/* put_msg_print_msg(task->msg); */
	if (r_buf->remote_buffers.curr_end >= r_buf->remote_buffers.start &&
	    r_buf->remote_buffers.curr_end + task->msg_payload_size <= r_buf->remote_buffers.end)
		return 1;
	return 0;
}

static void regs_wait_for_replication_turn(struct work_task *task)
{
	assert(task);
	int64_t lsn = get_lsn_id(put_msg_get_lsn_offset(task->msg));
	// log_debug("LSN got %ld next: %ld", lsn, region_desc_get_next_lsn(task->r_desc));
	if (lsn != region_desc_get_next_lsn(task->r_desc))
		return; /*its not my turn yet*/

	/*only 1 threads enters this region at a time*/
	/*find which rdma_buffer must be appended*/
	struct ru_master_log_buffer *rdma_buffer_to_fill = task->kv_category == TEBIS_BIG ?
								   region_desc_get_primary_big_log_buf(task->r_desc) :
								   region_desc_get_primary_L0_log_buf(task->r_desc);

	if (!globals_get_send_index())
		rdma_buffer_to_fill = region_desc_get_primary_big_log_buf(task->r_desc);

	task->kreon_operation_status = REPLICATE;
	if (!regs_buffer_have_enough_space(rdma_buffer_to_fill, task))
		task->kreon_operation_status = SEND_FLUSH_COMMANDS;
}

// This function is called by the poll_cq thread every time a notification
// arrives
/* static void regs_wait_for_replication_completion_callback(struct rdma_message_context *r_cnxt) */
/* { */
/* 	if (r_cnxt->__is_initialized != 1) { */
/* 		log_debug("replication completion callback %u", r_cnxt->__is_initialized); */
/* 		assert(0); */
/* 		_exit(EXIT_FAILURE); */
/* 	} */
/* 	//sem_post(&r_cnxt->wait_for_completion); */
/* 	r_cnxt->completion_flag = 1; */
/* } */

static void regs_copy_kv_to_primary_buffer(struct ru_master_log_buffer *r_buf, struct msg_header *msg_hdr,
					   uint32_t payload_size)
{
	assert(r_buf && msg_hdr);

	char *primary_buffer = (char *)r_buf->primary_buffer->addr;
	char *msg_payload = (char *)msg_hdr + sizeof(struct msg_header);
	memcpy(&primary_buffer[r_buf->primary_buffer_curr_end], msg_payload, payload_size);
}

static void regs_replicate_kv_to_replicas(struct regs_server_desc const *server, struct work_task *task,
					  struct ru_master_log_buffer *r_buf)
{
	struct region_desc *r_desc = task->r_desc;
	uint32_t remote_offset = r_buf->remote_buffers.curr_end;
	char *primary_buffer = (char *)r_buf->primary_buffer->addr;

	task->replicated_bytes = &r_buf->remote_buffers.replicated_bytes;
	for (uint32_t backup_id = task->last_replica_to_ack; backup_id < region_desc_get_num_backup(r_desc);
	     ++backup_id) {
		struct connection_rdma *r_conn = regs_get_data_conn(server,
								    region_desc_get_backup_hostname(r_desc, backup_id),
								    region_desc_get_backup_IP(r_desc, backup_id));
		//client_rdma_init_message_context(&task->msg_ctx[backup_id], NULL);

		//task->msg_ctx[backup_id].args = task;
		//task->msg_ctx[backup_id].on_completion_callback = regs_wait_for_replication_completion_callback;
		char *msg_payload = &primary_buffer[r_buf->primary_buffer_curr_end];
		while (1) {
			int ret = rdma_post_write(r_conn->rdma_cm_id, NULL /*&task->msg_ctx[backup_id]*/, msg_payload,
						  task->msg_payload_size, r_buf->primary_buffer, IBV_SEND_SIGNALED,
						  (uint64_t)r_buf->remote_buffers.mr[backup_id].addr + remote_offset,
						  r_buf->remote_buffers.mr[backup_id].rkey);

			if (ret == 0)
				break;
		}
	}
}

void regs_replicate_task(struct regs_server_desc const *server, struct work_task *task)
{
	assert(server && task);
	//log_debug("replicate task");
	struct region_desc *r_desc = task->r_desc;

	struct ru_master_log_buffer *r_buf = task->kv_category == TEBIS_BIG ?
						     region_desc_get_primary_big_log_buf(r_desc) :
						     region_desc_get_primary_L0_log_buf(r_desc);
	//for build index case
	if (!globals_get_send_index())
		r_buf = region_desc_get_primary_big_log_buf(r_desc);

	regs_copy_kv_to_primary_buffer(r_buf, task->msg, task->msg_payload_size);

	regs_replicate_kv_to_replicas(server, task, r_buf);

	//log_debug("Adding %lu in curr_end", task->msg_payload_size);
	r_buf->remote_buffers.curr_end += task->msg_payload_size;
	r_buf->primary_buffer_curr_end += task->msg_payload_size;
	assert(r_buf->primary_buffer_curr_end == r_buf->remote_buffers.curr_end);
	region_desc_increase_lsn(task->r_desc);

	task->kreon_operation_status = ALL_REPLICAS_ACKED;
}

void regs_wait_for_replication_completion(struct work_task *task)
{
	for (uint32_t i = task->last_replica_to_ack; i < region_desc_get_num_backup(task->r_desc); ++i) {
		//if (sem_trywait(&task->msg_ctx[i].wait_for_completion) != 0) {
		if (0 == task->msg_ctx[i].completion_flag) {
			task->last_replica_to_ack = i;
			return;
		}

		if (task->msg_ctx[i].wc.status != IBV_WC_SUCCESS && task->msg_ctx[i].wc.status != IBV_WC_WR_FLUSH_ERR) {
			log_fatal("Replication RDMA write error: %s", ibv_wc_status_str(task->msg_ctx[i].wc.status));
			_exit(EXIT_FAILURE);
		}
	}

	/*count bytes replicated for this segment*/
	// __sync_fetch_and_add(task->replicated_bytes, task->msg_payload_size);
	//log_debug("replicated bytes %lu", *task->replicated_bytes[i]);
	//log_info(" key is %u:%s Bytes now %llu i =%u kv size was %u full event? %u",
	//	 *(uint32_t *)task->ins_req.key_value_buf, task->ins_req.key_value_buf + 4,
	//	 *task->replicated_bytes[i], i, task->kv_size,
	//	 task->ins_req.metadata.segment_full_event);
	// assert(*task->replicated_bytes <= SEGMENT_SIZE);
	task->kreon_operation_status = ALL_REPLICAS_ACKED;
}

static void regs_insert_kv_pair(struct regs_server_desc const *server, struct work_task *task)
{
	//############## fsm state logic follows ###################
	while (1) {
		switch (task->kreon_operation_status) {
		case INS_TO_KREON: {
			regs_insert_kv_to_store(task);
			break;
		}

		case SEND_FLUSH_COMMANDS: {
			regs_send_flush_commands(server, task);
			break;
		}

		case WAIT_FOR_FLUSH_REPLIES: {
			regs_wait_for_flush_replies(task);
			break;
		}

		case WAIT_FOR_REPLICATION_TURN: {
			regs_wait_for_replication_turn(task);
			break;
		}

		case REPLICATE: {
			regs_replicate_task(server, task);
			break;
		}
		case WAIT_FOR_REPLICATION_COMPLETION: {
			regs_wait_for_replication_completion(task);

			break;
		}
		case ALL_REPLICAS_ACKED:
			task->kreon_operation_status = TASK_COMPLETE;
			break;
		case TASK_COMPLETE:
			return;

		default:
			log_fatal("Ended up in faulty state %u", task->kreon_operation_status);
			assert(0);
			return;
		}
	}
}

/**
  * @brief Fills reply headers from server to clients
  * payload and msg_type must be provided as they defer from msg to msgs
  */
static void regs_fill_reply_header(msg_header *reply_msg, struct work_task *task, uint32_t payload_size,
				   uint16_t msg_type)
{
	uint32_t reply_size = sizeof(struct msg_header) + payload_size + TU_TAIL_SIZE;
	uint32_t padding = MESSAGE_SEGMENT_SIZE - (reply_size % MESSAGE_SEGMENT_SIZE);

	reply_msg->padding_and_tail_size = 0;
	reply_msg->payload_length = payload_size;
	if (reply_msg->payload_length != 0)
		reply_msg->padding_and_tail_size = padding + TU_TAIL_SIZE;

	reply_msg->offset_reply_in_recv_buffer = UINT32_MAX;
	reply_msg->reply_length_in_recv_buffer = UINT32_MAX;
	reply_msg->offset_in_send_and_target_recv_buffers = task->msg->offset_reply_in_recv_buffer;
	reply_msg->triggering_msg_offset_in_send_buffer = task->msg->triggering_msg_offset_in_send_buffer;
	reply_msg->session_id = task->msg->session_id;
	reply_msg->msg_type = msg_type;
	reply_msg->op_status = 0;
	reply_msg->receive = TU_RDMA_REGULAR_MSG;
}

void regs_execute_put_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(task->msg->msg_type == PUT_REQUEST || task->msg->msg_type == PUT_IF_EXISTS_REQUEST);
	/* retrieve region handle for the corresponding key, find_region
	 * initiates internally rdma connections if needed
	 * */
	if (task->kv == NULL) {
		task->kv = put_msg_get_kv_offset(task->msg);
		uint32_t key_length = kv_splice_get_key_size(task->kv);
		char *key = kv_splice_get_key_offset_in_kv(task->kv);
		if (key_length == 0) {
			assert(0);
			_exit(EXIT_FAILURE);
		}
		/*calculate kv_payload size*/
		task->msg_payload_size = put_msg_get_payload_size(task->msg);
		//log_debug("task msg paylaod size is %u", task->msg_payload_size);
		task->r_desc = regs_get_region_desc(region_server_desc, key, key_length);
		if (task->r_desc == NULL) {
			log_fatal("Region not found for key size key %u:%s", key_length, key);
			_exit(EXIT_FAILURE);
		}
		if (task->msg->msg_type == PUT_IF_EXISTS_REQUEST) {
			if (!regs_key_exists(task)) {
				log_warn("Key %.*s in update if exists for region %s not found!", key_length, key,
					 region_desc_get_id(task->r_desc));
				_exit(EXIT_FAILURE);
			}
		}
	}

	if (task->kreon_operation_status == TASK_START)
		task->kreon_operation_status = INS_TO_KREON;

	if (!region_desc_enter_parallax(task->r_desc, task))
		return;
	regs_insert_kv_pair(region_server_desc, task);
	// log_debug("task status is %d", task->kreon_operation_status);
	if (task->kreon_operation_status != TASK_COMPLETE)
		return;

	region_desc_leave_parallax(task->r_desc);

	/*prepare the reply*/
	task->reply_msg = (struct msg_header *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
						task->msg->offset_reply_in_recv_buffer);

	uint32_t actual_reply_size = sizeof(msg_header) + sizeof(msg_put_rep) + TU_TAIL_SIZE;
	if (task->msg->reply_length_in_recv_buffer >= actual_reply_size) {
		regs_fill_reply_header(task->reply_msg, task, sizeof(msg_put_rep), PUT_REPLY);
		msg_put_rep *put_rep = (msg_put_rep *)((char *)task->reply_msg + sizeof(msg_header));
		put_rep->status = TEBIS_SUCCESS;
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	} else {
		log_fatal("SERVER: mr CLIENT reply space not enough  size %" PRIu32 " FIX XXX TODO XXX\n",
			  task->msg->reply_length_in_recv_buffer);
		_exit(EXIT_FAILURE);
	}
}

void regs_execute_get_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(task->msg->msg_type == GET_REQUEST);
	struct msg_data_get_request request_data = get_request_get_msg_data(task->msg);
	struct region_desc *r_desc = regs_get_region_desc(region_server_desc, request_data.key, request_data.key_size);

	if (r_desc == NULL) {
		log_fatal("Region not found for key %s", request_data.key);
		_exit(EXIT_FAILURE);
	}

	task->kreon_operation_status = TASK_GET_KEY;
	task->r_desc = r_desc;
	if (!region_desc_enter_parallax(r_desc, task)) {
		// later...
		return;
	}
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);

	par_handle par_hd = region_desc_get_db(task->r_desc);
	struct par_value lookup_value = { .val_buffer = get_reply_get_kv_offset(task->reply_msg),
					  .val_buffer_size = request_data.bytes_to_read };
	const char *error_message = NULL;
	par_get_serialized(par_hd, get_msg_get_key_slice_t(task->msg), &lookup_value, &error_message);
	region_desc_leave_parallax(r_desc);

	if (error_message) {
		log_warn("key not found key %s : length %u", request_data.key, request_data.key_size);

		struct msg_data_get_reply reply_data = { 0 };
		create_get_reply_msg(reply_data, task->reply_msg);
		goto exit;
	}
	uint32_t offset = request_data.offset;
	uint32_t msg_bytes_to_read = request_data.bytes_to_read;
	int32_t fetch_value = request_data.fetch_value;
	// tranlate now
	if (offset > lookup_value.val_size) {
		struct msg_data_get_reply reply_data = { .key_found = 1,
							 .offset_too_large = 1,
							 .value_size = 0,
							 .value = NULL,
							 .bytes_remaining = lookup_value.val_size };
		create_get_reply_msg(reply_data, task->reply_msg);
		goto exit;
	}

	if (!fetch_value) {
		struct msg_data_get_reply reply_data = { .key_found = 1,
							 .offset_too_large = 0,
							 .value_size = 0,
							 .value = NULL,
							 .bytes_remaining = lookup_value.val_size - offset };
		create_get_reply_msg(reply_data, task->reply_msg);
		goto exit;
	}
	uint32_t value_bytes_remaining = lookup_value.val_size - offset;
	uint32_t bytes_to_read = value_bytes_remaining;
	bytes_to_read = value_bytes_remaining;
	int32_t bytes_remaining = 0;
	if (msg_bytes_to_read <= value_bytes_remaining) {
		bytes_to_read = msg_bytes_to_read;
		bytes_remaining = lookup_value.val_size - (offset + bytes_to_read);
	}

	struct msg_data_get_reply reply_data = { .key_found = 1,
						 .offset_too_large = 0,
						 .value_size = bytes_to_read,
						 .value = NULL,
						 .bytes_remaining = bytes_remaining };
	create_get_reply_msg(reply_data, task->reply_msg);

exit:;
	//finally fix the header
	uint32_t payload_length = get_reply_get_payload_size(task->reply_msg);

	regs_fill_reply_header(task->reply_msg, task, payload_length, GET_REPLY);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_multi_get_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_debug("Close scans since we dont use them for now (the tebis-parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
}

void regs_execute_delete_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_debug("Closing delete ops since we dont use them for now (tebis-parallax) replication");
	assert(0);
	_exit(EXIT_FAILURE);
}

void regs_execute_flush_command_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(task->msg->msg_type == FLUSH_COMMAND_REQ);
#if ENABLE_MONITORING
	struct timeval start;
	gettimeofday(&start, NULL);
#endif
	// log_debug("Primary orders a flush!");
	struct s2s_msg_flush_cmd_req *flush_req =
		(struct s2s_msg_flush_cmd_req *)((char *)task->msg + sizeof(struct msg_header));
	struct region_desc *r_desc =
		regs_get_region_desc(region_server_desc, flush_req->region_key, flush_req->region_key_size);
	// log_debug("Flushing region: %s with min key %s r_desc is %lu", region_desc_get_id((r_desc)),
	// 	  region_desc_get_min_key(r_desc), (unsigned long)r_desc);
	if (region_desc_get_replica_state(r_desc) == NULL) {
		log_fatal("No state for backup region %s", region_desc_get_id(r_desc));
		_exit(EXIT_FAILURE);
	}

	enum log_category log_type_to_flush = flush_req->log_type;
	struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
	uint32_t rdma_buffer_size = r_state->l0_recovery_rdma_buf.rdma_buf_size;

	// log_debug("primary offt %lu", flush_req->primary_segment_offt);
	if (!globals_get_send_index()) {
		build_index_add_buffer(region_desc_get_worker(r_desc), (char *)r_state->big_recovery_rdma_buf.mr->addr,
				       SEGMENT_SIZE);
		// build_index_procedure(r_desc);
		// region_desc_zero_rdma_buffer(r_desc, L0_RECOVERY);
		region_desc_zero_rdma_buffer(r_desc, BIG);
	} else {
		if (!region_desc_is_segment_in_logmap(r_desc, flush_req->primary_segment_offt)) {
			uint64_t replica_new_segment_offt = send_index_flush_rdma_buffer(
				r_desc, rdma_buffer_size, rdma_buffer_size, log_type_to_flush);
			region_desc_add_to_logmap(r_desc, flush_req->primary_segment_offt, replica_new_segment_offt);
			region_desc_zero_rdma_buffer(r_desc, log_type_to_flush);
		} else {
			/*L0 compaction flushed the segment before getting full, append only the part that was not replicated*/
			uint32_t rdma_segment_offt = flush_req->last_flushed_offt;
			if (rdma_segment_offt % ALIGNMENT_SIZE != 0)
				rdma_segment_offt -= rdma_segment_offt % ALIGNMENT_SIZE;
			uint32_t IO_size = rdma_buffer_size - rdma_segment_offt;
			uint32_t buf_size = rdma_buffer_size - flush_req->last_flushed_offt;
			struct db_handle *db = (struct db_handle *)region_desc_get_db(r_desc);
			if (log_type_to_flush == BIG) {
				char *rdma_buffer = (char *)r_state->big_recovery_rdma_buf.mr->addr;
				pr_flush_buffer_to_log(&db->db_desc->big_log, rdma_segment_offt, IO_size, rdma_buffer,
						       buf_size);
			} else if (log_type_to_flush == L0_RECOVERY) {
				char *rdma_buffer = (char *)r_state->l0_recovery_rdma_buf.mr->addr;
				pr_flush_buffer_to_log(&db->db_desc->small_log, rdma_segment_offt, IO_size, rdma_buffer,
						       buf_size);
			} else {
				log_fatal("Flushing medium log in flush commands should never happen");
				assert(0);
				_exit(EXIT_FAILURE);
			}
		}
	}

#if ENABLE_MONITORING
	struct timeval end;
	gettimeofday(&end, NULL);
	uint64_t diff = (end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec);
	region_desc_add_latency_sample(r_desc, diff);
	region_desc_print_latency_stats(r_desc);
#endif
	//time for reply :-)
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_flush_cmd_rep *flush_rep =
		(struct s2s_msg_flush_cmd_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	flush_rep->status = TEBIS_SUCCESS;
	flush_rep->uuid = flush_req->uuid;
	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_flush_cmd_rep), FLUSH_COMMAND_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_get_rdma_buffer_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(task->msg->msg_type == GET_RDMA_BUFFER_REQ);
	struct s2s_msg_get_rdma_buffer_req *get_log_buffer =
		(struct s2s_msg_get_rdma_buffer_req *)((char *)task->msg + sizeof(struct msg_header));

	mregion_t mregion =
		MREG_deserialize_region(get_log_buffer->mregion_buffer, get_log_buffer->mregion_buffer_size);
	region_desc_t region_desc = regs_get_region_desc(region_server_desc, MREG_get_region_min_key(mregion),
							 MREG_get_region_min_key_size(mregion));
	if (region_desc != NULL) {
		log_fatal("Region already present with id %s", MREG_get_region_id(mregion));
		_exit(EXIT_FAILURE);
	}

	region_desc = regs_open_region((struct regs_server_desc *)region_server_desc, mregion,
				       MREG_get_region_backup_role(mregion, get_log_buffer->backup_id));
	log_debug("I am a backup for region %s region_desc is %lu", region_desc_get_id(region_desc),
		  (unsigned long)region_desc);

	region_desc_init_replica_state(region_desc, get_log_buffer->buffer_size, task->conn->rdma_cm_id);
	region_desc_lock_region_mngmt(region_desc);

	region_desc_unlock_region_mngmt(region_desc);
	task->reply_msg = (void *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
				   task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_get_rdma_buffer_rep *rep =
		(struct s2s_msg_get_rdma_buffer_rep *)((char *)task->reply_msg + sizeof(msg_header));
	rep->status = TEBIS_SUCCESS;

	struct ru_replica_rdma_buffer *log_buffer = region_desc_get_backup_L0_log_buf(region_desc);
	rep->l0_recovery_mr = *log_buffer->mr;
	log_buffer = region_desc_get_backup_medium_log_buf(region_desc);
	rep->medium_recovery_mr = *log_buffer->mr;
	log_buffer = region_desc_get_backup_big_log_buf(region_desc);
	rep->big_recovery_mr = *log_buffer->mr;

	regs_fill_reply_header(task->reply_msg, task,
			       sizeof(struct s2s_msg_get_rdma_buffer_rep) + (sizeof(struct ibv_mr)),
			       GET_RDMA_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	log_debug("Initialized region: %s as backup", MREG_get_region_id(mregion));
	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_replica_index_get_buffer_req(struct regs_server_desc const *region_server_desc,
					       struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_GET_BUFFER_REQ);
	assert(globals_get_send_index());

	struct s2s_msg_replica_index_get_buffer_req *req =
		(struct s2s_msg_replica_index_get_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct region_desc *r_desc = regs_get_region_desc(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", req->region_key);
		_exit(EXIT_FAILURE);
	}

	log_debug("Starting compaction for dst level %u at region %s", req->level_id, region_desc_get_id(r_desc));

	//initialize and reg write buffers for recieving the primary's segments ready to be flushed.
	//The buffers follow Parallax compaction index
	struct send_index_create_compactions_rdma_buffer_params create_buffers_params = {
		.level_id = req->level_id,
		.conn = task->conn,
		.r_desc = r_desc,
		.number_of_rows = req->num_rows,
		.number_of_columns = req->num_cols,
		.size_of_entry = req->entry_size
	};

	send_index_create_compactions_rdma_buffer(create_buffers_params);
	// also initialize a buffer for sending the flush replies to the primary
	// we need to reg write this memory region since replicas rdma write segment flush replies into primary's status buffers
	struct send_index_create_mr_for_segment_replies_params create_flush_reply_mr_params = {
		.conn = task->conn, .level_id = req->level_id, .r_desc = r_desc
	};
	send_index_create_mr_for_segment_replies(create_flush_reply_mr_params);
	struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
	r_state->index_rewriter[req->level_id] = send_index_rewriter_init(r_desc);
	struct db_handle *db = (struct db_handle *)region_desc_get_db(r_desc);
	uint32_t src_level_id = req->level_id - 1;
	r_state->comp_req[req->level_id] = compaction_create_req(db->db_desc, &db->db_options, UINT64_MAX, UINT64_MAX,
								 src_level_id, req->tree_id, req->level_id, 1);
	//time for reply
	task->reply_msg = (struct msg_header *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
						task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_replica_index_get_buffer_rep *reply =
		(struct s2s_msg_replica_index_get_buffer_rep *)((char *)task->reply_msg + sizeof(msg_header));
	reply->mr = *r_state->index_buffer[req->level_id];
	reply->uuid = req->uuid;

	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_replica_index_get_buffer_rep),
			       REPLICA_INDEX_GET_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	log_debug("Registed a new memory region in region %s at offt %lu", region_desc_get_id(r_desc),
		  (uint64_t)reply->mr.addr);
	task->kreon_operation_status = TASK_COMPLETE;
}

/**
 * @brief Acknowledges a NO_OP operation. Client spins for server's reply.
 *  This operation happens only when there is no space in server's recv circular buffer for a client to
 * allocate and send its msg
 */
void regs_execute_no_op(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	(void)region_server_desc;
	assert(region_server_desc && task);
	assert(task->msg->msg_type == NO_OP);

	task->kreon_operation_status = TASK_NO_OP;
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];

	regs_fill_reply_header(task->reply_msg, task, 0, NO_OP_ACK);
	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_test_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	(void)region_server_desc;
	assert(region_server_desc);
	assert(task->msg->msg_type == TEST_REQUEST);
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	/*initialize message*/
	if (task->msg->reply_length_in_recv_buffer < TU_HEADER_SIZE) {
		log_fatal("CLIENT reply space not enough  size %" PRIu32 " FIX XXX TODO XXX\n",
			  task->msg->reply_length_in_recv_buffer);
		_exit(EXIT_FAILURE);
	}
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	regs_fill_reply_header(task->reply_msg, task, task->msg->payload_length, TEST_REPLY);
	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_replica_index_flush_req(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_FLUSH_REQ);
	assert(globals_get_send_index());

	struct s2s_msg_replica_index_flush_req *req =
		(struct s2s_msg_replica_index_flush_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	region_desc_t r_desc = regs_get_region_desc(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", req->region_key);
		_exit(EXIT_FAILURE);
	}
	uint64_t segment_offt = region_desc_get_indexmap_seg(r_desc, req->primary_segment_offt, req->level_id);
	struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
	if (!segment_offt) {
		segment_offt = wappender_allocate_space(r_state->wappender[req->level_id]);
		region_desc_add_to_indexmap(r_desc, req->primary_segment_offt, segment_offt, req->level_id);
	}

	char *rdma_buffer = r_state->index_buffer[req->level_id]->addr;
	uint32_t row_size = req->entry_size * req->number_of_columns;
	struct segment_header *inmem_segment =
		(struct segment_header *)&rdma_buffer[req->height * row_size + req->clock * req->entry_size];

	send_index_rewriter_rewrite_index(r_state->index_rewriter[req->level_id], r_desc, inmem_segment, req->level_id);

	struct wappender_append_index_segment_params flush_index_segment = { .buffer = (char *)inmem_segment,
									     .buffer_size = req->entry_size,
									     .segment_offt = segment_offt };
	wappender_append_index_segment(r_state->wappender[req->level_id], flush_index_segment);
	//rdma write to primary's status
	uint64_t reply_value = WCURSOR_STATUS_OK;

	char *reply_address = (char *)r_state->index_segment_flush_replies[req->level_id]->addr;
	memcpy(reply_address, &reply_value, sizeof(uint64_t));
	log_debug("Replying at primary offt %lu", (uint64_t)req->reply_offt);
	//rdma write it back to the primary's write cursor status buffers
	while (1) {
		int ret = rdma_post_write(task->conn->rdma_cm_id, NULL, reply_address, WCURSOR_ALIGNMNENT,
					  r_state->index_segment_flush_replies[req->level_id], IBV_SEND_SIGNALED,
					  (uint64_t)req->reply_offt, req->mr_of_primary.rkey);
		if (!ret)
			break;
	}

	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_test_req_fetch_payload(struct regs_server_desc const *mydesc, struct work_task *task)
{
	(void)mydesc;
	(void)task;
	assert(task->msg->msg_type == TEST_REQUEST_FETCH_PAYLOAD);
	log_fatal("This message is not supported yet...");
	_exit(EXIT_FAILURE);
}

void regs_execute_flush_L0_op(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == FLUSH_L0_REQUEST);
	assert(globals_get_send_index());
	struct s2s_msg_flush_L0_req *flush_req =
		(struct s2s_msg_flush_L0_req *)((char *)task->msg + sizeof(struct msg_header));
	struct region_desc *r_desc =
		regs_get_region_desc(region_server_desc, flush_req->region_key, flush_req->region_key_size);
	if (region_desc_get_replica_state(r_desc) == NULL) {
		log_fatal("No state for backup region %s", region_desc_get_id(r_desc));
		_exit(EXIT_FAILURE);
	}

	task->kreon_operation_status = TASK_FLUSH_L0;
	// persist the buffers
	uint32_t small_IO_size = flush_req->small_rdma_buffer_curr_end +
				 (ALIGNMENT_SIZE - (flush_req->small_rdma_buffer_curr_end % ALIGNMENT_SIZE));
	uint64_t new_small_log_tail =
		send_index_flush_rdma_buffer(r_desc, flush_req->small_rdma_buffer_curr_end, small_IO_size, L0_RECOVERY);
	log_debug("Flush L0: IO_size is %u, offt is at %lu", small_IO_size, flush_req->small_rdma_buffer_curr_end);
	uint32_t big_IO_size = flush_req->big_rdma_buffer_curr_end +
			       (ALIGNMENT_SIZE - (flush_req->big_rdma_buffer_curr_end % ALIGNMENT_SIZE));
	uint64_t new_big_small_log_tail =
		send_index_flush_rdma_buffer(r_desc, flush_req->big_rdma_buffer_curr_end, big_IO_size, BIG);

	par_flush_superblock(region_desc_get_db(r_desc));

	// append new segments to logmap
	region_desc_add_to_logmap(r_desc, flush_req->small_log_tail_dev_offt, new_small_log_tail);
	region_desc_add_to_logmap(r_desc, flush_req->big_log_tail_dev_offt, new_big_small_log_tail);

	// create and send the reply
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];
	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_flush_L0_rep), FLUSH_L0_REPLY);

	// for debugging purposes
	struct s2s_msg_flush_L0_rep *reply_payload =
		(struct s2s_msg_flush_L0_rep *)((char *)task->reply_msg + sizeof(struct msg_header));
	reply_payload->uuid = flush_req->uuid;
	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_send_index_close_compaction(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == CLOSE_COMPACTION_REQUEST);
	assert(globals_get_send_index());

	// get the region descriptor
	struct s2s_msg_close_compaction_request *req =
		(struct s2s_msg_close_compaction_request *)((char *)task->msg + sizeof(struct msg_header));
	struct region_desc *r_desc = regs_get_region_desc(region_server_desc, req->region_key, req->region_key_size);
	if (region_desc_get_replica_state(r_desc) == NULL) {
		log_fatal("No state for backup region %s", region_desc_get_id(r_desc));
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_CLOSE_COMPACTION;
	log_debug("Closing compaction for region %s level %u", region_desc_get_id(r_desc), req->level_id);
	send_index_translate_primary_metadata(r_desc, req->level_id, req->compaction_last_segment_offt,
					      req->compaction_first_segment_offt, req->new_root_offt);
	struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
	compaction_close(r_state->comp_req[req->level_id]);
	send_index_close_mr_for_segment_replies(r_desc, req->level_id);
	region_desc_free_indexmap(r_desc, req->level_id);
	send_index_rewriter_destroy(&r_state->index_rewriter[req->level_id]);
	send_index_close_compactions_rdma_buffer(r_desc, req->level_id);

	// create and send the reply
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];
	struct s2s_msg_close_compaction_reply *reply =
		(struct s2s_msg_close_compaction_reply *)((char *)task->reply_msg + sizeof(struct msg_header));
	reply->uuid = req->uuid;
	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_close_compaction_reply),
			       CLOSE_COMPACTION_REPLY);

	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_replica_index_swap_levels(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_SWAP_LEVELS_REQUEST);
	assert(globals_get_send_index());

	// get the region descriptor
	struct s2s_msg_swap_levels_request *req =
		(struct s2s_msg_swap_levels_request *)((char *)task->msg + sizeof(struct msg_header));
	struct region_desc *r_desc = regs_get_region_desc(region_server_desc, req->region_key, req->region_key_size);
	if (region_desc_get_replica_state(r_desc) == NULL) {
		log_fatal("No state for backup region %s", region_desc_get_id(r_desc));
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_CLOSE_COMPACTION;

	log_debug("Swap levels for region %s level %u", region_desc_get_id(r_desc), req->level_id);

	// create and send the reply
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];

	struct s2s_msg_swap_levels_reply *reply =
		(struct s2s_msg_swap_levels_reply *)((char *)task->reply_msg + sizeof(struct msg_header));
	reply->uuid = req->uuid;
	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_swap_levels_reply),
			       REPLICA_INDEX_SWAP_LEVELS_REPLY);

	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_flush_medium_log(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_FLUSH_MEDIUM_LOG_REQUEST);
	assert(globals_get_send_index());

	// get the region descriptor
	struct s2s_msg_replica_flush_medium_log_req *req =
		(struct s2s_msg_replica_flush_medium_log_req *)((char *)task->msg + sizeof(struct msg_header));
	struct region_desc *r_desc = regs_get_region_desc(region_server_desc, req->region_key, req->region_key_size);
	if (region_desc_get_replica_state(r_desc) == NULL) {
		log_fatal("No state for backup region %s", region_desc_get_id(r_desc));
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_FLUSH_MEDIUM_LOG;
	struct db_handle *dbhandle = (struct db_handle *)region_desc_get_db(r_desc);
	struct ru_replica_rdma_buffer *medium_log_buf = region_desc_get_backup_medium_log_buf(r_desc);
	uint64_t replica_medium_log_segment =
		region_desc_get_medium_log_segment_offt(r_desc, req->primary_segment_offt);
	pr_append_segment_to_log(&dbhandle->db_desc->medium_log, (char *)medium_log_buf->mr->addr,
				 replica_medium_log_segment);
	log_debug("Flushing a chunk of segment at offt %lu", replica_medium_log_segment);

	pr_flush_buffer_to_log(&dbhandle->db_desc->medium_log, req->IO_starting_offt, req->IO_size,
			       medium_log_buf->mr->addr, req->IO_size);

	//time for reply
	task->reply_msg = (struct msg_header *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
						task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_replica_flush_medium_log_rep *reply =
		(struct s2s_msg_replica_flush_medium_log_rep *)((char *)task->reply_msg + sizeof(msg_header));
	reply->uuid = req->uuid;

	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_replica_flush_medium_log_rep),
			       REPLICA_FLUSH_MEDIUM_LOG_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_compact_L0(struct regs_server_desc const *region_server_desc, struct work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == BUILD_INDEX_COMPACT_L0_REQUEST);
	assert(!globals_get_send_index());
	// get the region descriptor
	struct s2s_msg_compact_L0_request *req =
		(struct s2s_msg_compact_L0_request *)((char *)task->msg + sizeof(struct msg_header));
	struct region_desc *r_desc = regs_get_region_desc(region_server_desc, req->region_key, req->region_key_size);
	if (region_desc_get_replica_state(r_desc) == NULL) {
		log_fatal("No state for backup region %s", region_desc_get_id(r_desc));
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_COMPACT_L0;
	// struct db_handle *dbhandle = (struct db_handle *)region_desc_get_db(r_desc);
	// compactiond_force_L0_compaction(dbhandle->db_desc->compactiond);
	//time for reply
	task->reply_msg = (struct msg_header *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
						task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_compact_L0_reply *reply =
		(struct s2s_msg_compact_L0_reply *)((char *)task->reply_msg + sizeof(msg_header));
	reply->uuid = req->uuid;

	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_compact_L0_reply),
			       BUILD_INDEX_COMPACT_L0_REPLY);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

static struct connection_rdma *regs_get_conn(struct regs_server_desc const *region_server, char *hostname,
					     char *IP_address, struct regs_conn_per_server **root_map)
{
	struct regs_conn_per_server *connection = NULL;

	uint64_t server_key = djb2_hash((unsigned char *)hostname, strlen(hostname));
	HASH_FIND_PTR(*root_map, &server_key, connection);
	if (connection)
		return connection->conn;

	pthread_mutex_lock((pthread_mutex_t *)&region_server->conn_map_lock);
	log_debug("Creating connection for hostname: %s with IP address: %s", hostname, IP_address);
	connection = calloc(1UL, sizeof(struct regs_conn_per_server));
	connection->conn = crdma_client_create_connection_list_hosts(ds_get_channel(region_server), &IP_address, 1,
								     MASTER_TO_REPLICA_CONNECTION);

	/*init list here*/
	connection->server_key = server_key;
	HASH_ADD_PTR(*root_map, server_key, connection);
	pthread_mutex_unlock((pthread_mutex_t *)&region_server->conn_map_lock);

	return connection->conn;
}

struct connection_rdma *regs_get_data_conn(struct regs_server_desc const *region_server, char *hostname,
					   char *IP_address)
{
	return regs_get_conn(region_server, hostname, IP_address,
			     (struct regs_conn_per_server **)&region_server->root_data_conn_map);
}

struct connection_rdma *regs_get_compaction_conn(struct regs_server_desc *region_server, char *hostname,
						 char *IP_address)
{
	return regs_get_conn(region_server, hostname, IP_address,
			     (struct regs_conn_per_server **)&region_server->root_compaction_conn_map);
}
