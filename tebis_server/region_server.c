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
#include "../tebis_rdma_client/msg_factory.h"
#include "../utilities/spin_loop.h"
#include "allocator/volume_manager.h"
#include "build_index/build_index.h"
#include "djb2.h"
#include "globals.h"
#include "list.h"
#include "master/command.h"
#include "metadata.h"
#include "send_index/send_index.h"
#include "send_index/send_index_uuid_checker.h"
#include "zk_utils.h"
#include <arpa/inet.h>
#include <assert.h>
#include <btree/gc.h>
#include <btree/kv_pairs.h>
#include <cJSON.h>
#include <ifaddrs.h>
#include <include/parallax/parallax.h>
#include <libgen.h>
#include <log.h>
#include <pthread.h>
#include <rdma/rdma_verbs.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
uint64_t ds_hash_key;

static int regs_enter_parallax(struct krm_region_desc *r_desc, struct krm_work_task *task)
{
	if (r_desc == NULL) {
		log_fatal("NULL region?");
		_exit(EXIT_FAILURE);
	}
	if (r_desc->region->num_of_backup == 0)
		return 1;

	pthread_rwlock_rdlock(&r_desc->kreon_lock);
	if (r_desc->status == KRM_HALTED) {
		int ret;
		switch (task->kreon_operation_status) {
		case TASK_GET_KEY:
		case TASK_MULTIGET:
		case TASK_DELETE_KEY:
		case INS_TO_KREON:
			// log_info("Do not enter Kreon task status is %d region status =
			// %d",task->kreon_operation_status,r_desc->status);
			ret = 0;
			break;
		case REPLICATE:
		case WAIT_FOR_REPLICATION_COMPLETION:
		case ALL_REPLICAS_ACKED:
		case SEND_FLUSH_COMMANDS:
		case WAIT_FOR_FLUSH_REPLIES:
			ret = 1;
			break;
		default:
			log_fatal("Unhandled state");
			_exit(EXIT_FAILURE);
		}
		pthread_rwlock_unlock(&r_desc->kreon_lock);
		return ret;
	} else {
		switch (task->kreon_operation_status) {
		case TASK_GET_KEY:
		case TASK_MULTIGET:
		case TASK_DELETE_KEY:
		case INS_TO_KREON:
			__sync_fetch_and_add(&r_desc->pending_region_tasks, 1);
			break;
		case REPLICATE:
		case WAIT_FOR_REPLICATION_TURN:
		case WAIT_FOR_REPLICATION_COMPLETION:
		case ALL_REPLICAS_ACKED:
		case SEND_FLUSH_COMMANDS:
		case WAIT_FOR_FLUSH_REPLIES:
			break;
		default:
			log_fatal("Unhandled state");
			_exit(EXIT_FAILURE);
		}
		pthread_rwlock_unlock(&r_desc->kreon_lock);
		return 1;
	}
}

static void regs_leave_parallax(struct krm_region_desc *r_desc)
{
	if (r_desc->region->num_of_backup == 0)
		return;
	__sync_fetch_and_sub(&r_desc->pending_region_tasks, 1);
}

char *regs_get_server_name(struct regs_server_desc *region_server)
{
	return region_server->name.kreon_ds_hostname;
}

static void *regs_run_region_server(void *args);

struct regs_server_desc *regs_create_server(void)
{
	return calloc(1UL, sizeof(struct regs_server_desc));
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

par_handle open_db(const char *path)
{
	disable_gc();
	par_db_options db_options = { .volume_name = (char *)path,
				      .create_flag = PAR_CREATE_DB,
				      .db_name = "tebis_storage_engine",
				      .options = par_get_default_options() };
	const char *error_message = NULL;
	par_handle handle = par_open(&db_options, &error_message);
	if (error_message) {
		log_fatal("Error uppon opening the DB, error %s", error_message);
		_exit(EXIT_FAILURE);
	}
	return handle;
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
		log_info("RDMA IP prefix %s match for Interface: %s Full IP Address: %s", globals_get_RDMA_IP_filter(),
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

#if 0
static void krm_resend_open_command(struct krm_server_desc *desc, struct krm_region *region, char *kreon_ds_hostname,
				    enum krm_msg_type type)
{
	int mail_id_len = 128;
	char mail_id[128];
	struct krm_msg msg;
	char *path =
		zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, kreon_ds_hostname, KRM_MAIL_TITLE);

	assert(type == KRM_OPEN_REGION_AS_PRIMARY || type == KRM_OPEN_REGION_AS_BACKUP);
	msg.type = type;
	msg.region = *region;
	strcpy(msg.sender, desc->name.kreon_ds_hostname);
	msg.epoch = desc->name.epoch;

	int rc = zoo_create(desc->zh, path, (char *)&msg, sizeof(struct krm_msg), &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE,
			    mail_id, mail_id_len);
	if (rc != ZOK) {
		log_fatal("failed to send open region command to path %s with error code %s", path, zku_op2String(rc));
		_exit(EXIT_FAILURE);
	}
	free(path);
}


static void krm_send_open_command(struct krm_server_desc *desc, struct krm_region *region)
{
	int rc;
	uint32_t i;
	struct krm_msg msg;
	char *path;
	struct krm_leader_ds_map *dataserver;
	struct krm_leader_ds_region_map *region_map;
	int mail_id_len = 128;
	char mail_id[128];

	/*check if I, aka the Leader, am the Primary for this region*/
	if (strcmp(region->primary.kreon_ds_hostname, desc->name.kreon_ds_hostname) == 0) {
		/*added to the dataservers table, I ll open them later*/
		ds_hash_key = djb2_hash((unsigned char *)region->primary.kreon_ds_hostname,
					strlen(region->primary.kreon_ds_hostname));
		dataserver = NULL;
		HASH_FIND_PTR(desc->dataservers_map, &ds_hash_key, dataserver);
		if (dataserver == NULL) {
			log_fatal("entry missing for DataServer (which is me?) %s", region->primary.kreon_ds_hostname);
			_exit(EXIT_FAILURE);
		}
		region_map = init_region_map(region, KRM_PRIMARY);
		log_debug("Adding region %s (As a primary) for server %s hash key %lu", region->id,
			  dataserver->server_id.kreon_ds_hostname, dataserver->hash_key);
		HASH_ADD_PTR(dataserver->region_map, hash_key, region_map);
	} else {
		path = zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
					  region->primary.kreon_ds_hostname, KRM_MAIL_TITLE);

		msg.type = KRM_OPEN_REGION_AS_PRIMARY;
		msg.region = *region;
		strcpy(msg.sender, desc->name.kreon_ds_hostname);
		/*fill the epoch which we think the dataserver is*/
		dataserver = NULL;
		ds_hash_key = djb2_hash((unsigned char *)region->primary.kreon_ds_hostname,
					strlen(region->primary.kreon_ds_hostname));
		HASH_FIND_PTR(desc->dataservers_map, &ds_hash_key, dataserver);
		if (dataserver == NULL) {
			log_fatal("entry missing for DataServer %s", region->primary.kreon_ds_hostname);
			_exit(EXIT_FAILURE);
		}
		msg.epoch = dataserver->server_id.epoch;
		region_map = init_region_map(region, KRM_PRIMARY);
		log_info("Adding region %s (As a primary) for server %s hash key %lu", region->id,
			 region->primary.kreon_ds_hostname, region_map->hash_key);
		HASH_ADD_PTR(dataserver->region_map, hash_key, region_map);
		log_info("Sending open command (as primary) to %s", path);

		rc = zoo_create(desc->zh, path, (char *)&msg, sizeof(struct krm_msg), &ZOO_OPEN_ACL_UNSAFE,
				ZOO_SEQUENCE, mail_id, mail_id_len);

		if (rc != ZOK) {
			log_fatal("failed to send open region command to path %s with error code %s", path,
				  zku_op2String(rc));
			_exit(EXIT_FAILURE);
		}

		free(path);
	}
	/*The same procedure for backups*/
	for (i = 0; i < region->num_of_backup; i++) {
		/*check if I, aka the Leader, am a BackUp for this region*/
		if (strcmp(region->backups[i].kreon_ds_hostname, desc->name.kreon_ds_hostname) == 0) {
			log_info("Kreon master Sending open region as backup to myself %s for "
				 "region %s",
				 desc->name.kreon_ds_hostname, region->id);
			dataserver = NULL;
			/*added to the dataservers table, I ll open them later*/
			uint64_t hash_key = djb2_hash((unsigned char *)region->backups[i].kreon_ds_hostname,
						      strlen(region->backups[i].kreon_ds_hostname));
			HASH_FIND_PTR(desc->dataservers_map, &hash_key, dataserver);
			if (dataserver == NULL) {
				log_fatal("entry missing for DataServer (which is me?) %s",
					  region->primary.kreon_ds_hostname);
				_exit(EXIT_FAILURE);
			}
			region_map = init_region_map(region, KRM_BACKUP);
			log_info("Adding region %s (As a backup) for server %s hash key %lu", region->id, /*  */
				 region->backups[i].kreon_ds_hostname, region_map->hash_key);
			HASH_ADD_PTR(dataserver->region_map, hash_key, region_map);
		} else {
			path = zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
						  region->backups[i].kreon_ds_hostname, KRM_MAIL_TITLE);

			msg.type = KRM_OPEN_REGION_AS_BACKUP;
			msg.region = *region;
			strcpy(msg.sender, desc->name.kreon_ds_hostname);
			dataserver = NULL;
			uint64_t hash_key = djb2_hash((unsigned char *)region->backups[i].kreon_ds_hostname,
						      strlen(region->backups[i].kreon_ds_hostname));
			HASH_FIND_PTR(desc->dataservers_map, &hash_key, dataserver);
			if (dataserver == NULL) {
				log_fatal("entry missing for DataServer %s", region->backups[i].kreon_ds_hostname);
				_exit(EXIT_FAILURE);
			}
			region_map = init_region_map(region, KRM_BACKUP);
			log_info("Adding region %s (As a backup) for server %s hash key %lu", region->id,
				 region->backups[i].kreon_ds_hostname, region_map->hash_key);
			HASH_ADD_PTR(dataserver->region_map, hash_key, region_map);
			msg.epoch = dataserver->server_id.epoch;

			rc = zoo_create(desc->zh, path, (char *)&msg, sizeof(struct krm_msg), &ZOO_OPEN_ACL_UNSAFE,
					ZOO_SEQUENCE, mail_id, mail_id_len);
			if (rc != ZOK) {
				log_fatal("failed to send open region command to path %s with error code %s", path,
					  zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			free(path);
		}
	}
}
#endif
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
 * Searches for a region in the region table of the Region Server. If it finds it returns the position and sets found flag to true. If it does not is sets
 * found to false and returns the position in the array that shoud be present. Caution in case where the region array is empty it returns -1 as its position.
 */
static int get_region_pos(struct krm_ds_regions const *ds_regions, char *key, uint32_t key_size, bool *found)
{
	*found = false;
	if (ds_regions->num_ds_regions)
		return -1;

	int start_idx = 0;
	int end_idx = ds_regions->num_ds_regions - 1;
	/*log_info("start %d end %d", start_idx, end_idx);*/
	int middle = 0;
	int ret_code = 0;

	while (start_idx <= end_idx) {
		middle = (start_idx + end_idx) / 2;
		int ret_code = zku_key_cmp(ds_regions->r_desc[middle]->region->min_key_size,
					   ds_regions->r_desc[middle]->region->min_key, key_size, key);
		if (0 == ret_code) {
			*found = true;
			return middle;
		}
		if (ret_code > 0)
			start_idx = middle + 1;
		else
			end_idx = middle - 1;
	}

	return ret_code < 0 ? --middle : middle;
}

#if 0
/**
  * Inserts a region in the region table. If the region is already present it reports an error.
  * @param server_desc the Region Server descriptor
  * @param r_desc the descriptor of the new region
  * @
  */
static int krm_insert_ds_region(struct krm_ds_regions *region_table, struct krm_region_desc *region_desc)
{
	/*XXX TODO XXX needs a lock, remove lamprot counter*/
	bool found = false;
	int pos = get_region_pos(region_table, region_desc->region->min_key, region_desc->region->min_key_size, &found);

	if (found) {
		log_fatal("Region with min key %.*s already present", region_desc->region->min_key_size,
			  region_desc->region->min_key);
		_exit(EXIT_FAILURE);
	}

	memmove(&region_table->r_desc[pos + 2], &region_table->r_desc[pos + 1],
		(region_table->num_ds_regions - (pos + 1)) * sizeof(struct krm_region_desc *));
	region_table->r_desc[pos + 1] = region_desc;
	++region_table->num_ds_regions;
	return KRM_SUCCESS;
	/**XXX NEEDS UNLOCK remove lamport counters***/
}
#endif

/**
 * Returns r_desc that key should be hosted. Returns NULL if region_table is empty
 */
struct krm_region_desc *regs_get_region(struct regs_server_desc const *region_server, char *key, uint32_t key_size)
{
	bool found = false;
	int pos = get_region_pos(region_server->ds_regions, key, key_size, &found);
	if (-1 == pos) {
		log_warn("Querying an empty region table returning NULL");
		return NULL;
	}
	return region_server->ds_regions->r_desc[pos];
}

static struct krm_region_desc *open_region(struct regs_server_desc *region_server, struct krm_region *new_region_info,
					   enum krm_region_role server_role)
{
	(void)region_server;
	struct krm_region_desc *r_desc = (struct krm_region_desc *)calloc(1, sizeof(struct krm_region_desc));

	struct krm_region *region = (struct krm_region *)calloc(1, sizeof(struct krm_region));
	*region = *new_region_info;
	r_desc->region = region;
	r_desc->role = server_role;

	r_desc->replica_buf_status = KRM_BUFS_UNINITIALIZED;
	pthread_mutex_init(&r_desc->region_mgmnt_lock, NULL);
	r_desc->pending_region_tasks = 0;
	if (RWLOCK_INIT(&r_desc->kreon_lock, NULL) != 0) {
		log_fatal("Failed to init region read write lock");
		_exit(EXIT_FAILURE);
	}

	if (RWLOCK_INIT(&r_desc->replica_log_map_lock, NULL) != 0) {
		log_fatal("Failed to init replica log map lock");
		_exit(EXIT_FAILURE);
	}

	r_desc->status = KRM_OPEN;
	/*We have performed calloc so the code below is useless*/
	r_desc->replica_log_map = NULL;
#if 0
  for (int i = 0; i < MAX_LEVELS; i++)
		r_desc->replica_index_map[i] = NULL;

	r_desc->db = custom_db_open(globals_get_dev(), 0, globals_get_dev_size(), region->id, CREATE_DB,
				    globals_get_l0_size(), globals_get_growth_factor());

	krm_insert_ds_region(region_server->ds_regions, r_desc);
	if (KRM_PRIMARY == server_role)
		return r_desc;
	log_info("Setting DB %s in replicated mode", r_desc->db->db_desc->db_name);
	bt_set_db_in_replicated_mode(r_desc->db);
	set_init_index_transfer(r_desc->db->db_desc, &rco_init_index_transfer);
	set_destroy_local_rdma_buffer(r_desc->db->db_desc, &rco_destroy_local_rdma_buffer);
	set_send_index_segment_to_replicas(r_desc->db->db_desc, &rco_send_index_segment_to_replicas);
	bt_set_flush_replicated_logs_callback(r_desc->db->db_desc, rco_flush_last_log_segment);
	rco_add_db_to_pool(region_server->compaction_pool, r_desc);
#endif
	return r_desc;
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
	free(zk_path);
	if (ret_code != ZOK)
		return ret_code;

	// Parse json string with server's krm_server_name struct
	cJSON *json = cJSON_ParseWithLength(buffer, buffer_len);
	if (!cJSON_IsObject(json)) {
		cJSON_Delete(json);
		return -1;
	}

	cJSON *hostname = cJSON_GetObjectItem(json, "hostname");
	cJSON *dataserver_name_retrieved = cJSON_GetObjectItem(json, "dataserver_name");
	cJSON *rdma_ip = cJSON_GetObjectItem(json, "rdma_ip_addr");
	cJSON *epoch = cJSON_GetObjectItem(json, "epoch");
	cJSON *leader = cJSON_GetObjectItem(json, "leader");
	if (!cJSON_IsString(hostname) || !cJSON_IsString(dataserver_name_retrieved) || !cJSON_IsString(rdma_ip) ||
	    !cJSON_IsNumber(epoch) || !cJSON_IsString(leader)) {
		cJSON_Delete(json);
		return -1;
	}
	strncpy((char *)region_server_desc->name.hostname, cJSON_GetStringValue(hostname), KRM_HOSTNAME_SIZE);
	strncpy((char *)region_server_desc->name.kreon_ds_hostname, cJSON_GetStringValue(dataserver_name_retrieved),
		KRM_HOSTNAME_SIZE);
	server_info->kreon_ds_hostname_length = strlen(cJSON_GetStringValue(dataserver_name_retrieved));
	strncpy(server_info->RDMA_IP_addr, cJSON_GetStringValue(rdma_ip), KRM_MAX_RDMA_IP_SIZE);
	server_info->epoch = cJSON_GetNumberValue(epoch);
	strncpy(region_server_desc->name.kreon_leader, cJSON_GetStringValue(leader), KRM_HOSTNAME_SIZE);

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

static void regs_handle_master_command(struct regs_server_desc *region_server, MC_command_t command)
{
	(void)region_server;
	log_debug("Region server: %s got the following command", regs_get_server_name(region_server));
	MC_print_command(command);
	if (!region_server)
		open_region(NULL, NULL, 0);

	switch (MC_get_command_code(command)) {
	case OPEN_REGION_START:
		log_debug("RegionServer: %s opening region: %s", regs_get_server_name(region_server),
			  MC_get_region_id(command));
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
		char buffer[512] = { 0 };
		int buf_len = sizeof(buffer);
		struct Stat stat = { 0 };
		int ret_code = zoo_get(zkh, command_path, 0, (char *)buffer, &buf_len, &stat);
		if (ret_code != ZOK) {
			log_fatal("Error could not get command from path %s reason: %s", command_path,
				  zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		MC_command_t command = MC_deserialize_command(buffer, sizeof(buffer));
		regs_handle_master_command(server, command);
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
	regs_update_server_info(region_server, &server_info);
	++region_server->name.epoch;

	char tebis_name[KRM_HOSTNAME_SIZE] = { 0 };
	if (snprintf(tebis_name, KRM_HOSTNAME_SIZE, "%s:%lu", region_server->name.kreon_ds_hostname, epoch) < 0) {
		log_fatal("Failed to create region server Tebis name");
		_exit(EXIT_FAILURE);
	}
	memcpy(region_server->name.kreon_ds_hostname, tebis_name, KRM_HOSTNAME_SIZE);
	regs_get_IP_addresses(region_server);
	log_debug("Everything ok I decided that my Tebis name is %s", regs_get_server_name(region_server));
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
	/*init ds_regions table*/
	server->ds_regions = (struct krm_ds_regions *)calloc(1, sizeof(struct krm_ds_regions));
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
	pthread_setname_np(pthread_self(), "rserverd");
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

	region_server->state = KRM_BOOTING;

	while (1) {
		struct klist_node *node = NULL;

		pthread_mutex_lock(&region_server->msg_list_lock);
		node = tebis_klist_remove_first(region_server->msg_list);
		pthread_mutex_unlock(&region_server->msg_list_lock);
		if (!node) {
			sem_wait(&region_server->wake_up);
			continue;
		}
		region_server->state = KRM_PROCESSING_MSG;
		regs_process_command(region_server, (struct krm_msg *)node->data);
		free(node->data);
		free(node);
	}
	return NULL;
}

static void regs_send_get_rdma_buffers_requests(struct krm_region_desc *r_desc, struct regs_server_desc const *server)
{
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		struct connection_rdma *conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);

		if (r_desc->m_state->primary_to_backup[i].stat == RU_BUFFER_UNINITIALIZED) {
			log_debug("Sending GET_RDMA_BUFFERs req to Server %s",
				  r_desc->region->backups[i].kreon_ds_hostname);

			r_desc->m_state->primary_to_backup[i].msg_pair = sc_allocate_rpc_pair(
				conn, sizeof(struct s2s_msg_get_rdma_buffer_req) + r_desc->region->min_key_size,
				sizeof(struct s2s_msg_get_rdma_buffer_rep) + (sizeof(struct ibv_mr)),
				GET_RDMA_BUFFER_REQ);

			if (r_desc->m_state->primary_to_backup[i].msg_pair.stat != ALLOCATION_IS_SUCCESSFULL)
				continue;

			/*inform the req about its buddy*/
			msg_header *req_header = r_desc->m_state->primary_to_backup[i].msg_pair.request;
			msg_header *rep_header = r_desc->m_state->primary_to_backup[i].msg_pair.reply;
			req_header->triggering_msg_offset_in_send_buffer =
				real_address_to_triggering_msg_offt(conn, req_header);
			/*location where server should put the reply*/
			req_header->offset_reply_in_recv_buffer =
				(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
			req_header->reply_length_in_recv_buffer =
				sizeof(msg_header) + rep_header->payload_length + rep_header->padding_and_tail_size;
			/*time to send the message*/
			req_header->session_id = (uint64_t)r_desc->region;
			struct s2s_msg_get_rdma_buffer_req *g_req =
				(struct s2s_msg_get_rdma_buffer_req *)((char *)req_header + sizeof(struct msg_header));
			g_req->buffer_size = SEGMENT_SIZE;
			g_req->region_key_size = r_desc->region->min_key_size;
			strcpy(g_req->region_key, r_desc->region->min_key);
			__send_rdma_message(conn, req_header, NULL);

			r_desc->m_state->primary_to_backup[i].stat = RU_BUFFER_REQUESTED;
		}
	}
}

static uint8_t regs_key_exists(struct krm_work_task *task)
{
	assert(task);
	par_handle par_hd = (par_handle)task->r_desc->db;
	struct par_key pkey = { 0 };
	pkey.size = kv_splice_get_key_size(task->kv);
	pkey.data = kv_splice_get_key_offset_in_kv(task->kv);
	if (par_exists(par_hd, &pkey) == PAR_KEY_NOT_FOUND)
		return 0;

	return 1;
}

static void regs_initialize_rdma_buf_metadata(struct ru_master_log_buffer *rdma_buf, uint32_t backup_id,
					      struct s2s_msg_get_rdma_buffer_rep *rep,
					      enum tb_rdma_buf_category rdma_buf_cat)
{
	rdma_buf->segment_size = SEGMENT_SIZE;
	rdma_buf->segment.start = 0;
	rdma_buf->segment.end = SEGMENT_SIZE;
	rdma_buf->segment.curr_end = 0;
	rdma_buf->segment.mr[backup_id] = rep->l0_recovery_mr;
	if (rdma_buf_cat == TEBIS_BIG_RECOVERY_RDMA_BUF)
		rdma_buf->segment.mr[backup_id] = rep->big_recovery_mr;
	rdma_buf->segment.replicated_bytes = 0;
	assert(rdma_buf->segment.mr[backup_id].length == SEGMENT_SIZE);
}

static uint32_t regs_got_all_get_rdma_buffers_replies(struct krm_region_desc *r_desc)
{
	/*check replies*/
	uint32_t ready_buffers = 0;
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		if (r_desc->m_state->primary_to_backup[i].stat == RU_BUFFER_REQUESTED) {
			/*check reply and process
			 *wait first for the header and then the payload
			*/
			if (r_desc->m_state->primary_to_backup[i].msg_pair.reply->receive != TU_RDMA_REGULAR_MSG)
				continue;
			/*Check arrival of payload*/
			uint8_t tail = get_receive_field(r_desc->m_state->primary_to_backup[i].msg_pair.reply);
			if (tail != TU_RDMA_REGULAR_MSG)
				continue;

			struct s2s_msg_get_rdma_buffer_rep *rep =
				(struct s2s_msg_get_rdma_buffer_rep *)(((char *)r_desc->m_state->primary_to_backup[i]
										.msg_pair.reply) +
								       sizeof(struct msg_header));
			assert(rep->status == TEBIS_SUCCESS);

			regs_initialize_rdma_buf_metadata(&r_desc->m_state->l0_recovery_rdma_buf, i, rep,
							  TEBIS_L0_RECOVERY_RDMA_BUF);
			regs_initialize_rdma_buf_metadata(&r_desc->m_state->big_recovery_rdma_buf, i, rep,
							  TEBIS_BIG_RECOVERY_RDMA_BUF);

			r_desc->m_state->primary_to_backup[i].stat = RU_BUFFER_OK;
			/*finally free the message*/
			sc_free_rpc_pair(&r_desc->m_state->primary_to_backup[i].msg_pair);
		}
		if (r_desc->m_state->primary_to_backup[i].stat == RU_BUFFER_OK)
			++ready_buffers;
	}

	if (ready_buffers != r_desc->region->num_of_backup) {
		// log_info("Not all replicas ready waiting status %d",
		// task->kreon_operation_status);
		return 0;
	}
	return 1;
}
static int regs_init_replica_connections(struct regs_server_desc const *server, struct krm_work_task *task)
{
	while (1) {
		switch (task->kreon_operation_status) {
		case TASK_START: {
			task->kreon_operation_status = GET_RSTATE;
			break;
		}
		case GET_RSTATE: {
			if (task->r_desc->region->num_of_backup) {
				if (task->r_desc->replica_buf_status == KRM_BUFS_READY) {
					task->kreon_operation_status = INS_TO_KREON;
					return 1;
				}

				pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
				if (task->r_desc->replica_buf_status == KRM_BUFS_INITIALIZING) {
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					return 0;
				}
				if (task->r_desc->replica_buf_status == KRM_BUFS_UNINITIALIZED) {
					/*log_info("Initializing log buffers with replicas for DB %s",
						 task->r_desc->db->db_desc->db_name);*/
					task->r_desc->replica_buf_status = KRM_BUFS_INITIALIZING;
					task->kreon_operation_status = INIT_LOG_BUFFERS;
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
				} else {
					task->kreon_operation_status = INS_TO_KREON;
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					return 1;
				}
			} else {
				task->kreon_operation_status = INS_TO_KREON;
				return 1;
			}
			break;
		}
		case INIT_LOG_BUFFERS: {
			struct krm_region_desc *r_desc = task->r_desc;
			if (r_desc->m_state == NULL)
				r_desc->m_state = (struct ru_master_state *)calloc(1, sizeof(struct ru_master_state));

			regs_send_get_rdma_buffers_requests(r_desc, server);
			if (!regs_got_all_get_rdma_buffers_replies(r_desc))
				return 0;

			log_info("Success RDMA buffers initialized for all replicas of region %s", r_desc->region->id);
			task->r_desc->replica_buf_status = KRM_BUFS_READY;
			task->kreon_operation_status = INS_TO_KREON;
			return 1;
		}
		default:
			return 1;
		}
	}
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

void regs_insert_kv_to_store(struct krm_work_task *task)
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

	/*insert kv to data store*/
	const char *error_message = NULL;
	struct par_put_metadata metadata = par_put_serialized(task->r_desc->db, (char *)task->kv, &error_message);
	if (error_message) {
		regs_leave_parallax(task->r_desc);
		return;
	}
	task->insert_metadata = metadata;

	/*replication path*/
	if (task->r_desc->region->num_of_backup) {
		regs_fill_replication_fields(task->msg, metadata);
		task->kv_category = TEBIS_SMALLORMEDIUM;
		if (metadata.key_value_category == BIG_INLOG)
			task->kv_category = TEBIS_BIG;

		task->kreon_operation_status = WAIT_FOR_REPLICATION_TURN;
	} else
		task->kreon_operation_status = TASK_COMPLETE;
}

static void regs_fill_flush_request(struct krm_region_desc *r_desc, struct s2s_msg_flush_cmd_req *flush_request,
				    struct krm_work_task *task)
{
	flush_request->primary_segment_offt = task->insert_metadata.flush_segment_offt;
	flush_request->log_type = task->insert_metadata.log_type;
	flush_request->region_key_size = r_desc->region->min_key_size;
	strcpy(flush_request->region_key, r_desc->region->min_key);
	flush_request->uuid = (uint64_t)flush_request;
}

void regs_send_flush_commands(struct regs_server_desc const *server, struct krm_work_task *task)
{
	//log_debug("Send flush commands");
	struct krm_region_desc *r_desc = task->r_desc;

	for (uint32_t i = task->last_replica_to_ack; i < r_desc->region->num_of_backup; ++i) {
		if (r_desc->m_state->flush_cmd[i].stat == RU_BUFFER_UNINITIALIZED) {
			/*allocate and send command*/
			struct connection_rdma *r_conn =
				sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
			/*send flush req and piggyback it with the seg id num*/
			uint32_t req_size = sizeof(struct s2s_msg_flush_cmd_req) + r_desc->region->min_key_size;
			uint32_t rep_size = sizeof(struct s2s_msg_flush_cmd_rep);
			r_desc->m_state->flush_cmd[i].msg_pair =
				sc_allocate_rpc_pair(r_conn, req_size, rep_size, FLUSH_COMMAND_REQ);

			if (r_desc->m_state->flush_cmd[i].msg_pair.stat != ALLOCATION_IS_SUCCESSFULL) {
				task->last_replica_to_ack = i;
				return;
			}

			msg_header *req_header = r_desc->m_state->flush_cmd[i].msg_pair.request;

			//time to send the message
			req_header->session_id = (uint64_t)task->r_desc->region;
			struct s2s_msg_flush_cmd_req *f_req =
				(struct s2s_msg_flush_cmd_req *)((char *)req_header + sizeof(struct msg_header));
			regs_fill_flush_request(r_desc, f_req, task);
			__send_rdma_message(r_conn, req_header, NULL);
			r_desc->m_state->primary_to_backup[i].stat = RU_BUFFER_REQUESTED;
			// log_info("Sent flush command req_header %llu", req_header);
		}
	}
	task->last_replica_to_ack = 0;
	task->kreon_operation_status = WAIT_FOR_FLUSH_REPLIES;
}

void regs_wait_for_flush_replies(struct krm_work_task *task)
{
	//log_debug("wait for flush replies");
	struct krm_region_desc *r_desc = task->r_desc;
	for (uint32_t i = task->last_replica_to_ack; i < r_desc->region->num_of_backup; ++i) {
		/*check if header has arrived*/
		msg_header *reply = r_desc->m_state->flush_cmd[i].msg_pair.reply;

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
	pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
		// re-zero the metadat of the flushed segment
		struct ru_master_log_buffer_seg *flushed_segment = &r_desc->m_state->l0_recovery_rdma_buf.segment;
		if (task->insert_metadata.log_type == BIG)
			flushed_segment = &r_desc->m_state->big_recovery_rdma_buf.segment;

		flushed_segment->curr_end = 0;
		flushed_segment->replicated_bytes = 0;
		//for debuging purposes
		send_index_uuid_checker_validate_uuid(&r_desc->m_state->flush_cmd[i].msg_pair, FLUSH_COMMAND_REQ);
		/*free the flush msg*/
		sc_free_rpc_pair(&r_desc->m_state->flush_cmd[i].msg_pair);
	}
	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
	task->last_replica_to_ack = 0;
	task->kreon_operation_status = REPLICATE;
}

inline static uint8_t regs_buffer_have_enough_space(struct ru_master_log_buffer *r_buf, struct krm_work_task *task)
{
	/*if (task->kv_category == TEBIS_BIG)
		log_debug("[current end of big buf %lu end %lu]", r_buf->segment.curr_end, r_buf->segment.end);
	else
		log_debug("[current end of small buf %lu end %lu]", r_buf->segment.curr_end, r_buf->segment.end);
	put_msg_print_msg(task->msg);
	*/
	if (r_buf->segment.curr_end >= r_buf->segment.start &&
	    r_buf->segment.curr_end + task->msg_payload_size < r_buf->segment.end)
		return 1;
	return 0;
}

static void regs_wait_for_replication_turn(struct krm_work_task *task)
{
	assert(task);
	int64_t lsn = get_lsn_id(put_msg_get_lsn_offset(task->msg));
	if (lsn != task->r_desc->next_lsn_to_be_replicated)
		return; /*its not my turn yet*/

	/*only 1 threads enters this region at a time*/
	/*find which rdma_buffer must be appended*/
	struct ru_master_state *primary = task->r_desc->m_state;
	struct ru_master_log_buffer *rdma_buffer_to_fill = &primary->l0_recovery_rdma_buf;
	if (task->kv_category == TEBIS_BIG)
		rdma_buffer_to_fill = &primary->big_recovery_rdma_buf;

	if (!regs_buffer_have_enough_space(rdma_buffer_to_fill, task))
		task->kreon_operation_status = SEND_FLUSH_COMMANDS;
	else
		task->kreon_operation_status = REPLICATE;
}

// This function is called by the poll_cq thread every time a notification
// arrives
static void regs_wait_for_replication_completion_callback(struct rdma_message_context *r_cnxt)
{
	if (r_cnxt->__is_initialized != 1) {
		log_debug("replication completion callback %u", r_cnxt->__is_initialized);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	sem_post(&r_cnxt->wait_for_completion);
}

void regs_replicate_task(struct regs_server_desc const *server, struct krm_work_task *task)
{
	assert(server && task);
	//log_debug("replicate task");
	struct krm_region_desc *r_desc = task->r_desc;
	struct ru_master_state *primary = task->r_desc->m_state;
	struct ru_master_log_buffer *r_buf = &primary->l0_recovery_rdma_buf;
	if (task->kv_category == TEBIS_BIG)
		r_buf = &primary->big_recovery_rdma_buf;

	uint32_t remote_offset = r_buf->segment.curr_end;
	task->replicated_bytes = &r_buf->segment.replicated_bytes;
	for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; ++i) {
		struct connection_rdma *r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);

		client_rdma_init_message_context(&task->msg_ctx[i], NULL);

		task->msg_ctx[i].args = task;
		task->msg_ctx[i].on_completion_callback = regs_wait_for_replication_completion_callback;
		char *msg_payload = (char *)task->msg + sizeof(struct msg_header);
		while (1) {
			int ret = rdma_post_write(
				r_conn->rdma_cm_id, &task->msg_ctx[i], msg_payload, task->msg_payload_size,
				task->conn->rdma_memory_regions->remote_memory_region, IBV_SEND_SIGNALED,
				(uint64_t)r_buf->segment.mr[i].addr + remote_offset, r_buf->segment.mr[i].rkey);

			if (ret == 0)
				break;
		}
	}

	r_buf->segment.curr_end += task->msg_payload_size;
	__sync_fetch_and_add(&task->r_desc->next_lsn_to_be_replicated, 1);
	task->kreon_operation_status = WAIT_FOR_REPLICATION_COMPLETION;
}

void regs_wait_for_replication_completion(struct krm_work_task *task)
{
	for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; ++i) {
		if (sem_trywait(&task->msg_ctx[i].wait_for_completion) != 0) {
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

static void regs_insert_kv_pair(struct regs_server_desc const *server, struct krm_work_task *task)
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
static void regs_fill_reply_header(msg_header *reply_msg, struct krm_work_task *task, uint32_t payload_size,
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

void regs_execute_put_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
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
		task->r_desc = regs_get_region(region_server_desc, key, key_length);
		if (task->r_desc == NULL) {
			log_fatal("Region not found for key size key %u:%s", key_length, key);
			_exit(EXIT_FAILURE);
		}
		if (task->msg->msg_type == PUT_IF_EXISTS_REQUEST) {
			if (!regs_key_exists(task)) {
				log_warn("Key %.*s in update if exists for region %s not found!", key_length, key,
					 task->r_desc->region->id);
				_exit(EXIT_FAILURE);
			}
		}
	}

	if (!regs_init_replica_connections(region_server_desc, task))
		return;

	if (!regs_enter_parallax(task->r_desc, task))
		return;
	regs_insert_kv_pair(region_server_desc, task);
	if (task->kreon_operation_status == TASK_COMPLETE) {
		regs_leave_parallax(task->r_desc);

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
}

void regs_execute_get_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	assert(task->msg->msg_type == GET_REQUEST);
	struct msg_data_get_request request_data = get_request_get_msg_data(task->msg);
	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, request_data.key, request_data.key_size);

	if (r_desc == NULL) {
		log_fatal("Region not found for key %s", request_data.key);
		_exit(EXIT_FAILURE);
	}

	task->kreon_operation_status = TASK_GET_KEY;
	task->r_desc = r_desc;
	if (!regs_enter_parallax(r_desc, task)) {
		// later...
		return;
	}
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);

	par_handle par_hd = (par_handle)task->r_desc->db;
	struct par_value lookup_value = { .val_buffer = get_reply_get_kv_offset(task->reply_msg),
					  .val_buffer_size = request_data.bytes_to_read };
	const char *error_message = NULL;
	par_get_serialized(par_hd, get_msg_get_key_slice_t(task->msg), &lookup_value, &error_message);
	regs_leave_parallax(r_desc);

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

void regs_execute_multi_get_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_debug("Close scans since we dont use them for now (the tebis-parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
}

void regs_execute_delete_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_debug("Closing delete ops since we dont use them for now (tebis-parallax) replication");
	assert(0);
	_exit(EXIT_FAILURE);
}

/*zero both RDMA buffers*/
static void zero_rdma_buffer(struct krm_region_desc *r_desc, enum log_category log_type)
{
	if (log_type == L0_RECOVERY)
		memset(r_desc->r_state->l0_recovery_rdma_buf.mr->addr, 0x00,
		       r_desc->r_state->l0_recovery_rdma_buf.rdma_buf_size);
	else
		memset(r_desc->r_state->big_recovery_rdma_buf.mr->addr, 0x00,
		       r_desc->r_state->big_recovery_rdma_buf.rdma_buf_size);
}

static void regs_add_segment_into_HT(struct krm_region_desc *r_desc, uint64_t primary_segment_offt,
				     uint64_t replica_segment_offt)
{
	//log_debug("Inserting primary seg offt %lu replica seg offt %lu", primary_segment_offt, replica_segment_offt);
	struct krm_segment_entry *entry = (struct krm_segment_entry *)calloc(1, sizeof(struct krm_segment_entry));
	entry->primary_segment_offt = primary_segment_offt;
	entry->replica_segment_offt = replica_segment_offt;
	pthread_rwlock_wrlock(&r_desc->replica_log_map_lock);
	HASH_ADD_PTR(r_desc->replica_log_map, primary_segment_offt, entry);
	pthread_rwlock_unlock(&r_desc->replica_log_map_lock);
}

static int8_t regs_is_segment_in_HT_mappings(struct krm_region_desc *r_desc, uint64_t primary_segment_offt)
{
	struct krm_segment_entry *index_entry;

	pthread_rwlock_rdlock(&r_desc->replica_log_map_lock);
	HASH_FIND_PTR(r_desc->replica_log_map, &primary_segment_offt, index_entry);
	pthread_rwlock_unlock(&r_desc->replica_log_map_lock);

	if (!index_entry)
		return 0;
	return 1;
}

void regs_execute_flush_command_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	(void)region_server_desc;
	assert(task->msg->msg_type == FLUSH_COMMAND_REQ);
	// log_debug("Primary orders a flush!");
	struct s2s_msg_flush_cmd_req *flush_req =
		(struct s2s_msg_flush_cmd_req *)((char *)task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc =
		regs_get_region(region_server_desc, flush_req->region_key, flush_req->region_key_size);
	if (r_desc->r_state == NULL) {
		log_fatal("No state for backup region %s", r_desc->region->id);
		_exit(EXIT_FAILURE);
	}

	enum log_category log_type_to_flush = flush_req->log_type;

	if (!globals_get_send_index()) {
		build_index_procedure(r_desc, log_type_to_flush);
		zero_rdma_buffer(r_desc, log_type_to_flush);
	} else {
		uint64_t replica_new_segment_offt = send_index_flush_rdma_buffer(r_desc, log_type_to_flush);
		int8_t segment_exist_in_HT = regs_is_segment_in_HT_mappings(r_desc, flush_req->primary_segment_offt);
		if (!segment_exist_in_HT) {
			regs_add_segment_into_HT(r_desc, flush_req->primary_segment_offt, replica_new_segment_offt);
			zero_rdma_buffer(r_desc, log_type_to_flush);
		}
	}

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

static struct ru_replica_rdma_buffer regs_initialize_rdma_buffer(uint32_t size, struct rdma_cm_id *rdma_cm_id)
{
	void *addr = NULL;
	if (posix_memalign(&addr, ALIGNMENT, size)) {
		log_fatal("Failed to allocate aligned RDMA buffer");
		perror("Reason\n");
		_exit(EXIT_FAILURE);
	}
	/*Zero RDMA buffer*/
	memset(addr, 0, size);
	/*initialize the replicas rdma buffer*/
	struct ru_replica_rdma_buffer rdma_buf = { .rdma_buf_size = size,
						   .mr = rdma_reg_write(rdma_cm_id, addr, size) };
	return rdma_buf;
}

void regs_execute_get_rdma_buffer_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	assert(task->msg->msg_type == GET_RDMA_BUFFER_REQ);
	struct s2s_msg_get_rdma_buffer_req *get_log =
		(struct s2s_msg_get_rdma_buffer_req *)((char *)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc =
		regs_get_region(region_server_desc, get_log->region_key, get_log->region_key_size);
	if (r_desc == NULL) {
		log_fatal("No region found for min key %s", get_log->region_key);
		_exit(EXIT_FAILURE);
	}

	log_debug("Region-master wants to initialize rdma buffers for region %s", r_desc->region->id);
	pthread_mutex_lock(&r_desc->region_mgmnt_lock);
	if (r_desc->r_state == NULL) {
		r_desc->r_state = (struct ru_replica_state *)calloc(1, sizeof(struct ru_replica_state));
		uint32_t size = get_log->buffer_size;
		struct rdma_cm_id *rdma_cm_id = task->conn->rdma_cm_id;
		/*initalize l0_recovery_buffer*/
		r_desc->r_state->l0_recovery_rdma_buf = regs_initialize_rdma_buffer(size, rdma_cm_id);
		/*initialize big recovery buffer*/
		r_desc->r_state->big_recovery_rdma_buf = regs_initialize_rdma_buffer(size, rdma_cm_id);
	} else {
		log_fatal("remote buffers already initialized, what?");
		_exit(EXIT_FAILURE);
	}

	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
	task->reply_msg = (void *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
				   task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_get_rdma_buffer_rep *rep =
		(struct s2s_msg_get_rdma_buffer_rep *)((char *)task->reply_msg + sizeof(msg_header));
	rep->status = TEBIS_SUCCESS;
	rep->l0_recovery_mr = *r_desc->r_state->l0_recovery_rdma_buf.mr;
	rep->big_recovery_mr = *r_desc->r_state->big_recovery_rdma_buf.mr;

	regs_fill_reply_header(task->reply_msg, task,
			       sizeof(struct s2s_msg_get_rdma_buffer_rep) + (sizeof(struct ibv_mr)),
			       GET_RDMA_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	log_debug("Region master wants rdma buffers...DONE");
	task->kreon_operation_status = TASK_COMPLETE;
}

void regs_execute_replica_index_get_buffer_req(struct regs_server_desc const *region_server_desc,
					       struct krm_work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_GET_BUFFER_REQ);
	assert(globals_get_send_index());

	struct s2s_msg_replica_index_get_buffer_req *req =
		(struct s2s_msg_replica_index_get_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", req->region_key);
		_exit(EXIT_FAILURE);
	}

	log_debug("Starting compaction for level %u at region %s", req->level_id, r_desc->region->id);
	uint32_t dst_level_id = req->level_id + 1;

	//initialize and reg write buffers for recieving the primary's segments ready to be flushed.
	//The buffers follow Parallax compaction index
	struct send_index_create_compactions_rdma_buffer_params create_buffers_params = {
		.level_id = req->level_id,
		.tree_id = req->tree_id,
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

	//time for reply
	task->reply_msg = (struct msg_header *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
						task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_replica_index_get_buffer_rep *reply =
		(struct s2s_msg_replica_index_get_buffer_rep *)((char *)task->reply_msg + sizeof(msg_header));
	reply->mr = *r_desc->r_state->index_buffer[dst_level_id];
	reply->uuid = req->uuid;

	regs_fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_replica_index_get_buffer_rep),
			       REPLICA_INDEX_GET_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	log_debug("Registed a new memory region in region %s at offt %lu", r_desc->region->id,
		  (uint64_t)reply->mr.addr);
	task->kreon_operation_status = TASK_COMPLETE;
}
