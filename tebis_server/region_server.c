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
#include "../utilities/spin_loop.h"
#include "djb2.h"
#include "globals.h"
#include "list.h"
#include "metadata.h"
#include "zk_utils.h"
#include <arpa/inet.h>
#include <assert.h>
#include <btree/gc.h>
#include <cJSON.h>
#include <ifaddrs.h>
#include <include/parallax/parallax.h>
#include <libgen.h>
#include <log.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
uint64_t ds_hash_key;

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

char *krm_msg_type_tostring(enum krm_msg_type type)
{
	static char *const tostring_array[KRM_BUILD_PRIMARY + 1] = { NULL,
								     "KRM_OPEN_REGION_AS_PRIMARY",
								     "KRM_ACK_OPEN_PRIMARY",
								     "KRM_NACK_OPEN_PRIMARY",
								     "KRM_OPEN_REGION_AS_BACKUP",
								     "KRM_ACK_OPEN_BACKUP",
								     "KRM_NACK_OPEN_BACKUP",
								     "KRM_CLOSE_REGION",
								     "KRM_BUILD_PRIMARY" };

	return tostring_array[type];
}

static void krm_get_IP_Addresses(struct regs_server_desc *server)
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

	/*open kreon db*/
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

static void krm_process_msg(struct regs_server_desc *region_server, struct krm_msg *msg)
{
	char *zk_path;
	struct krm_msg reply;
	int rc;
	switch (msg->type) {
	case KRM_OPEN_REGION_AS_PRIMARY:
	case KRM_OPEN_REGION_AS_BACKUP:
		/*first check if the msg responds to the epoch I am currently in*/
		if (msg->epoch != region_server->name.epoch) {
			log_warn("Epochs mismatch I am at epoch %lu msg refers to epoch %lu", region_server->name.epoch,
				 msg->epoch);
			reply.type = (msg->type == KRM_OPEN_REGION_AS_PRIMARY) ? KRM_NACK_OPEN_PRIMARY :
										 KRM_NACK_OPEN_BACKUP;
			reply.error_code = KRM_BAD_EPOCH;
			strcpy(reply.sender, region_server->name.kreon_ds_hostname);
			reply.region = msg->region;
		} else {
			open_region(region_server, &msg->region,
				    msg->type == KRM_OPEN_REGION_AS_PRIMARY ? KRM_PRIMARY : KRM_BACKUP);

			reply.type = (msg->type == KRM_OPEN_REGION_AS_PRIMARY) ? KRM_ACK_OPEN_PRIMARY :
										 KRM_ACK_OPEN_BACKUP;
			reply.error_code = KRM_SUCCESS;
			strcpy(reply.sender, region_server->name.kreon_ds_hostname);
			reply.region = msg->region;
		}
#define MAIL_ID_LENGTH 128
		char mail_id[MAIL_ID_LENGTH] = { 0 };
		int mail_id_len = MAIL_ID_LENGTH;
		zk_path =
			zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, msg->sender, KRM_MAIL_TITLE);
		rc = zoo_create(region_server->zh, zk_path, (char *)&reply, sizeof(struct krm_msg),
				&ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, mail_id, mail_id_len);

		if (rc != ZOK) {
			log_fatal("Failed to respond path is %s code is %s", zk_path, zku_op2String(rc));
			_exit(EXIT_FAILURE);
		}
		log_debug("Successfully sent ACK to %s for region %s", msg->sender, msg->region.id);
		free(zk_path);
		break;
	case KRM_CLOSE_REGION:
		log_fatal("Unsupported types KRM_CLOSE_REGION");
		_exit(EXIT_FAILURE);
	case KRM_BUILD_PRIMARY:
		log_fatal("Unsupported types KRM_BUILD_PRIMARY");
		_exit(EXIT_FAILURE);
	default:
		log_fatal("No action for message type %d (Probably corrupted message type)", msg->type);
		_exit(EXIT_FAILURE);
	}
}

int krm_zk_get_server_name(char *dataserver_name, struct regs_server_desc const *my_desc, struct krm_server_name *dst,
			   int *zk_rc)
{
	/*check if you are hostname-RDMA_port belongs to the project*/
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, dataserver_name);
	struct Stat stat;
	char buffer[2048];
	int buffer_len = 2048;
	int rc = zoo_get(my_desc->zh, zk_path, 0, buffer, &buffer_len, &stat);
	if (zk_rc)
		*zk_rc = rc;
	free(zk_path);
	if (rc != ZOK)
		return -1;

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
	strncpy(dst->hostname, cJSON_GetStringValue(hostname), KRM_HOSTNAME_SIZE);
	strncpy(dst->kreon_ds_hostname, cJSON_GetStringValue(dataserver_name_retrieved), KRM_HOSTNAME_SIZE);
	dst->kreon_ds_hostname_length = strlen(cJSON_GetStringValue(dataserver_name_retrieved));
	strncpy(dst->RDMA_IP_addr, cJSON_GetStringValue(rdma_ip), KRM_MAX_RDMA_IP_SIZE);
	dst->epoch = cJSON_GetNumberValue(epoch);
	strncpy(dst->kreon_leader, cJSON_GetStringValue(leader), KRM_HOSTNAME_SIZE);

	cJSON_Delete(json);

	return 0;
}

static int krm_zk_update_server_name(struct regs_server_desc *my_desc, struct krm_server_name *src)
{
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, src->kreon_ds_hostname);
	cJSON *obj = cJSON_CreateObject();
	cJSON_AddStringToObject(obj, "hostname", src->hostname);
	cJSON_AddStringToObject(obj, "dataserver_name", src->kreon_ds_hostname);
	cJSON_AddStringToObject(obj, "leader", src->kreon_leader);
	cJSON_AddStringToObject(obj, "RDMA_IP_addr", src->RDMA_IP_addr);
	cJSON_AddNumberToObject(obj, "epoch", src->epoch);

	const char *json_string = cJSON_Print(obj);

	int rc = zoo_set(my_desc->zh, zk_path, json_string, strlen(json_string), -1);

	cJSON_Delete(obj);
	free((void *)json_string);
	free(zk_path);
	return rc;
}

/**
 * Announces Region Server presence in the Tebis cluster. More precisely, it creates an ephemeral znode under /aliveservers
 */
static void announce_region_server_presence(struct regs_server_desc *server)
{
	char path[KRM_HOSTNAME_SIZE] = { 0 };
	char *zk_path =
		zku_concat_strings(4, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH, KRM_SLASH, server->name.kreon_ds_hostname);
	int ret_code = zoo_create(server->zh, zk_path, (const char *)&server->name, sizeof(struct krm_server_name),
				  &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, path, KRM_HOSTNAME_SIZE);
	if (ret_code != ZOK) {
		log_fatal("Failed to annouce my presence code %s", zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(zk_path);
}

static void handle_master_command(struct regs_server_desc *server, struct krm_msg *msg)
{
	(void)server;
	switch (msg->type) {
	case KRM_OPEN_REGION_AS_PRIMARY:
	case KRM_OPEN_REGION_AS_BACKUP:
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
		char *command = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SLASH, server->name.kreon_ds_hostname,
						   commands.data[i]);
		struct krm_msg cmd = { 0 };
		struct Stat stat = { 0 };
		int cmd_len = sizeof(cmd);
		int rc = zoo_get(zkh, command, 0, (char *)&cmd, &cmd_len, &stat);
		if (rc != ZOK) {
			log_fatal("Error could not get command from path %s", command);
			_exit(EXIT_FAILURE);
		}
		handle_master_command(server, &cmd);
		free(command);
	}
}

static void clean_region_server_mailbox(struct regs_server_desc *server)
{
	log_debug("Cleaning stale messages from my mailbox from previous epoch "
		  "and leaving a watcher");

	char *mailbox =
		zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, server->name.kreon_ds_hostname);
	struct String_vector stale_msgs = { 0 };
	int ret_code = zoo_wget_children(server->zh, mailbox, command_watcher, server, &stale_msgs);
	if (ret_code != ZOK) {
		log_fatal("failed to query zookeeper for path %s contents with code %s", mailbox,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	for (int i = 0; i < stale_msgs.count; i++) {
		/*iterate old mails and delete them*/
		char *mail = zku_concat_strings(6, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
						server->name.kreon_ds_hostname, KRM_SLASH, stale_msgs.data[i]);
		log_debug("Deleting %s", mail);
		ret_code = zoo_delete(server->zh, mail, -1);
		if (ret_code != ZOK) {
			log_fatal("failed to delete stale mail msg %s error %s", mail, zku_op2String(ret_code));
			_exit(EXIT_FAILURE);
		}
		free(mail);
	}
	free(mailbox);
}

static void init_zookeeper(struct regs_server_desc *server)
{
	if (gethostname(server->name.hostname, KRM_HOSTNAME_SIZE) != 0) {
		log_fatal("failed to get my hostname");
		_exit(EXIT_FAILURE);
	}
	/*now fill your cluster hostname*/
	if (sprintf(server->name.kreon_ds_hostname, "%s:%u", server->name.hostname, server->RDMA_port) < 0) {
		log_fatal("Failed to create hostname string");
		_exit(EXIT_FAILURE);
	}
	krm_get_IP_Addresses(server);

	server->zh = zookeeper_init(globals_get_zk_host(), zk_main_watcher, 15000, 0, server, 0);
	if (!server->zh) {
		log_fatal("failed to connect to zk %s", globals_get_zk_host());
		_exit(EXIT_FAILURE);
	}
	field_spin_for_value(&server->zconn_state, KRM_CONNECTED);
}

static void init_region_server_state(struct regs_server_desc *server)
{
	server->mail_path =
		zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, server->name.kreon_ds_hostname);

	sem_init(&server->wake_up, 0, 0);
	server->msg_list = tebis_klist_init();
	/*init ds_regions table*/
	server->ds_regions = (struct krm_ds_regions *)calloc(1, sizeof(struct krm_ds_regions));
}

static bool is_server_part_of_the_cluster(struct regs_server_desc *server)
{
	int ignored_value = 0;

	return krm_zk_get_server_name(server->name.kreon_ds_hostname, server, &server->name, &ignored_value) == 0;
}

static bool is_first_time_joining_cluster(struct regs_server_desc *server)
{
	return server->name.epoch == 0;
}

static void update_region_server_clock(struct regs_server_desc *server)
{
	// Update my zookeeper entry
	++server->name.epoch;
	krm_zk_update_server_name(server, &server->name);
}

void *regs_run_region_server(void *args)
{
	pthread_setname_np(pthread_self(), "rserverd");
	zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);

	struct regs_server_desc *server = (struct regs_server_desc *)args;
	init_region_server_state(server);
	init_zookeeper(server);

	if (!is_server_part_of_the_cluster(server)) {
		log_fatal("Host %s cannot determine that is part of the cluster", server->name.kreon_ds_hostname);
		_exit(EXIT_FAILURE);
	}
	update_region_server_clock(server);
	clean_region_server_mailbox(server);
	announce_region_server_presence(server);

	if (is_first_time_joining_cluster(server))
		globals_init_volume();

	sem_wait(&server->wake_up);

	server->state = KRM_BOOTING;

	while (1) {
		struct klist_node *node = NULL;

		pthread_mutex_lock(&server->msg_list_lock);
		node = tebis_klist_remove_first(server->msg_list);
		pthread_mutex_unlock(&server->msg_list_lock);
		if (!node) {
			sem_wait(&server->wake_up);
			continue;
		}
		log_debug("New command: %s", krm_msg_type_tostring(((struct krm_msg *)node->data)->type));
		server->state = KRM_PROCESSING_MSG;
		krm_process_msg(server, (struct krm_msg *)node->data);
		free(node->data);
		free(node);
	}
	return NULL;
}
