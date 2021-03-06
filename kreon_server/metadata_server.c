#define _GNU_SOURCE
#include "../utilities/spin_loop.h"
#include "djb2.h"
#include "globals.h"
#include "metadata.h"
#include "zk_utils.h"
#include <arpa/inet.h>
#include <assert.h>
#include <cJSON.h>
#include <ifaddrs.h>
#include <libgen.h>
#include <log.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>

uint64_t ds_hash_key;

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

static void krm_get_IP_Addresses(struct krm_server_desc *server)
{
	char addr[INET_ADDRSTRLEN] = { 0 };
	struct ifaddrs *ifaddr, *ifa;
	int family;

	if (getifaddrs(&ifaddr) == -1) {
		perror("getifaddrs");
		_exit(EXIT_FAILURE);
	}
	for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr == NULL)
			continue;
		family = ifa->ifa_addr->sa_family;
		if (family == AF_INET) {
			struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
			// addr = inet_ntoa(sa->sin_addr);
			inet_ntop(AF_INET, &(sa->sin_addr), addr, INET_ADDRSTRLEN);
			char *ip_filter = globals_get_RDMA_IP_filter();
			if (strncmp(addr, ip_filter, strlen(ip_filter)) == 0) {
				log_info("RDMA IP prefix accepted %s Interface: %s Full IP Address: %s",
					 globals_get_RDMA_IP_filter(), ifa->ifa_name, addr);
				sprintf(server->name.RDMA_IP_addr, "%s:%u", addr, server->RDMA_port);
				log_info("Set my RDMA ip addr to %s", server->name.RDMA_IP_addr);
				freeifaddrs(ifaddr);
				return;
			}
		}
	}
	log_fatal("Failed to find an IP for RDMA in the subnet %s", globals_get_RDMA_IP_filter());
	_exit(EXIT_FAILURE);
}

static uint8_t krm_check_ld_regions_sorted(struct krm_leader_regions *ld_regions)
{
	int i;
	for (i = 0; i < ld_regions->num_regions; i++) {
		if (zku_key_cmp(ld_regions->regions[i].min_key_size, ld_regions->regions[i].min_key,
				ld_regions->regions[i].max_key_size, ld_regions->regions[i].max_key) >= 0) {
			log_fatal("Unsorted min max key within region id %s min key %s, max key "
				  "%s max key size %u",
				  ld_regions->regions[i].id, ld_regions->regions[i].min_key,
				  ld_regions->regions[i].max_key, ld_regions->regions[i].max_key_size);
			_exit(EXIT_FAILURE);
		}
		if (i == ld_regions->num_regions - 1) {
			if (memcmp(ld_regions->regions[i].max_key, "+oo", 3) != 0) {
				log_fatal("Fault last key of region %d is %s should be +oo", i,
					  ld_regions->regions[i].max_key);
				_exit(EXIT_FAILURE);
			}
		} else {
			if (zku_key_cmp(ld_regions->regions[i].max_key_size, ld_regions->regions[i + 1].min_key,
					ld_regions->regions[i].max_key_size, ld_regions->regions[i].max_key) != 0) {
				log_fatal("Gap left in key range for region %s", ld_regions->regions[i].id);
				log_info("Compared key %s with key %s", ld_regions->regions[i + 1].min_key,
					 ld_regions->regions[i].max_key);
				/*raise(SIGINT);*/
				_exit(EXIT_FAILURE);
			}
		}
	}
	return 1;
}

static void krm_iterate_servers_state(struct krm_server_desc *desc)
{
	struct krm_leader_ds_map *current;
	struct krm_leader_ds_map *tmp;
	struct krm_leader_ds_region_map *tmp_r;
	struct krm_leader_ds_region_map *current_r;
	struct krm_leader_region_state *r_state;
	log_debug("Kreon master: view of servers");
	HASH_ITER(hh, desc->dataservers_map, current, tmp)
	{
		log_debug("Server: %s hash_key current: %x", current->server_id.kreon_ds_hostname, current->hash_key);
		HASH_ITER(hh, current->region_map, current_r, tmp_r)
		{
			r_state = &current_r->lr_state;
			log_debug("hosting region %s status %u", r_state->region->id, r_state->status);
		}
	}
}

static void krm_iterate_ld_regions(struct krm_server_desc *desc)
{
	if (!desc)
		return;
	log_debug("Leader's regions view");

	for (int i = 0; i < desc->ld_regions->num_regions; i++) {
		struct krm_region *r = &desc->ld_regions->regions[i];
		log_debug("Region id %s min key %s max key %s region overall status", r->id, r->min_key, r->max_key,
			  r->stat);
	}
}

static uint8_t krm_insert_ld_region(struct krm_server_desc *desc, struct krm_region *region)
{
	int64_t ret;
	int start_idx = 0;
	int end_idx = desc->ld_regions->num_regions - 1;
	int middle = 0;
	uint8_t rc = KRM_SUCCESS;

	if (desc->ld_regions->num_regions == KRM_MAX_REGIONS) {
		log_warn("Warning! Adding new region failed, max_regions %d reached", KRM_MAX_REGIONS);
		rc = KRM_DS_TABLE_FULL;
		goto exit;
	}

	if (desc->ld_regions->num_regions > 0) {
		while (start_idx <= end_idx) {
			middle = (start_idx + end_idx) / 2;
			ret = zku_key_cmp(desc->ld_regions->regions[middle].min_key_size,
					  desc->ld_regions->regions[middle].min_key, region->min_key_size,
					  region->min_key);
			if (ret == 0) {
				log_warn("Warning failed to add region, range already present\n");
				rc = KRM_REGION_EXISTS;
				break;
			} else if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx) {
					memmove(&desc->ld_regions->regions[middle + 1],
						&desc->ld_regions->regions[middle],
						(desc->ld_regions->num_regions - middle) * sizeof(struct krm_region));
					desc->ld_regions->regions[middle] = *region;
					++desc->ld_regions->num_regions;
					rc = KRM_SUCCESS;
					goto exit;
				}
			} else {
				start_idx = middle + 1;
				if (start_idx > end_idx) {
					middle++;
					memmove(&desc->ld_regions->regions[middle + 1],
						&desc->ld_regions->regions[middle],
						(desc->ld_regions->num_regions - middle) * sizeof(struct krm_region));
					desc->ld_regions->regions[middle] = *region;
					++desc->ld_regions->num_regions;
					rc = KRM_SUCCESS;
					goto exit;
				}
			}
		}
	} else {
		desc->ld_regions->regions[0] = *region;
		++desc->ld_regions->num_regions;
		rc = KRM_SUCCESS;
	}

exit:
	return rc;
}

uint8_t krm_insert_ds_region(struct krm_server_desc *desc, struct krm_region_desc *r_desc,
			     struct krm_ds_regions *reg_table)
{
	int64_t ret;
	int start_idx = 0;
	int end_idx = reg_table->num_ds_regions - 1;
	int middle = 0;
	uint8_t rc = KRM_SUCCESS;

	++reg_table->lamport_counter_1;
	log_debug("Adding region min key %s max key %s", r_desc->region->min_key, r_desc->region->max_key);

	if (reg_table->num_ds_regions == KRM_MAX_DS_REGIONS) {
		log_warn("Warning! Adding new region failed, max_regions %d reached", KRM_MAX_DS_REGIONS);
		rc = KRM_DS_TABLE_FULL;
		goto exit;
	}

	if (reg_table->num_ds_regions > 0) {
		while (start_idx <= end_idx) {
			middle = (start_idx + end_idx) / 2;
			ret = zku_key_cmp(desc->ds_regions->r_desc[middle]->region->min_key_size,
					  desc->ds_regions->r_desc[middle]->region->min_key,
					  r_desc->region->min_key_size, r_desc->region->min_key);

			if (ret == 0) {
				log_warn("failed to add region, range already present");
				rc = KRM_REGION_EXISTS;
				break;
			} else if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx) {
					memmove(&desc->ds_regions->r_desc[middle + 1],
						&desc->ds_regions->r_desc[middle],
						(reg_table->num_ds_regions - middle) *
							sizeof(struct krm_region_desc *));
					desc->ds_regions->r_desc[middle] = r_desc;
					++reg_table->num_ds_regions;
					rc = KRM_SUCCESS;
					break;
				}
			} else {
				start_idx = middle + 1;
				if (start_idx > end_idx) {
					middle++;
					memmove(&desc->ds_regions->r_desc[middle + 1],
						&desc->ds_regions->r_desc[middle],
						(reg_table->num_ds_regions - middle) *
							sizeof(struct krm_region_desc *));
					desc->ds_regions->r_desc[middle] = r_desc;
					++reg_table->num_ds_regions;
					rc = KRM_SUCCESS;
					goto exit;
				}
			}
		}
	} else {
		desc->ds_regions->r_desc[0] = r_desc;
		++reg_table->num_ds_regions;
		rc = KRM_SUCCESS;
	}

exit:
	++reg_table->lamport_counter_2;
	return rc;
}

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

static struct krm_leader_ds_region_map *init_region_map(struct krm_region *region, enum krm_region_role role)
{
	struct krm_leader_ds_region_map *region_map =
		(struct krm_leader_ds_region_map *)calloc(1, sizeof(struct krm_leader_ds_region_map));
	region_map->lr_state.region = region;
	region_map->lr_state.role = role;
	region_map->lr_state.status = KRM_OPENING;
	region_map->hash_key = djb2_hash((unsigned char *)region->id, strlen(region->id));
	return region_map;
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
		log_debug("Adding region %s (As a primary) for server %s hash key %x", region->id,
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
		log_info("Adding region %s (As a primary) for server %s hash key %x", region->id,
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
			log_info("Adding region %s (As a backup) for server %s hash key %x", region->id, /*  */
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
			log_info("Adding region %s (As a backup) for server %s hash key %x", region->id,
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

/**
 * Watcher we use to process session events. In particular,
 * when it receives a ZOO_CONNECTED_STATE event, we set the
 * connected variable so that we know that the session has
 * been established.
 */
void zk_main_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	struct krm_server_desc *my_desc = (struct krm_server_desc *)context;
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

void leader_health_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	(void)zh;
	(void)state;
	struct krm_server_desc *my_desc = (struct krm_server_desc *)watcherCtx;
	struct Stat stat;
	if (type == ZOO_DELETED_EVENT) {
		log_warn("Leader %s died unhandled situation TODO");
		_exit(EXIT_FAILURE);
	} else {
		log_warn("Got unhandled type %d resetting watcher for path %s", type, path);
		int rc = zoo_wexists(my_desc->zh, path, leader_health_watcher, NULL, &stat);
		if (rc != ZOK) {
			log_fatal("failed to reset watcher for path %s", path);
			_exit(EXIT_FAILURE);
		}
	}
}

static void zoo_rmr_folder(zhandle_t *zh, const char *path)
{
	struct String_vector children;
	int rc = zoo_get_children(zh, path, 0, &children);
	assert(rc == ZOK);
	if (children.count != 0) {
		for (int i = 0; i < children.count; ++i) {
			char *child_path = children.data[i];
			zoo_rmr_folder(zh, child_path);
		}
	}
	rc = zoo_delete(zh, path, -1);
	assert(rc == ZOK);
}

static int is_primary_of_region(struct krm_region *region, struct krm_server_name *dataserver_name)
{
	return !strncmp(region->primary.kreon_ds_hostname, dataserver_name->kreon_ds_hostname,
			dataserver_name->kreon_ds_hostname_length);
}

static char is_backup_of_region(struct krm_region *region, struct krm_server_name *dataserver_name,
				int *backup_array_index)
{
	struct krm_server_name *backups = region->backups;
	int backups_length = region->num_of_backup;
	int i;
	for (i = 0; i < backups_length; ++i) {
		if (!strncmp(backups[i].kreon_ds_hostname, dataserver_name->kreon_ds_hostname,
			     backups[i].kreon_ds_hostname_length))
			break;
	}
	if (backup_array_index)
		*backup_array_index = (i < backups_length) ? i : -1;
	return i < backups_length;
}

void dataserver_health_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	(void)state;
	struct Stat stat;
	struct krm_server_desc *my_desc = (struct krm_server_desc *)watcherCtx;
	int rc;
	char *dataserver_name = basename((char *)path);
	static char *const krm_region_role_tostring[KRM_BACKUP + 1] = { "Primary", "Backup" };
	if (type == ZOO_SESSION_EVENT) {
		log_warn("Lost connection with zookeeper, someone else will handle it I suppose");
		return;
	} else if (type == ZOO_DELETED_EVENT) {
		log_warn("Leader: dataserver %s died! [TODO] Unhandled error, exiting...", dataserver_name);
		struct krm_leader_ds_map *dataserver = NULL;
		// Find dataserver's region map
		ds_hash_key = djb2_hash((unsigned char *)dataserver_name, strlen(dataserver_name));
		HASH_FIND_PTR(my_desc->dataservers_map, &ds_hash_key, dataserver);
		assert(dataserver);
		// Remove the server from Zookeeper
		char *dataserver_hosts_path =
			zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, dataserver_name);
		zoo_delete(my_desc->zh, dataserver_hosts_path,
			   -1); // FIXME where do I find the version?
		free(dataserver_hosts_path);
		char *dataserver_mailbox_path =
			zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, dataserver_name);
		zoo_rmr_folder(zh, dataserver_mailbox_path);
		free(dataserver_mailbox_path);
		// Remove the now dead dataserver from the dataservers map
		HASH_DEL(my_desc->dataservers_map, dataserver);

		// The next dataserver to assign a region to is picked in round-robin order
		static struct krm_leader_ds_map *next_assignee = NULL;
		// Iterate over the dataserver's regions
		log_info("Listing %s's regions:", dataserver_name);
		struct krm_leader_ds_region_map *current;
		struct krm_leader_ds_region_map *tmp;
		char *zk_regions_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_REGIONS_PATH, KRM_SLASH);
		HASH_ITER(hh, dataserver->region_map, current, tmp)
		{
			if (!next_assignee)
				next_assignee = my_desc->dataservers_map;

			struct krm_region *current_region = current->lr_state.region;
			// Make sure next_assignee does not play a role in this region
			if (is_primary_of_region(current_region, &next_assignee->server_id) ||
			    is_backup_of_region(current_region, &next_assignee->server_id, NULL)) {
				int dataservers_map_size = HASH_COUNT(my_desc->dataservers_map);
				int i;
				for (i = 0; i < dataservers_map_size; ++i) {
					next_assignee = (struct krm_leader_ds_map *)next_assignee->hh.next;
					if (!next_assignee)
						next_assignee = my_desc->dataservers_map;
					if (!is_primary_of_region(current_region, &next_assignee->server_id) &&
					    !is_backup_of_region(current_region, &next_assignee->server_id, NULL))
						break;
				}
				if (i >= dataservers_map_size) {
					log_fatal("Leader: No available dataserver for region %s. Exiting...",
						  current_region->id);
					_exit(EXIT_FAILURE);
				}
			}

			char *region_name = current_region->id;
			log_info("\t%s [%s] -> %s", region_name, krm_region_role_tostring[current->lr_state.role],
				 next_assignee->server_id.kreon_ds_hostname);
			// Apply the new assignment to the leader's state
			current->lr_state.server_id = next_assignee->server_id;
			if (current->lr_state.role == KRM_PRIMARY) {
				current_region->primary = next_assignee->server_id;
			} else {
				assert(current->lr_state.role == KRM_BACKUP);
				int backups_array_index;
				// Find the entry of the now dead dataserver in the backups array
				if (is_backup_of_region(current_region, &dataserver->server_id, &backups_array_index))
					current_region->backups[backups_array_index] = next_assignee->server_id;
				else {
					log_fatal("Leader: Cannot find %s in region's %s backups while he "
						  "should be there. Exiting...",
						  dataserver_name, region_name);
					_exit(EXIT_FAILURE);
				}
			}
			HASH_ADD_PTR(next_assignee->region_map, hash_key, current);
			// Update Zookeeper region info
			char *current_zk_region_path = zku_concat_strings(2, zk_regions_path, region_name);
			rc = zoo_set(my_desc->zh, current_zk_region_path, (char *)current_region,
				     sizeof(struct krm_region), -1);
			assert(rc == ZOK);
			enum krm_msg_type msg_type = (current->lr_state.role == KRM_PRIMARY) ?
							     KRM_OPEN_REGION_AS_PRIMARY :
								   KRM_OPEN_REGION_AS_BACKUP;
			// Send open command to new assignee
			krm_resend_open_command(my_desc, current_region, next_assignee->server_id.kreon_ds_hostname,
						msg_type);
			free(current_zk_region_path);

			// Advance to next dataserver
			next_assignee = (struct krm_leader_ds_map *)next_assignee->hh.next;
		}
		free(zk_regions_path);
	} else if (type == ZOO_CREATED_EVENT) {
		log_warn("Leader: dataserver %s joined!", dataserver_name);
	} else {
		log_warn("Leader: unhandled event type %d", type);
	}
	// Reset the watcher
	log_info("Leader: resetting dataserver health watcher (path = %s)", path);
	rc = zoo_wexists(my_desc->zh, path, dataserver_health_watcher, my_desc, &stat);
	if (rc != ZOK && rc != ZNONODE) {
		log_fatal("failed to reset watcher for path %s", path);
		_exit(EXIT_FAILURE);
	}
}

void mailbox_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	(void)state;
	(void)path;
	struct Stat stat;

	struct krm_server_desc *s_desc = (struct krm_server_desc *)watcherCtx;
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
		int rc = zoo_wget_children(zh, s_desc->mail_path, mailbox_watcher, (void *)s_desc, mails);
		if (rc != ZOK) {
			log_fatal("failed to get mails from path %s error code ", s_desc->mail_path, zku_op2String(rc));
			_exit(EXIT_FAILURE);
		}
		for (int i = 0; i < mails->count; i++) {
			char *mail = zku_concat_strings(3, s_desc->mail_path, KRM_SLASH, mails->data[i]);
			struct krm_msg *msg = (struct krm_msg *)calloc(1, sizeof(struct krm_msg));

			int buffer_len = sizeof(struct krm_msg);
			rc = zoo_get(s_desc->zh, mail, 0, (char *)msg, &buffer_len, &stat);
			if (rc != ZOK) {
				log_fatal("Failed to fetch email %s", mail);
				_exit(EXIT_FAILURE);
			}

			//log_info("fetched mail %s for region %s", mail, msg->region.id);
			pthread_mutex_lock(&s_desc->msg_list_lock);
			klist_add_last(s_desc->msg_list, msg, NULL, NULL);
			sem_post(&s_desc->wake_up);
			pthread_mutex_unlock(&s_desc->msg_list_lock);
			//log_info("Deleting %s", mail);
			rc = zoo_delete(s_desc->zh, mail, -1);
			if (rc != ZOK) {
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

static struct krm_leader_ds_region_map *krm_leader_get_ds_region(struct krm_server_desc *my_desc,
								 struct krm_region *region, char *ds_hostname)
{
	struct krm_leader_ds_map *dataserver;
	struct krm_leader_ds_region_map *ds_region_map;
	// Find dataserver
	ds_hash_key = djb2_hash((unsigned char *)ds_hostname, strlen(ds_hostname));
	HASH_FIND_PTR(my_desc->dataservers_map, &ds_hash_key, dataserver);
	if (dataserver == NULL) {
		log_fatal("No entry found for server %s", ds_hostname);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	// Refresh dataserver's info
	struct Stat stat;
	char *path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, ds_hostname);
	int buffer_len = sizeof(struct krm_server_name);
	int rc = zoo_get(my_desc->zh, path, 0, (char *)&dataserver->server_id, &buffer_len, &stat);
	if (rc != ZOK) {
		log_fatal("Failed to refresh info for host %s", path);
		_exit(EXIT_FAILURE);
	}
	free(path);
	// Find dataserver's region
	uint64_t hash_key = djb2_hash((unsigned char *)region->id, strlen(region->id));
	HASH_FIND_PTR(dataserver->region_map, &hash_key, ds_region_map);
	if (ds_region_map == NULL) {
		log_fatal("Failed to locate region %s in dataserver %s", region->id, region->primary.kreon_ds_hostname);
		return NULL;
	}
	return ds_region_map;
}

static void krm_process_msg(struct krm_server_desc *server, struct krm_msg *msg)
{
	char *zk_path;
	struct krm_msg reply;
	int rc;
	switch (msg->type) {
	case KRM_OPEN_REGION_AS_PRIMARY:
	case KRM_OPEN_REGION_AS_BACKUP:
		/*first check if the msg responds to the epoch I am currently in*/
		if (msg->epoch != server->name.epoch) {
			log_warn("Epochs mismatch I am at epoch %lu msg refers to epoch %lu", server->name.epoch,
				 msg->epoch);
			reply.type = (msg->type == KRM_OPEN_REGION_AS_PRIMARY) ? KRM_NACK_OPEN_PRIMARY :
										       KRM_NACK_OPEN_BACKUP;
			reply.error_code = KRM_BAD_EPOCH;
			strcpy(reply.sender, server->name.kreon_ds_hostname);
			reply.region = msg->region;
		} else {
			struct krm_region_desc *r_desc =
				(struct krm_region_desc *)calloc(1, sizeof(struct krm_region_desc));

			struct krm_region *region = (struct krm_region *)calloc(1, sizeof(struct krm_region));
			*region = msg->region;
			r_desc->region = region;
			if (msg->type == KRM_OPEN_REGION_AS_PRIMARY) {
				r_desc->role = KRM_PRIMARY;
				r_desc->m_state = NULL;
			} else {
				r_desc->role = KRM_BACKUP;
				r_desc->r_state = NULL;
			}

			r_desc->replica_buf_status = KRM_BUFS_UNINITIALIZED;
			pthread_mutex_init(&r_desc->region_mgmnt_lock, NULL);
			r_desc->pending_region_tasks = 0;
			if (pthread_rwlock_init(&r_desc->kreon_lock, NULL) != 0) {
				log_fatal("Failed to init region read write lock");
				_exit(EXIT_FAILURE);
			}

			pthread_rwlock_init(&r_desc->replica_log_map_lock, NULL);
			r_desc->status = KRM_OPEN;
			r_desc->replica_log_map = NULL;
			for (int i = 0; i < MAX_LEVELS; i++)
				r_desc->replica_index_map[i] = NULL;
			/*open kreon db*/
			r_desc->db = custom_db_open(globals_get_dev(), 0, globals_get_dev_size(), region->id, CREATE_DB,
						    globals_get_l0_size(), globals_get_growth_factor());

			/*this copies r_desc struct to the regions array!*/
			krm_insert_ds_region(server, r_desc, server->ds_regions);
			/*find system ref*/
			struct krm_region_desc *t = krm_get_region(server, region->min_key, region->min_key_size);
			/*set the callback and context for remote compaction*/
			log_info("Setting DB %s in replicated mode", t->db->db_desc->db_name);
			bt_set_db_in_replicated_mode(t->db);
			set_init_index_transfer(t->db->db_desc, &rco_init_index_transfer);
			set_destroy_local_rdma_buffer(t->db->db_desc, &rco_destroy_local_rdma_buffer);
			set_send_index_segment_to_replicas(t->db->db_desc, &rco_send_index_segment_to_replicas);
			bt_set_flush_replicated_logs_callback(t->db->db_desc, rco_flush_last_log_segment);
			rco_add_db_to_pool(server->compaction_pool, t);

			reply.type = (msg->type == KRM_OPEN_REGION_AS_PRIMARY) ? KRM_ACK_OPEN_PRIMARY :
										       KRM_ACK_OPEN_BACKUP;
			reply.error_code = KRM_SUCCESS;
			strcpy(reply.sender, server->name.kreon_ds_hostname);
			reply.region = msg->region;
		}
		char mail_id[128];
		int mail_id_len = 128;
		zk_path =
			zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, msg->sender, KRM_MAIL_TITLE);
		rc = zoo_create(server->zh, zk_path, (char *)&reply, sizeof(struct krm_msg), &ZOO_OPEN_ACL_UNSAFE,
				ZOO_SEQUENCE, mail_id, mail_id_len);

		if (rc != ZOK) {
			log_fatal("Failed to respond path is %s code is %s", zk_path, zku_op2String(rc));
			_exit(EXIT_FAILURE);
		}
		log_info("Sending ACK to %s for region %s", msg->sender, msg->region.id);
		free(zk_path);
		break;
	case KRM_CLOSE_REGION:
		log_fatal("Unsupported types KRM_CLOSE_REGION");
		_exit(EXIT_FAILURE);
	case KRM_BUILD_PRIMARY:
		log_fatal("Unsupported types KRM_BUILD_PRIMARY");
		_exit(EXIT_FAILURE);
	case KRM_ACK_OPEN_PRIMARY:
	case KRM_ACK_OPEN_BACKUP: {
		assert(server->role == KRM_LEADER);
		log_info("Received message %s for region %s", krm_msg_type_tostring(msg->type), msg->region.id);

		// Find sender's region
		struct krm_leader_ds_region_map *ds_region =
			krm_leader_get_ds_region(server, &msg->region, msg->sender);
		assert(ds_region);
		// The first time a region is opened, the status should be KRM_OPENING. If a
		// failure has occured and a reopen
		// command was sent to a new dataserver, the region's status will be
		// KRM_OPEN
		if (ds_region->lr_state.status != KRM_OPEN) {
			assert(ds_region->lr_state.status == KRM_OPENING);
			// Mark region as open
			ds_region->lr_state.status = KRM_OPEN;
		}
		break;
	}
	case KRM_NACK_OPEN_PRIMARY:
	case KRM_NACK_OPEN_BACKUP: {
		assert(server->role == KRM_LEADER);
		// Find sender's region
		struct krm_leader_ds_region_map *ds_region =
			krm_leader_get_ds_region(server, &msg->region, msg->sender);
		assert(ds_region);
		assert(ds_region->lr_state.status == KRM_OPENING || ds_region->lr_state.status == KRM_OPEN);
		// Resend open command
		log_info("Resend open command for region %s to dataserver %s", msg->region.id, msg->sender);
		enum krm_msg_type open_command_type =
			(msg->type == KRM_NACK_OPEN_PRIMARY) ? KRM_OPEN_REGION_AS_PRIMARY : KRM_OPEN_REGION_AS_BACKUP;
		krm_resend_open_command(server, &msg->region, msg->sender, open_command_type);
		break;
	}
	default:
		log_fatal("wrong type %d", msg->type);
		assert(0);
		_exit(EXIT_FAILURE);
	}
}

int krm_zk_get_server_name(char *dataserver_name, struct krm_server_desc const *my_desc, struct krm_server_name *dst,
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

static int krm_zk_update_server_name(struct krm_server_desc *my_desc, struct krm_server_name *src)
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

void *krm_metadata_server(void *args)
{
	struct krm_server_desc *my_desc = (struct krm_server_desc *)args;
	pthread_setname_np(pthread_self(), "meta_server");
	zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
	struct String_vector mail_msgs;
	memset(&mail_msgs, 0x00, sizeof(struct String_vector));
	struct Stat stat;
	int rc;
	my_desc->state = KRM_BOOTING;

	if (gethostname(my_desc->name.hostname, KRM_HOSTNAME_SIZE) != 0) {
		log_fatal("failed to get my hostname");
		_exit(EXIT_FAILURE);
	}
	/*now fix your kreon hostname*/
	sprintf(my_desc->name.kreon_ds_hostname, "%s:%u", my_desc->name.hostname, my_desc->RDMA_port);
	krm_get_IP_Addresses(my_desc);
	char *mail_path =
		zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, my_desc->name.kreon_ds_hostname);
	assert(strlen(mail_path) <= KRM_HOSTNAME_SIZE - 1);
	strcpy(my_desc->mail_path, mail_path);
	log_info("mail path is %s", my_desc->mail_path);
	free(mail_path);
	log_info("Creating pool for remote compactions of size %d", RCO_POOL_SIZE);
	my_desc->compaction_pool = rco_init_pool(my_desc, RCO_POOL_SIZE);
	while (1) {
		switch (my_desc->state) {
		case KRM_BOOTING: {
			sem_init(&my_desc->wake_up, 0, 0);
			my_desc->msg_list = klist_init();
			log_info("Booting kreonR server, my hostname is %s checking my presence "
				 "at zookeeper %s",
				 my_desc->name.kreon_ds_hostname, globals_get_zk_host());
			log_info("Initializing connection with zookeeper at %s", globals_get_zk_host());
			my_desc->zh = zookeeper_init(globals_get_zk_host(), zk_main_watcher, 15000, 0, my_desc, 0);
			if (my_desc->zh == NULL) {
				log_fatal("failed to connect to zk %s", globals_get_zk_host());
				perror("Reason");
				_exit(EXIT_FAILURE);
			}
			field_spin_for_value(&my_desc->zconn_state, KRM_CONNECTED);

			int zk_rc;
			rc = krm_zk_get_server_name(my_desc->name.kreon_ds_hostname, my_desc, &my_desc->name, &zk_rc);
			if (rc != 0) {
				if (zk_rc != ZOK)
					log_fatal("Could not retrieve my entry. Zookeeper error: %s",
						  zku_op2String(rc));
				else
					log_fatal("Error while parsing my entry's json string (dataserver name = %s)",
						  my_desc->name.kreon_ds_hostname);
				_exit(EXIT_FAILURE);
			}

			if (my_desc->name.epoch == 0) {
				log_debug("First time I join setting my epoch to 1 and initializing "
					  "volume %s",
					  globals_get_dev());
				globals_init_volume();
				log_info("Volume %s formatted successfully", globals_get_dev());
				my_desc->name.epoch = 1;
			} else {
				log_info("Rebooted, my previous epoch was %lu setting to %lu", my_desc->name.epoch,
					 my_desc->name.epoch + 1);
				++my_desc->name.epoch;
			}

			// Update RDMA IP
			krm_get_IP_Addresses(my_desc);

			// Update leader
			struct String_vector *leader = (struct String_vector *)calloc(1, sizeof(struct String_vector));
			log_debug("Ok I am part of the team now what is my role, Am I the leader?");
			char *leader_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_LEADER_PATH);
			rc = zoo_get_children(my_desc->zh, leader_path, 0, leader);
			if (rc != ZOK) {
				log_fatal("Can't find leader! error %s", zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			if (leader->count == 0) {
				log_fatal("leader hostname is missing!");
				_exit(EXIT_FAILURE);
			}
			strcpy(my_desc->name.kreon_leader, leader->data[0]);
			free(leader);
			free(leader_path);

			if (strcmp(my_desc->name.kreon_leader, my_desc->name.kreon_ds_hostname) == 0) {
				log_debug("Hello I am the Leader %s", my_desc->name.kreon_ds_hostname);
				my_desc->role = KRM_LEADER;
			} else {
				log_debug("Hello I am %s just a slave Leader is %s", my_desc->name.kreon_ds_hostname,
					  my_desc->name.kreon_leader);
				my_desc->role = KRM_DATASERVER;
			}

			// Update my zookeeper entry
			krm_zk_update_server_name(my_desc, &my_desc->name);

			/*init ds_regions table*/
			my_desc->ds_regions = (struct krm_ds_regions *)calloc(1, sizeof(struct krm_ds_regions));
			memset(my_desc->ds_regions, 0x00, sizeof(struct krm_ds_regions));
			my_desc->state = KRM_CLEAN_MAILBOX;
			break;
		}
		case KRM_CLEAN_MAILBOX: {
			struct krm_msg msg;
			int buffer_len;
			log_debug("Cleaning stale messages from my mailbox from previous epoch "
				  "and leaving a watcher");
			char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
							   my_desc->name.kreon_ds_hostname);
			rc = zoo_get_children(my_desc->zh, zk_path, 0, &mail_msgs);
			if (rc != ZOK) {
				log_fatal("failed to query zookeeper for path %s contents with code %s", zk_path,
					  zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			int i;
			log_debug("message count %d", mail_msgs.count);
			for (i = 0; i < mail_msgs.count; i++) {
				/*iterate old mails and delete them*/
				char *mail = zku_concat_strings(6, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
								my_desc->name.kreon_ds_hostname, KRM_SLASH,
								mail_msgs.data[i]);
				/*get message first to reply*/
				buffer_len = sizeof(struct krm_msg);
				rc = zoo_get(my_desc->zh, mail, 0, (char *)&msg, &buffer_len, &stat);
				if (rc != ZOK) {
					log_fatal("Failed to fetch email %s with code %s", mail, zku_op2String(rc));
					_exit(EXIT_FAILURE);
				}
				log_info("fetched mail %s", mail);
				krm_process_msg(my_desc, &msg);
				/*now delete it*/
				log_info("Deleting %s", mail);
				rc = zoo_delete(my_desc->zh, mail, -1);
				if (rc != ZOK) {
					log_fatal("failed to delete stale mail msg %s error %s", mail,
						  zku_op2String(rc));
					_exit(EXIT_FAILURE);
				}
				free(mail);
			}

			strcpy(my_desc->mail_path, zk_path);
			log_debug("Setting watcher for mailbox %s", my_desc->mail_path);
			rc = zoo_wget_children(my_desc->zh, my_desc->mail_path, mailbox_watcher, my_desc, &mail_msgs);
			if (rc != ZOK) {
				log_fatal("failed to set watcher for my mailbox %s with error code %s", zk_path,
					  zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			free(zk_path);

			if (my_desc->role == KRM_LEADER)
				my_desc->state = KRM_LD_ANNOUNCE_JOINED;
			else
				my_desc->state = KRM_SET_DS_WATCHERS;
			break;
		}
		case KRM_BUILD_DATASERVERS_TABLE: {
			/*leader gets all team info*/
			struct String_vector dataservers;
			char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_SERVERS_PATH);
			rc = zoo_get_children(my_desc->zh, zk_path, 0, &dataservers);
			if (rc != ZOK) {
				log_fatal("Leader (path %s)failed to build dataservers table with code %s", zk_path,
					  zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}

			struct krm_server_name ds;
			char dataserver_json[2048];
			int dataserver_json_length = sizeof(dataserver_json);
			memset(dataserver_json, 0, dataserver_json_length);
			for (int i = 0; i < dataservers.count; i++) {
				int zk_rc;
				rc = krm_zk_get_server_name(dataservers.data[i], my_desc, &ds, &zk_rc);
				if (rc) {
					if (zk_rc != ZOK)
						log_fatal("Cannot find entry for dataserver %s in zookeeper",
							  dataservers.data[i]);
					else
						log_fatal(
							"Error while parsing json string data of dataserver %s from zookeeper",
							dataservers.data[i]);
					_exit(EXIT_FAILURE);
				}

				struct krm_leader_ds_map *dataserver =
					(struct krm_leader_ds_map *)calloc(1, sizeof(struct krm_leader_ds_map));
				dataserver->server_id = ds;
				dataserver->hash_key =
					djb2_hash((unsigned char *)ds.kreon_ds_hostname, strlen(ds.kreon_ds_hostname));

				dataserver->region_map = NULL;
				dataserver->num_regions = 0;
				// hash_key = dataserver->hash_key;
				/*added to hash table*/
				HASH_ADD_PTR(my_desc->dataservers_map, hash_key, dataserver);
				// Set a watcher to check dataserver's health
				char *zk_alive_dataserver_path = zku_concat_strings(
					4, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH, KRM_SLASH, ds.kreon_ds_hostname);
				log_debug("Leader: set watcher for %s", zk_alive_dataserver_path);
				rc = zoo_wexists(my_desc->zh, zk_alive_dataserver_path, dataserver_health_watcher,
						 my_desc, &stat);
				if (rc != ZOK && rc != ZNONODE) {
					log_fatal("Failed to set watcher for path %s", zk_alive_dataserver_path);
					_exit(EXIT_FAILURE);
				}
				free(zk_alive_dataserver_path);
			}
			free(zk_path);

			my_desc->state = KRM_BUILD_REGION_TABLE;
			break;
		}
		case KRM_BUILD_REGION_TABLE: {
			my_desc->ld_regions = (struct krm_leader_regions *)calloc(1, sizeof(struct krm_leader_regions));
			struct String_vector regions;
			/*read all regions and construct table*/
			char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_REGIONS_PATH);
			rc = zoo_get_children(my_desc->zh, zk_path, 0, &regions);
			if (rc != ZOK) {
				log_fatal("Leader failed to read regions with code %s", zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			assert(regions.count <= KRM_MAX_REGIONS);
			struct krm_region r;
			char region_json_string[2048];
			memset(region_json_string, 0, sizeof(region_json_string));
			for (int i = 0; i < regions.count; i++) {
				char *region_path = zku_concat_strings(3, zk_path, KRM_SLASH, regions.data[i]);
				int region_json_string_length = sizeof(region_json_string);
				rc = zoo_get(my_desc->zh, region_path, 0, region_json_string,
					     &region_json_string_length, &stat);
				if (rc != ZOK) {
					log_fatal("Failed to retrieve region %s from Zookeeper", region_path);
					_exit(EXIT_FAILURE);
				} else if (stat.dataLength > (int64_t)sizeof(region_json_string)) {
					log_fatal(
						"Statically allocated buffer is not large enough to hold the json region entry."
						"Json region entry length is %d and buffer size is %d",
						stat.dataLength, sizeof(region_json_string));
					_exit(EXIT_FAILURE);
				}
				cJSON *region_json =
					cJSON_ParseWithLength(region_json_string, region_json_string_length);
				if (cJSON_IsInvalid(region_json)) {
					log_fatal("Failed to parse json string %s of region %s", region_json_string,
						  region_path);
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
				strncpy(r.id, cJSON_GetStringValue(id), KRM_MAX_REGION_ID_SIZE);
				strncpy(r.min_key, cJSON_GetStringValue(min_key), KRM_MAX_KEY_SIZE);
				if (!strcmp(r.min_key, "-oo")) {
					memset(r.min_key, 0, KRM_MAX_KEY_SIZE);
					r.min_key_size = 1;
				} else {
					r.min_key_size = strlen(r.min_key);
				}
				strncpy(r.max_key, cJSON_GetStringValue(max_key), KRM_MAX_KEY_SIZE);
				r.max_key_size = strlen(r.max_key);
				r.stat = (enum krm_region_status)cJSON_GetNumberValue(status);
				// Find primary's krm_server_name struct
				struct krm_leader_ds_map *ds_map;
				uint64_t hash_key = djb2_hash((unsigned char *)cJSON_GetStringValue(primary),
							      strlen(cJSON_GetStringValue(primary)));
				HASH_FIND_PTR(my_desc->dataservers_map, &hash_key, ds_map);
				r.primary = ds_map->server_id;
				// Find each backup's krm_server_name struct
				r.num_of_backup = cJSON_GetArraySize(backups);
				for (int j = 0; j < cJSON_GetArraySize(backups); ++j) {
					char *backup_dataserver_name =
						cJSON_GetStringValue(cJSON_GetArrayItem(backups, j));
					hash_key = djb2_hash((unsigned char *)backup_dataserver_name,
							     strlen(backup_dataserver_name));
					HASH_FIND_PTR(my_desc->dataservers_map, &hash_key, ds_map);
					r.backups[j] = ds_map->server_id;
				}
				/*log_info("Adding region %s, %s, %s", r.id, r.min_key, r.max_key);*/

				if (krm_insert_ld_region(my_desc, &r) != KRM_SUCCESS) {
					log_fatal("Failed to add region %s, %s, %s", r.id, r.min_key, r.max_key);
					_exit(EXIT_FAILURE);
				}

				cJSON_Delete(region_json);
				free(region_path);
			}
			free(zk_path);

			krm_iterate_ld_regions(my_desc);
			krm_check_ld_regions_sorted(my_desc->ld_regions);
			// krm_iterate_servers_state(&my_desc);
			my_desc->state = KRM_ASSIGN_REGIONS;
			break;
		}
		case KRM_ASSIGN_REGIONS: {
			int i;
			for (i = 0; i < my_desc->ld_regions->num_regions; i++) {
				krm_send_open_command(my_desc, &my_desc->ld_regions->regions[i]);
			}

			krm_iterate_servers_state(my_desc);
			my_desc->state = KRM_OPEN_LD_REGIONS;
			break;
		}
		case KRM_OPEN_LD_REGIONS: {
			log_debug("Leader opening my regions", my_desc->name.kreon_ds_hostname);

			struct krm_leader_ds_map *ds_map;
			uint64_t hash_key = djb2_hash((unsigned char *)my_desc->name.kreon_ds_hostname,
						      strlen(my_desc->name.kreon_ds_hostname));
			HASH_FIND_PTR(my_desc->dataservers_map, &hash_key, ds_map);
			if (ds_map == NULL) {
				log_fatal("entry missing for DataServer (which is me?) %s",
					  my_desc->name.kreon_ds_hostname);
				_exit(EXIT_FAILURE);
			}

			/*iterate over regions*/
			struct krm_leader_ds_region_map *current, *tmp = NULL;

			HASH_ITER(hh, ds_map->region_map, current, tmp)
			{
				log_info("Opening DB %s", current->lr_state.region->id);
				struct krm_region_desc *r_desc =
					(struct krm_region_desc *)calloc(1, sizeof(struct krm_region_desc));

				pthread_mutex_init(&r_desc->region_mgmnt_lock, NULL);
				if (pthread_rwlock_init(&r_desc->kreon_lock, NULL) != 0) {
					log_fatal("Failed to init region read write lock");
					_exit(EXIT_FAILURE);
				}

				pthread_rwlock_init(&r_desc->replica_log_map_lock, NULL);
				r_desc->region = current->lr_state.region;
				r_desc->role = current->lr_state.role;
				r_desc->replica_buf_status = KRM_BUFS_UNINITIALIZED;
				r_desc->m_state = NULL;
				r_desc->r_state = NULL;

				// open Kreon db
				r_desc->db = custom_db_open(globals_get_dev(), 0, globals_get_dev_size(),
							    r_desc->region->id, CREATE_DB, globals_get_l0_size(),
							    globals_get_growth_factor());

				r_desc->status = KRM_OPEN;
				/*this copies r_desc struct to the regions array!*/
				r_desc->replica_log_map = NULL;
				for (int i = 0; i < MAX_LEVELS; i++)
					r_desc->replica_index_map[i] = NULL;
				krm_insert_ds_region(my_desc, r_desc, my_desc->ds_regions);
				/*find system ref*/
				struct krm_region_desc *t = krm_get_region(my_desc, current->lr_state.region->min_key,
									   current->lr_state.region->min_key_size);
				/*set the callback and context for remote compaction*/
				log_debug("Setting DB %s in replicated mode", t->db->db_desc->db_name);
				bt_set_db_in_replicated_mode(t->db);
				set_init_index_transfer(t->db->db_desc, &rco_init_index_transfer);
				set_destroy_local_rdma_buffer(t->db->db_desc, &rco_destroy_local_rdma_buffer);
				set_send_index_segment_to_replicas(t->db->db_desc, &rco_send_index_segment_to_replicas);
				bt_set_flush_replicated_logs_callback(t->db->db_desc, rco_flush_last_log_segment);
				rco_add_db_to_pool(my_desc->compaction_pool, t);
			}
			my_desc->state = KRM_WAITING_FOR_MSG;
			break;
		}
		case KRM_SET_DS_WATCHERS: {
			struct Stat;
			/*wait until leader is up*/
			char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_ALIVE_LEADER_PATH, KRM_SLASH,
							   my_desc->name.kreon_leader);
			while (1) {
				rc = zoo_exists(my_desc->zh, zk_path, 0, &stat);
				if (rc == ZOK)
					break;
				else {
					log_warn("Waiting for leader %s to join where is he?",
						 my_desc->name.kreon_leader);
					sleep(2);
				}
			}
			log_info("Leaving a watcher to detect possible leader failure of %s",
				 my_desc->name.kreon_leader);
			rc = zoo_wexists(my_desc->zh, zk_path, leader_health_watcher, my_desc, &stat);
			if (rc != ZOK) {
				log_fatal("Failed to set watcher for leader health path %s", zk_path);
				_exit(EXIT_FAILURE);
			}
			free(zk_path);
			log_info("already Set mailbox watcher");
			my_desc->state = KRM_DS_ANNOUNCE_JOINED;
			break;
		}
		case KRM_LD_ANNOUNCE_JOINED: {
			char path[KRM_HOSTNAME_SIZE];
			/*create an ephemeral node under /kreonR/aliveservers*/
			char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_ALIVE_LEADER_PATH, KRM_SLASH,
							   my_desc->name.kreon_ds_hostname);
			rc = zoo_create(my_desc->zh, zk_path, (const char *)&my_desc->name,
					sizeof(struct krm_server_name), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, path,
					KRM_HOSTNAME_SIZE);
			if (rc == ZOK) {
				log_debug("LEADER: Ok announced my presence path created %s", path);
			} else {
				log_fatal("Failed to annouce my presence code %s", zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			free(zk_path);
			my_desc->state = KRM_BUILD_DATASERVERS_TABLE;
			break;
		}
		case KRM_DS_ANNOUNCE_JOINED: {
			char path[KRM_HOSTNAME_SIZE];
			/*create an ephemeral node under /kreonR/aliveservers*/
			char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH, KRM_SLASH,
							   my_desc->name.kreon_ds_hostname);
			rc = zoo_create(my_desc->zh, zk_path, (const char *)&my_desc->name,
					sizeof(struct krm_server_name), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, path,
					KRM_HOSTNAME_SIZE);
			if (rc == ZOK) {
				log_info("Ok announced my presence as a dataservver path created %s", path);
			} else {
				log_fatal("Failed to annouce my presence code %s", zku_op2String(rc));
				_exit(EXIT_FAILURE);
			}
			free(zk_path);
			my_desc->state = KRM_WAITING_FOR_MSG;
			break;
		}
		case KRM_WAITING_FOR_MSG: {
			struct klist_node *node;

			pthread_mutex_lock(&my_desc->msg_list_lock);
			node = klist_remove_first(my_desc->msg_list);
			pthread_mutex_unlock(&my_desc->msg_list_lock);
			if (!node)
				/*go to sleep*/
				sem_wait(&my_desc->wake_up);
			else {
				log_info("new message: %s",
					 krm_msg_type_tostring(((struct krm_msg *)node->data)->type));
				my_desc->state = KRM_PROCESSING_MSG;
				krm_process_msg(my_desc, (struct krm_msg *)node->data);
				free(node->data);
				free(node);
				my_desc->state = KRM_WAITING_FOR_MSG;
			}
			break;
		}
		default:
			break;
		}
	}
	return NULL;
}

#if 0
struct krm_region_desc *krm_get_region_based_on_id(struct krm_server_desc *desc, char *region_id,
						   uint32_t region_id_size)
{
	struct krm_region_desc *r_desc = NULL;

	uint64_t lc2, lc1;
retry:
	lc2 = desc->ds_regions->lamport_counter_2;
	int start_idx;
	int end_idx;
	int middle;
	int ret;
	start_idx = 0;
	end_idx = desc->ds_regions->num_ds_regions - 1;
	r_desc = NULL;
	/*log_info("start %d end %d", start_idx, end_idx);*/
	while (start_idx <= end_idx) {
		middle = (start_idx + end_idx) / 2;
		ret = zku_key_cmp(desc->ds_regions->r_desc[middle]->region->min_key_size,
				  desc->ds_regions->r_desc[middle]->region->min_key, region_id_size, region_id);

		if (ret < 0 || ret == 0) {
			/*log_info("got 0 checking with max key %s",
* desc->ds_regions->r_desc[middle].region->max_key);*/
			start_idx = middle + 1;
			if (zku_key_cmp(desc->ds_regions->r_desc[middle]->region->max_key_size,
					desc->ds_regions->r_desc[middle]->region->max_key, region_id_size,
					region_id) > 0) {
				r_desc = desc->ds_regions->r_desc[middle];
				break;
			}
		} else
			end_idx = middle - 1;
	}
	/*cornercase*/
	if (r_desc == NULL) {
		int ret1;
		int ret2;
		assert(desc->ds_regions->num_ds_regions > 0);
		end_idx = desc->ds_regions->num_ds_regions - 1;
		ret1 = zku_key_cmp(desc->ds_regions->r_desc[end_idx]->region->min_key_size,
				   desc->ds_regions->r_desc[end_idx]->region->min_key, region_id_size, region_id);
		ret2 = zku_key_cmp(region_id_size, region_id, desc->ds_regions->r_desc[end_idx]->region->max_key_size,
				   desc->ds_regions->r_desc[end_idx]->region->max_key);
		log_info("region_min_key %d:%s   key %d:%s  end idx %d",
			 desc->ds_regions->r_desc[end_idx]->region->min_key_size,
			 desc->ds_regions->r_desc[end_idx]->region->min_key, region_id_size, region_id, end_idx);

		log_info("region_max_key %d:%s   key %d:%s  end idx %d",
			 desc->ds_regions->r_desc[end_idx]->region->max_key_size,
			 desc->ds_regions->r_desc[end_idx]->region->max_key, region_id_size, region_id, end_idx);
		if (ret1 >= 0 && ret2 < 0)
			r_desc = desc->ds_regions->r_desc[end_idx];
	}

	lc1 = desc->ds_regions->lamport_counter_2;

	if (lc1 != lc2)
		goto retry;

	if (r_desc == NULL) {
		log_fatal("NULL region for region_id %s", region_id);
		raise(SIGINT);
		_exit(EXIT_FAILURE);
	}
	return r_desc;
}
#endif

struct krm_region_desc *krm_get_region(struct krm_server_desc const *server_desc, char *key, uint32_t key_size)
{
	struct krm_region_desc *r_desc = NULL;

	uint64_t lc2, lc1;
retry:
	lc2 = server_desc->ds_regions->lamport_counter_2;
	int start_idx = 0;
	int end_idx = server_desc->ds_regions->num_ds_regions - 1;
	r_desc = NULL;
	/*log_info("start %d end %d", start_idx, end_idx);*/
	while (start_idx <= end_idx) {
		int middle = (start_idx + end_idx) / 2;
		int ret = zku_key_cmp(server_desc->ds_regions->r_desc[middle]->region->min_key_size,
				      server_desc->ds_regions->r_desc[middle]->region->min_key, key_size, key);

		if (ret < 0 || ret == 0) {
			/*log_info("got 0 checking with max key %s",
* desc->ds_regions->r_desc[middle].region->max_key);*/
			start_idx = middle + 1;
			if (zku_key_cmp(server_desc->ds_regions->r_desc[middle]->region->max_key_size,
					server_desc->ds_regions->r_desc[middle]->region->max_key, key_size, key) > 0) {
				r_desc = server_desc->ds_regions->r_desc[middle];
				break;
			}
		} else
			end_idx = middle - 1;
	}
	/*cornercase*/
	if (r_desc == NULL) {
		int ret1;
		int ret2;
		end_idx = server_desc->ds_regions->num_ds_regions - 1;
		ret1 = zku_key_cmp(server_desc->ds_regions->r_desc[end_idx]->region->min_key_size,
				   server_desc->ds_regions->r_desc[end_idx]->region->min_key, key_size, key);
		ret2 = zku_key_cmp(key_size, key, server_desc->ds_regions->r_desc[end_idx]->region->max_key_size,
				   server_desc->ds_regions->r_desc[end_idx]->region->max_key);
		log_info("region_min_key %d:%s   key %d:%s  end idx %d",
			 server_desc->ds_regions->r_desc[end_idx]->region->min_key_size,
			 server_desc->ds_regions->r_desc[end_idx]->region->min_key, key_size, key, end_idx);

		log_info("region_max_key %d:%s   key %d:%s  end idx %d",
			 server_desc->ds_regions->r_desc[end_idx]->region->max_key_size,
			 server_desc->ds_regions->r_desc[end_idx]->region->max_key, key_size, key, end_idx);
		if (ret1 >= 0 && ret2 < 0)
			r_desc = server_desc->ds_regions->r_desc[end_idx];
	}

	lc1 = server_desc->ds_regions->lamport_counter_2;

	if (lc1 != lc2)
		goto retry;

	if (r_desc == NULL) {
		log_fatal("NULL region for key %s", key);
		/*raise(SIGINT);*/
		_exit(EXIT_FAILURE);
	}
	return r_desc;
}
