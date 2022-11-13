#define _GNU_SOURCE
#include "../../utilities/spin_loop.h"
#include "../djb2.h"
#include "../globals.h"
#include "../metadata.h"
#include "../zk_utils.h"
#include "command.h"
#include "mregion_server.h"
#include "region.h"
#include "region_log.h"
#include "uthash.h"
#include "zookeeper.h"
#include "zookeeper.jute.h"
#include <cJSON.h>
#include <log.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>

#define ZOOKEEPER_SESSION_TIMEOUT 15000
#define JSON_BUFFER_SIZE (2048)
#define PROPOSAL_VALUE_LEN (256)

#define MAX_MAILBOX_PATH_SIZE (256)
#define MAX_REGION_SERVERS (1024)
/**
 * Watcher for failed Zookeeper connections
 */
typedef void (*master_watcher_t)(zhandle_t *, int, int, const char *, void *);

struct region_table_s {
	region_t region;
	uint64_t region_key;
	UT_hash_handle hh;
};

struct region_server_table_entry_s {
	region_server_t region_server;
	uint64_t server_key;
	UT_hash_handle hh;
	bool slot_in_use;
};

struct region_server_table_s {
	struct region_server_table_entry_s region_servers[MAX_REGION_SERVERS];
	struct region_server_table_entry_s *hash_table_root;
	int num_elements;
	int capacity;
};

struct region_server_table_iterator_s {
	struct region_server_table_s *table;
	int pos;
};

static struct region_server_table_s *create_region_server_table(void)
{
	struct region_server_table_s *table = calloc(1UL, sizeof(struct region_server_table_s));
	table->capacity = MAX_REGION_SERVERS;
	return table;
}

static region_server_t find_server(struct region_server_table_s *table, char *region_server_name)
{
	uint64_t server_key = djb2_hash((unsigned char *)region_server_name, strlen(region_server_name));
	struct region_server_table_entry_s *server = NULL;
	HASH_FIND_PTR(table->hash_table_root, &server_key, server);
	return server ? server->region_server : NULL;
}

struct region_server_table_entry_s *allocate_region_server_slot(struct region_server_table_s *table)
{
	for (int i = 0; i < table->capacity; ++i) {
		if (table->region_servers[i].slot_in_use)
			continue;
		table->region_servers[i].slot_in_use = true;
		table->region_servers[i].region_server = NULL;
		++table->num_elements;
		return &table->region_servers[i];
	}
	return NULL;
}

static struct region_server_table_iterator_s *create_region_server_table_iterator(struct region_server_table_s *table)
{
	struct region_server_table_iterator_s *iterator = calloc(1UL, sizeof(*iterator));
	iterator->table = table;
	iterator->pos = -1;
	return iterator;
}

struct region_server_table_entry_s *get_next_region_server_table_entry(struct region_server_table_iterator_s *iterator)
{
start:
	if (++iterator->pos >= iterator->table->num_elements)
		return NULL;
	if (!iterator->table->region_servers[iterator->pos].slot_in_use)
		goto start;
	return &iterator->table->region_servers[iterator->pos];
}

static void close_region_server_table_iterator(struct region_server_table_iterator_s *iterator)
{
	free(iterator);
}

struct master_s {
	char master_hostname[KRM_HOSTNAME_SIZE];
	char proposal[PROPOSAL_VALUE_LEN];
	char *mailbox_path;
	pthread_mutex_t master_lock;
	pthread_mutex_t fresh_boot_lock;
	master_watcher_t master_watcher;
	sem_t barrier;
	int64_t leadership_clock;
	int64_t command_counter;
	zhandle_t *zhandle;
	region_log_t region_log;
	struct region_table_s *region_table;
	struct region_server_table_s *alive_servers;
	bool master_started;
	uint8_t zookeeper_conn_state;
};

#if 0
static void clear_region_server_slot(struct region_server *server_table)
{
	memset(server_table, 0x00, sizeof(struct region_server));
	server_table->slot_in_use = true;
}
#endif

/**
 * Watcher we use to process session events. In particular,
 * when it receives a ZOO_CONNECTED_STATE event, we set the
 * connected variable so that we know that the session has
 * been established.
 */
static void zk_main_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	struct master_s *master = (struct master_s *)context;
	/**
   * zookeeper_init might not have returned, so we use zkh instead.
  */
	log_debug("MAIN watcher type %d state %d path %s", type, state, path);
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			master->zookeeper_conn_state = KRM_CONNECTED;

		} else if (state == ZOO_CONNECTING_STATE) {
			if (master->zookeeper_conn_state == KRM_CONNECTED) {
				log_warn("Disconnected from zookeeper %s trying to reconnect", globals_get_zk_host());
				master->zhandle = zookeeper_init(globals_get_zk_host(), zk_main_watcher,
								 ZOOKEEPER_SESSION_TIMEOUT, 0, master, 0);
				log_warn("Connected! TODO re register watchers");
			}
		}
		return;
	}
	log_warn("Unhandled event");
}

static void MASTER_process_command_rep(MC_command_t reply)
{
	MC_print_command(reply);
	(void)reply;
}
/**
 * Watches for incoming messages from Region Servers. In case when a master
 * fails and another takes over it reads possible missing messages.
 */
static void MASTER_mailbox_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	(void)context;
	struct master_s *master = (struct master_s *)context;
	MUTEX_LOCK(&master->master_lock);
	struct String_vector mails = { 0 };
	int ret_code = zoo_wget_children(master->zhandle, path, MASTER_mailbox_watcher, master, &mails);
	if (ZOK != ret_code) {
		log_fatal("failed to read alive_servers %s error code: %s ", path, zerror(ret_code));
		_exit(EXIT_FAILURE);
	}

	for (int i = 0; i < mails.count; ++i) {
		char *mail_path = zku_concat_strings(3, path, KRM_SLASH, mails.data[i]);
		char *cmd_buf = calloc(1UL, MC_get_command_size());
		int cmd_buf_len = MC_get_command_size();
		struct Stat stat = { 0 };
		int ret_code = zoo_get(master->zhandle, mail_path, 0, cmd_buf, &cmd_buf_len, &stat);
		if (ret_code != ZOK) {
			log_fatal("Master failed to fetch mail %s reason: %s", mail_path, zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		MASTER_process_command_rep((MC_command_t)cmd_buf);
		ret_code = zoo_delete(master->zhandle, mail_path, -1);
		if (ret_code != ZOK) {
			log_fatal("Master failed to delete mail %s reason: %s", mail_path, zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		free(mail_path);
		mail_path = NULL;
	}
	MUTEX_UNLOCK(&master->master_lock);
}

static bool MASTER_get_region_server_name(char *region_server_name, zhandle_t *zhandle,
					  struct krm_server_name *server_name)
{
	/*check if you are hostname-RDMA_port belongs to the project*/
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, region_server_name);
	struct Stat stat;
	char buffer[JSON_BUFFER_SIZE];
	int buffer_len = JSON_BUFFER_SIZE;
	int ret_code = zoo_get(zhandle, zk_path, 0, buffer, &buffer_len, &stat);

	if (ret_code != ZOK) {
		log_warn("Failed fetching info from zookeeper for server %s Reason: %s", zk_path,
			 zku_op2String(ret_code));
		free(zk_path);
		return NULL;
	}

	free(zk_path);

	// Parse json string with server's krm_server_name struct
	cJSON *json = cJSON_ParseWithLength(buffer, buffer_len);
	if (!cJSON_IsObject(json)) {
		cJSON_Delete(json);
		log_warn("Failed to parser server json info");
		return NULL;
	}

	cJSON *hostname = cJSON_GetObjectItem(json, "hostname");
	cJSON *dataserver_name_retrieved = cJSON_GetObjectItem(json, "dataserver_name");
	cJSON *rdma_ip = cJSON_GetObjectItem(json, "rdma_ip_addr");
	cJSON *epoch = cJSON_GetObjectItem(json, "epoch");
	cJSON *leader = cJSON_GetObjectItem(json, "leader");
	if (!cJSON_IsString(hostname) || !cJSON_IsString(dataserver_name_retrieved) || !cJSON_IsString(rdma_ip) ||
	    !cJSON_IsNumber(epoch) || !cJSON_IsString(leader)) {
		cJSON_Delete(json);
		log_warn("Failed to retrieve all of the server info from json. Possible data corruption?");
		return false;
	}
	strncpy(server_name->hostname, cJSON_GetStringValue(hostname), KRM_HOSTNAME_SIZE);
	strncpy(server_name->kreon_ds_hostname, cJSON_GetStringValue(dataserver_name_retrieved), KRM_HOSTNAME_SIZE);
	server_name->kreon_ds_hostname_length = strlen(cJSON_GetStringValue(dataserver_name_retrieved));
	strncpy(server_name->RDMA_IP_addr, cJSON_GetStringValue(rdma_ip), KRM_MAX_RDMA_IP_SIZE);
	server_name->epoch = cJSON_GetNumberValue(epoch);
	strncpy(server_name->kreon_leader, cJSON_GetStringValue(leader), KRM_HOSTNAME_SIZE);

	cJSON_Delete(json);

	return true;
}

/**
 * Takes as input the server name notation we use in Tebis which is
 * <hostname>:<rdma_port>:<epoch> and returns the epoch of the server. Epoch
 * indicates how many times the same server has rejoined the system.
 */
#if 0
static uint64_t get_server_epoch(char *region_server_name)
{
	char name[MAX_SERVER_NAME] = { 0 };
	memcpy(name, region_server_name, strlen(region_server_name));
	int token_count = 0;
	for (char *token = strtok(name, ":"); token != NULL; token = strtok(name, ":")) {
		++token_count;
		if (3 != token_count)
			continue;
		return strtoull(token, NULL, 10);
	}
	log_fatal("Wrong server name %s", region_server_name);
	_exit(EXIT_FAILURE);
}
#endif

struct server_vector_s {
	int count;
	int capacity;
	char **names;
};

/**
 * Creates an empty server_vector with initial capacity
 */
static struct server_vector_s *MASTER_create_server_vector(int capacity)
{
	struct server_vector_s *vector = calloc(1UL, sizeof(struct server_vector_s));
	vector->capacity = capacity;

	vector->names = calloc(vector->capacity, sizeof(*vector->names));

	return vector;
}

/**
 * Adds an item in the vector
 */
static void MASTER_add_item_in_server_vector(struct server_vector_s *vector, void *item)
{
	int size = sizeof(vector->names);
	if (vector->count >= vector->capacity) {
		vector->names = realloc(vector->names, 2L * vector->capacity * size);
		if (NULL == vector->names) {
			log_fatal("Resizing failed to capacity %d!", 2 * vector->capacity);
			_exit(EXIT_FAILURE);
		}
		vector->capacity *= 2;
	}
	vector->names[vector->count++] = strdup(item);
}

/**
 * Transforms a String_vector struct obtained from Zookeeper to server_names
 * struct. We use the server_names struct because String_vector struct from
 * Zookeeper is immutable.
 */
static struct server_vector_s *MASTER_copy_from_string_vector(struct String_vector *servers)
{
	struct server_vector_s *server_vector = MASTER_create_server_vector(servers->count);
	server_vector->count = server_vector->capacity = servers->count;
	for (int i = 0; i < servers->count; ++i)
		server_vector->names[i] = strdup(servers->data[i]);
	return server_vector;
}

/**
 * Destroy a server_names struct
 */
static void MASTER_deallocate_server_names(struct server_vector_s *server_vector)
{
	for (int i = 0; i < server_vector->count; ++i)
		free(server_vector->names[i]);
	free(server_vector);
}

/**
 * Removes the server in the given position
 */
static void MASTER_remove_from_server_names(struct server_vector_s *server_vector, int position)
{
	free(server_vector->names[position]);
	memmove(&server_vector->names[position], &server_vector->names[position + 1],
		(server_vector->count - (position + 1)) * sizeof(char *));
	server_vector->count--;
}

static int MASTER_children_comparator(const void *child_1, const void *child_2)
{
	char **left_child = (char **)child_1;
	char **right_child = (char **)child_2;
	return strcmp(*left_child, *right_child);
}

/**
 * Searches if the server name is present in alive servers. Alive servers
 * contain server names in the form <hostname>:<rdma_port>:<epoch>.
 * @param alive_servers is the vector returned by Zookeeper get_children operation
 * @param region_server is the information of the region server that we need to
 * check if is alive or not
 * @returns the position (>= 0) of the server in the vector or -1 if it does
 * not find it.
 */
static int MASTER_get_server_pos(struct server_vector_s *server_vector, region_server_t region_server)
{
	for (int start = 0, end = server_vector->count - 1, middle = (server_vector->count - 1) / 2; start <= end;
	     middle = (end + start) / 2) {
		struct krm_server_name *region_server_hostname = RS_get_region_server_krm_hostname(region_server);
		char *hostname = region_server_hostname->kreon_ds_hostname;
		// log_debug("server name %s:len(%u) middle is %d name %s count %d",
		// 	  region_server_hostname->kreon_ds_hostname, strlen(region_server_hostname->kreon_ds_hostname),
		// 	  middle, server_vector->names[middle], server_vector->count);

		int ret_code = MASTER_children_comparator(&server_vector->names[middle], &hostname);
		if (ret_code < 0)
			start = middle + 1;
		else if (ret_code > 0)
			end = middle - 1;
		else
			return middle;
	}

	return -1;
}

/**
 * Iterates the region server array and for each valid entry it searches the
 * name of the server. Server names in the alives_vector are in the form
 * <hostname>:<rdma_port>:<epoch>. Alive_vector is sorted so it uses binary
 * search to locate them. In case of a it it removes the server name from the
 * alive_servers vector. In case of a miss it adds the server_name in the
 * dead_servers vector. On return, alive_servers vector contains the newly
 * joined servers whereas dead_servers contain the dead ones.
 */
static struct server_vector_s *MASTER_find_dead_servers(struct master_s *master, struct server_vector_s *alive_servers)
{
	qsort(alive_servers->names, alive_servers->count, sizeof(char *), MASTER_children_comparator);
	// for (int i = 0; i < alive_servers->count; i++)
	// 	log_debug("Found alive server %s", alive_servers->names[i]);

	struct server_vector_s *dead_servers = MASTER_create_server_vector(16);

	struct region_server_table_iterator_s *iter = create_region_server_table_iterator(master->alive_servers);

	for (struct region_server_table_entry_s *region_server_table_entry = get_next_region_server_table_entry(iter);
	     region_server_table_entry != NULL; region_server_table_entry = get_next_region_server_table_entry(iter)) {
		int position = MASTER_get_server_pos(alive_servers, region_server_table_entry->region_server);
		if (position >= 0) {
			MASTER_remove_from_server_names(alive_servers, position);
			continue;
		}
		struct krm_server_name *server_name =
			RS_get_region_server_krm_hostname(region_server_table_entry->region_server);
		// log_debug("Oops found dead server %s position is %d", server_name->kreon_ds_hostname, position);
		MASTER_add_item_in_server_vector(dead_servers, server_name->kreon_ds_hostname);
	}
	close_region_server_table_iterator(iter);
	return dead_servers;
}

/**
 * Checks if a server with a given epoch is alive
 */
static bool MASTER_is_server_alive(const char *server_name, struct master_s *master, uint64_t server_epoch)
{
	uint64_t region_server_key = djb2_hash((const unsigned char *)server_name, strlen(server_name));
	struct region_server_table_entry_s *region_server_entry = NULL;
	HASH_FIND_PTR(master->alive_servers->hash_table_root, &region_server_key, region_server_entry);
	if (!region_server_entry)
		return false;
	if (DEAD == RS_get_region_server_status(region_server_entry->region_server))
		return false;
	if (RS_get_server_clock(region_server_entry->region_server) != server_epoch)
		return false;
	return true;
}

static size_t MASTER_calculate_prefix_size(char *server_name)
{
	size_t prefix_size = strlen(server_name);

	for (; prefix_size != 0; --prefix_size) {
		if (':' == server_name[prefix_size])
			break;
	}
	return prefix_size;
}

static char *MASTER_choose_random_server(struct region_server_table_s *alive_servers, region_t region)
{
	int capacity = alive_servers->capacity;
	int start = rand() % capacity;
	struct krm_server_name *hostname = NULL;
	for (int i = start; i < start + capacity; ++i) {
		if (!alive_servers->region_servers[i % capacity].slot_in_use)
			continue;
		hostname = RS_get_region_server_krm_hostname(alive_servers->region_servers[i % capacity].region_server);
		size_t prefix_size = MASTER_calculate_prefix_size(hostname->kreon_ds_hostname);
		if (!REG_is_server_prefix_in_region_group(hostname->kreon_ds_hostname, prefix_size, region))
			break;
		hostname = NULL;
	}
	return hostname == NULL ? NULL : hostname->kreon_ds_hostname;
}

/**
 * Replaces dead backup or dead servers in the region, this should go
 */
static void MASTER_reconfigure_region(struct master_s *master, region_t region)
{
	int num_of_backup = REG_get_region_num_of_backups(region);
	for (int i = num_of_backup - 1; i >= 0; --i) {
		if (BACKUP_DEAD != REG_get_region_backup_role(region, i))
			continue;
		REG_remove_backup_from_region(region, i);
		char *new_host = MASTER_choose_random_server(master->alive_servers, region);
		if (NULL == new_host) {
			log_fatal("Cannot replace faulty server, no suitable server found");
			_exit(EXIT_FAILURE);
		}

		REG_append_backup_in_region(region, new_host);
		log_debug("I replaced faulty backup with %s for region:%s num backups: %d", new_host,
			  REG_get_region_id(region), REG_get_region_num_of_backups(region));
		i = REG_get_region_num_of_backups(region); //recheck
	}

	if (PRIMARY_DEAD != REG_get_region_primary_role(region))
		return;
	REG_remove_and_upgrade_primary(region);
	char *new_host = MASTER_choose_random_server(master->alive_servers, region);
	log_debug("I add backup %s due to faulty primary %s for region:%s", new_host, REG_get_region_primary(region),
		  REG_get_region_id(region));
	REG_append_backup_in_region(region, new_host);
}

/**
 * Handles or better reports that guys we lost data Sorry
 */
static void MASTER_handle_data_loss(region_t region)
{
	log_fatal("We lost data for region %s", REG_get_region_id(region));
	_exit(EXIT_FAILURE);
}
/**
 * Checks if primary and replicas are ok and healthy. If PRIMARY or BACK are
 * dead it characterizes them as PRIMARY_DEAD or BACKUP_DEAD respectively.
 * @param region: Region to be checked
 * @return number of faulty servers
 */
static int MASTER_check_replica_group_health(struct master_s *master, region_t region)
{
	int n_failures = 0;
	/*is primary healthy?*/

	if (!MASTER_is_server_alive(REG_get_region_primary(region), master, REG_get_region_primary_clock(region))) {
		log_debug("Setting primary %s of region %s to PRIMARY_DEAD", REG_get_region_primary(region),
			  REG_get_region_id(region));
		REG_set_region_primary_role(region, PRIMARY_DEAD);
		++n_failures;
	}

	for (int i = 0; i < REG_get_region_num_of_backups(region); ++i) {
		if (MASTER_is_server_alive(REG_get_region_backup(region, i), master,
					   REG_get_region_backup_clock(region, i)))
			continue;
		log_debug("Setting backup[%d] %s of region %s to BACKUP_DEAD", i, REG_get_region_backup(region, i),
			  REG_get_region_id(region));
		REG_set_region_backup_role(region, i, BACKUP_DEAD);
		++n_failures;
	}
	return n_failures;
}

static void MASTER_update_region_info(void)
{
}

static void MASTER_full_regions_check(struct master_s *master)
{
	struct region_table_s *curr = NULL;
	struct region_table_s *tmp = NULL;
	struct region_table_s *region_table = master->region_table;
	HASH_ITER(hh, region_table, curr, tmp)
	{
		int n_failures = MASTER_check_replica_group_health(master, region_table->region);
		if (REG_get_region_num_of_backups(region_table->region) + 1 == n_failures)
			MASTER_handle_data_loss(region_table->region);
		if (n_failures) {
			MASTER_reconfigure_region(master, region_table->region);
			//lock_region_table
			//send_message_to_primary(master, region_table->region);
			MASTER_update_region_info();
			//unlock region_table
		}
	}
}

/**
 * HASH table structure that keeps all the final state of the regions
 * reconfigured after suffering N failures.
 */
struct region_reconfiguration_s {
	struct region *region;
	UT_hash_handle hh;
};

/**
 * Iterates the regions of the failed server and 1)reconfigures them and 2)
 * sends the appropriate commands to the primary of the corresponding region.
 * Master keeps replica group info in a logical order. In case of a failed server
 * @param master Tebis master
 * @param server_name is the name of the failed server in the form
 * <hostname>:<port>:<epoch>
 */
static void MASTER_handle_region_server_failure(struct master_s *master, region_server_t region_server,
						struct region_reconfiguration_s **affected_regions)
{
	region_server_iterator_t region_it = RS_create_region_server_iterator(region_server);

	for (region_info_t region_info = RS_get_next_region_info(region_it); region_info != NULL;
	     region_info = RS_get_next_region_info(region_it)) {
		int n_failures = MASTER_check_replica_group_health(master, RS_get_region(region_info));
		if (0 == n_failures) {
			log_fatal("All regions in iterator must be related with failed server and thus have failures");
			_exit(EXIT_FAILURE);
		}
		MASTER_reconfigure_region(master, RS_get_region(region_info));
		struct region_reconfiguration_s *updated_region = NULL;
		HASH_FIND_PTR(*affected_regions, RS_get_region(region_info), updated_region);
		if (updated_region)
			continue;
		updated_region = calloc(1UL, sizeof(*updated_region));
		updated_region->region = RS_get_region(region_info);
		HASH_ADD_PTR(*affected_regions, region, updated_region);
	}

	RS_close_region_server_iterator(region_it);
}

/**
  * Update the server table to characterize servers as dead. We need this step
  * because during region reconfiguration we need to choose only alive
  * servers.
*/
static void MASTER_mark_servers_dead(struct master_s *master, struct server_vector_s *dead_server_vector)
{
	for (int i = 0; i < dead_server_vector->count; ++i) {
		struct region_server_table_entry_s *region_server_entry = NULL;
		uint64_t region_server_key =
			djb2_hash((unsigned char *)dead_server_vector->names[i], strlen(dead_server_vector->names[i]));
		HASH_FIND_PTR(master->alive_servers->hash_table_root, &region_server_key, region_server_entry);
		if (!region_server_entry) {
			log_fatal("Could not find freshly dead server %s in alive servers table",
				  dead_server_vector->names[i]);
			assert(0);
			_exit(EXIT_FAILURE);
		}
		RS_set_region_server_status(region_server_entry->region_server, DEAD);
	}
}

/**
 * Adds newly added servers in the server table After this step
 * alive_server_vector contains the newly added servers whereas
 * dead_servers_vector contains the dead ones. First we insert the newbies and
 * then handle the failures.
*/
static void MASTER_add_newbie_servers(struct master_s *master, struct server_vector_s *alive_server_vector)
{
	for (int i = 0; i < alive_server_vector->count; ++i) {
		struct region_server_table_entry_s *newbie = allocate_region_server_slot(master->alive_servers);
		assert(newbie);
		struct krm_server_name server_name = { 0 };
		if (!MASTER_get_region_server_name(alive_server_vector->names[i], master->zhandle, &server_name)) {
			log_fatal("Cannot find entry for server %s in zookeeper", alive_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}

		newbie->region_server = RS_create_region_server(server_name, ALIVE);

		log_debug("Adding server hostname: %s", server_name.kreon_ds_hostname);
		newbie->server_key = djb2_hash((const unsigned char *)server_name.kreon_ds_hostname,
					       strlen(server_name.kreon_ds_hostname));
		HASH_ADD_PTR(master->alive_servers->hash_table_root, server_key, newbie);
	}
}

/**
 * Removes recently dead servers from the server table of the master
 */
static void MASTER_remove_dead_servers(struct master_s *master, struct server_vector_s *dead_server_vector)
{
	for (int i = 0; i < dead_server_vector->count; ++i) {
		struct region_server_table_entry_s *region_server_entry = NULL;
		uint64_t region_server_key =
			djb2_hash((unsigned char *)dead_server_vector->names[i], strlen(dead_server_vector->names[i]));
		HASH_FIND_PTR(master->alive_servers->hash_table_root, &region_server_key, region_server_entry);
		if (!region_server_entry) {
			log_fatal("Could not find freshly dead server %s in alive servers table",
				  dead_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}
		HASH_DEL(master->alive_servers->hash_table_root, region_server_entry);
		RS_destroy_region_server(region_server_entry->region_server);
		--master->alive_servers->num_elements;
		memset(region_server_entry, 0x00, sizeof(*region_server_entry));
	}
}

static void MASTER_region_server_health_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	(void)context;

	struct master_s *master = (struct master_s *)context;
	MUTEX_LOCK(&master->master_lock);

	struct String_vector alive_servers = { 0 };
	char *alive_servers_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);
	int ret_code = zoo_wget_children(master->zhandle, alive_servers_path, MASTER_region_server_health_watcher,
					 master, &alive_servers);
	if (ZOK != ret_code) {
		log_fatal("failed to read alive_servers %s error code: %s ", alive_servers_path,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(alive_servers_path);
	alive_servers_path = NULL;

	struct server_vector_s *alive_server_vector = MASTER_copy_from_string_vector(&alive_servers);
	// log_debug("Real alive servers are total number: %d", alive_server_vector->count);
	// for (int i = 0; i < alive_server_vector->count; i++)
	// 	log_debug("Real alive server is %s", alive_server_vector->names[i]);

	struct server_vector_s *dead_server_vector = MASTER_find_dead_servers(master, alive_server_vector);

	MASTER_add_newbie_servers(master, alive_server_vector);

	MASTER_deallocate_server_names(alive_server_vector);

	MASTER_mark_servers_dead(master, dead_server_vector);

	for (int i = 0; i < dead_server_vector->count; ++i) {
		region_server_t region_server = find_server(master->alive_servers, dead_server_vector->names[i]);
		if (!region_server) {
			log_fatal("Where is server %s this should not happen", dead_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}
		RS_set_region_server_status(region_server, DEAD);
	}

	struct region_reconfiguration_s *affected_regions = NULL;

	for (int i = 0; i < dead_server_vector->count; ++i) {
		log_debug("Dead server is %s", dead_server_vector->names[i]);

		region_server_t region_server = find_server(master->alive_servers, dead_server_vector->names[i]);
		if (!region_server) {
			log_fatal("Where is server %s this should not happen", dead_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}
		MASTER_handle_region_server_failure(master, region_server, &affected_regions);
	}

	log_debug("Gathered info about infected regions proceeding to recongifuration");
	/*Log all new region configuration in Zookeeper*/
	struct region_reconfiguration_s *updated_region = NULL;
	struct region_reconfiguration_s *tmp = NULL;
	HASH_ITER(hh, affected_regions, updated_region, tmp)
	{
		REG_print_region_configuration(updated_region->region);
		/*log new region configuration in Zookeeper*/
		/*send message to corresponding primary*/
		HASH_DEL(affected_regions, updated_region);
		free(updated_region);
	}

	MASTER_remove_dead_servers(master, dead_server_vector);
	MASTER_deallocate_server_names(dead_server_vector);
	MUTEX_UNLOCK(&master->master_lock);
}

static void MASTER_apply_for_master(struct master_s *master)
{
	char *zk_election_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_ELECTIONS_PATH, KRM_SLASH);
	char *zk_election_full_path = zku_concat_strings(2, zk_election_path, KRM_GUID);
	char value[PROPOSAL_VALUE_LEN] = { 0 };
	char created_path[PROPOSAL_VALUE_LEN] = { 0 };
	int ret_code = zoo_create(master->zhandle, zk_election_full_path, value, PROPOSAL_VALUE_LEN,
				  &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE | ZOO_EPHEMERAL, created_path, PROPOSAL_VALUE_LEN);

	if (ZOK != ret_code) {
		log_fatal("Server: %s failed to apply for master reason: %s", master->master_hostname,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}

	strcpy(master->proposal, &created_path[strlen(zk_election_path)]);
	free(zk_election_full_path);
	free(zk_election_path);
	log_debug("Server: %s vote for leadership is %s", master->master_hostname, master->proposal);
}

static void MASTER_take_over_as_master(struct master_s *master)
{
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ELECTIONS_PATH);
	while (1) {
		struct String_vector children = { 0 };
		int ret_code = zoo_get_children(master->zhandle, zk_path, 0, &children);
		if (ret_code != ZOK) {
			log_fatal("Failed to fetch master votes from zookeeper path %s", zk_path);
			_exit(EXIT_FAILURE);
		}
		if (!children.count) {
			log_fatal("No votes present in zookeeper path: %s", zk_path);
			_exit(EXIT_FAILURE);
		}

		int server_position = 0;
		for (; server_position < children.count; ++server_position) {
			log_debug("Child is %s", children.data[server_position]);
			if (0 == strcmp(children.data[server_position], master->proposal))
				break;
		}

		if (children.count == server_position) {
			log_fatal("Could not find my vote: %s in the votes set", master->proposal);
			_exit(EXIT_FAILURE);
		}

		if (0 == server_position) {
			log_info("I am the leader server: %s", master->master_hostname);
			free(zk_path);
			return;
		}

		char *watch_path = zku_concat_strings(3, zk_path, KRM_SLASH, children.data[server_position - 1]);
		struct Stat stat = { 0 };
		ret_code = zoo_wexists(master->zhandle, watch_path, master->master_watcher, master, &stat);
		if (ret_code == ZOK)
			sem_wait(&master->barrier);
		free(watch_path);
	}
}

static void MASTER_watcher(zhandle_t *zhandle, int type, int state, const char *path, void *context)
{
	(void)zhandle;
	(void)state;
	if (type == ZOO_SESSION_EVENT) {
		log_fatal("Got zk session event in master_failed_watcher XXXTODOXXX");
		_exit(EXIT_FAILURE);
	}
	if (type != ZOO_DELETED_EVENT) {
		log_fatal("Master failed watcher handles only ZOO_DELETED_EVENTS");
		_exit(EXIT_FAILURE);
	}
	log_warn("Server %s died", path);
	struct master_s *master = (struct master_s *)context;
	sem_post(&master->barrier);
}

static void MASTER_fill_hostname(struct master_s *master, int port)
{
	char hostname[KRM_HOSTNAME_SIZE] = { 0 };
	if (gethostname(hostname, KRM_HOSTNAME_SIZE) != 0) {
		log_fatal("failed to get my hostname");
		_exit(EXIT_FAILURE);
	}
	log_info("Master hostname is %s", hostname);
	if (snprintf(master->master_hostname, KRM_HOSTNAME_SIZE, "%s:%d", hostname, port) < 0) {
		log_fatal("Failed to create master hostname");
		_exit(EXIT_FAILURE);
	}
	log_info("Tebis master hostname is %s", master->master_hostname);
}

static void init_master(struct master_s *master, int port)
{
	MASTER_fill_hostname(master, port);
	MUTEX_INIT(&master->master_lock, NULL);
	MUTEX_INIT(&master->fresh_boot_lock, NULL);
	log_debug("Initializing connection with zookeeper at %s", globals_get_zk_host());
	master->master_watcher = MASTER_watcher;
	sem_init(&master->barrier, 0, 0);
	master->zhandle =
		zookeeper_init(globals_get_zk_host(), zk_main_watcher, ZOOKEEPER_SESSION_TIMEOUT, 0, master, 0);

	if (!master->zhandle) {
		log_fatal("failed to connect to zk %s", globals_get_zk_host());
		perror("Reason");
		_exit(EXIT_FAILURE);
	}

	field_spin_for_value(&master->zookeeper_conn_state, KRM_CONNECTED);
	char *region_log_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_REGION_LOG, KRM_REGION_LOG_PREFIX);
	master->alive_servers = create_region_server_table();
	master->region_table = NULL;
	master->region_log = create_region_log(region_log_path, strlen(region_log_path), master->zhandle);
	free(region_log_path);
}

static void MASTER_increase_leadership_clock(struct master_s *master)
{
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_LEADER_CLOCK);
	char buffer[JSON_BUFFER_SIZE];
	int buffer_len = JSON_BUFFER_SIZE;
	struct Stat stat = { 0 };
	int ret_code = zoo_get(master->zhandle, zk_path, 0, buffer, &buffer_len, &stat);
	if (ret_code != ZOK) {
		log_fatal("Failed to read and update leader clock for path %s", zk_path);
		_exit(EXIT_FAILURE);
	}
	// Parse json string with server's krm_server_name struct
	cJSON *json = cJSON_ParseWithLength(buffer, buffer_len);
	if (!cJSON_IsObject(json)) {
		cJSON_Delete(json);
		log_fatal("Failed to parser server json info");
		_exit(EXIT_FAILURE);
	}

	cJSON *leader_clock = cJSON_GetObjectItem(json, "clock");
	master->leadership_clock = (int64_t)cJSON_GetNumberValue(leader_clock);
	cJSON_Delete(json);

	log_info("Current clock value is %lu", master->leadership_clock);
	cJSON *new_leader_clock = cJSON_CreateObject();
	cJSON_AddNumberToObject(new_leader_clock, "clock", (double)++master->leadership_clock);

	const char *json_string = cJSON_Print(new_leader_clock);

	ret_code = zoo_set(master->zhandle, zk_path, json_string, strlen(json_string), -1);
	if (ZOK != ret_code) {
		log_fatal("Failed to update clock for path %s", zk_path);
		_exit(EXIT_FAILURE);
	}
	log_info("Set clock value to %lu", master->leadership_clock);
	cJSON_Delete(new_leader_clock);
	free((void *)json_string);
	free(zk_path);
}

/**
 * This function builds the state of alive region servers in the system. It
 * first fills the size of the total servers. Master uses this information
 * during boot after a graceful shutdown. Then it gets alive servers of the
 * system and sets a watcher to build the table with the alive servers.
 */

static void MASTER_build_server_table(struct master_s *master)
{
	MUTEX_LOCK(&master->master_lock);
	struct String_vector alive_servers = { 0 };
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);
	int ret_code = zoo_wget_children(master->zhandle, zk_path, MASTER_region_server_health_watcher, master,
					 &alive_servers);
	if (ret_code != ZOK) {
		log_fatal("Leader (path %s)failed to fetch dataservers info with error code %s", zk_path,
			  zerror(ret_code));
		_exit(EXIT_FAILURE);
	}

	for (int i = 0; i < alive_servers.count; i++) {
		struct region_server_table_entry_s *server_entry = allocate_region_server_slot(master->alive_servers);
		assert(server_entry);
		struct krm_server_name server_name = { 0 };
		if (!MASTER_get_region_server_name(alive_servers.data[i], master->zhandle, &server_name)) {
			log_fatal("Cannot find entry for server %s in zookeeper", alive_servers.data[i]);
			_exit(EXIT_FAILURE);
		}
		server_entry->region_server = RS_create_region_server(server_name, ALIVE);
		server_entry->server_key =
			djb2_hash((unsigned char *)server_name.kreon_ds_hostname, server_name.kreon_ds_hostname_length);
		log_debug("Adding server hostname: %s with hash key %lu", server_name.kreon_ds_hostname,
			  server_entry->server_key);
		HASH_ADD_PTR(master->alive_servers->hash_table_root, server_key, server_entry);
	}

	MUTEX_UNLOCK(&master->master_lock);
	free(zk_path);
	zk_path = NULL;
}

static void MASTER_build_region_table(struct master_s *master)
{
	MUTEX_LOCK(&master->master_lock);

	struct String_vector region_names = { 0 };
	/*read all regions and construct table*/
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_REGIONS_PATH);
	int ret_code = zoo_get_children(master->zhandle, zk_path, 0, &region_names);
	if (ret_code != ZOK) {
		log_fatal("Leader failed to read regions with code %s", zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}

	char region_json_string[JSON_BUFFER_SIZE] = { 0 };

	for (int i = 0; i < region_names.count; i++) {
		char *region_path = zku_concat_strings(3, zk_path, KRM_SLASH, region_names.data[i]);
		int region_json_string_length = sizeof(region_json_string);
		struct Stat stat = { 0 };
		ret_code =
			zoo_get(master->zhandle, region_path, 0, region_json_string, &region_json_string_length, &stat);
		if (ret_code != ZOK) {
			log_fatal("Failed to retrieve region %s from Zookeeper", region_path);
			_exit(EXIT_FAILURE);
		}
		if (stat.dataLength > (int64_t)sizeof(region_json_string)) {
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
		struct region_table_s *region_entry = calloc(1UL, sizeof(*region_entry));
		// log_debug("Region id is %s", cJSON_GetStringValue(id));
		region_entry->region = REG_create_region(cJSON_GetStringValue(min_key), cJSON_GetStringValue(max_key),
							 cJSON_GetStringValue(id),
							 (enum krm_region_status)cJSON_GetNumberValue(status));

		region_entry->region_key = djb2_hash((unsigned char *)REG_get_region_id(region_entry->region),
						     strlen(REG_get_region_id(region_entry->region)));
		REG_set_region_primary(region_entry->region, cJSON_GetStringValue(primary));
		REG_set_region_primary_role(region_entry->region,
					    master->leadership_clock > 1 ? PRIMARY : PRIMARY_INFANT);
		region_server_t region_server =
			find_server(master->alive_servers, REG_get_region_primary(region_entry->region));
		if (!region_server) {
			log_fatal("Could not find server %s this should not happen",
				  REG_get_region_primary(region_entry->region));
			_exit(EXIT_FAILURE);
		}
		RS_add_region_in_server(region_server, region_entry->region,
					REG_get_region_primary_role(region_entry->region));

		for (int j = 0; j < cJSON_GetArraySize(backups); ++j) {
			REG_set_region_backup(region_entry->region, j,
					      cJSON_GetStringValue(cJSON_GetArrayItem(backups, j)));
			REG_set_region_backup_role(region_entry->region, j,
						   master->leadership_clock > 1 ? BACKUP : BACKUP_INFANT);
			region_server_t region_server =
				find_server(master->alive_servers, REG_get_region_backup(region_entry->region, j));
			if (!region_server) {
				log_fatal("Could not find server %s, this should not happen",
					  REG_get_region_backup(region_entry->region, j));
				_exit(EXIT_FAILURE);
			}
			RS_add_region_in_server(region_server, region_entry->region,
						REG_get_region_backup_role(region_entry->region, j));
		}
		HASH_ADD_PTR(master->region_table, region_key, region_entry);

		cJSON_Delete(region_json);
		free(region_path);
	}
	free(zk_path);
	replay_region_log(master->region_log);
	MUTEX_UNLOCK(&master->master_lock);
	log_debug("Successfully Build Region Table");
}

/**
 * Queries ZK and returns the number of all servers of the system
 */
static int MASTER_get_num_of_servers_in_cluster(zhandle_t *zkh)
{
	struct String_vector server_hostnames = { 0 };
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_SERVERS_PATH);
	int ret_code = zoo_get_children(zkh, zk_path, 0, &server_hostnames);
	if (ret_code != ZOK) {
		log_fatal("Leader (path %s)failed to fetch dataservers info with error code %s", zk_path,
			  zerror(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(zk_path);
	zk_path = NULL;
	return server_hostnames.count;
}
/**
  *Waits until all servers join during a fresh boot
  */
static void MASTER_fresh_boot_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	struct master_s *master = (struct master_s *)context;
	MUTEX_LOCK(&master->fresh_boot_lock);
	log_debug("Master started?: %d", master->master_started);
	if (master->master_started)
		goto exit;
	struct String_vector alive_servers = { 0 };
	char *alive_servers_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);

	int ret_code = zoo_wget_children(master->zhandle, alive_servers_path, MASTER_fresh_boot_watcher, master,
					 &alive_servers);
	free(alive_servers_path);
	alive_servers_path = NULL;
	if (ret_code != ZOK) {
		log_fatal("failed to read alive_servers %s error code %s ", alive_servers_path, zerror(ret_code));
		_exit(EXIT_FAILURE);
	}

	int num_of_servers_in_cluster = MASTER_get_num_of_servers_in_cluster(master->zhandle);
	if (alive_servers.count < num_of_servers_in_cluster) {
		log_debug("Waiting for more dataservers to join current number %d out of %u", alive_servers.count,
			  num_of_servers_in_cluster);
		goto exit;
	}

	master->master_started = true;
	sem_post(&master->barrier);
exit:
	MUTEX_UNLOCK(&master->fresh_boot_lock);
}

typedef enum { OPEN_REGION_AS_PRIMARY = 0 } command_t;
typedef struct {
	command_t command_code;
	region_t region;
} region_server_command_t;

/**
 * Sends an OPEN region command as primary. Region contains the new region
 * configuration (the hostnames of the Backups). Primary of the region is
 * responsible to notify its Backups. hostname is of the form <hostname>:<rdma
 * port>,clock which is the mailbox path of the Region Server.
 */
static void MASTER_send_open_region_command_to_primary(struct master_s *master, region_t region, MC_command_t command)
{
	region_server_t region_server = find_server(master->alive_servers, REG_get_region_primary(region));
	if (NULL == region_server) {
		log_fatal("Cannot find server %s in the alive servers table", REG_get_region_primary(region));
	}

	char mail_id[MAX_MAILBOX_PATH_SIZE] = { 0 };
	char mailbox_path[MAX_MAILBOX_PATH_SIZE] = { 0 };
	if (snprintf(mailbox_path, MAX_MAILBOX_PATH_SIZE, "%s", REG_get_region_primary(region)) < 0) {
		log_fatal("Failed to create mailbox path");
		_exit(EXIT_FAILURE);
	}

	char *zk_path = zku_concat_strings(5, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, mailbox_path, KRM_MAIL_TITLE);
	int ret_code = zoo_create(master->zhandle, zk_path, (char *)command, MC_get_command_size(),
				  &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, mail_id, MAX_MAILBOX_PATH_SIZE);
	if (ZOK != ret_code) {
		log_fatal("Server: %s failed to send message in the message queue  %s", zk_path, zerror(ret_code));
		_exit(EXIT_FAILURE);
	}
	// log_debug("Send open region as primary command at zk_path %s", mail_id);
	free(zk_path);
	zk_path = NULL;
}

static uint64_t MASTER_create_uuid(struct master_s *master)
{
	char uuid[KRM_HOSTNAME_SIZE] = { 0 };
	if (snprintf(uuid, KRM_HOSTNAME_SIZE, "%s:%ld%ld", master->master_hostname, master->leadership_clock,
		     ++master->command_counter) < 0) {
		log_fatal("Failed to create uuid");
		_exit(EXIT_FAILURE);
	}
	return djb2_hash((const unsigned char *)uuid, strlen(uuid));
}
static void MASTER_assign_regions(struct master_s *master)
{
	struct region_table_s *region_entry = NULL;
	struct region_table_s *tmp = NULL;

	HASH_ITER(hh, master->region_table, region_entry, tmp)
	{
		MC_command_t command = MC_create_command(OPEN_REGION_START, REG_get_region_id(region_entry->region),
							 PRIMARY_INFANT, MASTER_create_uuid(master));

		if (!append_req_to_region_log(master->region_log, command)) {
			log_fatal("Failed to append to region log ");
			_exit(EXIT_FAILURE);
		}
		REG_set_region_primary_role(region_entry->region, PRIMARY_INFANT);
		for (int i = 0; i < REG_get_region_num_of_backups(region_entry->region); ++i)
			REG_set_region_backup_role(region_entry->region, i, BACKUP_INFANT);
		MASTER_send_open_region_command_to_primary(master, region_entry->region, command);
	}
}

static void MASTER_boot_master(struct master_s *master, int port)
{
	init_master(master, port);

	MASTER_apply_for_master(master);

	MASTER_take_over_as_master(master);

	MASTER_increase_leadership_clock(master);

	log_debug("Master clock is %lu", master->leadership_clock);

	if (1 == master->leadership_clock) {
		log_warn("Fresh boot of the system detected");
		master->master_started = false;
		MASTER_fresh_boot_watcher(master->zhandle, -1, -1, NULL, master);
		sem_wait(&master->barrier);
	}

	log_debug("Tebis Master beginning its reign epoch is %lu", master->leadership_clock);

	MASTER_build_server_table(master);

	MASTER_build_region_table(master);
	if (1 == master->leadership_clock)
		MASTER_assign_regions(master);
	master->command_counter = 0;
	master->mailbox_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_LEADER_PATH);
	MASTER_mailbox_watcher(master->zhandle, -1, -1, master->mailbox_path, master);
	log_debug("Registered mailbox watcher: %s", master->mailbox_path);

	MASTER_full_regions_check(master);
	master->master_started = true;
}

void *run_master(void *args)
{
	pthread_setname_np(pthread_self(), "masterd");
	int port = *(int *)args;
	struct master_s *master = calloc(1UL, sizeof(*master));

	MASTER_boot_master(master, port);

	return NULL;
}
