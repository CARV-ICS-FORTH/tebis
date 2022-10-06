#define _GNU_SOURCE
#include "../utilities/spin_loop.h"
#include "djb2.h"
#include "globals.h"
#include "metadata.h"
#include "region_log.h"
#include "uthash.h"
#include "zk_utils.h"
#include "zookeeper.jute.h"
#include <cJSON.h>
#include <log.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>

#define ZOOKEEPER_SESSION_TIMEOUT 15000
#define JSON_BUFFER_SIZE (2048)
#define PROPOSAL_VALUE_LEN (256)
#define MAX_REGION_SERVERS (1024)

/**
 * Watcher for failed Zookeeper connections
 */
typedef void (*master_watcher_t)(zhandle_t *, int, int, const char *, void *);

#define SERVER_REGIONS_LIST_CAPACITY (128)

struct region_info {
	uint64_t region_key;
	server_role_t role;
};

struct server_info {
	uint64_t server_key;
	uint64_t server_clock;
	server_role_t role;
};

enum region_status { ERROR_STATUS = 0, HEALTHY, RECONFIGURATION };

/**
 * We keep all regions in a hash table.
 */
struct region {
	char id[KRM_MAX_REGION_ID_SIZE];
	char min_key[KRM_MAX_KEY_SIZE];
	char max_key[KRM_MAX_KEY_SIZE];
	struct server_info primary;
	struct server_info backups[RU_MAX_NUM_REPLICAS];
	uint64_t region_key;
	uint32_t min_key_size;
	uint32_t max_key_size;
	int num_of_backup;
	UT_hash_handle hh;
	enum region_status status;
};

/**
 * For each server we keep an array of initial capacity
 * SERVER_REGIONS_LIST_CAPACITY. This array resizes automatically in case of a
 * overflow. The array represents the server - region relation. In particular,
 * it keeps an array with the regions that the Region Server hosts and its
 * respective role.
 */
struct server_to_region_relation {
	uint64_t server_key;
	int32_t len;
	int32_t capacity;
	UT_hash_handle hh;
	struct region_info *region_info_table;
};

typedef enum region_server_status { ALIVE = 0, DEAD } region_server_status_t;
/**
 * We store Region Servers in a preallocated array and we also keep a hash
 * table to quickly locate a Region Server. We store them in an array because
 * we need to search them quickly in cases for example where we want to choose
 * randomly a Region Server to reconfigure a replica group. When we allocate a
 * new RegionServer we use a custom allocator that searches this array and it
 * returns the first slot not in use. To iterate region_server fast we use an
 * array. slot_in_use field indicates us if the slot is in use aka contains a
 * valid server.
 */
struct region_server {
	struct krm_server_name server_name;
	struct server_to_region_relation *server_to_region;
	uint64_t server_key;
	region_server_status_t status;
	bool slot_in_use;
	UT_hash_handle hh;
};

struct master {
	struct region_server alive_servers_array[MAX_REGION_SERVERS];
	char proposal[PROPOSAL_VALUE_LEN];
	pthread_mutex_t fresh_boot_lock;
	pthread_mutex_t alive_servers_lock;
	pthread_mutex_t region_table_lock;
	struct krm_server_name server_name;
	master_watcher_t master_watcher;
	sem_t barrier;
	int64_t leadership_clock;
	zhandle_t *zhandle;
	struct region_server *alive_servers;
	region_log_t *region_log;
	struct region *region_table;
	struct server_to_region_relation *server_to_region_map;
	int cluster_servers_num;
	bool master_started;
	uint8_t zookeeper_conn_state;
};

/**
 * Searches the server array to find an empty slot to store region server state
 */
static struct region_server *allocate_region_server_slot(struct region_server *server_table)
{
	for (int i = 0; i < MAX_REGION_SERVERS; ++i) {
		if (server_table[i].slot_in_use)
			continue;
		memset(&server_table[i], 0x00, sizeof(struct region_server));
		server_table[i].slot_in_use = true;
		return &server_table[i];
	}
	return NULL;
}
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
	struct master *master = (struct master *)context;
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

static bool zk_get_server_name(char *dataserver_name, zhandle_t *zhandle, struct krm_server_name *server_name)
{
	/*check if you are hostname-RDMA_port belongs to the project*/
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_SERVERS_PATH, KRM_SLASH, dataserver_name);
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
struct server_vector {
	int count;
	int capacity;
	char **names;
};

/**
 * Creates an empty server_vector with initial capacity
 */
static struct server_vector *create_server_vector(int capacity)
{
	struct server_vector *vector = calloc(1, sizeof(struct server_vector));
	vector->capacity = capacity;

	vector->names = calloc(vector->capacity, sizeof(vector->names));

	return vector;
}

/**
 * Adds an item in the vector
 */
static void add_item_in_server_vector(struct server_vector *vector, void *item)
{
	int size = sizeof(vector->names);
	if (vector->count >= vector->capacity) {
		vector->names = realloc(vector->names, 2L * vector->capacity * size);
		if (vector->names) {
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
static struct server_vector *copy_from_string_vector(struct String_vector *servers)
{
	struct server_vector *server_vector = calloc(1, sizeof(struct server_vector));
	server_vector->count = server_vector->capacity = servers->count;
	for (int i = 0; i < server_vector->count; ++i)
		server_vector->names[i] = strdup(servers->data[i]);
	return server_vector;
}

/**
 * Destroy a server_names struct
 */
static void deallocate_server_names(struct server_vector *server_vector)
{
	for (int i = 0; i < server_vector->count; ++i)
		free(server_vector->names[i]);
	free(server_vector);
}

/**
 * Removes the server in the given position
 */
static void remove_from_server_names(struct server_vector *server_vector, int position)
{
	free(server_vector->names[position]);
	memmove(server_vector->names[position], server_vector->names[position],
		(server_vector->count - 1) * sizeof(char *));
	--server_vector->count;
}

static int zk_children_comparator(const void *child_1, const void *child_2)
{
	char *left_child = *(char **)child_1;
	char *right_child = *(char **)child_2;
	return strcmp(left_child, right_child);
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
static int get_server_pos(struct server_vector *server_vector, struct region_server *server)
{
	for (int start = 0, end = server_vector->count - 1, middle = (server_vector->count - 1) / 2; start <= end;
	     middle = (end + start) / 2) {
		int ret_code =
			zk_children_comparator(server_vector->names[middle], server->server_name.kreon_ds_hostname);
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
static struct server_vector *find_dead_servers(struct server_vector *alive_servers, struct region_server *servers)
{
	qsort(alive_servers->names, alive_servers->count, sizeof(char *), zk_children_comparator);
	struct server_vector *dead_servers = create_server_vector(16);

	for (int i = 0; i < MAX_REGION_SERVERS; ++i) {
		if (!servers[i].slot_in_use)
			continue;
		int position = get_server_pos(alive_servers, servers);
		if (position >= 0) {
			remove_from_server_names(alive_servers, position);
			continue;
		}
		add_item_in_server_vector(dead_servers, servers[i].server_name.kreon_ds_hostname);
	}
	return dead_servers;
}

/**
  * Iterator for retrieving regions hosted in a server.
*/
struct server_region_iterator {
	struct master *master;
	struct server_to_region_relation *regions;
	struct region *region;
	int pos;
};

/**
  * Creates and initializes a region iterator.
  * @param master: the tebis master descriptor
  * @param hostname: The server name in the form hostname:rdma_port
  * @return: the iterator handle
  */
static struct server_region_iterator *init_server_region_iterator(struct master *master, char *hostname)
{
	struct server_region_iterator *iterator = calloc(1, sizeof(*iterator));
	uint64_t server_key = djb2_hash((unsigned char *)hostname, strlen(hostname));
	HASH_FIND_PTR(master->server_to_region_map, &server_key, iterator->regions);
	if (!iterator->regions) {
		log_fatal("Cannot find entry for server %s", hostname);
		_exit(EXIT_FAILURE);
	}
	iterator->pos = -1;
	iterator->master = master;
	return iterator;
}

/**
  * Retrieves the next region host in the configured server
  * @param: iterator
  * @return the next region or NULL when iterator reaches end.
  */
static struct region *get_next_region(struct server_region_iterator *iterator)
{
	if (++iterator->pos == iterator->regions->len)
		return NULL;

	struct region *region = NULL;
	HASH_FIND_PTR(iterator->master->region_table, &iterator->regions->region_info_table[iterator->pos].region_key,
		      region);
	if (!region) {
		log_fatal("Could not retrieve region with region key %lu",
			  iterator->regions->region_info_table[iterator->pos].region_key);
		_exit(EXIT_FAILURE);
	}
	return region;
}

/**
 * Removes the server_region_relation
 */
static void remove_server_region_relation(struct server_region_iterator *iterator)
{
	HASH_DEL(iterator->master->server_to_region_map, iterator->regions);
	free(iterator->regions);
}

/**
 * Closes and deallocates any resources associated with the iterator
 * @param iterator Address of the iterator to be freed.
 */
static void close_server_region_iterator(struct server_region_iterator *iterator)
{
	free(iterator);
}

/**
 * Checks if a server with a given epoch is alive
 */
bool is_server_alive(uint64_t server_key, struct master *master, uint64_t server_epoch)
{
	struct region_server *server = NULL;
	MUTEX_LOCK(&master->alive_servers_lock);
	HASH_FIND_PTR(master->alive_servers, &server_key, server);
	MUTEX_UNLOCK(&master->alive_servers_lock);
	if (!server)
		return false;
	if (server->server_name.epoch != server_epoch)
		return false;
	return true;
}

static struct server_info choose_random_region_server(struct master *master, struct region *region, server_role_t role)
{
	(void)master;
	(void)role;
	(void)region;
	struct server_info server_info = { 0 };
	return server_info;
}

/**
 * Replaces dead backup or dead servers in the region
 */
static void reconfigure_region(struct master *master, struct region *region)
{
	for (int i = region->num_of_backup - 1; i >= 0; --i) {
		if (BACKUP_DEAD != region->backups[i].role)
			continue;
		memmove(&region->backups[i], &region->backups[i + 1],
			(region->num_of_backup - (i + 1)) * sizeof(struct server_info));
		region->backups[region->num_of_backup--] = choose_random_region_server(master, region, BACKUP_NEWBIE);
	}

	if (PRIMARY_DEAD != region->primary.role)
		return;

	memmove(&region->backups[0], &region->backups[1], (region->num_of_backup - 1) * sizeof(struct server_info));
	region->backups[region->num_of_backup--] = choose_random_region_server(master, region, PRIMARY_NEWBIE);
}

/**
 * Handles or better reports that guys we lost data Sorry
 */
static void handle_data_loss(struct region *region)
{
	log_fatal("We lost data for region %s", region->id);
	_exit(EXIT_FAILURE);
}
/**
 * Checks if primary and replicas are ok and healthy. If PRIMARY or BACK are
 * dead it characterizes them as PRIMARY_DEAD or BACKUP_DEAD respectively.
 * @param region: Region to be checked
 * @return number of faulty servers
 */
static int check_replica_group_health(struct master *master, struct region *region)
{
	int n_failures = 0;
	/*is primary healthy?*/
	if (!is_server_alive(region->primary.server_key, master, region->primary.server_clock)) {
		region->primary.role = PRIMARY_DEAD;
		++n_failures;
	}

	for (int i = 0; i < region->num_of_backup; ++i) {
		if (is_server_alive(region->backups[i].server_key, master, region->backups[i].server_clock))
			continue;
		region->backups[i].role = BACKUP_DEAD;
		++n_failures;
	}
	return n_failures;
}

static void send_message_to_primary(struct master *master, struct region *region)
{
#define MESSAGE_QUEUE_PATH_SIZE 256
	enum command_code { FAULTY_CMD, FAILURE_SYNC_NEWBIES, UPGRADE_SYNC_NEWBIES };
	struct master_command {
		struct region region;
		enum command_code code;
	};

	struct master_command command = { .region = *region };
	command.code = FAILURE_SYNC_NEWBIES;
	if (region->primary.role == PRIMARY_NEWBIE)
		command.code = UPGRADE_SYNC_NEWBIES;
	struct region_server *region_server = NULL;
	HASH_FIND_PTR(master->alive_servers, &region->primary.server_key, region_server);
	if (!region_server) {
		log_fatal("Where is server with key %lu", region->primary.server_key);
		_exit(EXIT_FAILURE);
	}

	if (region_server->server_name.epoch != region->primary.server_clock) {
		log_fatal("Current region server clock %lu mismatch with region's desired clock %lu",
			  region_server->server_name.epoch, region->primary.server_clock);
		_exit(EXIT_FAILURE);
	}

	char *cmd_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH,
					    region_server->server_name.kreon_ds_hostname);
	char created_path[MESSAGE_QUEUE_PATH_SIZE] = { 0 };
	int ret_code = zoo_create(master->zhandle, cmd_path, (char *)&command, sizeof(command), &ZOO_OPEN_ACL_UNSAFE,
				  ZOO_SEQUENCE, created_path, MESSAGE_QUEUE_PATH_SIZE);
	if (ZOK != ret_code) {
		log_fatal("Server: %s failed to send message in the message queue of region server %s",
			  region_server->server_name.kreon_ds_hostname, zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(cmd_path);
	log_debug("Master: Send message for region reconfiguration to primary %s",
		  region_server->server_name.kreon_ds_hostname);
}

static void update_region_info(void)
{
}

static void full_regions_check(struct master *master)
{
	struct region *region = NULL;
	struct region *tmp = NULL;
	struct region *region_table = master->region_table;
	HASH_ITER(hh, region_table, region, tmp)
	{
		int n_failures = check_replica_group_health(master, region);
		if (region->num_of_backup + 1 == n_failures)
			handle_data_loss(region);
		if (n_failures) {
			reconfigure_region(master, region);
			//lock_region_table
			send_message_to_primary(master, region);
			update_region_info();
			//unlock region_table
		}
	}
}
#if 0
static void remove_dead_server(struct master *master, char *server_name)
{
	struct region_server *server = NULL;
	uint64_t server_key = djb2_hash((const unsigned char *)server_name, strlen(server_name));
	HASH_FIND_PTR(master->alive_servers, &server_key, server);
	if (NULL == server) {
		log_fatal("Failed to locate freshly dead server %s in servers table", server_name);
		_exit(EXIT_FAILURE);
	}

	HASH_DEL(master->alive_servers, server);
	memset(server, 0x00, sizeof(struct region_server));
}
#endif

/**
 * HASH table structure that keeps all the final state of the regions
 * reconfigured after suffering N failures.
 */
typedef struct {
	struct region *region;
	UT_hash_handle hh;
} region_reconfiguration_t;

/**
 * Iterates the regions of the failed server and 1)reconfigures them and 2)
 * sends the appropriate commands to the primary of the corresponding region.
 * Master keeps replica group info in a logical order. In case of a failed server
 * @param master Tebis master
 * @param server_name is the name of the failed server in the form
 * <hostname>:<port>:<epoch>
 */
static void handle_region_server_failure(struct master *master, char *server_name,
					 region_reconfiguration_t *affected_regions)
{
	(void)master;
	(void)server_name;

	struct server_region_iterator *region_it = init_server_region_iterator(master, server_name);

	for (struct region *region = get_next_region(region_it); region != NULL; region = get_next_region(region_it)) {
		int n_failures = check_replica_group_health(master, region);
		if (0 == n_failures) {
			log_fatal("All regions in iterator must be related with failed server and thus have failures");
			_exit(EXIT_FAILURE);
		}
		reconfigure_region(master, region);
		region_reconfiguration_t *updated_region = NULL;
		HASH_FIND_PTR(affected_regions, region, updated_region);
		if (updated_region)
			continue;
		updated_region = calloc(1, sizeof(region_reconfiguration_t));
		updated_region->region = region;
		HASH_ADD_PTR(affected_regions, region, updated_region);
	}

	remove_server_region_relation(region_it);
	close_server_region_iterator(region_it);
}

/**
  * Update the server table to characterize servers as dead. We need this step
  * because during region reconfiguration we need to choose only alive
  * servers.
*/
static void mark_servers_dead(struct master *master, struct server_vector *dead_server_vector)
{
	for (int i = 0; i < dead_server_vector->count; ++i) {
		struct region_server *region_server = NULL;
		uint64_t region_server_key =
			djb2_hash((unsigned char *)dead_server_vector->names[i], strlen(dead_server_vector->names[i]));
		HASH_FIND_PTR(master->alive_servers, &region_server_key, region_server);
		if (!region_server) {
			log_fatal("Could not find freshly dead server %s in alive servers table",
				  dead_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}
		region_server->status = DEAD;
	}
}

/**
 * Adds newly added servers in the server table After this step
 * alive_server_vector contains the newly added servers whereas
 * dead_servers_vector contains the dead ones. First we insert the newbies and
 * then handle the failures.
*/
static void add_newbie_servers(struct master *master, struct server_vector *alive_server_vector)
{
	for (int i = 0; i < alive_server_vector->count; ++i) {
		struct region_server *newbie = allocate_region_server_slot(master->alive_servers_array);
		if (!zk_get_server_name(alive_server_vector->names[i], master->zhandle, &newbie->server_name)) {
			log_fatal("Cannot find entry for server %s in zookeeper", alive_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}

		newbie->server_key = djb2_hash((unsigned char *)newbie->server_name.kreon_ds_hostname,
					       newbie->server_name.kreon_ds_hostname_length);
		log_debug("Adding server hostname: %s with hash key %lu", newbie->server_name.kreon_ds_hostname,
			  newbie->server_key);
		HASH_ADD_PTR(master->alive_servers, server_key, newbie);
	}
}

/**
 * Removes recently dead servers from the server table of the master
 */
static void remove_dead_servers(struct master *master, struct server_vector *dead_server_vector)
{
	for (int i = 0; i < dead_server_vector->count; ++i) {
		struct region_server *region_server = NULL;
		uint64_t region_server_key =
			djb2_hash((unsigned char *)dead_server_vector->names[i], strlen(dead_server_vector->names[i]));
		HASH_FIND_PTR(master->alive_servers, &region_server_key, region_server);
		if (!region_server) {
			log_fatal("Could not find freshly dead server %s in alive servers table",
				  dead_server_vector->names[i]);
			_exit(EXIT_FAILURE);
		}
		HASH_DEL(master->alive_servers, region_server);
		free(region_server);
	}
}

static void region_server_health_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	(void)context;

	struct master *master = (struct master *)context;

	struct String_vector alive_servers = { 0 };
	char *alive_servers_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);
	int ret_code = zoo_wget_children(master->zhandle, alive_servers_path, region_server_health_watcher, master,
					 &alive_servers);
	if (ZOK != ret_code) {
		log_fatal("failed to read alive_servers %s error code: %s ", alive_servers_path,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(alive_servers_path);

	MUTEX_LOCK(&master->alive_servers_lock);
	MUTEX_LOCK(&master->region_table_lock);
	struct server_vector *alive_server_vector = copy_from_string_vector(&alive_servers);

	struct server_vector *dead_server_vector = find_dead_servers(alive_server_vector, master->alive_servers_array);

	add_newbie_servers(master, alive_server_vector);

	deallocate_server_names(alive_server_vector);

	mark_servers_dead(master, dead_server_vector);
	region_reconfiguration_t *affected_regions = NULL;
	for (int i = 0; i < dead_server_vector->count; ++i)
		handle_region_server_failure(master, dead_server_vector->names[i], affected_regions);

	/*Log all new region configuration in Zookeeper*/
	region_reconfiguration_t *updated_region = NULL;
	region_reconfiguration_t *tmp = NULL;
	HASH_ITER(hh, affected_regions, updated_region, tmp)
	{
		/*log new region configuration in Zookeeper*/
		/*send message to corresponding primary*/
		HASH_DEL(affected_regions, updated_region);
		free(updated_region);
	}

	remove_dead_servers(master, dead_server_vector);
	deallocate_server_names(dead_server_vector);
	/*Remove dead servers from server table*/
	for (int i = 0; i < dead_server_vector->count; ++i)
		handle_region_server_failure(master, dead_server_vector->names[i], affected_regions);
	MUTEX_UNLOCK(&master->alive_servers_lock);
	MUTEX_LOCK(&master->region_table_lock);
}

static void apply_for_master(struct master *master)
{
	char *zk_election_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_ELECTIONS_PATH, KRM_SLASH);
	char *zk_election_full_path = zku_concat_strings(2, zk_election_path, KRM_GUID);
	char value[PROPOSAL_VALUE_LEN] = { 0 };
	char created_path[PROPOSAL_VALUE_LEN] = { 0 };
	int ret_code = zoo_create(master->zhandle, zk_election_full_path, value, PROPOSAL_VALUE_LEN,
				  &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE | ZOO_EPHEMERAL, created_path, PROPOSAL_VALUE_LEN);

	if (ZOK != ret_code) {
		log_fatal("Server: %s failed to apply for master reason: %s", master->server_name.kreon_ds_hostname,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}

	strcpy(master->proposal, &created_path[strlen(zk_election_path)]);
	free(zk_election_full_path);
	free(zk_election_path);
	log_debug("Server: %s vote for leadership is %s", master->server_name.kreon_ds_hostname, master->proposal);
}

static void take_over_as_master(struct master *master)
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
			log_info("I am the leader server: %s", master->server_name.kreon_ds_hostname);
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

static void master_watcher(zhandle_t *zhandle, int type, int state, const char *path, void *context)
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
	struct master *master = (struct master *)context;
	sem_post(&master->barrier);
}

static void fill_hostname(struct master *master, int port)
{
	if (gethostname(master->server_name.hostname, KRM_HOSTNAME_SIZE) != 0) {
		log_fatal("failed to get my hostname");
		_exit(EXIT_FAILURE);
	}
	log_info("Master hostname is %s", master->server_name.hostname);
	char port_to_string[16];
	sprintf(port_to_string, ":%d", port);
	strcpy(master->server_name.kreon_ds_hostname, master->server_name.hostname);
	master->server_name.kreon_ds_hostname_length = strlen(master->server_name.kreon_ds_hostname);
	strcpy(&master->server_name.kreon_ds_hostname[master->server_name.kreon_ds_hostname_length], port_to_string);
	master->server_name.kreon_ds_hostname_length += strlen(port_to_string);
	log_info("Tebis master hostname is %s", master->server_name.kreon_ds_hostname);
}

static void init_master(struct master *master, int port)
{
	fill_hostname(master, port);
	MUTEX_INIT(&master->fresh_boot_lock, NULL);
	MUTEX_INIT(&master->alive_servers_lock, NULL);
	MUTEX_INIT(&master->region_table_lock, NULL);
	log_debug("Initializing connection with zookeeper at %s", globals_get_zk_host());
	master->master_watcher = master_watcher;
	sem_init(&master->barrier, 0, 0);
	master->zhandle =
		zookeeper_init(globals_get_zk_host(), zk_main_watcher, ZOOKEEPER_SESSION_TIMEOUT, 0, master, 0);

	if (!master->zhandle) {
		log_fatal("failed to connect to zk %s", globals_get_zk_host());
		perror("Reason");
		_exit(EXIT_FAILURE);
	}

	field_spin_for_value(&master->zookeeper_conn_state, KRM_CONNECTED);
	char *region_log_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_SLASH, KRM_REGION_LOG);
	master->region_log = create_region_log(region_log_path, strlen(region_log_path), master->zhandle);
	free(region_log_path);

	log_debug("Fetching server: %s info from zookeeper", master->server_name.kreon_ds_hostname);

	if (!zk_get_server_name(master->server_name.kreon_ds_hostname, master->zhandle, &master->server_name)) {
		log_fatal("Failed to fetch info for server: %s from zookeeper", master->server_name.kreon_ds_hostname);
		_exit(EXIT_FAILURE);
	}
}

static void increase_leadership_clock(struct master *master)
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

static void build_server_table(struct master *master)
{
	struct String_vector server_hostnames = { 0 };
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_SERVERS_PATH);
	int ret_code = zoo_get_children(master->zhandle, zk_path, 0, &server_hostnames);
	if (ret_code != ZOK) {
		log_fatal("Leader (path %s)failed to fetch dataservers info with error code %s", zk_path,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	free(zk_path);
	master->cluster_servers_num = server_hostnames.count;

	MUTEX_LOCK(&master->alive_servers_lock);
	struct String_vector alive_servers = { 0 };
	zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);
	ret_code = zoo_wget_children(master->zhandle, zk_path, region_server_health_watcher, master, &alive_servers);
	if (ret_code != ZOK) {
		log_fatal("Leader (path %s)failed to fetch dataservers info with error code %s", zk_path,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}

	for (int i = 0; i < alive_servers.count; i++) {
		struct region_server *server = allocate_region_server_slot(master->alive_servers_array);
		if (!zk_get_server_name(server_hostnames.data[i], master->zhandle, &server->server_name)) {
			log_fatal("Cannot find entry for server %s in zookeeper", server_hostnames.data[i]);
			_exit(EXIT_FAILURE);
		}

		server->server_key = djb2_hash((unsigned char *)server->server_name.kreon_ds_hostname,
					       server->server_name.kreon_ds_hostname_length);
		log_debug("Adding server hostname: %s with hash key %lu", server->server_name.kreon_ds_hostname,
			  server->server_key);
		HASH_ADD_PTR(master->alive_servers, server_key, server);
	}

	MUTEX_UNLOCK(&master->alive_servers_lock);
	free(zk_path);
}

static server_role_t get_server_role(char *role)
{
	log_fatal("Corrupted role %s", role);
	_exit(EXIT_FAILURE);
}

static void fill_server_info(struct server_info *server, char *server_info)
{
	int token_count = 0;

	// Keep printing tokens while one of the
	// delimiters present in str[].
	for (char *token = strtok(server_info, ","); token != NULL; token = strtok(server_info, ",")) {
		if (0 == token_count)
			server->server_key = djb2_hash((unsigned char *)token, strlen(token));
		else if (1 == token_count)
			server->role = get_server_role(token);
		else if (2 == token_count)
			server->server_clock = strtoull(token, NULL, 10);
		else
			log_fatal("Wrong format for server info in JSON");
		_exit(EXIT_FAILURE);
		++token_count;
	}
}

/**
 * Adds a region to the corresponding server list, along with the role that the
 * server has.
 * @param master: The master that is responsible for these servers/regions
 * @param server_info: Contains the server key and its role in the region
 * @param region: The region to be added in the server list
 */
#if 0
static void add_region_in_server(struct master *master, struct server_info *server_info, struct region *region)
{
	struct server_to_region_relation *server_to_region = NULL;
	HASH_FIND_PTR(master->server_to_region_map, &server_info->server_key, server_to_region);
	if (!server_to_region) {
		server_to_region = calloc(1, sizeof(*server_to_region));
		server_to_region->server_key = server_info->server_key;
		server_to_region->capacity = SERVER_REGIONS_LIST_CAPACITY;
		server_to_region->len = 0;
		server_to_region->region_info_table = calloc(SERVER_REGIONS_LIST_CAPACITY, sizeof(struct region_info));
		HASH_ADD_PTR(master->server_to_region_map, server_key, server_to_region);
	}
	/*resize if needed*/
	if (server_to_region->len == server_to_region->capacity) {
		server_to_region->capacity *= 2;
		server_to_region->region_info_table =
			realloc(server_to_region->region_info_table, server_to_region->capacity);
		if (!server_to_region->region_info_table) {
			log_fatal("Resizing of the array failed");
			_exit(EXIT_FAILURE);
		}
	}
	server_to_region->region_info_table[server_to_region->len].region_key = region->region_key;
	server_to_region->region_info_table[server_to_region->len++].role = server_info->role;
}
#endif
#if 0
static struct server_to_region_relation *get_server_regions(struct master *master, uint64_t server_hash_key)
{
	struct server_to_region_relation *server_regions = NULL;
	HASH_FIND_PTR(master->server_to_region_map, &server_hash_key, server_regions);
	if (!server_regions) {
		log_fatal("Could not retrieve the regions for server with hash key %lu", server_hash_key);
		_exit(EXIT_FAILURE);
	}
	return server_regions;
}
#endif

static void build_region_table(struct master *master)
{
	MUTEX_LOCK(&master->region_table_lock);

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

		struct region *region = calloc(1, sizeof(*region));
		strncpy(region->id, cJSON_GetStringValue(id), KRM_MAX_REGION_ID_SIZE);
		strncpy(region->min_key, cJSON_GetStringValue(min_key), KRM_MAX_KEY_SIZE);
		region->min_key_size = strlen(region->min_key);
		if (!strcmp(region->min_key, "-oo")) {
			memset(region->min_key, 0, KRM_MAX_KEY_SIZE);
			region->min_key_size = 1;
		}
		strncpy(region->max_key, cJSON_GetStringValue(max_key), KRM_MAX_KEY_SIZE);
		region->max_key_size = strlen(region->max_key);
		region->status = (enum region_status)cJSON_GetNumberValue(status);

		region->region_key = djb2_hash((unsigned char *)region->id, strlen(region->id));
		log_debug("Adding region with id %s min key %s and max key %s", region->id, region->min_key,
			  region->max_key);

		/*Setting primary and backups info*/
		fill_server_info(&region->primary, cJSON_GetStringValue(primary));
		for (int j = 0; j < cJSON_GetArraySize(backups); ++j)
			fill_server_info(&region->backups[j], cJSON_GetStringValue(cJSON_GetArrayItem(backups, j)));
		HASH_ADD_PTR(master->region_table, region_key, region);

		cJSON_Delete(region_json);
		free(region_path);
	}
	free(zk_path);
	replay_region_log(master->region_log);
	MUTEX_UNLOCK(&master->region_table_lock);
}

/**

  *Waits until all servers join during a fresh boot
  */
static void fresh_boot_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	struct master *master = (struct master *)context;
	MUTEX_LOCK(&master->fresh_boot_lock);
	if (master->master_started)
		goto exit;
	struct String_vector *alive_servers = { 0 };
	char *alive_servers_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);

	int ret_code =
		zoo_wget_children(master->zhandle, alive_servers_path, fresh_boot_watcher, master, alive_servers);
	free(alive_servers_path);
	alive_servers_path = NULL;
	if (ret_code != ZOK) {
		log_fatal("failed to read alive_servers %s error code: %s ", alive_servers_path,
			  zku_op2String(ret_code));
		_exit(EXIT_FAILURE);
	}
	if (alive_servers->count == master->cluster_servers_num) {
		log_debug("System ready for fresh boot all %d servers joined!", alive_servers->count);
		master->master_started = true;
		sem_post(&master->barrier);
	} else
		log_debug("Waiting for more dataservers to join current number %d out of %u", alive_servers->count,
			  master->cluster_servers_num);

exit:
	MUTEX_UNLOCK(&master->fresh_boot_lock);
}

static void tm_boot_master(struct master *master, int port)
{
	init_master(master, port);

	apply_for_master(master);

	take_over_as_master(master);

	increase_leadership_clock(master);

	log_debug("Master clock is %lu", master->leadership_clock);

	if (0 == master->leadership_clock) {
		log_warn("Fresh boot of the system detected");
		master->master_started = false;
		fresh_boot_watcher(master->zhandle, -1, -1, NULL, master);
		sem_wait(&master->barrier);
	}

	/*Register watcher for region log*/
	build_server_table(master);

	/*Register watcher for servers health*/
	build_region_table(master);

	region_server_health_watcher(master->zhandle, -1, -1, NULL, master);
	/*Full region check*/
	full_regions_check(master);
	master->master_started = true;
}

void *run_master(void *args)
{
	pthread_setname_np(pthread_self(), "masterd");
	int port = *(int *)args;
	struct master *master = calloc(1, sizeof(*master));

	tm_boot_master(master, port);

	return NULL;
}
