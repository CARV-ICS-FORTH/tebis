#define _GNU_SOURCE
#include "../utilities/spin_loop.h"
#include "djb2.h"
#include "globals.h"
#include "metadata.h"
#include "uthash.h"
#include "zk_utils.h"
#include <cJSON.h>
#include <log.h>
#include <stdbool.h>

#define JSON_BUFFER_SIZE (2048)
#define PROPOSAL_VALUE_LEN (256)

typedef void (*master_watcher_t)(zhandle_t *, int, int, const char *, void *);

struct server_to_region_relation {
	uint64_t server_key;
	int32_t len;
	int32_t capacity;
	UT_hash_handle hh;
	struct region_info *region_info_table;
};

struct region_server {
	struct krm_server_name server_name;
	struct server_to_region_relation *server_to_region;
	uint64_t server_key;
	UT_hash_handle hh;
};

struct region {
	char id[KRM_MAX_REGION_ID_SIZE + 1];
	char min_key[KRM_MAX_KEY_SIZE + 1];
	char max_key[KRM_MAX_KEY_SIZE + 1];
	uint64_t region_key;
	uint32_t min_key_size;
	uint32_t max_key_size;
	uint32_t num_of_backup;
	enum krm_region_status status;
	UT_hash_handle hh;
};

struct region_to_server_relation {
	uint64_t region;
	uint64_t primary;
	uint64_t backups[KRM_MAX_BACKUPS];
	uint32_t num_of_backup;
	UT_hash_handle hh;
};

struct master {
	char proposal[PROPOSAL_VALUE_LEN];
	pthread_mutex_t management_lock;
	struct krm_server_name server_name;
	master_watcher_t master_watcher;
	sem_t barrier;
	int64_t leadership_clock;
	zhandle_t *zhandle;
	struct region_server *alive_server_table;
	struct region_server *dead_server_table;
	struct region *region_table;
	struct region_to_server_relation *region_to_server_map;
	struct server_to_region_relation *server_to_region_map;
	bool master_started;
	uint8_t zookeeper_conn_state;
};

#define SERVER_TO_REGION_CAPACITY (128)
enum region_role { PRIMARY = 1, BACKUP };
struct region_info {
	uint64_t region_key;
	enum region_role role;
};

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
	/*
* zookeeper_init might not have returned, so we
* use zkh instead.
*/
	log_debug("MAIN watcher type %d state %d path %s", type, state, path);
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			master->zookeeper_conn_state = KRM_CONNECTED;

		} else if (state == ZOO_CONNECTING_STATE) {
			if (master->zookeeper_conn_state == KRM_CONNECTED) {
				log_warn("Disconnected from zookeeper %s trying to reconnect", globals_get_zk_host());
				master->zhandle =
					zookeeper_init(globals_get_zk_host(), zk_main_watcher, 15000, 0, master, 0);
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
	int rc = zoo_get(zhandle, zk_path, 0, buffer, &buffer_len, &stat);

	if (rc != ZOK) {
		log_warn("Failed fetching info from zookeeper for server %s Reason: %s", zk_path, zku_op2String(rc));
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

static void apply_for_master(struct master *master)
{
	char *zk_election_path = zku_concat_strings(3, KRM_ROOT_PATH, KRM_ELECTIONS_PATH, KRM_SLASH);
	char *zk_election_full_path = zku_concat_strings(2, zk_election_path, KRM_GUID);
	char value[PROPOSAL_VALUE_LEN] = { 0 };
	char created_path[PROPOSAL_VALUE_LEN] = { 0 };
	int rc = zoo_create(master->zhandle, zk_election_full_path, value, PROPOSAL_VALUE_LEN, &ZOO_OPEN_ACL_UNSAFE,
			    ZOO_SEQUENCE | ZOO_EPHEMERAL, created_path, PROPOSAL_VALUE_LEN);
	if (ZOK != rc) {
		log_fatal("Server: %s failed to apply for master reason: %s", master->server_name.kreon_ds_hostname,
			  zku_op2String(rc));
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
		int rc = zoo_get_children(master->zhandle, zk_path, 0, &children);
		if (rc != ZOK) {
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
		rc = zoo_wexists(master->zhandle, watch_path, master->master_watcher, master, &stat);
		if (rc == ZOK)
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
	MUTEX_INIT(&master->management_lock, NULL);
	log_debug("Initializing connection with zookeeper at %s", globals_get_zk_host());
	master->master_watcher = master_watcher;
	sem_init(&master->barrier, 0, 0);
	master->zhandle = zookeeper_init(globals_get_zk_host(), zk_main_watcher, 15000, 0, master, 0);
	if (!master->zhandle) {
		log_fatal("failed to connect to zk %s", globals_get_zk_host());
		perror("Reason");
		_exit(EXIT_FAILURE);
	}

	field_spin_for_value(&master->zookeeper_conn_state, KRM_CONNECTED);

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
	int rc = zoo_get(master->zhandle, zk_path, 0, buffer, &buffer_len, &stat);
	if (rc != ZOK) {
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

	rc = zoo_set(master->zhandle, zk_path, json_string, strlen(json_string), -1);
	if (ZOK != rc) {
		log_fatal("Failed to update clock for path %s", zk_path);
		_exit(EXIT_FAILURE);
	}
	log_info("Set clock value to %lu", master->leadership_clock);
	cJSON_Delete(new_leader_clock);
	free((void *)json_string);
	free(zk_path);
}

static void build_server_table(struct master *master)
{
	struct String_vector server_hostnames = { 0 };
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_SERVERS_PATH);
	int rc = zoo_get_children(master->zhandle, zk_path, 0, &server_hostnames);
	if (rc != ZOK) {
		log_fatal("Leader (path %s)failed to fetch dataservers info with error code %s", zk_path,
			  zku_op2String(rc));
		_exit(EXIT_FAILURE);
	}

	char dataserver_json[JSON_BUFFER_SIZE];
	int dataserver_json_length = sizeof(dataserver_json);
	memset(dataserver_json, 0, dataserver_json_length);
	for (int i = 0; i < server_hostnames.count; i++) {
		struct region_server *server = calloc(1, sizeof(*server));
		if (!zk_get_server_name(server_hostnames.data[i], master->zhandle, &server->server_name)) {
			log_fatal("Cannot find entry for server %s in zookeeper", server_hostnames.data[i]);
			_exit(EXIT_FAILURE);
		}

		server->server_key = djb2_hash((unsigned char *)server->server_name.kreon_ds_hostname,
					       server->server_name.kreon_ds_hostname_length);
		// server->status = UNKNOWN;
		log_debug("Adding server hostname: %s with hash key %lu", server->server_name.kreon_ds_hostname,
			  server->server_key);
		HASH_ADD_PTR(master->alive_server_table, server_key, server);
	}

	free(zk_path);
}

static void add_region_to_server(uint64_t region_key, struct server_to_region_relation *server_to_region,
				 enum region_role role)
{
	if (server_to_region->len == server_to_region->capacity) {
		server_to_region->capacity *= 2;
		server_to_region->region_info_table =
			realloc(server_to_region->region_info_table, server_to_region->capacity);
	}
	server_to_region->region_info_table[server_to_region->len].region_key = region_key;
	server_to_region->region_info_table[server_to_region->len++].role = role;
}

static void build_relations(struct master *master, struct region *region, char *server, enum region_role role)
{
	struct region_to_server_relation *region_to_server = NULL;
	HASH_FIND_PTR(master->region_to_server_map, &region->region_key, region_to_server);
	if (!region_to_server) {
		region_to_server = calloc(1, sizeof(*region_to_server));
		region_to_server->region = region->region_key;
		HASH_ADD_PTR(master->region_to_server_map, region, region_to_server);
	}
	//Update region to server relation
	uint64_t server_key = djb2_hash((unsigned char *)server, strlen(server));
	if (PRIMARY == role)
		region_to_server->primary = server_key;
	else
		region_to_server->backups[region_to_server->num_of_backup++] = server_key;

	//Update server to region relation
	struct server_to_region_relation *server_to_region = { 0 };
	HASH_FIND_PTR(master->server_to_region_map, &server_key, server_to_region);
	if (!server_to_region) {
		server_to_region = calloc(1, sizeof(*server_to_region));
		server_to_region->server_key = server_key;
		server_to_region->capacity = SERVER_TO_REGION_CAPACITY;
		server_to_region->len = 0;
		server_to_region->region_info_table = calloc(SERVER_TO_REGION_CAPACITY, sizeof(struct region_info));
		log_debug("Adding relation for server %s", server);
		HASH_ADD_PTR(master->server_to_region_map, server_key, server_to_region);
	}
	add_region_to_server(region->region_key, server_to_region, role);
}

static void build_region_table(struct master *master)
{
	struct String_vector region_names = { 0 };
	/*read all regions and construct table*/
	char *zk_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_REGIONS_PATH);
	int rc = zoo_get_children(master->zhandle, zk_path, 0, &region_names);
	if (rc != ZOK) {
		log_fatal("Leader failed to read regions with code %s", zku_op2String(rc));
		_exit(EXIT_FAILURE);
	}
	char region_json_string[JSON_BUFFER_SIZE];
	memset(region_json_string, 0, sizeof(region_json_string));

	for (int i = 0; i < region_names.count; i++) {
		char *region_path = zku_concat_strings(3, zk_path, KRM_SLASH, region_names.data[i]);
		int region_json_string_length = sizeof(region_json_string);
		struct Stat stat = { 0 };
		rc = zoo_get(master->zhandle, region_path, 0, region_json_string, &region_json_string_length, &stat);
		if (rc != ZOK) {
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
		region->status = (enum krm_region_status)cJSON_GetNumberValue(status);

		region->region_key = djb2_hash((unsigned char *)region->id, strlen(region->id));
		log_debug("Adding region with id %s min key %s and max key %s", region->id, region->min_key,
			  region->max_key);
		HASH_ADD_PTR(master->region_table, region_key, region);
		build_relations(master, region, cJSON_GetStringValue(primary), PRIMARY);
		for (int j = 0; j < cJSON_GetArraySize(backups); ++j)
			build_relations(master, region, cJSON_GetStringValue(cJSON_GetArrayItem(backups, j)), BACKUP);

		cJSON_Delete(region_json);
		free(region_path);
	}
	free(zk_path);
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
  * @return the next region
  */
static struct region *get_next_region(struct server_region_iterator *iterator)
{
	if (++iterator->pos == iterator->regions->len)
		return NULL;
	struct region *region = { 0 };
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
  *Returns the role that the server has in the current region (PRIMARY or
  * BACKUP)
  * @param iterator the iterator handle
  * @return the role that his region has
*/

static enum region_role get_role(struct server_region_iterator *iterator)
{
	return iterator->regions->region_info_table[iterator->pos].role;
}

/**
  * Closes and frees the region iterator
**/
static void close_iterator(struct server_region_iterator *iterator)
{
	free(iterator);
}

static int zk_children_comparator(const void *child_1, const void *child_2)
{
	char *left_child = *(char **)child_1;
	char *right_child = *(char **)child_2;
	return strcmp(left_child, right_child);
}

static bool is_server_removed(struct String_vector *alive_servers, int idx)
{
	memmove(&alive_servers->data[idx], &alive_servers->data[idx + 1],
		(alive_servers->count - (idx + 1)) * sizeof(char *));
	--alive_servers->count;
	return true;
}

static bool is_server_alive(struct String_vector *alive_servers, char *server)
{
	int start = 0;
	int end = alive_servers->count - 1;
	while (start <= end) {
		int middle = (end + start) / 2;
		int ret = zk_children_comparator(&alive_servers->data[middle], &server);
		if (ret < 0)
			start = middle + 1;
		else if (ret > 0)
			end = middle - 1;
		else
			return is_server_removed(alive_servers, middle);
	}
	return false;
}

static void reassign_regions(struct master *master, struct region_server *server)
{
	struct server_region_iterator *it = init_server_region_iterator(master, server->server_name.kreon_ds_hostname);
	for (struct region *region = get_next_region(it); region != NULL; region = get_next_region(it))
		log_debug("I must reassign region %s with role %d of dead server %s", region->id, get_role(it),
			  server->server_name.kreon_ds_hostname);
}

static void print_watch_event(int type, const char *path)
{
	if (type == ZOO_CREATED_EVENT)
		log_debug("Node created for path %s", path);
	else if (type == ZOO_DELETED_EVENT)
		log_debug("Node deleted for path %s", path);
	else if (type == ZOO_CHANGED_EVENT)
		log_debug("Data changed for path %s", path);
	else if (type == ZOO_CHILD_EVENT)
		log_debug("Child add for path %s", path);
	else
		log_debug("Unknown type of watcher for path %s", path);
}

static void region_server_health_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	print_watch_event(type, path);
	struct master *master = (struct master *)context;
	MUTEX_LOCK(&master->management_lock);
	struct String_vector *alive_servers = calloc(1, sizeof(struct String_vector));
	char *alive_servers_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);
	int rc = zoo_wget_children(master->zhandle, alive_servers_path, region_server_health_watcher, master,
				   alive_servers);
	if (rc != ZOK) {
		log_fatal("failed to read alive_servers %s error code %s", alive_servers_path, zerror(rc));
		_exit(EXIT_FAILURE);
	}

	qsort(alive_servers->data, alive_servers->count, sizeof(char *), zk_children_comparator);
	struct region_server *fresh_dead_servers = { 0 };
	struct region_server *curr = { 0 };
	struct region_server *tmp = { 0 };
	HASH_ITER(hh, master->alive_server_table, curr, tmp)
	{
		if (!is_server_alive(alive_servers, curr->server_name.kreon_ds_hostname)) {
			log_debug("Server: %s is dead", curr->server_name.kreon_ds_hostname);
			HASH_DEL(master->alive_server_table, curr);
			HASH_ADD_PTR(fresh_dead_servers, server_key, curr);
		}
	}
	log_debug("Freshly joined servers number is %d", alive_servers->count);
	for (int i = 0; i < alive_servers->count; ++i) {
		uint64_t server_key =
			djb2_hash((const unsigned char *)alive_servers->data[i], strlen(alive_servers->data[i]));
		struct region_server *server = { 0 };
		HASH_FIND_PTR(master->dead_server_table, &server_key, server);
		if (!server) {
			log_fatal("Cannot locate server which should be fresh dead");
			_exit(EXIT_FAILURE);
		}
		HASH_DEL(master->dead_server_table, server);
		HASH_ADD_PTR(master->alive_server_table, server_key, server);
	}

	HASH_ITER(hh, fresh_dead_servers, curr, tmp)
	{
		reassign_regions(master, curr);
		HASH_DEL(fresh_dead_servers, curr);
		HASH_ADD_PTR(master->dead_server_table, server_key, curr);
	}
	MUTEX_UNLOCK(&master->management_lock);
	free(alive_servers);
	free(alive_servers_path);
}

#if 0
enum transaction_status { ERROR = 0, PENDING, COMMITED };

static void transaction_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)type;
	(void)state;
	(void)path;
	(void)context;
}
/**
  * Starts a two phase commit protocol (2PC). First, it writes, according to
  * the Zookeeper 2PC recipe, under root path the transaction id and the
  * transaction result. First it notifies the involved dataservers by writing
  * the corresponding command to their mailbox.G
  */
static void prepare_transaction(struct master *master)
{
	char *new_transaction = zku_concat_strings(3, KRM_ROOT_PATH, KRM_TRANSACTIONS, KRM_TRANSACTION_GUUID);
	char new_transaction_id[PROPOSAL_VALUE_LEN] = { 0 };
	int rc = zoo_create(master->zhandle, new_transaction, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
			    ZOO_PERSISTENT | ZOO_SEQUENCE, new_transaction_id, PROPOSAL_VALUE_LEN);
	if (ZOK != rc) {
		log_fatal("Master: %s failed to begin transaction reason: %s", master->server_name.kreon_ds_hostname,
			  zku_op2String(rc));
		_exit(EXIT_FAILURE);
	}
	char *new_transaction_result = zku_concat_strings(3, new_transaction_id, KRM_SLASH, KRM_TRANS_RESULT);
	enum transaction_status status = PENDING;
	rc = zoo_create(master->zhandle, new_transaction_result, (const char *)&status, sizeof(status),
			&ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, NULL, 0);
	if (ZOK != rc) {
		log_fatal("Master: %s failed to begin Two Phase Commit Reason %s",
			  master->server_name.kreon_ds_hostname, zku_op2String(rc));
		_exit(EXIT_FAILURE);
	}
	/*Set watch for transaction id*/
	struct Stat stat = { 0 };
	/*wait until leader is up*/
	rc = zoo_wexists(master->zhandle, new_transaction_id, transaction_watcher, master, &stat);
	if (rc != ZOK) {
		log_fatal("Cannot set watch for new transaction %s", new_transaction_id);
		_exit(EXIT_FAILURE);
	}
}
#endif

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
	MUTEX_LOCK(&master->management_lock);
	struct String_vector *alive_servers = calloc(1, sizeof(struct String_vector));
	char *alive_servers_path = zku_concat_strings(2, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH);
	if (master->master_started)
		goto exit;

	int rc = zoo_wget_children(master->zhandle, alive_servers_path, fresh_boot_watcher, master, alive_servers);
	if (rc != ZOK) {
		log_fatal("failed to read alive_servers %s error code %s", alive_servers_path, zerror(rc));
		_exit(EXIT_FAILURE);
	}
	log_debug("Total alive servers: %d Total team member: %u", alive_servers->count,
		  HASH_COUNT(master->alive_server_table));
	if (alive_servers->count == (int)HASH_COUNT(master->alive_server_table)) {
		log_debug("System ready for fresh boot all %d servers joined!", alive_servers->count);
		master->master_started = true;
		sem_post(&master->barrier);
	} else
		log_debug("Waiting for more dataservers to join current number %d out of %u", alive_servers->count,
			  HASH_COUNT(master->alive_server_table));

exit:
	MUTEX_UNLOCK(&master->management_lock);
	free(alive_servers_path);
	free(alive_servers);
}

static void tm_boot_master(struct master *master, int port)
{
	init_master(master, port);
	apply_for_master(master);
	/*Try to take over the system as master*/
	take_over_as_master(master);
	/*After this step I am the leader*/
	increase_leadership_clock(master);
	build_server_table(master);
	master->master_started = true;
	log_debug("Master clock is %lu", master->leadership_clock);
	if (1 == master->leadership_clock) {
		master->master_started = false;
		fresh_boot_watcher(master->zhandle, -1, -1, NULL, master);
		log_debug("Waiting for team to join");
		sem_wait(&master->barrier);
		log_debug("All team members are here");
	}
	build_region_table(master);
	region_server_health_watcher(master->zhandle, -1, -1, NULL, master);
}

void *run_master(void *args)
{
	pthread_setname_np(pthread_self(), "masterd");
	int port = *(int *)args;
	struct master *master = calloc(1, sizeof(*master));

	tm_boot_master(master, port);
	struct region_server *cur = { 0 };
	struct region_server *tmp = { 0 };
	HASH_ITER(hh, master->alive_server_table, cur, tmp)
	{
		struct server_region_iterator *it =
			init_server_region_iterator(master, cur->server_name.kreon_ds_hostname);
		struct region *region = get_next_region(it);
		while (region) {
			log_debug("Server: %s has region %s with role %d", cur->server_name.kreon_ds_hostname,
				  region->id, get_role(it));
			region = get_next_region(it);
		}
		close_iterator(it);
	}

	return NULL;
}
