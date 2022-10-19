/**
 * Kadmos simulates thousands of regions and Region Servers to stress the
 * Master ability to handle failures. It groups servers in N groups, where N is
 * the replication size which is set as an input parameters. Each group
 * consists of Region Servers that are in the same host but in different port.
 * The replication group of each region contains one server of each group. The
 * failures take place in rounds. Kadmos iteratively selects one replica group
 * and produces M Region Server failures. M is a random number. Then, it counts
 * the regions affected and waits from Master the corresponding messages. After
 * this step it causesfailures to the next group. The final correct state of
 * the system must N Region Server which host all the available regions of the
 * system.
 */
#include "../tebis_server/metadata.h"
#include "../tebis_server/zk_utils.h"
#include <fcntl.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zookeeper.h>
#define KADMOS_ZK_SESSION_TIMEOUT 15000
#define MIN_NUM_OF_SERVERS_PER_GROUP 1
#define MAX_NUM_OF_SERVERS_PER_GROUP 9999
#define MAX_HOSTNAME_SIZE (128)
#define INITIAL_CAPACITY (32)
const char *const hostname[] = { "Gamatos", "Feugatos", "Psilos", "Kontos", "Lignos" };
enum host_id { HOST0 = 0, HOST1, HOST2, HOST3, HOST4, HOST_SIZE };
#include <log.h>
#include <stdlib.h>

typedef struct {
	char hostname[MAX_HOSTNAME_SIZE];
} tebis_host_t;

typedef struct {
	int capacity;
	int num_of_servers;
	tebis_host_t *hosts;
} tebis_host_group_t;

static void kadmos_zk_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	/**
   * zookeeper_init might not have returned, so we use zkh instead.
  */
	int *connected = context;
	log_debug("MAIN watcher type %d state %d path %s", type, state, path);
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			*connected = 1;

		} else if (state == ZOO_CONNECTING_STATE) {
		}
		return;
	}
	log_warn("Unhandled event");
}

static tebis_host_group_t *create_empty_group(int capacity)
{
	tebis_host_group_t *group = calloc(1, sizeof(tebis_host_group_t));
	group->hosts = calloc(capacity, sizeof(tebis_host_t));
	group->capacity = capacity;
	return group;
}

static void add_tebis_host_to_group(tebis_host_group_t *group, char *host)
{
	if (group->num_of_servers >= group->capacity) {
		group->capacity *= 2;
		group->hosts = realloc(group->hosts, group->capacity * sizeof(tebis_host_t));
		if (!group->hosts) {
			log_fatal("Resizing failed");
			_exit(EXIT_FAILURE);
		}
	}
	strcpy(group->hosts[group->num_of_servers++].hostname, host);
}

#if 0
static void remove_tebis_host_from_group(tebis_host_group_t *group, int id)
{
	(void)group;
	(void)id;
}
#endif

static tebis_host_group_t **generate_groups(long int replication_size, long int num_of_servers_per_group)
{
	if (replication_size < 1) {
		log_fatal("Replication size %ld must be > 1 otherwise what is the point of the test?",
			  replication_size);
		_exit(EXIT_FAILURE);
	}

	if (replication_size > HOST_SIZE) {
		log_fatal("Sorry max replication can be up to %d", HOST_SIZE);
		_exit(EXIT_FAILURE);
	}

	if (num_of_servers_per_group < MIN_NUM_OF_SERVERS_PER_GROUP ||
	    num_of_servers_per_group > MAX_NUM_OF_SERVERS_PER_GROUP) {
		log_fatal("Num of servers per group should be from %d to %d", MIN_NUM_OF_SERVERS_PER_GROUP,
			  MAX_NUM_OF_SERVERS_PER_GROUP);
		_exit(EXIT_FAILURE);
	}

	tebis_host_group_t **groups = calloc(replication_size, sizeof(tebis_host_group_t *));

	for (int i = 0; i < replication_size; ++i) {
		groups[i] = create_empty_group(INITIAL_CAPACITY);
		for (int j = 0; j < num_of_servers_per_group; ++j) {
			char host_buffer[MAX_HOSTNAME_SIZE] = { 0 };
			strcpy(host_buffer, hostname[i]);
			if (sprintf(&host_buffer[strlen(hostname[i])], ":%d", j) < 0) {
				log_fatal("Sprintf failed");
				_exit(EXIT_FAILURE);
			}
			// log_debug("Adding host %s to group no %d", host_buffer, i);
			add_tebis_host_to_group(groups[i], host_buffer);
		}
	}
	return groups;
}

#define DECIMAL 10
#define REGION_ID_PREFIX_SIZE 4
#define REGION_CONFIGURATION_BUFFER_SIZE 1024

typedef struct {
	char *region_buffer;
	char *region_min_key;
	char *region_max_key;
	tebis_host_group_t **groups;
	int region_id;
	int region_buffer_size;
	int region_min_key_size;
	int region_max_key_size;
	long int replication_size;
} region_configuration_t;

static void increase_region_prefix(char *region_prefix, int prefix_size)
{
	for (int i = prefix_size - 1; i >= 0; --i) {
		if (region_prefix[i] >= 90) {
			region_prefix[i] = 'A';
			continue;
		}
		++region_prefix[i];
		break;
	}
}

static void create_new_region_configuration(region_configuration_t *region_configuration)
{
	memset(region_configuration->region_buffer, 0x00, region_configuration->region_buffer_size);

	if (sprintf(region_configuration->region_buffer, "%d %.*s %.*s", region_configuration->region_id,
		    region_configuration->region_min_key_size, region_configuration->region_min_key,
		    region_configuration->region_max_key_size, region_configuration->region_max_key) < 0) {
		log_fatal("sprintf failed");
		_exit(EXIT_FAILURE);
	}
	for (int i = 0; i < region_configuration->replication_size; ++i) {
		int ret = sprintf(&region_configuration->region_buffer[strlen(region_configuration->region_buffer)],
				  " %s,0 ",
				  region_configuration->groups[i]
					  ->hosts[rand() % region_configuration->groups[i]->num_of_servers]
					  .hostname);
		if (ret < 0) {
			log_fatal("sprintf failed");
			_exit(EXIT_FAILURE);
		}
	}
}
#define MASTER "sith6.cluster.ics.forth.gr:8080"
#define ZOOKEEPER_HOST "sith6.cluster.ics.forth.gr:2181"
#define ZOOKEEPER_TIMEOUT 15000
static void create_hosts_file(tebis_host_group_t **groups, long int num_of_groups, char *host_file_path)
{
	FILE *hosts_file = fopen(host_file_path, "we+");
	if (NULL == hosts_file) {
		log_fatal("Opening hosts file with reason");
		perror("Reason:");
		_exit(EXIT_FAILURE);
	}
	if (fprintf(hosts_file, "%s\n", MASTER) < 0) {
		log_fatal("Writing to hosts file failed");
		_exit(EXIT_FAILURE);
	}
	for (int i = 0; i < num_of_groups; ++i) {
		for (int j = 0; j < groups[i]->num_of_servers; j++) {
			if (fprintf(hosts_file, "%s\n", groups[i]->hosts[j].hostname) < 0) {
				log_fatal("Writing to hosts file failed");
				_exit(EXIT_FAILURE);
			}
		}
	}

	if (fflush(hosts_file)) {
		log_fatal("Flush of hosts files failed");
		perror("Reason:");
		_exit((EXIT_FAILURE));
	}

	if (fclose(hosts_file)) {
		log_fatal("Close of hosts files failed");
		perror("Reason:");
		_exit((EXIT_FAILURE));
	}
}

static void append_region_to_file(FILE *region_file, region_configuration_t *region_configuration)
{
	if (fprintf(region_file, "%s\n", region_configuration->region_buffer) < 0) {
		log_fatal("Writing to regions file failed");
		_exit(EXIT_FAILURE);
	}
}

static void create_region_file(char *regions_file_path, long int replication_size, tebis_host_group_t **groups,
			       long int num_of_regions)
{
	FILE *region_file = fopen(regions_file_path, "we+");
	if (NULL == region_file) {
		log_fatal("Failed to open region file %s", regions_file_path);
		perror("Reason:");
		_exit(EXIT_FAILURE);
	}

	char region_buffer[REGION_CONFIGURATION_BUFFER_SIZE] = { 0 };
	char region_prefix_max[REGION_ID_PREFIX_SIZE] = { 'A', 'A', 'A', 'A' };

	int region_id = 0;
	region_configuration_t region_configuration = { .region_buffer = region_buffer,
							.region_min_key = "-oo",
							.region_max_key = region_prefix_max,
							.groups = groups,
							.region_id = region_id++,
							.region_buffer_size = REGION_CONFIGURATION_BUFFER_SIZE,
							.region_min_key_size = strlen("-oo"),
							.region_max_key_size = REGION_ID_PREFIX_SIZE,
							.replication_size = replication_size };
	create_new_region_configuration(&region_configuration);

	append_region_to_file(region_file, &region_configuration);

	char region_prefix_min[REGION_ID_PREFIX_SIZE] = { 'A', 'A', 'A', 'A' };
	region_prefix_max[REGION_ID_PREFIX_SIZE - 1] = 'B';
	for (long int i = 1; i < num_of_regions; ++i) {
		++region_configuration.region_id;
		region_configuration.region_min_key_size = REGION_ID_PREFIX_SIZE;
		region_configuration.region_min_key = region_prefix_min;
		region_configuration.region_max_key_size = REGION_ID_PREFIX_SIZE;
		region_configuration.region_max_key = region_prefix_max;
		create_new_region_configuration(&region_configuration);
		append_region_to_file(region_file, &region_configuration);
		increase_region_prefix(region_prefix_min, REGION_ID_PREFIX_SIZE);
		increase_region_prefix(region_prefix_max, REGION_ID_PREFIX_SIZE);
	}

	++region_configuration.region_id;
	region_configuration.region_max_key_size = strlen("+oo");
	region_configuration.region_max_key = "+oo";
	create_new_region_configuration(&region_configuration);
	append_region_to_file(region_file, &region_configuration);

	if (fflush(region_file)) {
		log_fatal("Flush of region files failed");
		perror("Reason:");
		_exit((EXIT_FAILURE));
	}

	if (fclose(region_file)) {
		log_fatal("Close of region files failed");
		perror("Reason:");
		_exit((EXIT_FAILURE));
	}
}

void register_all_servers_as_alive(zhandle_t *zhandle, char *host_file_path)
{
#define HOSTNAME_BUFFER_SIZE 256
	FILE *hosts_file = fopen(host_file_path, "re");
	if (NULL == hosts_file) {
		log_fatal("Failed to open file %s", host_file_path);
		_exit(EXIT_FAILURE);
	}

	char hostname[HOSTNAME_BUFFER_SIZE] = { 0 };
	while (fgets(hostname, HOSTNAME_BUFFER_SIZE, hosts_file)) {
		hostname[strlen(hostname) - 1] = '\0';
		// log_debug("Host is %s", hostname);
		char *zk_path = zku_concat_strings(5, KRM_ROOT_PATH, KRM_ALIVE_SERVERS_PATH, KRM_SLASH, hostname);
		char created_path[HOSTNAME_BUFFER_SIZE] = { 0 };
		int ret_code = zoo_create(zhandle, zk_path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT,
					  created_path, HOSTNAME_BUFFER_SIZE);
		if (ZOK != ret_code) {
			log_fatal("Failed to create zookeeper node %s of size %lu code is %s", zk_path, strlen(zk_path),
				  zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		free(zk_path);
		char hostname_with_epoch[HOSTNAME_BUFFER_SIZE] = { 0 };
		if (snprintf(hostname_with_epoch, HOSTNAME_BUFFER_SIZE, "%s,0", hostname) < 0) {
			log_fatal("Failed to create hostname with epoch");
			_exit(EXIT_FAILURE);
		}
		zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_MAILBOX_PATH, KRM_SLASH, hostname_with_epoch);
		memset(created_path, 0x00, HOSTNAME_BUFFER_SIZE);
		ret_code = zoo_create(zhandle, zk_path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, created_path,
				      HOSTNAME_BUFFER_SIZE);
		if (ZOK != ret_code) {
			log_fatal("Failed to create zookeeper node %s of size %lu code is %s", zk_path, strlen(zk_path),
				  zerror(ret_code));
			_exit(EXIT_FAILURE);
		}
		free(zk_path);
	}

	if (fclose(hosts_file) < 0) {
		log_fatal("Failed to close host file: %s", host_file_path);
		_exit(EXIT_FAILURE);
	}
}

int main(int argc, char **argv)
{
	bool is_load = false;
	if (argc < 2) {
		log_fatal("Wrong arguments: Usage ./kadmos load or run");
		_exit(EXIT_FAILURE);
	}

	if (0 != strcmp(argv[1], "load") && 0 != strcmp(argv[1], "run")) {
		log_fatal(
			"Is it a ./kadmos load to generate host and region files or ./kadmos run to run a simulation?");
		_exit(EXIT_FAILURE);
	}

	if (0 == strcmp(argv[1], "load")) {
		is_load = true;
		if (argc != 6) {
			log_fatal(
				"Wrong arguments Usage: ./kadmos load <replication size> <number of servers per group> <num_of_regions> <directory to store hosts and region_file>");
			_exit(EXIT_FAILURE);
		}
	}

	if (0 == strcmp(argv[1], "run")) {
		if (argc != 3) {
			log_fatal("Wrong arguments Usage: ./kadmos run <directory with host/regions file>");
			_exit(EXIT_FAILURE);
		}
	}

	int arg_id = 2;

	if (is_load) {
		long int replication_size = strtol(argv[arg_id++], NULL, DECIMAL);
		long int num_of_servers_per_group = strtol(argv[arg_id++], NULL, DECIMAL);
		tebis_host_group_t **groups = generate_groups(replication_size, num_of_servers_per_group);
		long int num_of_regions = strtol(argv[arg_id++], NULL, DECIMAL);
		char *hosts_file_path = calloc(1, strlen(argv[arg_id]) + strlen("hosts_file") + 1);
		strcpy(hosts_file_path, argv[arg_id]);
		strcpy(&hosts_file_path[strlen(argv[arg_id])], "hosts_file");
		create_hosts_file(groups, replication_size, hosts_file_path);
		free(hosts_file_path);
		hosts_file_path = NULL;
		char *regions_file_path = calloc(1, strlen(argv[arg_id]) + strlen("regions_file") + 1);
		strcpy(regions_file_path, argv[arg_id]);
		strcpy(&regions_file_path[strlen(argv[arg_id])], "regions_file");
		create_region_file(regions_file_path, replication_size, groups, num_of_regions);
		free(regions_file_path);
		regions_file_path = NULL;
		return 0;
	}

	volatile int connected = 0;
	zhandle_t *handle =
		zookeeper_init(ZOOKEEPER_HOST, kadmos_zk_watcher, ZOOKEEPER_TIMEOUT, 0, (void *)&connected, 0);
	for (; 0 == connected;)
		;

	char *hosts_file_path = calloc(1, strlen(argv[arg_id]) + strlen("hosts_file") + 1);
	strcpy(hosts_file_path, argv[arg_id]);
	strcpy(&hosts_file_path[strlen(argv[arg_id])], "hosts_file");
	assert(handle);
	register_all_servers_as_alive(handle, hosts_file_path);
	sleep(5000);
	return 0;
}
