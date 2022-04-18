#include "../utilities/spin_loop.h"
#include "metadata.h"
#include "zk_utils.h"
#include <log.h>
#include <zookeeper/zookeeper.h>

uint8_t is_connected = 0;
static void zk_watcher(zhandle_t *zkh, int type, int state, const char *path, void *context)
{
	(void)zkh;
	(void)path;
	(void)context;
	/*
 	* zookeeper_init might not have returned, so we
 	* use zkh instead.
 	*/
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			is_connected = 1;

		} else if (state == ZOO_CONNECTING_STATE) {
			log_fatal("Disconnected from zookeeper");
			_exit(EXIT_FAILURE);
		}
	}
}

int main(int argc, char *argv[])
{
	struct krm_region region;
	if (argc < 5) {
		log_fatal(
			"Too few arguments (%d) example ./create_region <zookeeper_host:zookeeper_port> <region_id> <region_min_key> <region_max_key> <primary> <backup 1>,...,<backup N>",
			argc);
		_exit(EXIT_FAILURE);
	}
	/*init zookeeper connection*/
	log_info("Connecting to zookeeper server: %s", argv[1]);
	zhandle_t *zh = zookeeper_init(argv[1], zk_watcher, 15000, 0, 0, 0);
	field_spin_for_value(&is_connected, 1);

	if (strcmp(argv[3], "-oo") == 0) {
		region.min_key_size = 1;
		memset(region.min_key, 0x00, KRM_MAX_KEY_SIZE);
	} else {
		region.min_key_size = strlen(argv[3]);
		memset(region.min_key, 0x00, KRM_MAX_KEY_SIZE);
		strcpy(region.min_key, argv[3]);
	}
	region.max_key_size = strlen(argv[4]);
	memset(region.max_key, 0x00, KRM_MAX_KEY_SIZE);
	strcpy(region.max_key, argv[4]);

	strcpy(region.id, argv[2]);
	region.stat = KRM_FRESH;
	region.num_of_backup = argc - 6;
	/*primary server*/
	strcpy(region.primary.kreon_ds_hostname, argv[5]);
	region.primary.kreon_ds_hostname_length = strlen(region.primary.kreon_ds_hostname);
	char *token = strtok(argv[5], "-");
	strcpy(region.primary.hostname, token);
	region.primary.epoch = 0;
	for (uint32_t i = 0; i < region.num_of_backup; i++) {
		strcpy(region.backups[i].kreon_ds_hostname, argv[6 + i]);
		region.backups[i].kreon_ds_hostname_length = strlen(region.backups[i].kreon_ds_hostname);
		char *hostname_token = strtok(argv[6 + i], "-");
		strcpy(region.backups[i].hostname, hostname_token);

		region.backups[i].epoch = 0;
	}
	char *zk_path = zku_concat_strings(4, KRM_ROOT_PATH, KRM_REGIONS_PATH, "/", argv[2]);
	int rc = zoo_create(zh, zk_path, (char *)&region, sizeof(struct krm_region), &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
	if (rc != ZOK) {
		log_fatal("failed to create region %s with status %d", argv[3], rc);
		_exit(EXIT_FAILURE);
	}
	free(zk_path);
	return EXIT_SUCCESS;
}
