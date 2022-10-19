#include "region_log.h"
#include "../metadata.h"
#include "region.h"
#include "zookeeper.h"
#include <log.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void start_region_reconfiguration(region_log_t *region_log, region_t *region)
{
	(void)region_log;
	(void)region;
}

void trim_region_log(region_log_t *region_log, int64_t max_lsn)
{
	(void)region_log;
	(void)max_lsn;
}

void replay_region_log(region_log_t *region_log)
{
	(void)region_log;
}

region_log_t *create_region_log(char *path, unsigned int path_len, zhandle_t *zhandle)
{
	if (path_len > PATH_SIZE) {
		log_fatal("path size %d exceeds max path size %d", path_len, PATH_SIZE);
		_exit(EXIT_FAILURE);
	}

	region_log_t *region_log = calloc(1, sizeof(region_log_t));
	region_log->zk_handle = zhandle;
	memcpy(region_log->zk_path, path, path_len);
	return region_log;
}
