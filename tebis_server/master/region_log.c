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
#include "region_log.h"
#include "../tebis_server/configurables.h"
#include "../zk_utils.h"
#include <assert.h>
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zookeeper.h>

struct region_log {
	char zk_path[KRM_MAX_ZK_PATH_SIZE];
	zhandle_t *zk_handle;
};

region_log_t create_region_log(char *path, unsigned int path_len, zhandle_t *zhandle)
{
	if (path_len > KRM_MAX_ZK_PATH_SIZE) {
		log_fatal("path size %d exceeds max path size %lu", path_len, KRM_MAX_ZK_PATH_SIZE);
		_exit(EXIT_FAILURE);
	}

	region_log_t region_log = calloc(1UL, sizeof(*region_log));
	region_log->zk_handle = zhandle;
	memcpy(region_log->zk_path, path, path_len);
	return region_log;
}

void trim_region_log(region_log_t region_log, int64_t max_lsn)
{
	(void)region_log;
	(void)max_lsn;
}

void replay_region_log(region_log_t region_log)
{
	(void)region_log;
}

bool append_req_to_region_log(region_log_t region_log, MC_command_t command)
{
	char log_entry[KRM_MAX_ZK_PATH_SIZE] = { 0 };
	int ret_code = zoo_create(region_log->zk_handle, region_log->zk_path, (const char *)command,
				  MC_get_command_size(command), &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, log_entry,
				  KRM_MAX_ZK_PATH_SIZE);

	if (ZOK != ret_code) {
		log_warn("Failed to append command to region log:%s reason: %s", region_log->zk_path,
			 zku_op2String(ret_code));
		assert(0);
		return false;
	}
	return true;
}
