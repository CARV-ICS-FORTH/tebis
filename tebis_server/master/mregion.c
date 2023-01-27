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
#include "mregion.h"
#include "../configurables.h"
#include "../metadata.h"
#include "../tebis_server/messages.h"
#include <assert.h>
#include <log.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
struct replica_member_info {
	char hostname[KRM_HOSTNAME_SIZE];
	char RDMA_IP_addr[KRM_MAX_RDMA_IP_SIZE];
	enum server_role role;
};

struct mregion {
	char id[KRM_MAX_REGION_ID_SIZE];
	char min_key[KRM_MAX_KEY_SIZE];
	char max_key[KRM_MAX_KEY_SIZE];
	struct replica_member_info primary;

	struct replica_member_info backup[KRM_MAX_BACKUPS];
	uint32_t min_key_size;
	uint32_t max_key_size;
	int num_of_backup;
	enum krm_region_status status;
};

mregion_t MREG_create_region(const char *min_key, const char *max_key, const char *region_id,
			     enum krm_region_status status)
{
	_Static_assert(sizeof(struct s2s_msg_get_rdma_buffer_req) + sizeof(struct mregion) <= S2S_MSG_SIZE_VALUE,
		       "MRegion object too large cannot fit in RDMA S2S communication");

	mregion_t region = calloc(1UL, sizeof(*region));

	if (0 == strcmp(region->min_key, "-oo")) {
		memset(region->min_key, 0, KRM_MAX_KEY_SIZE);
		region->min_key_size = 1;
	} else {
		memcpy(region->min_key, min_key, strlen(min_key));
		region->min_key_size = strlen(min_key);
	}
	memcpy(region->max_key, max_key, strlen(max_key));
	memcpy(region->id, region_id, strlen(region_id));
	region->max_key_size = strlen(max_key);
	region->status = status;
	region->num_of_backup = 0;
	return region;
}

uint32_t MREG_serialize_region(mregion_t region, char *buffer, uint32_t buffer_size)
{
	if (buffer_size < MREG_get_region_size()) {
		log_fatal("Buffer too small to serial a region object");
		_exit(EXIT_FAILURE);
	}
	memcpy(buffer, region, MREG_get_region_size());
	uint32_t actual_size = sizeof(*region);
	return actual_size;
}

mregion_t MREG_deserialize_region(char *buffer, uint32_t buffer_size)
{
	if (buffer_size < sizeof(struct mregion)) {
		log_fatal("Buffer too small to host an mregion object");
		_exit(EXIT_FAILURE);
	}
	return (mregion_t)buffer;
}

uint32_t MREG_get_region_size(void)
{
	return sizeof(struct mregion);
}

void MREG_destroy_region(mregion_t region)
{
	(void)region;
}

int MREG_get_region_num_of_backups(mregion_t region)
{
	return region->num_of_backup;
}

enum server_role MREG_get_region_backup_role(mregion_t region, int backup_id)
{
	if (backup_id >= region->num_of_backup)
		return FAULTY_ROLE;
	return region->backup[backup_id].role;
}
void MREG_set_region_backup_role(mregion_t region, int backup_id, enum server_role role)
{
	if (backup_id >= region->num_of_backup)
		return;
	region->backup[backup_id].role = role;
}

void MREG_append_backup_in_region(mregion_t region, char *server)
{
	if (region->num_of_backup >= RU_MAX_NUM_REPLICAS) {
		log_fatal("Backup servers overflow");
		_exit(EXIT_FAILURE);
	}
	if (strlen(server) >= KRM_HOSTNAME_SIZE) {
		log_fatal("Server name: %s too large", server);
		_exit(EXIT_FAILURE);
	}

	memset(region->backup[region->num_of_backup].hostname, 0x00,
	       sizeof(region->backup[region->num_of_backup].hostname));
	strcpy(region->backup[region->num_of_backup].hostname, server);
	region->backup[region->num_of_backup++].role = BACKUP_NEWBIE;
}

void MREG_remove_backup_from_region(mregion_t region, int backup_id)
{
	memmove(&region->backup[backup_id], &region->backup[backup_id + 1],
		(region->num_of_backup-- - (backup_id + 1)) * sizeof(struct replica_member_info));
}

enum server_role MREG_get_region_primary_role(mregion_t region)
{
	return region->primary.role;
}

void MREG_set_region_primary_role(mregion_t region, enum server_role role)
{
	region->primary.role = role;
}

void MREG_remove_and_upgrade_primary(mregion_t region)
{
	region->primary = region->backup[0];
	region->primary.role = PRIMARY_NEWBIE;
	MREG_remove_backup_from_region(region, 0);
}

char *MREG_get_region_id(mregion_t region)
{
	return region->id;
}

char *MREG_get_region_primary(mregion_t region)
{
	log_debug("Region primary is %s", region->primary.hostname);
	return region->primary.hostname;
}

void MREG_set_region_primary(mregion_t region, char *hostname)
{
	log_debug("Setting region primary to %s", hostname);
	strncpy(region->primary.hostname, hostname, KRM_HOSTNAME_SIZE - 1);
}

char *MREG_get_region_backup(mregion_t region, int backup_id)
{
	return backup_id >= region->num_of_backup ? NULL : region->backup[backup_id].hostname;
}

char *MREG_get_region_backup_IP(mregion_t region, int backup_id)
{
	return backup_id >= region->num_of_backup ? NULL : region->backup[backup_id].RDMA_IP_addr;
}
void MREG_set_region_backup_IP(mregion_t region, int backup_id, char *IP_address, size_t IP_address_len)
{
	if (backup_id > region->num_of_backup) {
		log_warn("What are you doing? Invalid backup id");
		return;
	}
	if (IP_address_len > KRM_MAX_RDMA_IP_SIZE) {
		log_fatal("Overflow of RDMA_IP_addr given: %lu actual: %d", IP_address_len, KRM_MAX_RDMA_IP_SIZE);
		_exit(EXIT_FAILURE);
	}
	memcpy(region->backup[backup_id].RDMA_IP_addr, IP_address, IP_address_len);
}

void MREG_set_region_backup(mregion_t region, int backup_id, char *hostname)
{
	if (backup_id >= RU_MAX_NUM_REPLICAS) {
		log_warn(
			"Backup id:%d  num_of_backups:%d region->num_of_backup exceeds the maximum number of backups per region",
			backup_id, RU_MAX_NUM_REPLICAS);
		return;
	}

	strncpy(region->backup[backup_id].hostname, hostname, KRM_HOSTNAME_SIZE);
	if (backup_id >= region->num_of_backup)
		++region->num_of_backup;
}

bool MREG_is_server_prefix_in_region_group(char *server, size_t prefix_size, mregion_t region)
{
	if (prefix_size < strlen(region->primary.hostname) &&
	    0 == strncmp(region->primary.hostname, server, prefix_size))
		return true;

	for (int i = 0; i < region->num_of_backup; ++i) {
		if (prefix_size < strlen(region->backup[i].hostname) &&
		    0 == strncmp(region->backup[i].hostname, server, prefix_size))
			return true;
	}
	return false;
}

const char *const server_role_2_string[ROLE_NUM] = { "FAULTY_ROLE",    "PRIMARY",	"PRIMARY_NEWBIE",
						     "PRIMARY_INFANT", "PRIMARY_DEAD",	"BACKUP",
						     "BACKUP_NEWBIE",  "BACKUP_INFANT", "BACKUP_DEAD" };
void MREG_print_region_configuration(mregion_t region)
{
	log_info("\n***************************************************************************");
	log_info("Region: %s has primary: %s with role: %s", region->id, region->primary.hostname,
		 server_role_2_string[region->primary.role]);
	assert(region->num_of_backup < KRM_MAX_BACKUPS);
	log_info("Region has %d backups", region->num_of_backup);
	for (int i = 0; i < region->num_of_backup; ++i) {
		log_info("Backup %s has role %s", region->backup[i].hostname,
			 server_role_2_string[region->backup[i].role]);
	}
	log_info("\n***************************************************************************");
}

char *MREG_get_region_max_key(mregion_t mregion)
{
	return mregion->max_key;
}

uint32_t MREG_get_region_max_key_size(mregion_t mregion)
{
	return mregion->max_key_size;
}

char *MREG_get_region_min_key(mregion_t mregion)
{
	return mregion->min_key;
}

uint32_t MREG_get_region_min_key_size(mregion_t mregion)
{
	return mregion->min_key_size;
}
