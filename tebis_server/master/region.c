#include "region.h"
#include "../metadata.h"
#include <limits.h>
#include <log.h>
#include <uthash.h>
struct replica_member_info {
	char hostname[KRM_HOSTNAME_SIZE];
	uint64_t clock;
	enum server_role role;
};

struct region {
	char id[KRM_MAX_REGION_ID_SIZE];
	char min_key[KRM_MAX_KEY_SIZE];
	char max_key[KRM_MAX_KEY_SIZE];
	struct replica_member_info primary;

	struct replica_member_info backup[RU_MAX_NUM_REPLICAS];
	uint32_t min_key_size;
	uint32_t max_key_size;
	int num_of_backup;
	enum krm_region_status status;
};

uint64_t REG_get_region_primary_clock(region_t region)
{
	return region->primary.clock;
}

uint64_t REG_get_region_backup_clock(region_t region, int backup_id)
{
	return region->backup[backup_id].clock;
}

region_t REG_create_region(const char *min_key, const char *max_key, const char *region_id,
			   enum krm_region_status status)
{
	region_t region = calloc(1UL, sizeof(*region));

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
	return region;
}

void REG_destroy_region(region_t region)
{
	(void)region;
}

int REG_get_region_num_of_backups(region_t region)
{
	return region->num_of_backup;
}

enum server_role REG_get_region_backup_role(region_t region, int backup_id)
{
	if (backup_id >= region->num_of_backup)
		return FAULTY_ROLE;
	return region->backup[backup_id].role;
}
void REG_set_region_backup_role(region_t region, int backup_id, enum server_role role)
{
	if (backup_id >= region->num_of_backup)
		return;
	region->backup[backup_id].role = role;
}

void REG_append_backup_in_region(region_t region, char *server)
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
	region->backup[region->num_of_backup].clock = UINT64_MAX;
	region->backup[region->num_of_backup++].role = BACKUP_NEWBIE;
}

void REG_remove_backup_from_region(region_t region, int backup_id)
{
	memmove(&region->backup[backup_id], &region->backup[backup_id + 1],
		(region->num_of_backup-- - (backup_id + 1)) * sizeof(struct replica_member_info));
}

enum server_role REG_get_region_primary_role(region_t region)
{
	return region->primary.role;
}

void REG_set_region_primary_role(region_t region, enum server_role role)
{
	region->primary.role = role;
}

void REG_remove_and_upgrade_primary(region_t region)
{
	region->primary = region->backup[0];
	region->primary.role = PRIMARY_NEWBIE;
	REG_remove_backup_from_region(region, 0);
}

char *REG_get_region_id(region_t region)
{
	return region->id;
}

char *REG_get_region_primary(region_t region)
{
	return region->primary.hostname;
}

void REG_set_region_primary(region_t region, char *hostname)
{
	strncpy(region->primary.hostname, hostname, KRM_HOSTNAME_SIZE - 1);
}

char *REG_get_region_backup(region_t region, int backup_id)
{
	return backup_id >= region->num_of_backup ? NULL : region->backup[backup_id].hostname;
}

void REG_set_region_backup(region_t region, int backup_id, char *hostname)
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

bool REG_is_server_prefix_in_region_group(char *server, size_t prefix_size, region_t region)
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
void REG_print_region_configuration(region_t region)
{
	log_info("\n***************************************************************************");
	log_info("Region: %s has primary: %s with role: %s", region->id, region->primary.hostname,
		 server_role_2_string[region->primary.role]);
	log_info("Region has %d backups", region->num_of_backup);
	for (int i = 0; i < region->num_of_backup; ++i) {
		log_info("Backup %s has role %s", region->backup[i].hostname,
			 server_role_2_string[region->backup[i].role]);
	}
	log_info("\n***************************************************************************");
}
