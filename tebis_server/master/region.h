#ifndef REGION_H
#define REGION_H
#include "../metadata.h"
typedef struct region *region_t;

/**
  * Creates a region. Note initially there is not assignment to servers
*/
extern region_t REG_create_region(const char *min_key, const char *max_key, const char *region_id,
				  enum krm_region_status status);

/**
 * Deallocates any resources associated with this regions
*/
extern void REG_destroy_region(region_t region);

/**
 * Sets primary for the given region. Hostname must be in the form
 * hostname:port,region_server_epoch
*/
extern void REG_set_primary_in_region(region_t region, const char *hostname);

/**
 * Returns hostname of the primary fot the given region in the form
 * hostname:port,region_server_epoch
*/
extern char *REG_get_region_primary(region_t region);
extern void REG_set_region_primary(region_t region, char *hostname);
/**
 * Returns the primary role for this region.
*/
extern enum server_role REG_get_region_primary_role(region_t region);
extern void REG_set_region_primary_role(region_t region, enum server_role role);

extern uint64_t REG_get_region_primary_clock(region_t region);
extern void REG_set_region_primary_clock(region_t region);
/**
 * Return the hostname of the ist backup server. If the backup_id exceeds the
 * number of backups it returns NULL
*/
extern char *REG_get_region_backup(region_t region, int backup_id);
extern void REG_set_region_backup(region_t region, int backup_id, char *hostname);
/**
 * Returns the number of backups for this region
*/
extern int REG_get_region_num_of_backups(region_t region);

/**
 * Returns the backup role for this region.
*/
extern enum server_role REG_get_region_backup_role(region_t region, int backup_id);
extern void REG_set_region_backup_role(region_t region, int backup_id, enum server_role role);
extern uint64_t REG_get_region_backup_clock(region_t region, int backup_id);
extern void REG_set_region_backup_clock(region_t region, int backup_id, uint64_t clock);

extern void REG_remove_backup_from_region(region_t region, int backup_id);
extern void REG_remove_and_upgrade_primary(region_t region);
extern char *REG_get_region_id(region_t region);
extern void REG_append_backup_in_region(region_t region, char *server);

extern bool REG_is_server_prefix_in_region_group(char *server, size_t prefix_size, region_t region);
#endif
