#ifndef REGION_H
#define REGION_H
#include "../metadata.h"
typedef struct region *region_t;

/**
  * Creates a region. Note initially there is not assignment to servers
*/
region_t create_region(const char *min_key, const char *max_key, const char *region_id, enum krm_region_status status);

/**
 * Deallocates any resources associated with this regions
*/
void destroy_region(region_t region);

/**
 * Sets primary for the given region. Hostname must be in the form
 * hostname:port,region_server_epoch
*/
void set_primary_in_region(region_t region, const char *hostname);

/**
 * Returns hostname of the primary fot the given region in the form
 * hostname:port,region_server_epoch
*/
char *get_region_primary(region_t region);
void set_region_primary(region_t region, char *hostname);
/**
 * Return the hostname of the ist backup server. If the backup_id exceeds the
 * number of backups it returns NULL
*/
char *get_region_backup(region_t region, int backup_id);
void set_region_backup(region_t region, int backup_id, char *hostname);
/**
 * Returns the number of backups for this region
*/
int get_region_num_of_backups(region_t region);

/**
 * Returns the backup role for this region.
*/
enum server_role get_region_backup_role(region_t region, int backup_id);
void set_region_backup_role(region_t region, int backup_id, enum server_role role);
uint64_t get_region_backup_clock(region_t region, int backup_id);
void set_region_backup_clock(region_t region, int backup_id, uint64_t clock);

/**
 * Returns the primary role for this region.
*/
enum server_role get_region_primary_role(region_t region);
void set_region_primary_role(region_t region, enum server_role role);

uint64_t get_region_primary_clock(region_t region);
void set_region_primary_clock(region_t region);

void remove_backup_from_region(region_t region, int backup_id);
void remove_and_upgrade_primary(region_t region);
char *get_region_id(region_t region);
#endif
