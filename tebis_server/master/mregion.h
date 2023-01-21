#ifndef MREGION_H
#define MREGION_H
#include "../metadata.h"
typedef struct mregion *mregion_t;

/**
  * Creates a region. Note initially there is not assignment to servers
*/
extern mregion_t MREG_create_region(const char *min_key, const char *max_key, const char *region_id,
				    enum krm_region_status status);

/**
 * @brief Serializes and returns the buffer and its size
 * @param pointer to the region object
 * @param buffer which is the destination where to serialize the object
 * @param buffer_size size of the buffer
 * @return size of the object
 */
uint32_t MREG_serialize_region(mregion_t region, char *buffer, uint32_t buffer_size);

extern mregion_t MREG_deserialize_region(char *buffer, uint32_t buffer_size);

extern uint32_t MREG_get_region_size(void);

/**
 * Deallocates any resources associated with this regions
*/
extern void MREG_destroy_region(mregion_t region);

/**
 * Returns hostname of the primary fot the given region in the form
 * hostname:port,region_server_epoch
*/
extern char *MREG_get_region_primary(mregion_t region);
extern void MREG_set_region_primary(mregion_t region, char *hostname);
/**
 * Returns the primary role for this region.
*/
extern enum server_role MREG_get_region_primary_role(mregion_t region);
extern void MREG_set_region_primary_role(mregion_t region, enum server_role role);

/**
 * Return the hostname of the ist backup server. If the backup_id exceeds the
 * number of backups it returns NULL
*/
extern char *MREG_get_region_backup(mregion_t region, int backup_id);
extern void MREG_set_region_backup(mregion_t region, int backup_id, char *hostname);
/**
 * Returns the number of backups for this region
*/
extern int MREG_get_region_num_of_backups(mregion_t region);

/**
 * Returns the backup role for this region.
*/
extern enum server_role MREG_get_region_backup_role(mregion_t region, int backup_id);
extern void MREG_set_region_backup_role(mregion_t region, int backup_id, enum server_role role);

extern void MREG_remove_backup_from_region(mregion_t region, int backup_id);
extern void MREG_remove_and_upgrade_primary(mregion_t region);
extern char *MREG_get_region_id(mregion_t region);
extern void MREG_append_backup_in_region(mregion_t region, char *server);

extern bool MREG_is_server_prefix_in_region_group(char *server, size_t prefix_size, mregion_t region);
extern void MREG_print_region_configuration(mregion_t region);

extern char *MREG_get_region_min_key(mregion_t mregion);
extern uint32_t MREG_get_region_min_key_size(mregion_t mregion);
#endif
