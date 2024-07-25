#ifndef SERVER_CONFIG_H
#define SERVER_CONFIG_H

#include <stdint.h>

typedef struct SCONF_server_config *SCONF_server_config_t;

/**
 * @brief Creates a server configuration instance with default values.
 * @return A pointer to the newly created server configuration structure.
 */
SCONF_server_config_t SCONF_create_server_config(void);

/**
 * @brief Destroys a server configuration instance.
 * @param config A pointer to the server configuration structure to be destroyed.
 */
void SCONF_destroy_server_config(SCONF_server_config_t config);

/**
 * @brief Parses command-line arguments and populates the server configuration.
 * @param argc The number of command-line arguments.
 * @param argv An array of command-line argument strings.
 * @param config A pointer to the server configuration structure to be populated.
 */
void SCONF_parse_arguments(int argc, char *argv[], SCONF_server_config_t config);

/**
 * @brief Getter functions for server configuration fields.
 * @param config A pointer to the server configuration structure.
 * @return The corresponding field value from the server configuration structure.
 */
char *SCONF_get_device_name(const SCONF_server_config_t config);
char *SCONF_get_zk_host(const SCONF_server_config_t config);
char *SCONF_get_rdma_subnet(const SCONF_server_config_t config);
uint32_t SCONF_get_tebisl0_size(const SCONF_server_config_t config);
uint32_t SCONF_get_growth_factor(const SCONF_server_config_t config);
int SCONF_get_index(const SCONF_server_config_t config);
int SCONF_get_server_port(const SCONF_server_config_t config);
int SCONF_get_num_threads(const SCONF_server_config_t config);
int SCONF_get_device_size(const SCONF_server_config_t config);

#endif // SERVER_CONFIG_H
