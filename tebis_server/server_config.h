#ifndef SERVER_CONFIG_H
#define SERVER_CONFIG_H

#include <stdint.h>

typedef struct server_config *server_config_t;

server_config_t SCONF_create_server_config(void);
void SCONF_destroy_server_config(server_config_t config);

void SCONF_parse_arguments(int argc, char *argv[], server_config_t config);

char *SCONF_get_device_name(const server_config_t config);
char *SCONF_get_zk_host(const server_config_t config);
char *SCONF_get_rdma_subnet(const server_config_t config);
uint32_t SCONF_get_tebisl0_size(const server_config_t config);
uint32_t SCONF_get_growth_factor(const server_config_t config);
int SCONF_get_index(const server_config_t config);
int SCONF_get_server_port(const server_config_t config);
int SCONF_get_num_threads(const server_config_t config);
int SCONF_get_device_size(const server_config_t config);

#endif // SERVER_CONFIG_H
