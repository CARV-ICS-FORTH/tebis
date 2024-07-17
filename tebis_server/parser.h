#ifndef PARSER_H
#define PARSER_H

#include <stdint.h>

typedef struct server_config *server_config_t;

server_config_t create_server_config(void);
void destroy_server_config(server_config_t config);

void parse_arguments(int argc, char *argv[], server_config_t config);

char *get_device_name(const server_config_t config);
char *get_zk_host(const server_config_t config);
char *get_rdma_subnet(const server_config_t config);
uint32_t get_tebisl0_size(const server_config_t config);
uint32_t get_growth_factor(const server_config_t config);
int get_index(const server_config_t config);
int get_server_port(const server_config_t config);
int get_num_threads(const server_config_t config);
int get_device_size(const server_config_t config);

#endif // PARSER_H
