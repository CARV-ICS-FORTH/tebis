#ifndef PARSER_H
#define PARSER_H

#include <stdint.h>

struct server_config {
	char *device_name;
	char *zk_host;
	char *rdma_subnet;
	uint32_t tebisl0_size;
	uint32_t growth_factor;
	int index;
	int server_port;
	int num_threads;
	int device_size;
};

void parse_arguments(int argc, char *argv[], struct server_config *config);

#endif // PARSER_H
