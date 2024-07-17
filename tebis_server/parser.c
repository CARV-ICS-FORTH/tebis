#include "parser.h"
#include <argp.h>
#include <stdlib.h>
#include <string.h>

#define KB 1024
#define MB KB *KB

static char doc[] = "Tebis Region Server";
static char args_doc[] = "";

static struct argp_option options[] = {
	{ "device", 'd', "DEVICE", 0, "Path to tebis file", 0 },
	{ "zookeeper", 'z', "ZOOKEEPER", 0, "Zookeeper host and port <zk_host:port>", 0 },
	{ "rdma", 'r', "RDMA", 0, "RDMA subnet", 0 },
	{ "server-port", 'p', "SPORT", 0, "Server port", 0 },
	{ "num-of-threads", 'c', "THREADS", 0, "Number of threads (min: 2)", 0 },
	{ "tebis-l0", 't', "T_L0", 0, "TEBIS L0 size in MB (default: 8)", 0 },
	{ "growth-factor", 'g', "GF", 0, "Growth factor (default: 8)", 0 },
	{ "index", 'i', "INDEX", 0, "Send index or build index (default: send_index)", 0 },
	{ "device-size", 's', "DEVICESIZE", 0, "Device size in GB (min: 16) (default: 16)", 0 },
	{ 0 }
};

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

static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
	struct server_config *arguments = state->input;

	switch (key) {
	case 'd':
		arguments->device_name = arg;
		break;
	case 'z':
		arguments->zk_host = arg;
		break;
	case 'r':
		arguments->rdma_subnet = arg;
		break;
	case 'p':
		arguments->server_port = strtol(arg, NULL, 10);
		break;
	case 'c':
		arguments->num_threads = strtol(arg, NULL, 10);
		break;
	case 't':
		arguments->tebisl0_size = strtoul(arg, NULL, 10) * MB;
		break;
	case 'g':
		arguments->growth_factor = strtoul(arg, NULL, 10);
		break;
	case 'i':
		if (strcmp(arg, "send_index") == 0) {
			arguments->index = 1;
		} else if (strcmp(arg, "build_index") == 0) {
			arguments->index = 0;
		} else {
			fprintf(stderr, "Invalid value for send/build index\n");
			exit(EXIT_FAILURE);
		}
		break;
	case 's':
		arguments->device_size = strtoul(arg, NULL, 10);
		break;
	case ARGP_KEY_END:
		if (!arguments->device_name || !arguments->zk_host || !arguments->rdma_subnet ||
		    !arguments->server_port || arguments->num_threads < 2) {
			argp_usage(state);
		}
		break;
	default:
		return ARGP_ERR_UNKNOWN;
	}
	return 0;
}

static struct argp argp = { options, parse_opt, args_doc, doc, NULL, NULL, NULL };

server_config_t create_server_config(void)
{
	server_config_t config = malloc(sizeof(struct server_config));
	if (config) {
		config->tebisl0_size = 8 * MB;
		config->growth_factor = 8;
		config->index = 1;
		config->device_size = 16;
	}
	return config;
}

void destroy_server_config(server_config_t config)
{
	free(config);
}

void parse_arguments(int argc, char *argv[], server_config_t config)
{
	argp_parse(&argp, argc, argv, 0, 0, config);
}

char *get_device_name(const server_config_t config)
{
	return config->device_name;
}
char *get_zk_host(const server_config_t config)
{
	return config->zk_host;
}
char *get_rdma_subnet(const server_config_t config)
{
	return config->rdma_subnet;
}
uint32_t get_tebisl0_size(const server_config_t config)
{
	return config->tebisl0_size * 1024;
}
uint32_t get_growth_factor(const server_config_t config)
{
	return config->growth_factor;
}
int get_index(const server_config_t config)
{
	return config->index;
}
int get_server_port(const server_config_t config)
{
	return config->server_port;
}
int get_num_threads(const server_config_t config)
{
	return config->num_threads;
}
int get_device_size(const server_config_t config)
{
	return config->device_size;
}
