#include "mregion_server.h"
#include "../djb2.h"
#include "../metadata.h"
#include "region.h"
#include <log.h>
#include <stdlib.h>
#include <uthash.h>
#define SERVER_REGIONS_LIST_CAPACITY (128)

struct region_server {
	struct krm_server_name server_name;
	enum region_server_status status;
	int num_of_regions;
	int capacity;
	region_info_t regions;
};

struct region_info {
	region_t region;
	enum server_role role;
};

struct region_server_iterator {
	region_server_t region_server;
	region_info_t region_info;
	int position;
};

struct krm_server_name *RS_get_region_server_krm_hostname(region_server_t region_server)
{
	return &region_server->server_name;
}

region_server_t RS_create_region_server(struct krm_server_name server_name, enum region_server_status status)
{
	region_server_t region_server = calloc(1UL, sizeof(*region_server));
	region_server->server_name = server_name;
	region_server->status = status;
	region_server->capacity = SERVER_REGIONS_LIST_CAPACITY;
	region_server->regions = calloc(region_server->capacity, sizeof(struct region_info));
	return region_server;
}

void RS_add_region_in_server(region_server_t region_server, region_t region, enum server_role role)
{
	if (region_server->num_of_regions >= region_server->capacity) {
		region_server->capacity *= 2;
		region_server->regions = realloc(region_server->regions, region_server->capacity);
		if (!region_server->regions) {
			log_fatal("Failed to double capacity");
			_exit(EXIT_FAILURE);
		}
	}
	region_server->regions[region_server->num_of_regions].region = region;
	region_server->regions[region_server->num_of_regions++].role = role;
}

void RS_destroy_region_server(region_server_t region_server)
{
	free(region_server->regions);
	free(region_server);
}

char *get_server_krm_hostname(region_server_t region_server)
{
	return region_server->server_name.kreon_ds_hostname;
}

uint64_t RS_get_server_clock(region_server_t region_server)
{
	return region_server->server_name.epoch;
}

region_server_iterator_t RS_create_region_server_iterator(region_server_t region_server)
{
	region_server_iterator_t iterator = calloc(1UL, sizeof(*iterator));
	iterator->region_server = region_server;
	iterator->position = -1;
	return iterator;
}

region_info_t RS_get_next_region_info(region_server_iterator_t iterator)
{
	if (++iterator->position >= iterator->region_server->num_of_regions)
		return NULL;

	return &iterator->region_server->regions[iterator->position];
}
region_t RS_get_region(region_info_t region_info)
{
	return region_info->region;
}

enum server_role RS_get_role(region_info_t region_info)
{
	return region_info->role;
}

void RS_close_region_server_iterator(region_server_iterator_t iterator)
{
	free(iterator);
}

void RS_set_region_server_status(region_server_t region_server, enum region_server_status status)
{
	region_server->status = status;
}
