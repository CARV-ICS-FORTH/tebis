// Copyright [2023] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "mregion_server.h"
#include "../metadata.h"
#include "mregion.h"
#include <log.h>
#include <stdlib.h>
#include <unistd.h>

#define SERVER_REGIONS_LIST_CAPACITY (128)

struct mregion_server {
	struct krm_server_name server_name;
	enum region_server_status status;
	int num_of_regions;
	int capacity;
	region_info_t regions;
};

struct region_info {
	mregion_t region;
	enum server_role role;
};

struct region_server_iterator {
	mregion_server_t region_server;
	region_info_t region_info;
	int position;
};

struct krm_server_name *RS_get_region_server_krm_hostname(mregion_server_t region_server)
{
	return &region_server->server_name;
}

mregion_server_t RS_create_region_server(struct krm_server_name server_name, enum region_server_status status)
{
	mregion_server_t region_server = calloc(1UL, sizeof(*region_server));
	region_server->server_name = server_name;
	region_server->status = status;
	region_server->capacity = SERVER_REGIONS_LIST_CAPACITY;
	region_server->regions = calloc(region_server->capacity, sizeof(struct region_info));
	return region_server;
}

void RS_add_region_in_server(mregion_server_t region_server, mregion_t region, enum server_role role)
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

void RS_destroy_region_server(mregion_server_t region_server)
{
	free(region_server->regions);
	free(region_server);
}

char *get_server_krm_hostname(mregion_server_t region_server)
{
	return region_server->server_name.kreon_ds_hostname;
}

uint64_t RS_get_server_clock(mregion_server_t region_server)
{
	return region_server->server_name.epoch;
}

region_server_iterator_t RS_create_region_server_iterator(mregion_server_t region_server)
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
mregion_t RS_get_region(region_info_t region_info)
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

void RS_set_region_server_status(mregion_server_t region_server, enum region_server_status status)
{
	region_server->status = status;
}

enum region_server_status RS_get_region_server_status(mregion_server_t region_server)
{
	return region_server->status;
}
