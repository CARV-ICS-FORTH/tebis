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
#ifndef MREGION_SERVER_H
#define MREGION_SERVER_H
#include "../metadata.h"
#include "mregion.h"
#include <stdint.h>

enum region_server_status { ALIVE = 0, DEAD };

typedef struct region_info *region_info_t;
typedef struct mregion_server *mregion_server_t;
typedef struct region_server_iterator *region_server_iterator_t;

mregion_server_t RS_create_region_server(struct krm_server_name server_name, enum region_server_status status);
void RS_add_region_in_server(mregion_server_t region_server, mregion_t region, enum server_role role);
void RS_destroy_region_server(mregion_server_t region_server);

uint64_t RS_get_server_clock(mregion_server_t region_server);
struct krm_server_name *RS_get_region_server_krm_hostname(mregion_server_t region_server);
void RS_set_region_server_status(mregion_server_t region_server, enum region_server_status status);
enum region_server_status RS_get_region_server_status(mregion_server_t region_server);

region_server_iterator_t RS_create_region_server_iterator(mregion_server_t);
region_info_t RS_get_next_region_info(region_server_iterator_t iterator);
mregion_t RS_get_region(region_info_t region_info);
enum server_role RS_get_role(region_info_t region_info);
void RS_close_region_server_iterator(region_server_iterator_t iterator);

#endif
