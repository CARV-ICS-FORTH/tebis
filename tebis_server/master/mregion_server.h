#ifndef MREGION_SERVER_H
#define MREGION_SERVER_H
#include "../metadata.h"
#include "region.h"
#include <stdbool.h>
#include <stdint.h>

enum region_server_status { ALIVE = 0, DEAD };

typedef struct region_info *region_info_t;
typedef struct region_server *region_server_t;
typedef struct region_server_iterator *region_server_iterator_t;

region_server_t RS_create_region_server(struct krm_server_name server_name, enum region_server_status status);
void RS_add_region_in_server(region_server_t region_server, region_t region, enum server_role role);
void RS_destroy_region_server(region_server_t region_server);

uint64_t RS_get_server_clock(region_server_t region_server);
struct krm_server_name *RS_get_region_server_krm_hostname(region_server_t region_server);
void RS_set_region_server_status(region_server_t region_server, enum region_server_status status);
enum region_server_status RS_get_region_server_status(region_server_t region_server);

region_server_iterator_t RS_create_region_server_iterator(region_server_t);
region_info_t RS_get_next_region_info(region_server_iterator_t iterator);
region_t RS_get_region(region_info_t region_info);
enum server_role RS_get_role(region_info_t region_info);
void RS_close_region_server_iterator(region_server_iterator_t iterator);

#endif
