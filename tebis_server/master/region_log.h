#ifndef REGION_LOG_H
#define REGION_LOG_H
#include "../metadata.h"
#include "command.h"
#include "mregion_server.h"
#include <zookeeper/zookeeper.h>

typedef struct region_log *region_log_t;

extern bool append_req_to_region_log(region_log_t region_log, MC_command_t command);
extern region_log_t create_region_log(char *path, unsigned int path_len, zhandle_t *zhandle);
extern void replay_region_log(region_log_t region_log);
#endif
