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
#ifndef REGION_LOG_H
#define REGION_LOG_H
#include "command.h"
#include <stdbool.h>
#include <zookeeper/zookeeper.h>

typedef struct region_log *region_log_t;

extern bool append_req_to_region_log(region_log_t region_log, MC_command_t command);
extern region_log_t create_region_log(char *path, unsigned int path_len, zhandle_t *zhandle);
extern void replay_region_log(region_log_t region_log);
#endif
