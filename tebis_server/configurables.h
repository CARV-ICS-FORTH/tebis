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
#ifndef CONFIGURABLES_H
#define CONFIGURABLES_H
#define KRM_COMMAND_BUFFER_SIZE 1512
#define KRM_HOSTNAME_SIZE 128
#define IP_SIZE 4
#define KRM_MAX_REGIONS 1024
#define KRM_MAX_DS_REGIONS 512
#define KRM_MAX_KEY_SIZE 64
#define KRM_MAX_REGION_ID_SIZE 16
#define KRM_MAX_BACKUPS 4
#define KRM_MAX_RDMA_IP_SIZE 22
#define KRM_GUID "tebis-"
#define KRM_ROOT_PATH "/tebis"
#define KRM_SERVERS_PATH "/servers"
#define KRM_REGION_SERVERS_EPOCHS "/region_servers_epochs"
#define KRM_SLASH "/"
#define KRM_DASH ":"
#define KRM_LEADER_PATH "/leader"
#define KRM_REGION_LOG "/region_log"
#define KRM_REGION_LOG_PREFIX "/ts"
#define KRM_MAILBOX_PATH "/mailbox"
#define KRM_MAX_ZK_PATH_SIZE 128UL
#define KRM_MAIL_TITLE "/msg"
#define KRM_ALIVE_SERVERS_PATH "/alive_region_servers"
#define KRM_ALIVE_LEADER_PATH "/alive_leader"
#define KRM_REGIONS_PATH "/regions"
//new master staff
#define KRM_ELECTIONS_PATH "/elections"
#define KRM_LEADER_CLOCK "/clock"

#define RU_REGION_KEY_SIZE MSG_MAX_REGION_KEY_SIZE
#define RU_MAX_TREE_HEIGHT 12
#define RU_MAX_NUM_REPLICAS 4
#define MAX_REPLICA_GROUP_SIZE (RU_MAX_NUM_REPLICAS + 1)
#define MAX_SERVER_NAME (128)
#endif
