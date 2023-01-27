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
#ifndef REGION_DESC_H
#define REGION_DESC_H
#include "master/mregion.h"
#include "metadata.h"
#include "server_communication.h"
#include <infiniband/verbs.h>
#include <parallax/structures.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
struct rdma_cm_id;
struct work_task;
typedef struct region_desc *region_desc_t;

region_desc_t region_desc_create(mregion_t mregion, enum server_role server_role);

void region_desc_open_parallax_db(region_desc_t region_desc, const char *error_message);

char *region_desc_get_id(region_desc_t region_desc);

char *region_desc_get_max_key(region_desc_t region_desc);

uint32_t region_desc_get_max_key_size(region_desc_t region_desc);

char *region_desc_get_min_key(region_desc_t region_desc);

uint32_t region_desc_get_min_key_size(region_desc_t region_desc);

int64_t region_desc_get_pending_tasks(region_desc_t region_desc);

par_handle *region_desc_get_db(region_desc_t region_desc);
void region_desc_set_db(region_desc_t region_desc, par_handle *parallax_db);

struct ru_replica_state *region_desc_get_replica_state(region_desc_t region_desc);
void region_desc_init_replica_state(region_desc_t region_desc, size_t size, struct rdma_cm_id *rdma_cm_id);

uint32_t region_desc_get_num_backup(region_desc_t region_desc);

char *region_desc_get_backup_hostname(region_desc_t region_desc, int backup_id);

struct sc_msg_pair *region_desc_get_msg_pair(region_desc_t region_desc, int backup_id, int level_id);
void region_desc_set_msg_pair(region_desc_t region_desc, struct sc_msg_pair msg_pair, int backup_id, int level_id);

uint64_t region_desc_get_uuid(region_desc_t region_desc);

struct ibv_mr region_desc_get_remote_buf(region_desc_t region_desc, int backup_id, int level_id);
void region_desc_set_remote_buf(region_desc_t region_desc, int backup_id, struct ibv_mr remote_memory, int level_id);

struct ibv_mr *region_desc_get_primary_local_rdma_buffer(region_desc_t region_desc, uint8_t level_id);
void region_desc_set_primary_local_rdma_buffer(region_desc_t region_desc, uint8_t level_id,
					       struct ibv_mr *local_buffer);
int region_desc_enter_parallax(region_desc_t region_desc, struct work_task *task);
void region_desc_leave_parallax(region_desc_t region_desc);

enum ru_remote_buffer_status region_desc_get_primary2backup_buffer_stat(region_desc_t region_desc, int backup_id);
void region_desc_set_primary2backup_buffer_stat(region_desc_t region_desc, int backup_id,
						enum ru_remote_buffer_status status);

void region_desc_set_primary2backup_msg_pair(region_desc_t region_desc, int backup_id, struct sc_msg_pair msg_pair);
struct sc_msg_pair *region_desc_get_primary2backup_msg_pair(region_desc_t region_desc, int backup_id);

struct ru_master_log_buffer *region_desc_get_primary_L0_log_buf(region_desc_t region_desc);
struct ru_master_log_buffer *region_desc_get_primary_big_log_buf(region_desc_t region_desc);

enum krm_replica_buf_status region_desc_get_replicas_buf_statius(region_desc_t region_desc);
void region_desc_set_replicas_buf_statius(region_desc_t region_desc, enum krm_replica_buf_status status);

bool region_desc_init_master_state(region_desc_t region_desc);
void region_desc_lock_region_mngmt(region_desc_t region_desc);
void region_desc_unlock_region_mngmt(region_desc_t region_desc);

enum ru_remote_buffer_status region_desc_get_flush_cmd_status(region_desc_t region_desc, int backup_id);
void region_desc_set_flush_cmd_status(region_desc_t region_desc, enum ru_remote_buffer_status status, int backup_id);

void region_desc_set_flush_msg_pair(region_desc_t region_desc, int backup_id, struct sc_msg_pair msg_pair);
struct sc_msg_pair *region_desc_get_flush_msg_pair(region_desc_t region_desc, int backup_id);
int64_t region_desc_get_next_lsn(region_desc_t region_desc);
void region_desc_increase_lsn(region_desc_t region_desc);

void region_desc_zero_rdma_buffer(region_desc_t region_desc, enum log_category log_type);

void region_desc_add_segment_into_HT(region_desc_t region_desc, uint64_t primary_segment_offt,
				     uint64_t replica_segment_offt);

int8_t region_desc_is_segment_in_HT_mappings(region_desc_t region_desc, uint64_t primary_segment_offt);

struct sc_msg_pair region_desc_get_flush_index_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id,
								uint8_t level_id, uint8_t clock_dimension);

void region_desc_set_flush_index_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id, uint8_t level_id,
						  uint8_t clock_dimension, struct sc_msg_pair msg_pair);

void region_desc_free_flush_index_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id, uint8_t level_id,
						   uint8_t clock_dimension);
mregion_t region_desc_get_mregion(region_desc_t region_desc);

char *region_desc_get_backup_IP(region_desc_t region_desc, int backup_id);

struct ru_replica_rdma_buffer *region_desc_get_backup_L0_log_buf(region_desc_t region_desc);

struct ru_replica_rdma_buffer *region_desc_get_backup_big_log_buf(region_desc_t region_desc);

#endif
