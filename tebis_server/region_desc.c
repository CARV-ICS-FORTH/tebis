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
#include "region_desc.h"
#include "../tebis_rdma/rdma.h"
#include "allocator/log_structures.h"
#include "configurables.h"
#include "master/mregion.h"
#include "metadata.h"
#include "region_server.h"
#include "send_index/send_index_callbacks.h"
#include "server_communication.h"
#include "work_task.h"
#include <allocator/persistent_operations.h>
#include <assert.h>
#include <btree/btree.h>
#include <btree/conf.h>
#include <log.h>
#include <parallax/structures.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uthash.h>
// IWYU pragma: no_forward_declare rdma_cm_id

#define SEGMENT_START(x) (x - (x % SEGMENT_SIZE))

struct region_desc {
	pthread_mutex_t region_mgmnt_lock;
	pthread_mutex_t medium_log_managment;
	pthread_mutex_t send_medium_log_chunk;
	pthread_rwlock_t kreon_lock;

	mregion_t mregion;
	struct region_desc_msg_state *msg_state;
	pthread_mutex_t msg_state_lock;
	pthread_rwlock_t replica_log_map_lock;
	struct region_desc_map_entry *replica_log_map;
	struct region_desc_map_entry *replica_index_map[MAX_LEVELS];

//Primary to Backup compaction flush_index_segment
#define CLOCK_DIMENTIONS 2
	struct sc_msg_pair send_index_flush_index_segment_rpc[KRM_MAX_BACKUPS][MAX_LEVELS][CLOCK_DIMENTIONS];
	bool send_index_flush_index_segment_rpc_in_use[KRM_MAX_BACKUPS][MAX_LEVELS][CLOCK_DIMENTIONS];
	struct sc_msg_pair send_index_flush_medium_log_segment_rpc[KRM_MAX_BACKUPS][LOG_TAIL_NUM_BUFS];
	bool send_index_flush_medium_log_segment_in_use[KRM_MAX_BACKUPS][LOG_TAIL_NUM_BUFS];
	struct ibv_mr remote_mem_buf[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct ibv_mr *local_buffer[MAX_LEVELS];
	struct ibv_mr *medium_log_buffer[LOG_TAIL_NUM_BUFS];
	// struct sc_msg_pair rpc[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct rdma_message_context rpc_ctx[KRM_MAX_BACKUPS][MAX_LEVELS];
	// uint8_t rpc_in_use[KRM_MAX_BACKUPS][MAX_LEVELS];
	//staff for deserializing the index at the replicas
	// struct di_buffer *index_buffer[MAX_LEVELS][MAX_HEIGHT];
	enum server_role role;
	par_handle *db;
	union {
		struct ru_master_state *m_state;
		struct ru_replica_state *r_state;
	};
	volatile int64_t pending_region_tasks;
	int64_t next_lsn_to_be_replicated;
	enum krm_replica_buf_status replica_buf_status;
	enum krm_region_status status;
};

static const char *region_desc_role_to_string(region_desc_t region_desc)
{
	static const char *region_desc_roles[ROLE_NUM] = { "FAULTY_ROLE",    "PRIMARY",	      "PRIMARY_NEWBIE",
							   "PRIMARY_INFANT", "PRIMARY_DEAD",  "BACKUP",
							   "BACKUP_NEWBIE",  "BACKUP_INFANT", "BACKUP_DEAD" };
	return region_desc_roles[region_desc->role];
}

static bool region_desc_is_primary(region_desc_t region_desc)
{
	if (region_desc->role == PRIMARY_INFANT || region_desc->role == PRIMARY_DEAD ||
	    region_desc->role == PRIMARY_NEWBIE || region_desc->role == PRIMARY)
		return true;
	if (region_desc->role == BACKUP_INFANT || region_desc->role == BACKUP_DEAD ||
	    region_desc->role == BACKUP_NEWBIE || region_desc->role == BACKUP)
		return false;
	log_fatal("Corrupted role in region");
	_exit(EXIT_FAILURE);
}

par_handle *region_desc_get_db(region_desc_t region_desc)
{
	return region_desc->db;
}

region_desc_t region_desc_create(mregion_t mregion, enum server_role server_role)
{
	region_desc_t region_desc = calloc(1UL, sizeof(struct region_desc));
	char *buffer = calloc(1UL, MREG_get_region_size());
	MREG_serialize_region(mregion, buffer, MREG_get_region_size());
	region_desc->mregion = (mregion_t)buffer;
	region_desc->role = server_role;

	region_desc->replica_buf_status = KRM_BUFS_UNINITIALIZED;
	MUTEX_INIT(&region_desc->region_mgmnt_lock, NULL);
	MUTEX_INIT(&region_desc->medium_log_managment, NULL);
	MUTEX_INIT(&region_desc->send_medium_log_chunk, NULL);
	region_desc->pending_region_tasks = 0;
	if (RWLOCK_INIT(&region_desc->kreon_lock, NULL) != 0) {
		log_fatal("Failed to init region read write lock");
		_exit(EXIT_FAILURE);
	}

	if (RWLOCK_INIT(&region_desc->replica_log_map_lock, NULL) != 0) {
		log_fatal("Failed to init replica log map lock");
		_exit(EXIT_FAILURE);
	}

	region_desc->status = KRM_OPEN;
	/*We have performed calloc so the code below is useless*/
	region_desc->replica_log_map = NULL;
	if (region_desc_get_num_backup(region_desc))
		region_desc->m_state = calloc(1UL, sizeof(struct ru_master_state));
	return region_desc;
}

char *region_desc_get_id(region_desc_t region_desc)
{
	return MREG_get_region_id(region_desc->mregion);
}

int64_t region_desc_get_pending_tasks(region_desc_t region_desc)
{
	return region_desc->pending_region_tasks;
}

par_handle region_get_db(region_desc_t region_desc)
{
	return region_desc->db;
}

static struct ru_replica_rdma_buffer region_desc_initialize_rdma_buffer(uint32_t size, struct rdma_cm_id *rdma_cm_id)
{
	void *addr = NULL;
	if (posix_memalign(&addr, ALIGNMENT, size)) {
		log_fatal("Failed to allocate aligned RDMA buffer");
		perror("Reason\n");
		_exit(EXIT_FAILURE);
	}
	/*Zero RDMA buffer*/
	memset(addr, 0, size);
	/*initialize the replicas rdma buffer*/
	struct ru_replica_rdma_buffer rdma_buf = { .rdma_buf_size = size,
						   .mr = rdma_reg_write(rdma_cm_id, addr, size) };
	if (NULL == rdma_buf.mr) {
		log_fatal("Failed to register memory");
		perror("Reason");
		_exit(EXIT_FAILURE);
	}

	return rdma_buf;
}

void region_desc_init_replica_state(region_desc_t region_desc, size_t size, struct rdma_cm_id *rdma_cm_id)
{
	if (region_desc_is_primary(region_desc)) {
		log_fatal("Region's role is %s not a backup!", region_desc_role_to_string(region_desc));
		_exit(EXIT_FAILURE);
	}

	region_desc->r_state = calloc(1UL, sizeof(struct ru_replica_state));
	region_desc->r_state->l0_recovery_rdma_buf = region_desc_initialize_rdma_buffer(size, rdma_cm_id);
	region_desc->r_state->medium_recovery_rdma_buf = region_desc_initialize_rdma_buffer(size, rdma_cm_id);
	region_desc->r_state->big_recovery_rdma_buf = region_desc_initialize_rdma_buffer(size, rdma_cm_id);
}

struct ru_replica_state *region_desc_get_replica_state(region_desc_t region_desc)
{
	if (region_desc_is_primary(region_desc)) {
		log_fatal("Region's role is %s not a backup!", region_desc_role_to_string(region_desc));
		_exit(EXIT_FAILURE);
	}
	return region_desc->r_state;
}

char *region_desc_get_max_key(region_desc_t region_desc)
{
	return MREG_get_region_max_key(region_desc->mregion);
}

uint32_t region_desc_get_max_key_size(region_desc_t region_desc)
{
	return MREG_get_region_max_key_size(region_desc->mregion);
}

char *region_desc_get_min_key(region_desc_t region_desc)
{
	return MREG_get_region_min_key(region_desc->mregion);
}

uint32_t region_desc_get_min_key_size(region_desc_t region_desc)
{
	return MREG_get_region_min_key_size(region_desc->mregion);
}

uint32_t region_desc_get_num_backup(region_desc_t region_desc)
{
	return MREG_get_region_num_of_backups(region_desc->mregion);
}

char *region_desc_get_backup_hostname(region_desc_t region_desc, int backup_id)
{
	return MREG_get_region_backup(region_desc->mregion, backup_id);
}

char *region_desc_get_backup_IP(region_desc_t region_desc, int backup_id)
{
	return MREG_get_region_backup_IP(region_desc->mregion, backup_id);
}

// struct sc_msg_pair *region_desc_get_msg_pair(region_desc_t region_desc, int backup_id, int level_id)
// {
// 	return &region_desc->rpc[backup_id][level_id];
// }

// void region_desc_set_msg_pair(region_desc_t region_desc, struct sc_msg_pair msg_pair, int backup_id, int level_id)
// {
// 	region_desc->rpc[backup_id][level_id] = msg_pair;
// }

uint64_t region_desc_get_uuid(region_desc_t region_desc)
{
	return (uint64_t)region_desc->mregion;
}

void region_desc_set_remote_buf(region_desc_t region_desc, int backup_id, struct ibv_mr remote_memory, int level_id)
{
	region_desc->remote_mem_buf[backup_id][level_id] = remote_memory;
}

struct ibv_mr region_desc_get_remote_buf(region_desc_t region_desc, int backup_id, int level_id)
{
	return region_desc->remote_mem_buf[backup_id][level_id];
}

void region_desc_set_primary_local_rdma_buffer(region_desc_t region_desc, uint8_t level_id, struct ibv_mr *local_buffer)
{
	region_desc->local_buffer[level_id] = local_buffer;
}

struct ibv_mr *region_desc_get_primary_local_rdma_buffer(region_desc_t region_desc, uint8_t level_id)
{
	return region_desc->local_buffer[level_id];
}

int region_desc_enter_parallax(region_desc_t region_desc, struct work_task *task)
{
	if (region_desc == NULL) {
		log_fatal("NULL region?");
		_exit(EXIT_FAILURE);
	}
	if (region_desc_get_num_backup(region_desc) == 0)
		return 1;

	pthread_rwlock_rdlock(&region_desc->kreon_lock);
	if (region_desc->status == KRM_HALTED) {
		int ret;
		switch (task->kreon_operation_status) {
		case TASK_GET_KEY:
		case TASK_MULTIGET:
		case TASK_DELETE_KEY:
		case INS_TO_KREON:
			// log_info("Do not enter Kreon task status is %d region status =
			// %d",task->kreon_operation_status,r_desc->status);
			ret = 0;
			break;
		case REPLICATE:
		case WAIT_FOR_REPLICATION_COMPLETION:
		case ALL_REPLICAS_ACKED:
		case SEND_FLUSH_COMMANDS:
		case WAIT_FOR_FLUSH_REPLIES:
			ret = 1;
			break;
		default:
			log_fatal("Unhandled state");
			_exit(EXIT_FAILURE);
		}
		pthread_rwlock_unlock(&region_desc->kreon_lock);
		return ret;
	}
	switch (task->kreon_operation_status) {
	case TASK_GET_KEY:
	case TASK_MULTIGET:
	case TASK_DELETE_KEY:
	case INS_TO_KREON:
		__sync_fetch_and_add(&region_desc->pending_region_tasks, 1);
		break;
	case REPLICATE:
	case WAIT_FOR_REPLICATION_TURN:
	case WAIT_FOR_REPLICATION_COMPLETION:
	case ALL_REPLICAS_ACKED:
	case SEND_FLUSH_COMMANDS:
	case WAIT_FOR_FLUSH_REPLIES:
		break;
	default:
		log_fatal("Unhandled state: %d", task->kreon_operation_status);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	pthread_rwlock_unlock(&region_desc->kreon_lock);
	return 1;
}

void region_desc_leave_parallax(region_desc_t region_desc)
{
	if (region_desc_get_num_backup(region_desc) == 0)
		return;
	__sync_fetch_and_sub(&region_desc->pending_region_tasks, 1);
}

void region_desc_set_db(region_desc_t region_desc, par_handle *parallax_db)
{
	region_desc->db = parallax_db;
}

enum ru_remote_buffer_status region_desc_get_primary2backup_buffer_stat(region_desc_t region_desc, int backup_id)
{
	return region_desc->m_state->primary_to_backup[backup_id].stat;
}

void region_desc_set_primary2backup_buffer_stat(region_desc_t region_desc, int backup_id,
						enum ru_remote_buffer_status status)
{
	region_desc->m_state->primary_to_backup[backup_id].stat = status;
}

void region_desc_set_primary2backup_msg_pair(region_desc_t region_desc, int backup_id, struct sc_msg_pair msg_pair)
{
	region_desc->m_state->primary_to_backup[backup_id].msg_pair = msg_pair;
}

struct sc_msg_pair *region_desc_get_primary2backup_msg_pair(region_desc_t region_desc, int backup_id)
{
	return &region_desc->m_state->primary_to_backup[backup_id].msg_pair;
}

struct ru_master_log_buffer *region_desc_get_primary_L0_log_buf(region_desc_t region_desc)
{
	if (!region_desc_is_primary(region_desc)) {
		log_fatal("Region must have primary role not a backup!");
		_exit(EXIT_FAILURE);
	}
	return &region_desc->m_state->l0_recovery_rdma_buf;
}

struct ru_master_log_buffer *region_desc_get_primary_medium_log_buf(region_desc_t region_desc)
{
	if (!region_desc_is_primary(region_desc)) {
		log_fatal("Region must have primary role not a backup!");
		_exit(EXIT_FAILURE);
	}
	return &region_desc->m_state->medium_recovery_rdma_buf;
}

struct ru_master_log_buffer *region_desc_get_primary_big_log_buf(region_desc_t region_desc)
{
	if (!region_desc_is_primary(region_desc)) {
		log_fatal("Region must have a primary role not a backup!");
		_exit(EXIT_FAILURE);
	}
	return &region_desc->m_state->big_recovery_rdma_buf;
}

struct ru_replica_rdma_buffer *region_desc_get_backup_L0_log_buf(region_desc_t region_desc)
{
	if (region_desc_is_primary(region_desc)) {
		log_fatal("This is a primary region not a backup!");
		_exit(EXIT_FAILURE);
	}
	return &region_desc->r_state->big_recovery_rdma_buf;
}

struct ru_replica_rdma_buffer *region_desc_get_backup_medium_log_buf(region_desc_t region_desc)
{
	if (region_desc_is_primary(region_desc)) {
		log_fatal("This is a primary region not a backup!");
		_exit(EXIT_FAILURE);
	}
	return &region_desc->r_state->medium_recovery_rdma_buf;
}

struct ru_replica_rdma_buffer *region_desc_get_backup_big_log_buf(region_desc_t region_desc)
{
	if (region_desc_is_primary(region_desc)) {
		log_fatal("This is a primary region not a backup!");
		_exit(EXIT_FAILURE);
	}
	return &region_desc->r_state->big_recovery_rdma_buf;
}

enum krm_replica_buf_status region_desc_get_replicas_buf_statius(region_desc_t region_desc)
{
	return region_desc->replica_buf_status;
}

void region_desc_set_replicas_buf_statius(region_desc_t region_desc, enum krm_replica_buf_status status)
{
	region_desc->replica_buf_status = status;
}

bool region_desc_init_master_state(region_desc_t region_desc)
{
	if (region_desc->m_state == NULL) {
		region_desc->m_state = (struct ru_master_state *)calloc(1, sizeof(struct ru_master_state));
		return true;
	}
	return false;
}
void region_desc_lock_region_mngmt(region_desc_t region_desc)
{
	MUTEX_LOCK(&region_desc->region_mgmnt_lock);
}

void region_desc_unlock_region_mngmt(region_desc_t region_desc)
{
	MUTEX_UNLOCK(&region_desc->region_mgmnt_lock);
}

void region_desc_increase_lsn(region_desc_t region_desc)
{
	__sync_fetch_and_add(&region_desc->next_lsn_to_be_replicated, 1);
}

int64_t region_desc_get_next_lsn(region_desc_t region_desc)
{
	return region_desc->next_lsn_to_be_replicated;
}

/*zero both RDMA buffers*/
void region_desc_zero_rdma_buffer(region_desc_t region_desc, enum log_category log_type)
{
	if (log_type == L0_RECOVERY)
		memset(region_desc->r_state->l0_recovery_rdma_buf.mr->addr, 0x00,
		       region_desc->r_state->l0_recovery_rdma_buf.rdma_buf_size);
	else
		memset(region_desc->r_state->big_recovery_rdma_buf.mr->addr, 0x00,
		       region_desc->r_state->big_recovery_rdma_buf.rdma_buf_size);
}

void region_desc_set_flush_msg_pair(region_desc_t region_desc, int backup_id, struct sc_msg_pair msg_pair)
{
	region_desc->m_state->flush_cmd[backup_id].msg_pair = msg_pair;
}

struct sc_msg_pair *region_desc_get_flush_msg_pair(region_desc_t region_desc, int backup_id)
{
	return &region_desc->m_state->flush_cmd[backup_id].msg_pair;
}

enum ru_remote_buffer_status region_desc_get_flush_cmd_status(region_desc_t region_desc, int backup_id)
{
	return region_desc->m_state->flush_cmd[backup_id].stat;
}

void region_desc_set_flush_cmd_status(region_desc_t region_desc, enum ru_remote_buffer_status status, int backup_id)
{
	region_desc->m_state->flush_cmd[backup_id].stat = status;
}

struct sc_msg_pair region_desc_get_flush_index_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id,
								uint8_t level_id, uint8_t clock_dimension)
{
	return region_desc->send_index_flush_index_segment_rpc[backup_id][level_id][clock_dimension];
}

void region_desc_set_flush_index_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id, uint8_t level_id,
						  uint8_t clock_dimension, struct sc_msg_pair msg_pair)
{
	if (region_desc->send_index_flush_index_segment_rpc_in_use[backup_id][level_id][clock_dimension]) {
		log_fatal("Pair already in use");
		_exit(EXIT_FAILURE);
	}
	region_desc->send_index_flush_index_segment_rpc[backup_id][level_id][clock_dimension] = msg_pair;
	region_desc->send_index_flush_index_segment_rpc_in_use[backup_id][level_id][clock_dimension] = true;
}

void region_desc_set_flush_medium_log_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id, uint32_t tail_id,
						       struct sc_msg_pair msg_pair)
{
	if (region_desc->send_index_flush_medium_log_segment_in_use[backup_id][tail_id]) {
		log_fatal("Opa re bro poy pas");
		_exit(EXIT_FAILURE);
	}
	region_desc->send_index_flush_medium_log_segment_rpc[backup_id][tail_id] = msg_pair;
	region_desc->send_index_flush_medium_log_segment_in_use[backup_id][tail_id] = true;
}

struct sc_msg_pair *region_desc_get_flush_medium_log_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id,
								      uint32_t tail_id)
{
	return &region_desc->send_index_flush_medium_log_segment_rpc[backup_id][tail_id];
}

void region_desc_free_flush_medium_log_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id, uint32_t tail_id)
{
	if (!region_desc->send_index_flush_medium_log_segment_in_use[backup_id][tail_id]) {
		log_fatal("Opa re bro poy pas");
		_exit(EXIT_FAILURE);
	}
	sc_free_rpc_pair(&region_desc->send_index_flush_medium_log_segment_rpc[backup_id][tail_id]);
	region_desc->send_index_flush_medium_log_segment_in_use[backup_id][tail_id] = false;
}

void region_desc_free_flush_index_segment_msg_pair(region_desc_t region_desc, uint32_t backup_id, uint8_t level_id,
						   uint8_t clock_dimension)
{
	if (!region_desc->send_index_flush_index_segment_rpc_in_use[backup_id][level_id][clock_dimension]) {
		log_fatal("Pair not in use and you want to free it? Come on mf");
		_exit(EXIT_FAILURE);
	}
	sc_free_rpc_pair(&region_desc->send_index_flush_index_segment_rpc[backup_id][level_id][clock_dimension]);
	region_desc->send_index_flush_index_segment_rpc_in_use[backup_id][level_id][clock_dimension] = false;
}

mregion_t region_desc_get_mregion(region_desc_t region_desc)
{
	return region_desc->mregion;
}

struct region_desc_map_entry {
	uint64_t primary_segment_offt;
	uint64_t replica_segment_offt;
	UT_hash_handle hh;
};

bool region_desc_is_segment_in_logmap(region_desc_t region_desc, uint64_t primary_segment_offt)
{
	struct region_desc_map_entry *index_entry;

	pthread_rwlock_rdlock(&region_desc->replica_log_map_lock);
	HASH_FIND_PTR(region_desc->replica_log_map, &primary_segment_offt, index_entry);
	pthread_rwlock_unlock(&region_desc->replica_log_map_lock);

	return index_entry ? true : false;
}

void region_desc_add_to_logmap(region_desc_t region_desc, uint64_t primary_segment_offt, uint64_t replica_segment_offt)
{
	//log_debug("Inserting primary seg offt %lu replica seg offt %lu", primary_segment_offt, replica_segment_offt);
	struct region_desc_map_entry *entry = calloc(1UL, sizeof(struct region_desc_map_entry));
	entry->primary_segment_offt = primary_segment_offt;
	entry->replica_segment_offt = replica_segment_offt;
	pthread_rwlock_wrlock(&region_desc->replica_log_map_lock);
	HASH_ADD_PTR(region_desc->replica_log_map, primary_segment_offt, entry);
	pthread_rwlock_unlock(&region_desc->replica_log_map_lock);
}

uint64_t region_desc_get_logmap_seg(region_desc_t r_desc, uint64_t primary_segment_offt)
{
	struct region_desc_map_entry *index_entry;

	pthread_rwlock_rdlock(&r_desc->replica_log_map_lock);
	HASH_FIND_PTR(r_desc->replica_log_map, &primary_segment_offt, index_entry);
	pthread_rwlock_unlock(&r_desc->replica_log_map_lock);

	return index_entry ? index_entry->replica_segment_offt : 0;
}

uint64_t region_desc_get_medium_log_segment_offt(region_desc_t r_desc, uint64_t primary_segment_offt)
{
	assert(r_desc);
	MUTEX_LOCK(&r_desc->medium_log_managment);
	uint64_t primary_start_segment_offt = SEGMENT_START(primary_segment_offt);
	uint64_t replica_segment_offt = region_desc_get_logmap_seg(r_desc, primary_segment_offt);
	if (replica_segment_offt) {
		MUTEX_UNLOCK(&r_desc->medium_log_managment);
		return replica_segment_offt;
	}

	// allocate a segment and allocate it to the logmap HT
	//leaving allocation for for later
	struct db_handle *dbhandle = (struct db_handle *)region_desc_get_db(r_desc);
	replica_segment_offt = pr_allocate_segment_for_log(dbhandle->db_desc, &dbhandle->db_desc->medium_log, 1, 1);
	region_desc_add_to_logmap(r_desc, primary_start_segment_offt, replica_segment_offt);

	MUTEX_UNLOCK(&r_desc->medium_log_managment);
	return replica_segment_offt;
}

uint64_t region_desc_allocate_log_segment_offt(region_desc_t r_desc, uint64_t primary_segment_offt)
{
	assert(r_desc);
	MUTEX_LOCK(&r_desc->medium_log_managment);
	struct db_handle *dbhandle = (struct db_handle *)region_desc_get_db(r_desc);

	uint64_t replica_segment_offt =
		pr_allocate_segment_for_log(dbhandle->db_desc, &dbhandle->db_desc->medium_log, 1, 1);

	region_desc_add_to_logmap(r_desc, primary_segment_offt, replica_segment_offt);
	MUTEX_UNLOCK(&r_desc->medium_log_managment);
	return replica_segment_offt;
}

void region_desc_add_to_indexmap(region_desc_t region_desc, uint64_t primary_segment_offt,
				 uint64_t replica_segment_offt, uint32_t level_id)
{
	//log_debug("Inserting primary seg offt %lu replica seg offt %lu", primary_segment_offt, replica_segment_offt);
	struct region_desc_map_entry *entry = calloc(1UL, sizeof(struct region_desc_map_entry));
	entry->primary_segment_offt = primary_segment_offt;
	entry->replica_segment_offt = replica_segment_offt;
	HASH_ADD_PTR(region_desc->replica_index_map[level_id], primary_segment_offt, entry);
}

uint64_t region_desc_get_indexmap_seg(region_desc_t r_desc, uint64_t primary_segment_offt, uint32_t level_id)
{
	struct region_desc_map_entry *index_entry;

	HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment_offt, index_entry);

	return index_entry ? index_entry->replica_segment_offt : 0;
}

void region_desc_free_indexmap(region_desc_t r_desc, uint32_t level_id)
{
	uint32_t dst_level_id = level_id + 1;
	struct region_desc_map_entry *current_entry, *tmp;

	HASH_ITER(hh, r_desc->replica_index_map[dst_level_id], current_entry, tmp)
	{
		HASH_DEL(r_desc->replica_index_map[dst_level_id], current_entry);
		free(current_entry);
	}
}

void region_desc_register_medium_log_buffers(region_desc_t r_desc, struct connection_rdma *conn)
{
	assert(r_desc && conn);
	// TODO: develop a better way to find the protection domain
	struct db_handle *db = (struct db_handle *)region_desc_get_db(r_desc);
	struct log_buffer_iterator *iter = log_buffer_iterator_init(&db->db_desc->medium_log);
	uint32_t i = 0;
	while (log_buffer_iterator_is_valid(iter)) {
		char *medium_log_buf = log_buffer_iterator_get_buffer(iter);
		r_desc->medium_log_buffer[i] = rdma_reg_write(conn->rdma_cm_id, medium_log_buf, SEGMENT_SIZE);
		if (!r_desc->medium_log_buffer[i]) {
			log_fatal("Did not manage to reg write the medium log buffer");
			_exit(EXIT_FAILURE);
		}
		i++;
		log_buffer_iterator_next(iter);
	}
}

struct ibv_mr *region_desc_get_medium_log_buffer(region_desc_t r_desc, uint32_t tail_id)
{
	assert(r_desc);
	return r_desc->medium_log_buffer[tail_id];
}
