// Copyright [2020] [FORTH-ICS]
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
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <assert.h>
#include <emmintrin.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <pthread.h>

#include "btree.h"
#include "gc.h"
#include "segment_allocator.h"
#include "../allocator/dmap-ioctl.h"
#include "../scanner/scanner.h"
#include "../btree/stats.h"
#include "../btree/assertions.h"
#include "../btree/conf.h"
#include <log.h>

#define PREFIX_STATISTICS_NO
#define MIN(x, y) ((x > y) ? (y) : (x))

#define SYSTEM_NAME "kreon"

#define DEVICE_BLOCK_SIZE 4096
#define COULD_NOT_FIND_DB 0x02
#define PAGE_SIZE 4096
#define LEAF_ROOT_NODE_SPLITTED 0xFC

#define FAILURE 0

int32_t leaf_order = -1;
int32_t index_order = -1;
/*stats counters*/
extern uint64_t internal_tree_cow_for_leaf;
extern uint64_t internal_tree_cow_for_index;
extern uint64_t written_buffered_bytes;
extern char *pointer_to_kv_in_log;
extern volatile uint64_t snapshot_v1;
extern volatile uint64_t snapshot_v2;

extern unsigned long long ins_prefix_hit_l0;
extern unsigned long long ins_prefix_hit_l1;
extern unsigned long long ins_prefix_miss_l0;
extern unsigned long long ins_prefix_miss_l1;
extern unsigned long long ins_hack_hit;
extern unsigned long long ins_hack_miss;

pthread_mutex_t init_lock = PTHREAD_MUTEX_INITIALIZER;
/*number of locks per level*/
uint32_t size_per_height[MAX_HEIGHT] = { 8192, 4096, 2048, 1024, 512, 256, 128, 64, 32 };

static uint8_t _writers_join_as_readers(bt_insert_req *ins_req);
static uint8_t _concurrent_insert(bt_insert_req *ins_req);

// void assert_index_node(node_header *node);
#define BT_DELETE_MARKER_ID 0xFFFFFFFF
struct bt_delete_marker {
	uint32_t marker_id;
	uint32_t key_size;
	char key[];
};

void *bt_get_real_address(uint64_t dev_offt)
{
	return (void *)MAPPED + dev_offt;
}

struct bt_kv_log_address bt_get_kv_log_address(struct db_descriptor *db_desc, uint64_t dev_offt)
{
	struct bt_kv_log_address reply = { .addr = NULL, .tail_id = 0, .in_tail = UINT8_MAX };
	RWLOCK_RDLOCK(&db_desc->log_tail_buf_lock);

	for (int i = 0; i < LOG_TAIL_BUFS; ++i) {
		if (db_desc->log_tail_buf[i]->free)
			continue;

		if (dev_offt >= db_desc->log_tail_buf[i]->start && dev_offt <= db_desc->log_tail_buf[i]->end) {
			__sync_fetch_and_add(&db_desc->log_tail_buf[i]->pending_readers, 1);
			reply.in_tail = 1;
			// log_info("KV at tail! offt %llu in the device or %llu", dev_offt,
			// dev_offt % SEGMENT_SIZE);
			reply.addr = &db_desc->log_tail_buf[i]->buf[dev_offt % SEGMENT_SIZE];
			reply.tail_id = i;
			RWLOCK_UNLOCK(&db_desc->log_tail_buf_lock);
			return reply;
		}
		// log_info("KV NOT at tail %d! DB: %s offt %llu start %llu end %llu", i,
		// db_desc->db_name, dev_offt,
		//	 db_desc->log_tail_buf[i]->start, db_desc->log_tail_buf[i]->end);
	}

	reply.in_tail = 0;
	RWLOCK_UNLOCK(&db_desc->log_tail_buf_lock);
	reply.addr = (void *)(MAPPED + dev_offt);
	reply.tail_id = UINT8_MAX;
	return reply;
}

void bt_done_with_value_log_address(struct db_descriptor *db_desc, struct bt_kv_log_address *L)
{
	assert(db_desc->log_tail_buf[L->tail_id]->pending_readers > 0);
	__sync_fetch_and_sub(&db_desc->log_tail_buf[L->tail_id]->pending_readers, 1);
}

uint64_t bt_get_absolute_address(void *addr)
{
	return ((uint64_t)addr - MAPPED);
}

static inline void move_leaf_data(leaf_node *leaf, int32_t middle)
{
	char *src_addr, *dst_addr;
	const size_t nitems = leaf->header.numberOfEntriesInNode - middle;
	if (nitems == 0)
		return;

	src_addr = (char *)(&(leaf->kv_entry[middle]));
	dst_addr = src_addr + sizeof(struct leaf_kv_pointer);
	memmove(dst_addr, src_addr, nitems * sizeof(uint64_t));

	src_addr = (char *)(&(leaf->prefix[middle]));
	dst_addr = src_addr + PREFIX_SIZE;
	memmove(dst_addr, src_addr, nitems * PREFIX_SIZE);
}

#ifdef PREFIX_STATISTICS
static inline void update_leaf_index_stats(char key_format)
{
	if (key_format == KV_FORMAT)
		__sync_fetch_and_add(&ins_prefix_miss_l0, 1);
	else
		__sync_fetch_and_add(&ins_prefix_miss_l1, 1);
}
#endif

static bt_split_result split_index(node_header *node, bt_insert_req *ins_req);

// void bt_set_compaction_callback(struct db_descriptor *db_desc,
// bt_compaction_callback t)
//{
//	db_desc->t = t;
//	return;
//}

void set_init_index_transfer(struct db_descriptor *db_desc, init_index_transfer idx_init)
{
	db_desc->idx_init = idx_init;
	return;
}

void set_destroy_local_rdma_buffer(struct db_descriptor *db_desc, destroy_local_rdma_buffer destroy_rdma_buf)
{
	db_desc->destroy_rdma_buf = destroy_rdma_buf;
	return;
}

void set_send_index_segment_to_replicas(struct db_descriptor *db_desc, send_index_segment_to_replicas send_idx)
{
	db_desc->send_idx = send_idx;
	return;
}

void bt_set_flush_replicated_logs_callback(struct db_descriptor *db_desc, bt_flush_replicated_logs fl)
{
	db_desc->fl = fl;
	return;
}

void bt_inform_engine_for_pending_op_callback(struct db_descriptor *db_desc, bt_flush_replicated_logs fl);

#if DEBUG_BTREE
void assert_leaf_node(node_header *leaf);
#endif

int prefix_compare(char *l, char *r, size_t prefix_size)
{
	return memcmp(l, r, prefix_size);
}

void bt_set_db_in_replicated_mode(db_handle *handle)
{
	uint32_t old_val = handle->db_desc->is_in_replicated_mode;
	while (1) {
		if (__sync_bool_compare_and_swap(&handle->db_desc->is_in_replicated_mode, old_val, 1))
			break;
		old_val = handle->db_desc->is_in_replicated_mode;
	}
	return;
}

void bt_decrease_level0_writers(db_handle *handle)
{
	if (!handle->db_desc->is_in_replicated_mode) {
		log_fatal("DB %s is not in replicated mode you are not allowed to do that!", handle->db_desc->db_name);
		assert(0);
		exit(EXIT_FAILURE);
	}
	__sync_fetch_and_sub(&handle->db_desc->pending_replica_operations, 1);
}

/**
 * @param   index_key: address of the index_key
 * @param   index_key_len: length of the index_key in encoded form first 2
 * significant bytes row_key_size least 2 significant bytes quallifier size
 * @param   query_key: address of query_key
 * @param   query_key_len: query_key length again in encoded form
 */

int64_t bt_key_cmp(void *key1, void *key2, char key1_format, char key2_format)
{
	int64_t ret;
	uint32_t size;

	/*we need the left most entry*/
	if (key2 == NULL)
		return 1;

	struct kv_format *key1f = NULL;
	struct kv_format *key2f = NULL;
	struct kv_prefix *key1p = NULL;
	struct kv_prefix *key2p = NULL;

	if (key1_format == KV_FORMAT && key2_format == KV_FORMAT) {
		key1f = (struct kv_format *)key1;
		key1p = NULL;
		key2f = (struct kv_format *)key2;
		key2p = NULL;

		size = key1f->key_size;
		if (size > key2f->key_size)
			size = key2f->key_size;

		ret = memcmp(key1f->key_buf, key2f->key_buf, size);
		if (ret != 0)
			return ret;
		else {
			/*finally larger key wins*/
			if (key1f->key_size < key2f->key_size)
				return -1;
			else if (key1f->key_size > key2f->key_size)
				return 1;
			else
				/*equal*/
				return 0;
		}
	} else if (key1_format == KV_FORMAT && key2_format == KV_PREFIX) {
		key1f = (struct kv_format *)key1;
		key1p = NULL;
		key2f = NULL;
		key2p = (struct kv_prefix *)key2;

		if (key1f->key_size >= PREFIX_SIZE)
			ret = prefix_compare(key1f->key_buf, key2p->prefix, PREFIX_SIZE);
		else
			ret = prefix_compare(key1f->key_buf, key2p->prefix, key1f->key_size);
		if (ret == 0) {
			/*we have a tie, prefix didn't help, fetch query_key form KV log*/
			key2f = (struct kv_format *)bt_get_real_address(key2p->device_offt);
			key2p = NULL;

			size = key1f->key_size;
			if (size > key2f->key_size)
				size = key2f->key_size;

			ret = memcmp(key1f->key_buf, key2f->key_buf, size);

			if (ret != 0)
				return ret;
			else {
				/*finally larger key wins*/
				if (key1f->key_size < key2f->key_size)
					return -1;
				else if (key1f->key_size > key2f->key_size)
					return 1;
				else
					/*equal*/
					return 0;
			}
		} else
			return ret;
	} else if (key1_format == KV_PREFIX && key2_format == KV_FORMAT) {
		key1f = NULL;
		key1p = (struct kv_prefix *)key1;
		key2f = (struct kv_format *)key2;
		key2p = NULL;

		if (key2f->key_size >= PREFIX_SIZE)
			ret = prefix_compare(key1p->prefix, key2f->key_buf, PREFIX_SIZE);
		else // check here TODO
			ret = prefix_compare(key1p->prefix, key2f->key_buf, key2f->key_size);

		if (ret == 0) {
			/* we have a tie, prefix didn't help, fetch query_key form KV log*/
			key1f = (struct kv_format *)bt_get_real_address(key1p->device_offt);
			key1p = NULL;

			size = key2f->key_size;
			if (size > key1f->key_size)
				size = key1f->key_size;

			ret = memcmp(key1f->key_buf, key2f->key_buf, size);
			if (ret != 0)
				return ret;
			else {
				/*finally larger key wins*/
				if (key1f->key_size < key2f->key_size)
					return -1;
				else if (key1f->key_size > key2f->key_size)
					return 1;
				else
					/*equal*/
					return 0;
			}
		} else
			return ret;
	} else {
		/*KV_PREFIX and KV_PREFIX*/
		key1f = NULL;
		key1p = (struct kv_prefix *)key1;
		key2f = NULL;
		key2p = (struct kv_prefix *)key2;
		ret = prefix_compare(key1p->prefix, key2p->prefix, PREFIX_SIZE);
		if (ret != 0)
			return ret;
		/*full comparison*/
		key1f = (struct kv_format *)bt_get_real_address(key1p->device_offt);
		key1p = NULL;
		key2f = (struct kv_format *)bt_get_real_address(key2p->device_offt);
		key2p = NULL;

		size = key2f->key_size;
		if (size > key1f->key_size) {
			size = key1f->key_size;
		}

		ret = memcmp(key1f->key_buf, key2f->key_buf, size);
		if (ret != 0)
			return ret;
		/*finally larger key wins*/
		if (key1f->key_size < key2f->key_size)
			return -1;
		else if (key1f->key_size > key2f->key_size)
			return 1;
		else
			/*equal*/
			return 0;
	}
	return 0;
}

static void init_level_locktable(db_descriptor *database, uint8_t level_id)
{
	unsigned int i, j;
	lock_table *init;

	for (i = 0; i < MAX_HEIGHT; ++i) {
		if (posix_memalign((void **)&database->levels[level_id].level_lock_table[i], 4096,
				   sizeof(lock_table) * size_per_height[i]) != 0) {
			log_fatal("memalign failed");
			exit(EXIT_FAILURE);
		}
		init = database->levels[level_id].level_lock_table[i];

		for (j = 0; j < size_per_height[i]; ++j) {
			if (RWLOCK_INIT(&init[j].rx_lock, NULL) != 0) {
				log_fatal("failed to initialize lock_table for level %u lock", level_id);
				exit(EXIT_FAILURE);
			}
		}
	}
}

static void destroy_level_locktable(db_descriptor *database, uint8_t level_id)
{
	int i;
	// log_info("Destroying lock table for DB %s level_id %u", database->db_name,
	// level_id);
	for (i = 0; i < MAX_HEIGHT; ++i)
		free(database->levels[level_id].level_lock_table[i]);
}

static void bt_init_fresh_db(struct db_handle *hd, char *db_name, int group_id, int group_index, uint8_t init_kv_log)
{
	memset(hd->db_desc, 0x00, sizeof(db_descriptor));
	/*initialize database descriptor, soft state first*/
	hd->db_desc->ref_count = 0;
	hd->db_desc->group_id = group_id;
	hd->db_desc->group_index = group_index;

	strcpy(hd->db_desc->db_name, db_name);
	hd->db_desc->dirty = 0x01;
	for (int level_id = 0; level_id < MAX_LEVELS; level_id++) {
		for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; tree_id++) {
			hd->db_desc->levels[level_id].root_r[tree_id] = NULL;
			hd->db_desc->levels[level_id].root_w[tree_id] = NULL;
			hd->db_desc->levels[level_id].level_size[tree_id] = 0;
			hd->db_desc->levels[level_id].first_segment[tree_id] = NULL;
			hd->db_desc->levels[level_id].last_segment[tree_id] = NULL;
			hd->db_desc->levels[level_id].offset[tree_id] = 0;
		}
	}

	// db_desc->commit_log = (commit_log_info *)get_space_for_system(volume_desc,
	// sizeof(commit_log_info));
	if (!init_kv_log) {
		log_warn("Ommiting KV log initialization for DB %s", hd->db_desc->db_name);
		hd->db_desc->KV_log_first_segment = NULL;
		hd->db_desc->KV_log_last_segment = NULL;
		hd->db_desc->KV_log_size = 0;
		hd->db_desc->L1_index_end_log_offset = 0;
		hd->db_desc->L1_segment = NULL;
		// db_desc->commit_log->first_kv_log = NULL;
		// db_desc->commit_log->last_kv_log = NULL;
		// db_desc->commit_log->kv_log_size = 0;
	} else {
		log_info("Initializing KV log for DB %s", hd->db_desc->db_name);
		hd->db_desc->KV_log_first_segment = seg_get_raw_log_segment(hd->volume_desc);
		memset((void *)hd->db_desc->KV_log_first_segment->garbage_bytes, 0x00,
		       2 * MAX_COUNTER_VERSIONS * sizeof(uint64_t));
		hd->db_desc->KV_log_last_segment = hd->db_desc->KV_log_first_segment;
		hd->db_desc->KV_log_last_segment->segment_id = 0;
		hd->db_desc->KV_log_last_segment->next_segment = NULL;
		hd->db_desc->KV_log_last_segment->prev_segment = NULL;
		hd->db_desc->KV_log_size = sizeof(segment_header);
		hd->db_desc->L1_index_end_log_offset = sizeof(segment_header);
		hd->db_desc->L1_segment = hd->db_desc->KV_log_last_segment;
	}
}

static void bt_recover_db(struct db_handle *hd, struct pr_db_entry *db_entry, int group_id, int group_index)
{
	/*initialize database descriptor, soft state first*/
	hd->db_desc->ref_count = 0;
	hd->db_desc->group_id = group_id;
	hd->db_desc->group_index = group_index;
	/*restore db name, in memory*/
	strcpy(hd->db_desc->db_name, db_entry->db_name);
	hd->db_desc->dirty = 0;

	// Zero l0
	for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; tree_id++) {
		hd->db_desc->levels[0].first_segment[tree_id] = NULL;
		hd->db_desc->levels[0].last_segment[tree_id] = NULL;
		hd->db_desc->levels[0].offset[tree_id] = 0;
		hd->db_desc->levels[0].root_w[tree_id] = NULL;
		hd->db_desc->levels[0].root_r[tree_id] = NULL;
		hd->db_desc->levels[0].level_size[tree_id] = 0;
	}

	/*restore now all device levels*/
	for (int level_id = 1; level_id < MAX_LEVELS; level_id++) {
		for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; tree_id++) {
			/*segments info per level*/
			if (db_entry->first_segment[level_id][tree_id] != 0)
				hd->db_desc->levels[level_id].first_segment[tree_id] =
					(struct segment_header *)bt_get_real_address(
						db_entry->first_segment[level_id][tree_id]);
			else
				hd->db_desc->levels[level_id].first_segment[tree_id] = NULL;

			if (db_entry->last_segment[level_id][tree_id] != 0)
				hd->db_desc->levels[level_id].last_segment[tree_id] =
					(struct segment_header *)bt_get_real_address(
						db_entry->last_segment[level_id][tree_id]);
			else
				hd->db_desc->levels[level_id].last_segment[tree_id] = NULL;
			hd->db_desc->levels[level_id].offset[tree_id] = db_entry->offset[level_id][tree_id];

			/*total keys*/
			hd->db_desc->levels[level_id].level_size[tree_id] = db_entry->level_size[level_id][tree_id];
			/*finally the roots*/
			if (db_entry->root_r[level_id][tree_id] != 0) {
				hd->db_desc->levels[level_id].root_r[tree_id] =
					(node_header *)bt_get_real_address(db_entry->root_r[level_id][tree_id]);
				// log_warn("Recovered root_r of [%lu][%lu] = %llu of DB %s", level_id,
				// tree_id,
				//	 hd->db_desc->levels[level_id].root_r[tree_id],
				// hd->db_desc->db_name);
			} else {
				hd->db_desc->levels[level_id].root_r[tree_id] = NULL;
				// log_info("NULL root for[%u][%u]", level_id, tree_id);
			}

			hd->db_desc->levels[level_id].root_w[tree_id] = NULL;
		}
	}
	/*recover value log for this database*/
	if (db_entry->KV_log_first_seg_offt != 0)
		hd->db_desc->KV_log_first_segment =
			(struct segment_header *)bt_get_real_address(db_entry->KV_log_first_seg_offt);
	else
		hd->db_desc->KV_log_first_segment = NULL;

	if (db_entry->KV_log_last_seg_offt != 0)
		hd->db_desc->KV_log_last_segment =
			(struct segment_header *)bt_get_real_address(db_entry->KV_log_last_seg_offt);
	else
		hd->db_desc->KV_log_last_segment = NULL;

	hd->db_desc->KV_log_size = db_entry->KV_log_size;
	hd->db_desc->L1_index_end_log_offset = db_entry->L1_index_end_log_offset;
	if (db_entry->L1_segment_offt != 0)
		hd->db_desc->L1_segment = (struct segment_header *)bt_get_real_address(db_entry->L1_segment_offt);
	else
		hd->db_desc->L1_segment = NULL;

	log_info("DB: %s KV log status - First segment: %llu Last segment: %llu KV "
		 "log size %llu",
		 hd->db_desc->db_name, (LLU)hd->db_desc->KV_log_first_segment, (LLU)hd->db_desc->KV_log_last_segment,
		 (LLU)hd->db_desc->KV_log_size);
}

static void bt_recover_L0(struct db_handle *hd)
{
	if (hd->db_desc->KV_log_size <= hd->db_desc->L1_index_end_log_offset) {
		log_info("No recovery needed for DB %s", hd->db_desc->db_name);
		return;
	}
	log_info("Recovering L0 of DB %s ...", hd->db_desc->db_name);
	struct segment_header *curr = hd->db_desc->L1_segment;
	char *cursor = (char *)((uint64_t)curr + (hd->db_desc->L1_index_end_log_offset % SEGMENT_SIZE));
	uint64_t log_offset = hd->db_desc->L1_index_end_log_offset;
	log_info("L1 index ends at offset %llu value log is at %llu", hd->db_desc->L1_index_end_log_offset,
		 hd->db_desc->KV_log_size);
	while (log_offset < hd->db_desc->KV_log_size) {
		struct kv_prefix p;
		struct bt_insert_req ins_req;
		ins_req.metadata.handle = hd;

		ins_req.metadata.level_id = 0;
		ins_req.metadata.key_format = KV_PREFIX;
		ins_req.metadata.append_to_log = 0;
		ins_req.metadata.gc_request = 0;
		ins_req.metadata.recovery_request = 1;
		ins_req.metadata.special_split = 0;

		if (*(uint32_t *)cursor == BT_DELETE_MARKER_ID) {
			cursor += sizeof(uint32_t);
			log_offset += sizeof(uint32_t);
			ins_req.metadata.is_tombstone = 1;
			// log_info("Recovering a delete for DB: %s key is %u:%s",
			// hd->db_desc->db_name,
			//	 *(uint32_t *)cursor, cursor + 4);
		} else
			ins_req.metadata.is_tombstone = 0;

		if (*(uint32_t *)cursor < PREFIX_SIZE) {
			memset(p.prefix, 0x00, PREFIX_SIZE);
			memcpy(p.prefix, cursor + sizeof(uint32_t), *(uint32_t *)cursor);
		} else
			memcpy(p.prefix, cursor + sizeof(uint32_t), PREFIX_SIZE);

		//log_info("Recovering key %u:%s log offset at %llu end of log %llu",
		// *(uint32_t *)cursor, cursor + 4,
		//	 log_offset, hd->db_desc->KV_log_size);
		p.device_offt = bt_get_absolute_address(cursor);
		p.tombstone = 0;
		ins_req.key_value_buf = &p;

		_insert_key_value(&ins_req);
		uint32_t kv_size = *(uint32_t *)cursor + sizeof(uint32_t);
		cursor = cursor + kv_size;
		log_offset += kv_size;
		// assert(*(uint32_t *)cursor > 0 && *(uint32_t *)cursor < 1200);
		kv_size = (*(uint32_t *)cursor + sizeof(uint32_t));
		cursor = cursor + kv_size;
		log_offset += kv_size;
		uint32_t remaining;
		if (log_offset % SEGMENT_SIZE == 0)
			remaining = 0;
		else
			remaining = SEGMENT_SIZE - (log_offset % SEGMENT_SIZE);
		// log_info("Remaining are %u",remaining);
		if (remaining < sizeof(uint32_t) || *(uint32_t *)cursor == 0) {
			// time to change segment
			if (curr->next_segment == NULL)
				break;
			curr = (struct segment_header *)bt_get_real_address((uint64_t)curr->next_segment);
			log_offset += remaining;
			log_offset += sizeof(struct segment_header);
			cursor = (char *)((uint64_t)curr + (log_offset % SEGMENT_SIZE));
			// log_info("Changed segment!");
		}
	}
	// assert(log_offset == hd->db_desc->KV_log_size);
	log_info("Done recovering L0 of DB %s !", hd->db_desc->db_name);
}

static void bt_reclaim_db_space(struct db_descriptor *db_desc, struct volume_descriptor *volume_desc)
{
	for (int level_id = 1; level_id < MAX_LEVELS; level_id++) {
		if (db_desc->levels[level_id].first_segment[1] != NULL) {
			log_info("Reclaiming space from pending compactions for DB %s after an "
				 "unclean shutdown",
				 db_desc->db_name);
			struct segment_header *curr_segment = db_desc->levels[level_id].first_segment[1];
			while (curr_segment != NULL) {
				struct segment_header *next = NULL;
				if (curr_segment->next_segment != NULL)
					next = (struct segment_header *)bt_get_real_address(
						(uint64_t)curr_segment->next_segment);
				free_block(volume_desc, curr_segment, SEGMENT_SIZE);
				curr_segment = next;
			}
			db_desc->levels[level_id].first_segment[1] = NULL;
			db_desc->levels[level_id].last_segment[1] = NULL;
			db_desc->levels[level_id].offset[1] = 0;
			db_desc->levels[level_id].level_size[1] = 0;
			db_desc->levels[level_id].root_w[1] = NULL;
			db_desc->levels[level_id].root_r[1] = NULL;
		}
	}
}

static void bt_reclaim_volume_space(struct volume_descriptor *volume_desc)
{
	if (volume_desc->mem_catalogue == NULL) {
		log_fatal("Null mem_catalogue");
		exit(EXIT_FAILURE);
	}
	for (int i = 0; i < NUM_OF_DB_GROUPS; i++) {
		if (volume_desc->mem_catalogue->db_group_index[i] != 0) {
			struct pr_db_group *db_group = (struct pr_db_group *)bt_get_real_address(
				(uint64_t)volume_desc->mem_catalogue->db_group_index[i]);
			for (int j = 0; j < GROUP_SIZE; j++) {
				if (db_group->db_entries[j].valid) {
					/*hosts a database*/
					struct pr_db_entry *db_entry = &db_group->db_entries[j];
					log_info(" Recovering database: %s found at index [%d,%d]", db_entry->db_name,
						 i, j);
					struct db_descriptor db_desc;

					/*initialize database descriptor, soft state first*/
					db_desc.ref_count = 0;
					db_desc.group_id = i;
					db_desc.group_index = j;
					/*restore db name, in memory*/
					memset(db_desc.db_name, 0x00, MAX_DB_NAME_SIZE);
					strcpy(db_desc.db_name, db_entry->db_name);
					db_desc.dirty = 0;
					/*restore now persistent levels*/
					for (int level_id = 1; level_id < MAX_LEVELS; level_id++) {
						for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; tree_id++) {
							uint64_t s_first_offt =
								db_entry->first_segment[level_id][tree_id];
							uint64_t s_last_offt =
								db_entry->last_segment[level_id][tree_id];
							uint64_t offt = db_entry->offset[level_id][tree_id];

							if (s_first_offt != 0)
								db_desc.levels[level_id].first_segment[tree_id] =
									(struct segment_header *)bt_get_real_address(
										s_first_offt);
							else
								db_desc.levels[level_id].first_segment[tree_id] = NULL;

							if (s_last_offt != 0)
								db_desc.levels[level_id].last_segment[tree_id] =
									(struct segment_header *)bt_get_real_address(
										s_last_offt);
							else
								db_desc.levels[level_id].last_segment[tree_id] = NULL;
							db_desc.levels[level_id].offset[tree_id] = offt;

							/*total keys*/
							db_desc.levels[level_id].level_size[tree_id] =
								db_entry->level_size[level_id][tree_id];
							/*finally the roots*/
							if (db_entry->root_r[level_id][tree_id] != 0) {
								db_desc.levels[level_id].root_r[tree_id] =
									(node_header *)bt_get_real_address(
										db_entry->root_r[level_id][tree_id]);
							} else
								db_desc.levels[level_id].root_r[tree_id] = NULL;

							db_desc.levels[level_id].root_w[tree_id] = NULL;
						}
					}
					bt_reclaim_db_space(&db_desc, volume_desc);
				}
			}
		}
	}
}

/**
 * @param   blockSize
 * @param   db_name
 * @return  db_handle
 **/
db_handle *db_open(char *volumeName, uint64_t start, uint64_t size2, char *db_name, char CREATE_FLAG)
{
	(void)size2;
	db_handle *handle;
	uint8_t level_id, tree_id;

	fprintf(stderr, "\n%s[%s:%s:%d](\"%s\", %" PRIu64 ", %s);%s\n", "\033[0;32m", __FILE__, __func__, __LINE__,
		volumeName, start, db_name, "\033[0m");

	MUTEX_LOCK(&init_lock);
	if (leaf_order == -1) {
		/*calculate max leaf,index order*/
		leaf_order = (LEAF_NODE_SIZE - sizeof(node_header)) / (sizeof(uint64_t) + PREFIX_SIZE);
		while (leaf_order % 2 != 0)
			--leaf_order;
		index_order = (INDEX_NODE_SIZE - sizeof(node_header)) / (2 * sizeof(uint64_t));
		index_order -= 2; /*more space for extra pointer, and for rebalacing (merge)*/
		while (index_order % 2 != 1)
			--index_order;

		if ((LEAF_NODE_SIZE - sizeof(node_header)) % 8 != 0) {
			log_fatal("Misaligned node header for leaf nodes, scans will not work");
			exit(EXIT_FAILURE);
		}
		if ((INDEX_NODE_SIZE - sizeof(node_header)) % 16 != 0) {
			log_fatal("Misaligned node header for index nodes, scans will not work "
				  "size of node_header %ld",
				  sizeof(node_header));
			exit(EXIT_FAILURE);
		}
		log_info("index order set to: %d leaf order is set to %d sizeof "
			 "node_header = %lu",
			 index_order, leaf_order, sizeof(node_header));
	}

	struct volume_descriptor *volume_desc = get_volume_desc(volumeName, start, 0);
	if (volume_desc == NULL) {
		volume_desc = get_volume_desc(volumeName, start, 1);
		bt_reclaim_volume_space(volume_desc);
	}
	// Before searching the actual volume's catalogue take a look at the current
	// open databases
	struct db_descriptor *db_desc = klist_find_element_with_key(volume_desc->open_databases, db_name);
	if (db_desc != NULL) {
		log_info("DB %s already open in volume %s", db_name, volumeName);
		handle = calloc(1, sizeof(db_handle));
		handle->volume_desc = volume_desc;
		handle->db_desc = db_desc;
		db_desc->ref_count++;
		MUTEX_UNLOCK(&init_lock);
		return handle;
	} else {
		pr_db_group *db_group;
		pr_db_entry *db_entry;
		int32_t empty_group;
		int32_t empty_index;
		int32_t j;

		log_info("Searching volume's %s catalogue for db %s...", volume_desc->volume_name, db_name);
		empty_group = -1;
		empty_index = -1;
		// we are going to search system's catalogue to find the root_r of the
		// corresponding database
		for (int i = 0; i < NUM_OF_DB_GROUPS; i++) {
			/*is group empty?*/
			if (volume_desc->mem_catalogue->db_group_index[i] != 0) {
				db_group = (pr_db_group *)bt_get_real_address(
					(uint64_t)volume_desc->mem_catalogue->db_group_index[i]);
				for (j = 0; j < GROUP_SIZE; j++) {
					/*empty slot keep in mind*/
					if (db_group->db_entries[j].valid == 0 && empty_index == -1) {
						/*Remember the location of the first empty slot within the group*/
						// log_info("empty slot %d in group %d\n", i, j);
						empty_group = i;
						empty_index = j;
					}
					if (db_group->db_entries[j].valid) {
						/*hosts a database*/
						db_entry = &db_group->db_entries[j];
						// log_info("entry at %s looking for %s offset %llu",
						// (uint64_t)db_entry->db_name,
						//	 db_name, db_entry->offset[0]);
						if (strcmp((const char *)db_entry->db_name, (const char *)db_name) ==
						    0) {
							// found database, recover state and create the appropriate handle
							// and store it in the open_db's list
							log_info("DB: %s found at index [%d,%d]", db_entry->db_name, i,
								 j);
							handle = calloc(1, sizeof(db_handle));
							db_desc = calloc(1, sizeof(db_descriptor));

							handle->volume_desc = volume_desc;
							handle->db_desc = db_desc;
							bt_recover_db(handle, &db_group->db_entries[j], i, j);
							goto finish_init;
						}
					}
				}
			} else if (empty_group == -1)
				empty_group = i;
		}
		if (CREATE_FLAG != CREATE_DB) {
			log_warn("DB %s not found instructed not to create one returning NULL", db_name);
			return NULL;
		}
		/*db not found allocate a new slot for it*/
		if (empty_group == -1 && empty_index == -1) {
			log_fatal("MAX DBS %d reached", NUM_OF_DB_GROUPS * GROUP_SIZE);
			exit(EXIT_FAILURE);
		}

		// log_info("mem epoch %llu", volume_desc->mem_catalogue->epoch);
		if (empty_index == -1) {
			/*space found in empty group*/
			pr_db_group *new_group = get_space_for_system(volume_desc, sizeof(pr_db_group), 1);
			memset(new_group, 0x00, sizeof(pr_db_group));
			new_group->epoch = volume_desc->mem_catalogue->epoch;
			volume_desc->mem_catalogue->db_group_index[empty_group] =
				(pr_db_group *)bt_get_absolute_address(new_group);
			empty_index = 0;
			// log_info("allocated new pr_db_group epoch at %llu volume epoch %llu",
			// new_group->epoch,
			//	 volume_desc->mem_catalogue->epoch);
		}
		log_info("DB %s not found, allocating slot [%d,%d] for it", (const char *)db_name, empty_group,
			 empty_index);
		pr_db_group *cur_group = (pr_db_group *)bt_get_real_address(
			(uint64_t)volume_desc->mem_catalogue->db_group_index[empty_group]);
		db_entry = &cur_group->db_entries[empty_index];
		db_entry->valid = 1;
		handle = calloc(1, sizeof(db_handle));
		db_desc = (db_descriptor *)calloc(1, sizeof(db_descriptor));
		handle->db_desc = db_desc;
		handle->volume_desc = volume_desc;
		if (CREATE_FLAG == CREATE_DB)
			bt_init_fresh_db(handle, db_name, empty_group, empty_index, 1);
		else
			bt_init_fresh_db(handle, db_name, empty_group, empty_index, 0);
	}

finish_init:
	/*init soft state for all levels*/
	for (level_id = 0; level_id < MAX_LEVELS; level_id++) {
		db_desc->levels[level_id].level_id = level_id;
		if (level_id == 0)
			db_desc->levels[level_id].max_level_size = L0_SIZE;
		else
			db_desc->levels[level_id].max_level_size =
				db_desc->levels[level_id - 1].max_level_size * GROWTH_FACTOR;

		RWLOCK_INIT(&db_desc->levels[level_id].guard_of_level.rx_lock, NULL);
		MUTEX_INIT(&db_desc->levels[level_id].spill_trigger, NULL);
		MUTEX_INIT(&db_desc->levels[level_id].level_allocation_lock, NULL);
		init_level_locktable(db_desc, level_id);
		db_desc->levels[level_id].active_writers = 0;
		db_desc->pending_replica_operations = 0;
		/*check again which tree should be active*/
		db_desc->levels[level_id].active_tree = 0;
		db_desc->levels[level_id].segments_allocated[0] = 0;
		db_desc->levels[level_id].segments_allocated[1] = 0;
		db_desc->levels[level_id].segments_allocated[2] = 0;
		db_desc->levels[level_id].segments_allocated[3] = 0;

		for (tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; tree_id++) {
			db_desc->levels[level_id].tree_status[tree_id] = NO_SPILLING;
#if ENABLE_BLOOM_FILTERS
			memset(&db_desc->levels[level_id].bloom_filter[tree_id], 0x00, sizeof(struct bloom));
#endif
		}
	}

	db_desc->stat = DB_START_COMPACTION_DAEMON;
	db_desc->idx_init = NULL;
	db_desc->destroy_rdma_buf = NULL;
	db_desc->send_idx = NULL;
	// db_desc->t = NULL;
	db_desc->fl = NULL;
	db_desc->is_in_replicated_mode = 0;
	db_desc->block_on_L0 = 1;
	MUTEX_INIT(&db_desc->lock_log, NULL);
	MUTEX_INIT(&db_desc->client_barrier_lock, NULL);
	if (pthread_cond_init(&db_desc->client_barrier, NULL) != 0) {
		log_fatal("Failed to init condition variable");
		perror("pthread_cond_init() error");
		exit(EXIT_FAILURE);
	}

#if VALUE_LOG_EXPLICIT_IO
	// value log tail related
	if (pthread_rwlock_init(&db_desc->log_tail_buf_lock, NULL) != 0) {
		log_fatal("Failed to init lock");
	}
	for (int i = 0; i < LOG_TAIL_BUFS; ++i) {
		if (posix_memalign((void **)&db_desc->log_tail_buf[i], ALIGNMENT, sizeof(struct log_tail)) != 0) {
			log_fatal("Failed to allocate value log tail buffer for DB:%s", db_desc->db_name);
			exit(EXIT_FAILURE);
		}
		db_desc->log_tail_buf[i]->dev_segment_offt = 0;
		db_desc->log_tail_buf[i]->start = 0;
		db_desc->log_tail_buf[i]->end = 0;
		db_desc->log_tail_buf[i]->pending_readers = 0;
		db_desc->log_tail_buf[i]->free = 1;
		db_desc->log_tail_buf[i]->IOs_completed_in_tail = SEGMENT_SIZE / LOG_TAIL_CHUNK_SIZE;
		db_desc->log_tail_buf[i]->fd = FD;
		for (int j = 0; j < (SEGMENT_SIZE / LOG_TAIL_CHUNK_SIZE); ++j) {
			db_desc->log_tail_buf[i]->bytes_written_in_log_chunk[j] = 0;
		}
	}

	// Read last segment of db in memory
	off_t dev_offt = bt_get_absolute_address((void *)db_desc->KV_log_last_segment);
	ssize_t bytes_read = 0;
	ssize_t bytes = 0;
	while (bytes_read < SEGMENT_SIZE) {
		bytes = pread(FD, db_desc->log_tail_buf[0]->buf, SEGMENT_SIZE - bytes_read, dev_offt + bytes_read);
		if (bytes == -1) {
			log_fatal("Failed to read error code");
			perror("Error");
			assert(0);
			exit(EXIT_FAILURE);
		}
		bytes_read += bytes;
	}
	db_desc->log_tail_buf[0]->dev_segment_offt = bt_get_absolute_address((void *)db_desc->KV_log_last_segment);
	db_desc->log_tail_buf[0]->start = db_desc->log_tail_buf[0]->dev_segment_offt;
	db_desc->log_tail_buf[0]->end = db_desc->log_tail_buf[0]->start + SEGMENT_SIZE;
	db_desc->log_tail_buf[0]->pending_readers = 0;
	uint32_t offt_in_seg = db_desc->KV_log_size % SEGMENT_SIZE;
	uint32_t chunk_id = offt_in_seg / LOG_TAIL_CHUNK_SIZE;
	uint32_t num_chunks = SEGMENT_SIZE / LOG_TAIL_CHUNK_SIZE;
	db_desc->log_tail_buf[0]->IOs_completed_in_tail = 0;
	for (uint32_t j = 0; j < num_chunks; ++j) {
		if (j < chunk_id) {
			db_desc->log_tail_buf[0]->bytes_written_in_log_chunk[j] = LOG_TAIL_CHUNK_SIZE;
			++db_desc->log_tail_buf[0]->IOs_completed_in_tail;
		} else if (j == chunk_id)
			db_desc->log_tail_buf[0]->bytes_written_in_log_chunk[j] = offt_in_seg % LOG_TAIL_CHUNK_SIZE;
		else
			db_desc->log_tail_buf[0]->bytes_written_in_log_chunk[j] = 0;
	}

	db_desc->log_tail_buf[0]->free = 0;
	log_info("Recovered last segment of DB: %s in memory IOs completed %u", db_desc->db_name,
		 db_desc->log_tail_buf[0]->IOs_completed_in_tail);
#endif

	sem_init(&db_desc->compaction_daemon_interrupts, PTHREAD_PROCESS_PRIVATE, 0);
	if (pthread_create(&(handle->db_desc->compaction_daemon), NULL, (void *)compaction_daemon, (void *)handle) !=
	    0) {
		log_fatal("Failed to start compaction_daemon for db %s", db_name);
		exit(EXIT_FAILURE);
	}
	int a = 0;
	while (db_desc->stat != DB_OPEN) {
		if (++a == 0)
			a++;
	}

	klist_add_first(volume_desc->open_databases, db_desc, db_name, NULL);
	bt_recover_L0(handle);
	MUTEX_UNLOCK(&init_lock);

	return handle;
}

char db_close(db_handle *handle)
{
	MUTEX_LOCK(&init_lock);
	/*verify that this is a valid db*/
	if (klist_find_element_with_key(handle->volume_desc->open_databases, handle->db_desc->db_name) == NULL) {
		log_warn("Received close for db: %s that is not listed as open", handle->db_desc->db_name);
		goto finish;
	}
	if (handle->db_desc->ref_count < 0) {
		log_fatal("Negative referece count for DB %s", handle->db_desc->db_name);
		exit(EXIT_FAILURE);
	}
	--handle->db_desc->ref_count;

	if (handle->db_desc->ref_count > 0) {
		log_info("More guys here");
		goto finish;
		snapshot(handle->volume_desc);
	}

	handle->db_desc->stat = DB_TERMINATE_COMPACTION_DAEMON;
	sem_post(&handle->db_desc->compaction_daemon_interrupts);
	while (handle->db_desc->stat != DB_IS_CLOSING)
		usleep(50);

	log_info("Closing DB %s", handle->db_desc->db_name);

	/*wait for all pending compactions to finish for L0*/
	for (int i = 0; i < NUM_TREES_PER_LEVEL; i++) {
		if (handle->db_desc->levels[0].tree_status[i] == SPILLING_IN_PROGRESS) {
			i = 0;
			usleep(500);
			continue;
		}
	}
	/*wait for all other pending compactions to finish*/
	for (int i = 1; i < MAX_LEVELS; i++) {
		if (handle->db_desc->levels[i].tree_status[0] == SPILLING_IN_PROGRESS) {
			i = 0;
			usleep(500);
			continue;
		}
	}
	log_info("All pending compactions done for db %s", handle->db_desc->db_name);
	snapshot(handle->volume_desc);

	if (!klist_remove_element(handle->volume_desc->open_databases, handle->db_desc)) {
		log_fatal("Failed to remove db_desc of DB %s", handle->db_desc->db_name);
		exit(EXIT_FAILURE);
	}

	// free L0
	for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; ++tree_id) {
		if (RWLOCK_WRLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock)) {
			exit(EXIT_FAILURE);
		}
		seg_free_level(handle, 0, tree_id);
		if (RWLOCK_UNLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock)) {
			exit(EXIT_FAILURE);
		}
	}

	for (int i = 0; i < MAX_LEVELS; i++) {
		if (pthread_rwlock_destroy(&handle->db_desc->levels[i].guard_of_level.rx_lock)) {
			log_fatal("Failed to destroy guard of level lock");
			exit(EXIT_FAILURE);
		}
		destroy_level_locktable(handle->db_desc, i);
	}
	// memset(handle->db_desc, 0x00, sizeof(struct db_descriptor));
	if (pthread_cond_destroy(&handle->db_desc->client_barrier) != 0) {
		log_fatal("Failed to destroy condition variable");
		perror("pthread_cond_destroy() error");
		exit(EXIT_FAILURE);
	}
	free(handle->db_desc);

finish:
	free(handle);
	MUTEX_UNLOCK(&init_lock);
	return KREON_OK;
}

void destroy_db_desc(void *handle)
{
	struct db_handle *hd = (struct db_handle *)handle;
	// free L0
	for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; ++tree_id) {
		if (RWLOCK_WRLOCK(&hd->db_desc->levels[0].guard_of_level.rx_lock)) {
			exit(EXIT_FAILURE);
		}
		seg_free_level(handle, 0, tree_id);
		if (RWLOCK_UNLOCK(&hd->db_desc->levels[0].guard_of_level.rx_lock)) {
			exit(EXIT_FAILURE);
		}
	}

	for (int i = 0; i < MAX_LEVELS; i++) {
		if (pthread_rwlock_destroy(&hd->db_desc->levels[i].guard_of_level.rx_lock)) {
			log_fatal("Failed to destroy guard of level lock");
			exit(EXIT_FAILURE);
		}
		destroy_level_locktable(hd->db_desc, i);
	}
	// memset(handle->db_desc, 0x00, sizeof(struct db_descriptor));
	if (pthread_cond_destroy(&hd->db_desc->client_barrier) != 0) {
		log_fatal("Failed to destroy condition variable");
		perror("pthread_cond_destroy() error");
		exit(EXIT_FAILURE);
	}
	free(hd->db_desc);
}

enum optype { insert_op, delete_op };

uint8_t bt_insert(db_handle *handle, void *key, void *value, uint32_t key_size, uint32_t value_size, enum optype type)
{
	bt_insert_req ins_req;
	char tmp[KV_MAX_SIZE];

	uint32_t kv_size;
	kv_size = sizeof(uint32_t) + key_size + sizeof(uint32_t) + value_size + sizeof(uint64_t);
	if (kv_size > MAX_SUPPORTED_KV_SIZE) {
		log_warn("Key value size %lu exceeds max supported size of %lu", kv_size, MAX_SUPPORTED_KV_SIZE);
		return FAILED;
	}
	char *key_buf = NULL;
	char malloced = 0;
	if (kv_size > KV_MAX_SIZE) {
		key_buf = (char *)malloc(kv_size);
		malloced = 1;
	} else {
		key_buf = tmp;
		malloced = 0;
	}

	/*prepare the request*/
	*(uint32_t *)key_buf = key_size;
	memcpy((void *)(uint64_t)key_buf + sizeof(uint32_t), key, key_size);
	*(uint32_t *)((uint64_t)key_buf + sizeof(uint32_t) + key_size) = value_size;
	if (value)
		memcpy((void *)(uint64_t)key_buf + sizeof(uint32_t) + key_size + sizeof(uint32_t), value, value_size);
	ins_req.metadata.handle = handle;
	ins_req.key_value_buf = key_buf;
	ins_req.metadata.level_id = 0;
	/*
* Note for L0 inserts since active_tree changes dynamically we decide which
* is the active_tree after
* acquiring the guard lock of the region
* */
	ins_req.metadata.key_format = KV_FORMAT;
	ins_req.metadata.append_to_log = 1;
	ins_req.metadata.gc_request = 0;
	ins_req.metadata.special_split = 0;
	switch (type) {
	case insert_op:
		ins_req.metadata.is_tombstone = 0;
		break;
	case delete_op:
		ins_req.metadata.is_tombstone = 1;
		break;
	}
	uint8_t ret = _insert_key_value(&ins_req);
	if (malloced)
		free(key_buf);
	return ret;
}

uint8_t insert_key_value(db_handle *handle, void *key, void *value, uint32_t key_size, uint32_t value_size)
{
	return bt_insert(handle, key, value, key_size, value_size, insert_op);
}

int8_t delete_key(db_handle *handle, void *key)
{
	int level_id;
	if (!find_kv_offt(handle, key, &level_id))
		// if (!find_key(handle, key, size))
		return FAILED;
	else
		return bt_insert(handle, key + sizeof(uint32_t), NULL, *(uint32_t *)key, 0, delete_op);
}

void extract_keyvalue_size(log_operation *req, metadata_tologop *data_size)
{
	switch (req->optype_tolog) {
	case insertOp:
		data_size->key_len = *(uint32_t *)req->ins_req->key_value_buf;
		data_size->value_len =
			*(uint32_t *)(req->ins_req->key_value_buf + sizeof(uint32_t) + (data_size->key_len));
		data_size->kv_size = req->metadata->kv_size;
		break;
	case deleteOp:
		data_size->key_len = *(uint32_t *)req->ins_req->key_value_buf;
		data_size->value_len = 0;
		data_size->kv_size = data_size->key_len + sizeof(struct bt_delete_marker) + sizeof(uint32_t);
		// log_info("data size is %lu key len
		// %lu",data_size->kv_size,data_size->key_len);
		break;
	default:
		log_fatal("Trying to append unknown operation in log! ");
		exit(EXIT_FAILURE);
	}
}

void write_keyvalue_inlog(log_operation *req, metadata_tologop *data_size, char *addr_inlog)
{
	switch (req->optype_tolog) {
	case insertOp:
		memcpy(addr_inlog, req->ins_req->key_value_buf,
		       sizeof(data_size->key_len) + data_size->key_len + sizeof(data_size->value_len) +
			       data_size->value_len);
		break;
	case deleteOp: {
		struct bt_delete_marker dm = { .marker_id = BT_DELETE_MARKER_ID, .key_size = data_size->key_len };
		memcpy(addr_inlog, &dm, sizeof(struct bt_delete_marker));
		addr_inlog += sizeof(struct bt_delete_marker);
		memcpy(addr_inlog, req->ins_req->key_value_buf + (sizeof(uint32_t)), data_size->key_len);
		addr_inlog += data_size->key_len;
		*(uint32_t *)addr_inlog = 0;
		// addr_inlog += (sizeof(data_size->key_len) + data_size->key_len);
		// memcpy(addr_inlog, &data_size->value_len, sizeof(data_size->value_len));
		break;
	}
	default:
		log_fatal("Trying to append unknown operation in log! ");
		exit(EXIT_FAILURE);
	}
}

struct log_ticket {
	// in var
	struct log_tail *tail;
	struct log_operation *req;
	struct metadata_tologop *data_size;
	uint64_t log_offt;
	// out var
	uint64_t IO_start_offt;
	uint32_t IO_size;
	uint32_t op_size;
};

static void copy_kv_to_tail(struct log_ticket *ticket)
{
	if (!ticket->req)
		return;
	uint64_t offt_in_seg = ticket->log_offt % SEGMENT_SIZE;
	switch (ticket->req->optype_tolog) {
	case insertOp: {
		ticket->op_size = sizeof(ticket->data_size->key_len) + ticket->data_size->key_len +
				  sizeof(ticket->data_size->value_len) + ticket->data_size->value_len;
		// log_info("Copying ta log offt %llu in buf %u bytes %u", ticket->log_offt,
		// offt_in_seg, ticket->op_size);
		memcpy(&ticket->tail->buf[offt_in_seg], ticket->req->ins_req->key_value_buf, ticket->op_size);
		break;
	}
	case deleteOp: {
		uint32_t offt = offt_in_seg;
		struct bt_delete_marker dm = { .marker_id = BT_DELETE_MARKER_ID,
					       .key_size = ticket->data_size->key_len };
		ticket->op_size = sizeof(struct bt_delete_marker);
		memcpy(&ticket->tail->buf[offt], &dm, ticket->op_size);
		offt += sizeof(struct bt_delete_marker);
		memcpy(&ticket->tail->buf[offt], ticket->req->ins_req->key_value_buf + (sizeof(uint32_t)),
		       ticket->data_size->key_len);
		offt += ticket->data_size->key_len;
		ticket->op_size += ticket->data_size->key_len;
		memset(&ticket->tail->buf[offt], 0x00, sizeof(uint32_t));
		ticket->op_size += sizeof(uint32_t);
		break;
	}
	case paddingOp: {
		if (offt_in_seg == 0)
			ticket->op_size = 0;
		else {
			ticket->op_size = SEGMENT_SIZE - offt_in_seg;
			// log_info("Time for padding for log_offset %llu offt in seg %llu pad bytes
			// %u", ticket->log_offt,
			//	 offt_in_seg, ticket->op_size);
			memset(&ticket->tail->buf[offt_in_seg], 0, ticket->op_size);
		}
		break;
	}
	default:
		log_fatal("Unknown op");
		exit(EXIT_FAILURE);
	}

	uint32_t remaining = ticket->op_size;
	uint32_t curr_offt_in_seg = offt_in_seg;
	while (remaining > 0) {
		uint32_t chunk_id = curr_offt_in_seg / LOG_TAIL_CHUNK_SIZE;
		int64_t offt_in_chunk = curr_offt_in_seg - (chunk_id * LOG_TAIL_CHUNK_SIZE);
		int64_t bytes = LOG_TAIL_CHUNK_SIZE - offt_in_chunk;
		if (remaining < bytes)
			bytes = remaining;
		//log_info("Charging %u bytes for chunk id %u op size %u", bytes, chunk_id, ticket->op_size);
		//assert(ticket->op_size < (2 * 1024 * 1024));
		__sync_fetch_and_add(&ticket->tail->bytes_written_in_log_chunk[chunk_id], bytes);
		remaining -= bytes;
		curr_offt_in_seg += bytes;
	}
}

static void do_log_chunk_IO(struct log_ticket *ticket)
{
	uint64_t offt_in_seg = ticket->log_offt % SEGMENT_SIZE;
	uint32_t chunk_offt = offt_in_seg % LOG_TAIL_CHUNK_SIZE;
	uint32_t chunk_id = offt_in_seg / LOG_TAIL_CHUNK_SIZE;
	uint32_t num_chunks = SEGMENT_SIZE / LOG_TAIL_CHUNK_SIZE;
	assert(chunk_id != num_chunks);
	int do_IO;

	if (chunk_offt + ticket->op_size >= LOG_TAIL_CHUNK_SIZE) {
		ticket->IO_start_offt = chunk_id * LOG_TAIL_CHUNK_SIZE;
		ticket->IO_size = LOG_TAIL_CHUNK_SIZE;
		do_IO = 1;
	} else {
		ticket->IO_start_offt = 0;
		ticket->IO_size = 0;
		do_IO = 0;
	}

	if (!do_IO)
		return;

	// log_info("Checking if all data for chunk id %u are there currently are %u",
	// chunk_id,
	// Can I set new segment for the others to proceed?
	//	 ticket->tail->bytes_written_in_log_chunk[chunk_id]);
	// wait until all pending bytes are written
	wait_for_value(&ticket->tail->bytes_written_in_log_chunk[chunk_id], LOG_TAIL_CHUNK_SIZE);
	// do the IO finally
	ssize_t total_bytes_written;
	if (chunk_id)
		total_bytes_written = 0;
	else
		total_bytes_written = sizeof(struct segment_header);
	ssize_t bytes_written = 0;
	uint32_t size = LOG_TAIL_CHUNK_SIZE;
	// log_info("IO time, start %llu size %llu segment dev_offt %llu offt in seg
	// %llu", total_bytes_written, size,
	//	 ticket->tail->dev_segment_offt, ticket->IO_start_offt);
	while (total_bytes_written < size) {
		bytes_written =
			pwrite(ticket->tail->fd, &ticket->tail->buf[ticket->IO_start_offt + total_bytes_written],
			       size - total_bytes_written,
			       ticket->tail->dev_segment_offt + ticket->IO_start_offt + total_bytes_written);
		if (bytes_written == -1) {
			log_fatal("Failed to write LOG_CHUNK reason follows");
			perror("Reason");
			exit(EXIT_FAILURE);
		}
		total_bytes_written += bytes_written;
	}
	__sync_fetch_and_add(&ticket->tail->IOs_completed_in_tail, 1);

	assert(ticket->tail->IOs_completed_in_tail <= num_chunks);
#if 0
	if (chunk_id == num_chunks - 1) {
		// log_info("Resetting segment start %llu end %llu ...",
		// ticket->tail->start, ticket->tail->end);
		// Wait for all chunk IOs to finish to characterize it free
		spin_loop(&ticket->tail->IOs_completed_in_tail, num_chunks);

		memset(ticket->tail->bytes_written_in_log_chunk, 0x00,
		       sizeof(ticket->tail->bytes_written_in_log_chunk));
		ticket->tail->dev_segment_offt = 0;
		ticket->tail->start = 0;
		ticket->tail->end = 0;
		while (!__sync_bool_compare_and_swap(&ticket->tail->IOs_completed_in_tail,num_chunks,0));
		while (!__sync_bool_compare_and_swap(&ticket->tail->free, 0, 1))
			;
	}
#endif
}

static void do_log_IO(struct log_ticket *ticket)
{
	uint64_t log_offt = ticket->log_offt;
	uint32_t op_size = ticket->op_size;
	uint32_t remaining = ticket->op_size;
	uint64_t c_log_offt = log_offt;
	while (remaining > 0) {
		ticket->log_offt = c_log_offt;
		if (remaining >= LOG_TAIL_CHUNK_SIZE)
			ticket->op_size = LOG_TAIL_CHUNK_SIZE;
		else
			ticket->op_size = remaining;
		do_log_chunk_IO(ticket);
		remaining -= ticket->op_size;
		c_log_offt += ticket->op_size;
	}
	ticket->log_offt = log_offt;
	ticket->op_size = op_size;
}

static void print_log_tail(struct db_descriptor *db_desc, struct log_tail *tail)
{
	int id = 0;
	log_info("log tail");
	uint32_t *start = (uint32_t *)&tail->buf[sizeof(struct segment_header)];
	uint32_t *value = (uint32_t *)((uint64_t)start + sizeof(uint32_t) + *start);
	uint32_t offt_in_buf = sizeof(struct segment_header);
	while (offt_in_buf < db_desc->KV_log_size % SEGMENT_SIZE) {
		printf("id %d Key len: %u key %s value len %u offt %u\n", id++, *start, (char *)&start[1], *value,
		       offt_in_buf);
		start = (uint32_t *)((uint64_t)value + sizeof(uint32_t) + *value);
		offt_in_buf += sizeof(uint32_t) + *start + sizeof(uint32_t) + *value;
		value = (uint32_t *)((uint64_t)start + sizeof(uint32_t) + *start);
	}
}

static uint64_t append_key_value_to_log_direct_IO(log_operation *req)
{
	db_handle *handle = req->metadata->handle;
	metadata_tologop data_size;
	extract_keyvalue_size(req, &data_size);

	struct log_ticket my_ticket = { .log_offt = 0, .IO_start_offt = 0, .IO_size = 0 };
	struct log_ticket pad_ticket = { .log_offt = 0, .IO_start_offt = 0, .IO_size = 0 };

	MUTEX_LOCK(&handle->db_desc->lock_log);
	// print_log_tail(handle->db_desc,
	// handle->db_desc->log_tail_buf[tail_id%LOG_TAIL_BUFS]);
	uint32_t available_space_in_log;
	// append data part in the data log
	if (handle->db_desc->KV_log_size % SEGMENT_SIZE != 0)
		available_space_in_log = SEGMENT_SIZE - (handle->db_desc->KV_log_size % SEGMENT_SIZE);
	else
		available_space_in_log = 0;

	uint32_t num_chunks = SEGMENT_SIZE / LOG_TAIL_CHUNK_SIZE;
	int segment_change = 0;
	if (available_space_in_log < data_size.kv_size) {
		// fill info for kreon master here
		req->metadata->log_segment_addr = bt_get_absolute_address(handle->db_desc->KV_log_last_segment);
		assert(req->metadata->log_segment_addr % SEGMENT_SIZE == 0);
		req->metadata->log_offset_full_event = handle->db_desc->KV_log_size;
		req->metadata->segment_id = handle->db_desc->KV_log_last_segment->segment_id;
		req->metadata->log_padding = available_space_in_log;
		req->metadata->end_of_log = handle->db_desc->KV_log_size + available_space_in_log;
		req->metadata->segment_full_event = 1;

		uint32_t curr_tail_id = handle->db_desc->curr_tail_id;

		// pad with zeroes remaining bytes in segment
		log_operation pad_op = { .metadata = NULL, .optype_tolog = paddingOp, .ins_req = NULL };
		pad_ticket.req = &pad_op;
		pad_ticket.data_size = NULL;
		pad_ticket.tail = handle->db_desc->log_tail_buf[curr_tail_id % LOG_TAIL_BUFS];
		pad_ticket.log_offt = handle->db_desc->KV_log_size;

		copy_kv_to_tail(&pad_ticket);

		// log_info("Resetting segment start %llu end %llu ...",
		// ticket->tail->start, ticket->tail->end);
		// Wait for all chunk IOs to finish to characterize it free
		uint32_t next_tail_id = ++curr_tail_id;
		struct log_tail *next_tail = handle->db_desc->log_tail_buf[next_tail_id % LOG_TAIL_BUFS];

		// if (next_tail->free)
		//	num_chunks = 0;

		wait_for_value(&next_tail->IOs_completed_in_tail, num_chunks);
		RWLOCK_WRLOCK(&handle->db_desc->log_tail_buf_lock);
		wait_for_value(&next_tail->pending_readers, 0);
		// int warn_print = 1;
		// warn_print = 1;
		// while (next_tail->pending_readers) {
		//	if (warn_print) {
		// log_warn("Still pending readers for DB: %s not ready yet maybe increase "
		//	 "LOG_TAIL_BUFS from %d?",
		//	 handle->db_desc->db_name, LOG_TAIL_BUFS);
		//		warn_print = 0;
		//	}
		//}

		handle->db_desc->KV_log_size += available_space_in_log;

		struct segment_header *d_header = seg_get_raw_log_segment(handle->volume_desc);
		memset(d_header->garbage_bytes, 0x00, 2 * MAX_COUNTER_VERSIONS * sizeof(uint64_t));
		d_header->segment_id = handle->db_desc->KV_log_last_segment->segment_id + 1;
		d_header->next_segment = NULL;
		handle->db_desc->KV_log_last_segment->next_segment = (void *)bt_get_absolute_address(d_header);
		handle->db_desc->KV_log_last_segment = d_header;
		// position the log to the newly added block
		handle->db_desc->KV_log_size += sizeof(segment_header);
		// Reset tail for new use
		for (int j = 0; j < (SEGMENT_SIZE / LOG_TAIL_CHUNK_SIZE); ++j)
			next_tail->bytes_written_in_log_chunk[j] = 0;
		next_tail->IOs_completed_in_tail = 0;
		next_tail->start = bt_get_absolute_address(d_header);
		next_tail->end = next_tail->start + SEGMENT_SIZE;
		next_tail->dev_segment_offt = bt_get_absolute_address(d_header);
		next_tail->bytes_written_in_log_chunk[0] = sizeof(struct segment_header);
		next_tail->free = 0;
		handle->db_desc->curr_tail_id = next_tail_id;
		segment_change = 1;
		RWLOCK_UNLOCK(&handle->db_desc->log_tail_buf_lock);
	}
	uint32_t tail_id = handle->db_desc->curr_tail_id;
	my_ticket.req = req;
	my_ticket.data_size = &data_size;
	my_ticket.tail = handle->db_desc->log_tail_buf[tail_id % LOG_TAIL_BUFS];
	my_ticket.log_offt = handle->db_desc->KV_log_size;
	req->metadata->log_offset = handle->db_desc->KV_log_size;
	handle->db_desc->KV_log_size += data_size.kv_size;
	MUTEX_UNLOCK(&handle->db_desc->lock_log);

	if (segment_change) {
		// do the padding IO as well
		do_log_IO(&pad_ticket);
		wait_for_value(&pad_ticket.tail->IOs_completed_in_tail, num_chunks);
		// Now time to retire
		RWLOCK_WRLOCK(&handle->db_desc->log_tail_buf_lock);
		pad_ticket.tail->free = 1;
		RWLOCK_UNLOCK(&handle->db_desc->log_tail_buf_lock);
	}
	copy_kv_to_tail(&my_ticket);
	do_log_IO(&my_ticket);
	uint64_t kv_dev_offt = my_ticket.tail->dev_segment_offt + (my_ticket.log_offt % SEGMENT_SIZE);
	if (my_ticket.req->optype_tolog == deleteOp) {
		//log_info("Delete size was %u",data_size.kv_size);
		kv_dev_offt += sizeof(uint32_t);
	}
	return kv_dev_offt;
}

static uint64_t append_key_value_to_log(log_operation *req)
{
	segment_header *d_header;
	void *addr_inlog; /*address at the device*/
	metadata_tologop data_size;
	uint32_t available_space_in_log;
	// uint32_t allocated_space;
	db_handle *handle = req->metadata->handle;
	extract_keyvalue_size(req, &data_size);
	MUTEX_LOCK(&handle->db_desc->lock_log);
	/*append data part in the data log*/
	if (handle->db_desc->KV_log_size % SEGMENT_SIZE != 0)
		available_space_in_log = SEGMENT_SIZE - (handle->db_desc->KV_log_size % SEGMENT_SIZE);
	else
		available_space_in_log = 0;

	if (available_space_in_log < data_size.kv_size) {
		/*fill info for kreon master here*/
		req->metadata->log_segment_addr = bt_get_absolute_address(handle->db_desc->KV_log_last_segment);
		assert(req->metadata->log_segment_addr % SEGMENT_SIZE == 0);
		req->metadata->log_offset_full_event = handle->db_desc->KV_log_size;
		req->metadata->segment_id = handle->db_desc->KV_log_last_segment->segment_id;
		req->metadata->log_padding = available_space_in_log;
		req->metadata->end_of_log = handle->db_desc->KV_log_size + available_space_in_log;
		req->metadata->segment_full_event = 1;

		/*pad with zeroes remaining bytes in segment*/
		addr_inlog = (void *)((uint64_t)handle->db_desc->KV_log_last_segment +
				      (handle->db_desc->KV_log_size % SEGMENT_SIZE));
		memset(addr_inlog, 0x00, available_space_in_log);

		// allocated_space = data_size.kv_size + sizeof(segment_header);
		// allocated_space += SEGMENT_SIZE - (allocated_space % SEGMENT_SIZE);
		d_header = seg_get_raw_log_segment(handle->volume_desc);
		memset(d_header->garbage_bytes, 0x00, 2 * MAX_COUNTER_VERSIONS * sizeof(uint64_t));
		d_header->segment_id = handle->db_desc->KV_log_last_segment->segment_id + 1;
		d_header->next_segment = NULL;
		handle->db_desc->KV_log_last_segment->next_segment = (void *)bt_get_absolute_address(d_header);
		handle->db_desc->KV_log_last_segment = d_header;
		// position the log to the newly added block
		handle->db_desc->KV_log_size += (available_space_in_log + sizeof(segment_header));
	}

	addr_inlog = (void *)((uint64_t)handle->db_desc->KV_log_last_segment +
			      (handle->db_desc->KV_log_size % SEGMENT_SIZE));
	req->metadata->log_offset = handle->db_desc->KV_log_size;
	handle->db_desc->KV_log_size += data_size.kv_size;

	MUTEX_UNLOCK(&handle->db_desc->lock_log);

	write_keyvalue_inlog(req, &data_size, addr_inlog);
	switch (req->optype_tolog) {
	case insertOp:
		break;
	case deleteOp:
		addr_inlog += sizeof(uint32_t);
		break;
	default:
		log_fatal("Unknown operation!");
		exit(EXIT_FAILURE);
	}
	return bt_get_absolute_address(addr_inlog);
}

uint8_t _insert_key_value(bt_insert_req *ins_req)
{
	db_descriptor *db_desc;
	unsigned key_size;
	unsigned val_size;
	uint8_t rc;

	db_desc = ins_req->metadata.handle->db_desc;

	int active_tree = db_desc->levels[0].active_tree;
	while (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
		pthread_mutex_lock(&db_desc->client_barrier_lock);
		active_tree = db_desc->levels[0].active_tree;

		if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
			sem_post(&db_desc->compaction_daemon_interrupts);
			if (pthread_cond_wait(&db_desc->client_barrier, &db_desc->client_barrier_lock) != 0) {
				log_fatal("failed to throttle");
				exit(EXIT_FAILURE);
			}
		}
		active_tree = db_desc->levels[0].active_tree;
		pthread_mutex_unlock(&db_desc->client_barrier_lock);
	}
	db_desc->dirty = 0x01;

	if (ins_req->metadata.key_format == KV_FORMAT) {
		key_size = *(uint32_t *)ins_req->key_value_buf;
		val_size = *(uint32_t *)(ins_req->key_value_buf + 4 + key_size);
		ins_req->metadata.kv_size = sizeof(uint32_t) + key_size + sizeof(uint32_t) + val_size;
	} else
		ins_req->metadata.kv_size = -1;
	rc = SUCCESS;
	if (_writers_join_as_readers(ins_req) == SUCCESS)
		rc = SUCCESS;
	else if (_concurrent_insert(ins_req) != SUCCESS) {
		log_warn("insert failed!");
		rc = FAILED;
	}
	return rc;
}

static struct leaf_kv_pointer *bt_find_key_addr_in_leaf(struct db_descriptor *db_desc, int level_id, leaf_node *leaf,
							struct kv_format *key)
{
	int32_t start_idx = 0;
	int32_t end_idx = leaf->header.numberOfEntriesInNode - 1;
	char key_buf_prefix[PREFIX_SIZE] = { '\0' };

	memcpy(key_buf_prefix, key->key_buf, MIN(key->key_size, PREFIX_SIZE));

	while (start_idx <= end_idx) {
		int32_t middle = (start_idx + end_idx) / 2;
		int64_t ret = prefix_compare(leaf->prefix[middle], key_buf_prefix, PREFIX_SIZE);
		if (ret < 0)
			start_idx = middle + 1;
		else if (ret > 0)
			end_idx = middle - 1;
		else {
			void *index_key = NULL;
			if (level_id) {
				index_key = (void *)bt_get_real_address(leaf->kv_entry[middle].device_offt);
				//log_info("level_id %u Comparing index key %u:%s with query key %u:%s", level_id,
				//	 *(uint32_t *)index_key, index_key + 4, *(uint32_t *)key, key + 4);
				ret = bt_key_cmp(index_key, key, KV_FORMAT, KV_FORMAT);
			} else {
				struct bt_kv_log_address L = { .addr = NULL, .in_tail = 0, .tail_id = UINT8_MAX };
				if (!level_id)
					L = bt_get_kv_log_address(db_desc, leaf->kv_entry[middle].device_offt);
				else
					L.addr = bt_get_real_address(leaf->kv_entry[middle].device_offt);
				// level-0 lookup
				index_key = L.addr;
				//log_info("level_id %u Comparing index key %u:%s with query key %u:%s", level_id,
				//	 *(uint32_t *)index_key, index_key + 4, *(uint32_t *)key, key + 4);
				ret = bt_key_cmp(index_key, key, KV_FORMAT, KV_FORMAT);
				if (L.in_tail)
					bt_done_with_value_log_address(db_desc, &L);
			}

			if (ret == 0)
				return &(leaf->kv_entry[middle]);
			else if (ret < 0)
				start_idx = middle + 1;
			else
				end_idx = middle - 1;
		}
	}

	return NULL;
}

static struct lookup_reply lookup_in_tree(db_descriptor *db_desc, void *key, int level_id, int tree_id)
{
	struct lookup_reply rep = { .kv_offt = 0, .tombstone = 0 };
	node_header *curr_node, *son_node = NULL;
	struct leaf_kv_pointer *leaf_entry = NULL;
	lock_table *prev_lock = NULL, *curr_lock = NULL;
	void *next_addr;

	if (db_desc->levels[level_id].root_w[tree_id] != NULL) {
		/* log_info("Level %d with tree_id %d has root_w",level_id,tree_id); */
		curr_node = db_desc->levels[level_id].root_w[tree_id];
	} else if (db_desc->levels[level_id].root_r[tree_id] != NULL) {
		/* log_info("Level %d with tree_id %d has root_w",level_id,tree_id); */
		curr_node = db_desc->levels[level_id].root_r[tree_id];
	} else {
		__sync_fetch_and_sub(&db_desc->levels[level_id].active_writers, 1);
		/* log_info("Level %d is empty with tree_id %d",level_id,tree_id); */
		return rep;
	}
#if ENABLE_BLOOM_FILTERS
	if (level_id > 0) {
		char prefix_key[PREFIX_SIZE];
		int check;
		if (*(uint32_t *)key < PREFIX_SIZE) {
			memset(prefix_key, 0x00, PREFIX_SIZE);
			memcpy(prefix_key, key + sizeof(uint32_t), *(uint32_t *)key);
			check = bloom_check(&db_desc->levels[level_id].bloom_filter[0], prefix_key, PREFIX_SIZE);
			// log_info("prefix key is %s ************", prefix_key);
		} else {
			check = bloom_check(&db_desc->levels[level_id].bloom_filter[0], key + sizeof(uint32_t),
					    PREFIX_SIZE);
		}
		assert(check != -1);
		if (0 == check) {
			// log_info("element %u : %s in not present %d\n", *(uint32_t *)key, key
			// +
			// 4, check);
			return rep;
		}
	}
#endif
	if (curr_node->type == leafRootNode) {
		curr_lock = _find_position(db_desc->levels[level_id].level_lock_table, curr_node);
		if (RWLOCK_RDLOCK(&curr_lock->rx_lock) != 0)
			exit(EXIT_FAILURE);

		leaf_entry =
			bt_find_key_addr_in_leaf(db_desc, level_id, (leaf_node *)curr_node, (struct kv_format *)key);
		if (leaf_entry == NULL)
			rep.kv_offt = 0;
		else {
			rep.kv_offt = leaf_entry->device_offt;
			rep.tombstone = leaf_entry->tombstone;
		}
	} else {
		while (curr_node->type != leafNode) {
			curr_lock = _find_position(db_desc->levels[level_id].level_lock_table, curr_node);
			if (RWLOCK_RDLOCK(&curr_lock->rx_lock) != 0)
				exit(EXIT_FAILURE);
			if (prev_lock) {
				if (RWLOCK_UNLOCK(&prev_lock->rx_lock) != 0)
					exit(EXIT_FAILURE);
			}

			next_addr = _index_node_binary_search((index_node *)curr_node, key, KV_FORMAT);
			son_node = (void *)bt_get_real_address(*(uint64_t *)next_addr);
			prev_lock = curr_lock;
			curr_node = son_node;
		}

		prev_lock = curr_lock;
		curr_lock = _find_position(db_desc->levels[level_id].level_lock_table, curr_node);
		if (RWLOCK_RDLOCK(&curr_lock->rx_lock) != 0) {
			exit(EXIT_FAILURE);
		}
		if (RWLOCK_UNLOCK(&prev_lock->rx_lock) != 0)
			exit(EXIT_FAILURE);

		leaf_entry =
			bt_find_key_addr_in_leaf(db_desc, level_id, (leaf_node *)curr_node, (struct kv_format *)key);

		if (leaf_entry == NULL) {
			rep.kv_offt = 0;
		} else {
			rep.kv_offt = leaf_entry->device_offt;
			rep.tombstone = leaf_entry->tombstone;
		}
	}
	if (RWLOCK_UNLOCK(&curr_lock->rx_lock) != 0)
		exit(EXIT_FAILURE);
	__sync_fetch_and_sub(&db_desc->levels[level_id].active_writers, 1);
	return rep;
}

// this function will be reused in various places such as deletes
uint64_t find_kv_offt(db_handle *handle, void *key, int *kv_level_id)
{
	struct lookup_reply rep;
	int level_id = 0;
	*kv_level_id = -1;
	// Acquiring guard lock for level 0
	if (RWLOCK_RDLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock) != 0)
		exit(EXIT_FAILURE);
	__sync_fetch_and_add(&handle->db_desc->levels[0].active_writers, 1);
	/*again special care for L0*/
	uint8_t tree_id = handle->db_desc->levels[0].active_tree;
	uint8_t base = tree_id;

	while (1) {
		rep = lookup_in_tree(handle->db_desc, key, 0, tree_id);

		if (rep.kv_offt != 0) {
			goto finish;
		}
		++tree_id;
		if (tree_id >= NUM_TREES_PER_LEVEL)
			tree_id = 0;
		if (tree_id == base)
			break;
	}

	/*search the rest trees of the level*/
	for (level_id = 1; level_id < MAX_LEVELS; ++level_id) {
		rep = lookup_in_tree(handle->db_desc, key, level_id, 0);
		if (rep.kv_offt != 0) {
			goto finish;
		}
	}

finish:

	if (RWLOCK_UNLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock) != 0)
		exit(EXIT_FAILURE);
	if (rep.kv_offt != 0 && !rep.tombstone) {
		*kv_level_id = level_id;
		return rep.kv_offt;
	} else {
		return 0;
	}
}

#if 0
void *find_key(db_handle *handle, void *key, uint32_t key_size)
{
	char buf[4000];
	void *key_buf = NULL;
	void *value;

	if (key_size <= (4000 - sizeof(uint32_t))) {
		key_buf = &(buf[0]);
		*(uint32_t *)key_buf = key_size;
		memcpy((void *)key_buf + sizeof(uint32_t), key, key_size);
		value = __find_key(handle, key_buf);
	} else {
		key_buf = malloc(key_size + sizeof(uint32_t));
		*(uint32_t *)key_buf = key_size;
		memcpy((void *)key_buf + sizeof(uint32_t), key, key_size);
		value = __find_key(handle, key_buf);
		free(key_buf);
	}

	return value;
}
#endif

/**
* @param   node:
* @param   left_child:
* @param   right_child:
* @param   key:
* @param   key_len:
|block_header|pointer_to_node|pointer_to_key|pointer_to_node |
pointer_to_key|...
*/
static int8_t update_index(index_node *node, node_header *left_child, node_header *right_child, void *key_buf)
{
	int64_t ret = 0;
	void *addr;
	void *dest_addr;
	uint64_t entry_val = 0;
	void *index_key_buf;
	int32_t middle = 0;
	int32_t start_idx = 0;
	int32_t end_idx = node->header.numberOfEntriesInNode - 1;
	size_t num_of_bytes;
	// addr = (void *)(uint64_t)node + sizeof(node_header);

	if (node->header.numberOfEntriesInNode > 0) {
		while (1) {
			middle = (start_idx + end_idx) / 2;
			addr = (void *)(uint64_t)node + (uint64_t)sizeof(node_header) + sizeof(uint64_t) +
			       (uint64_t)(middle * 2 * sizeof(uint64_t));
			index_key_buf = bt_get_real_address(*(uint64_t *)addr);
			ret = bt_key_cmp(index_key_buf, key_buf, KV_FORMAT, KV_FORMAT);
			if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx)
					// addr is the same
					break;
			} else if (ret == 0) {
				log_fatal("key already present*");
				raise(SIGINT);
				exit(EXIT_FAILURE);
			} else {
				start_idx = middle + 1;
				if (start_idx > end_idx) {
					middle++;
					if (middle >= (int64_t)node->header.numberOfEntriesInNode) {
						middle = node->header.numberOfEntriesInNode;
						addr = (void *)(uint64_t)node + (uint64_t)sizeof(node_header) +
						       (uint64_t)(middle * 2 * sizeof(uint64_t)) + sizeof(uint64_t);
					} else
						addr += (2 * sizeof(uint64_t));
					break;
				}
			}
		}

		dest_addr = addr + (2 * sizeof(uint64_t));
		num_of_bytes = (node->header.numberOfEntriesInNode - middle) * 2 * sizeof(uint64_t);
		memmove(dest_addr, addr, num_of_bytes);
		addr -= sizeof(uint64_t);
	} else
		addr = (void *)node + sizeof(node_header);

	/*update the entry*/
	if (left_child != 0)
		entry_val = bt_get_absolute_address(left_child);
	else
		entry_val = 0;

	memcpy(addr, &entry_val, sizeof(uint64_t));
	addr += sizeof(uint64_t);
	entry_val = bt_get_absolute_address(key_buf);
	memcpy(addr, &entry_val, sizeof(uint64_t));

	addr += sizeof(uint64_t);
	if (right_child != 0)
		entry_val = bt_get_absolute_address(right_child);
	else
		entry_val = 0;

	memcpy(addr, &entry_val, sizeof(uint64_t));
	return 1;
}

/**
* @param   handle: database handle
* @param   node: address of the index node where the key should be inserted
* @param   left_child: address to the left child (full not absolute)
* @param   right_child: address to the left child (full not absolute)
* @param   key: address of the key to be inserted
* @param   key_len: size of the key
*/
static void insert_key_at_index(bt_insert_req *ins_req, index_node *node, node_header *left_child,
				node_header *right_child, uint64_t kv_offt, char allocation_code)
{
	void *key_addr = NULL;
	struct db_handle *handle = ins_req->metadata.handle;
	IN_log_header *d_header = NULL;
	IN_log_header *last_d_header = NULL;
	int32_t avail_space;
	int32_t req_space;
	int32_t allocated_space;

	struct bt_kv_log_address L = { .addr = NULL, .in_tail = 0, .tail_id = 0 };
	if (left_child->height == 0)
		L = bt_get_kv_log_address(ins_req->metadata.handle->db_desc, kv_offt);
	else
		L.addr = bt_get_real_address(kv_offt);

	// log_info("Pivot (kv_offt %llu) is %u:%s in tail? %d height %u", kv_offt,
	// *(uint32_t *)L.addr, L.addr + 4,
	//	 L.in_tail, left_child->height);
	uint32_t key_len = *(uint32_t *)L.addr;
	// assert(key_len < 30);
	int8_t ret;

	// assert_index_node(node);
	if (node->header.key_log_size % KEY_BLOCK_SIZE == 0)
		avail_space = 0;
	else
		avail_space = (int32_t)KEY_BLOCK_SIZE - (node->header.key_log_size % (int32_t)KEY_BLOCK_SIZE);

	req_space = (key_len + sizeof(uint32_t));
	if (avail_space < req_space) {
		/*room not sufficient get new block*/
		allocated_space = (req_space + sizeof(IN_log_header)) / KEY_BLOCK_SIZE;
		if ((req_space + sizeof(IN_log_header)) % KEY_BLOCK_SIZE != 0)
			allocated_space++;
		allocated_space *= KEY_BLOCK_SIZE;

		if (allocated_space > KEY_BLOCK_SIZE) {
			log_fatal("Cannot host index key larger than KEY_BLOCK_SIZE");
			assert(0);
			exit(EXIT_FAILURE);
		}
		d_header =
			seg_get_IN_log_block(handle->volume_desc, &handle->db_desc->levels[ins_req->metadata.level_id],
					     ins_req->metadata.tree_id, allocation_code);

		d_header->next = NULL;
		d_header->type = keyBlockHeader;
		last_d_header = (IN_log_header *)bt_get_real_address((uint64_t)node->header.last_IN_log_header);
		last_d_header->next = (void *)bt_get_absolute_address((void *)d_header);
		node->header.last_IN_log_header = last_d_header->next;
		node->header.key_log_size += (avail_space + sizeof(IN_log_header)); /* position the log to the
newly added block*/
	}
	/* put the KV now */
	key_addr = (void *)bt_get_real_address((uint64_t)node->header.last_IN_log_header +
					       (uint64_t)(node->header.key_log_size % KEY_BLOCK_SIZE));
	memcpy(key_addr, L.addr, sizeof(uint32_t) + key_len); /*key length */
	node->header.key_log_size += (sizeof(uint32_t) + key_len);

	if (L.in_tail)
		bt_done_with_value_log_address(ins_req->metadata.handle->db_desc, &L);

	ret = update_index(node, left_child, right_child, key_addr);

	if (ret)
		node->header.numberOfEntriesInNode++;
	// assert_index_node(node);
}

/*
* gesalous: Added at 13/06/2014 16:22. After the insertion of a leaf it's
* corresponding index will be updated
* for later use in efficient searching.
*/
static int bt_update_leaf_index(bt_insert_req *req, leaf_node *leaf, void *kv_buf, uint64_t kv_offt)
{
	struct kv_format *k_format = NULL;
	struct kv_prefix *k_prefix = NULL;
	struct kv_prefix k_prefix2 = { .prefix = { '\0' }, .device_offt = 0, .tombstone = 0 };
	void *query_key = NULL;
	if (req->metadata.key_format == KV_FORMAT) {
		k_format = (struct kv_format *)kv_buf;
		memcpy(k_prefix2.prefix, k_format->key_buf, MIN(k_format->key_size, PREFIX_SIZE));
		query_key = kv_buf;
		k_prefix = &k_prefix2;
	} else {
		/* operation coming from spill request or recovery (i.e. KV_PREFIX) */
		k_prefix = (struct kv_prefix *)kv_buf;
		query_key = bt_get_real_address(k_prefix->device_offt);
	}

	int64_t ret = 1;
	char *index_key_prefix = NULL;
	int32_t start_idx, end_idx, middle = 0;

	start_idx = 0;
	end_idx = leaf->header.numberOfEntriesInNode - 1;

	struct leaf_kv_pointer *leaf_entry = NULL;
	// leaf_entry = &leaf->kv_entry[0];

	while (leaf->header.numberOfEntriesInNode > 0) {
		middle = (start_idx + end_idx) / 2;
		leaf_entry = &leaf->kv_entry[middle];
		index_key_prefix = leaf->prefix[middle];

		ret = prefix_compare(index_key_prefix, k_prefix->prefix, PREFIX_SIZE);
		if (ret < 0) {
			// update_leaf_index_stats(req->key_format);
			goto up_leaf_1;
		} else if (ret > 0) {
			// update_leaf_index_stats(req->key_format);
			goto up_leaf_2;
		}

#ifdef PREFIX_STATISTICS
		if (key_format == KV_PREFIX)
			__sync_fetch_and_add(&ins_hack_miss, 1);
#endif
		// update_leaf_index_stats(req->key_format);
		int level_id = req->metadata.level_id;
		struct bt_kv_log_address index_L = { .addr = NULL, .in_tail = 0, .tail_id = UINT8_MAX };
		if (!level_id)
			index_L = bt_get_kv_log_address(req->metadata.handle->db_desc, leaf_entry->device_offt);
		else
			index_L.addr = bt_get_real_address(leaf_entry->device_offt);

		ret = bt_key_cmp(index_L.addr, query_key, KV_FORMAT, KV_FORMAT);

		if (index_L.in_tail)
			bt_done_with_value_log_address(req->metadata.handle->db_desc, &index_L);

		if (ret == 0) {
			if (req->metadata.gc_request && pointer_to_kv_in_log != index_L.addr)
				return ret;
			break;
		} else if (ret < 0) {
		up_leaf_1:
			start_idx = middle + 1;
			if (start_idx > end_idx) {
				middle++;
				move_leaf_data(leaf, middle);
				break;
			}
		} else if (ret > 0) {
		up_leaf_2:
			end_idx = middle - 1;
			if (start_idx > end_idx) {
				move_leaf_data(leaf, middle);
				break;
			}
		}
	}

	/*setup offset in the device*/
	uint64_t device_offt = 0;
	if (req->metadata.key_format == KV_FORMAT)
		device_offt = kv_offt;
	else /* KV_PREFIX */
		device_offt = k_prefix->device_offt;

	leaf->kv_entry[middle].device_offt = device_offt;
	leaf->kv_entry[middle].tombstone = req->metadata.is_tombstone;
	/*setup the prefix*/
	memcpy(&leaf->prefix[middle], k_prefix->prefix, PREFIX_SIZE);
	return ret;
}

char *node_type(nodeType_t type)
{
	switch (type) {
	case leafNode:
		return "leafNode";
	case leafRootNode:
		return "leafRootnode";
	case rootNode:
		return "rootNode";
	case internalNode:
		return "internalNode";
	default:
		assert(0);
	}
}

#if DEBUG_BTREE
void assert_leaf_node(node_header *leaf1)
{
	struct leaf_kv_pointer *prev, *curr = NULL;
	struct leaf_node *leaf = (struct leaf_node *)leaf1;
	int64_t ret;
	if (leaf1->numberOfEntriesInNode == 1) {
		return;
	}
	prev = &leaf->kv_entry[0];

	for (uint64_t i = 1; i < leaf->header.numberOfEntriesInNode; i++) {
		curr = &leaf->kv_entry[i];
		void *prev_full = bt_get_real_address(prev->device_offt);
		void *curr_full = bt_get_real_address(curr->device_offt);
		ret = bt_key_cmp(prev_full, curr_full, KV_FORMAT, KV_FORMAT);
		if (ret > 0) {
			log_fatal("corrupted leaf index at index %llu total entries %llu", (LLU)i,
				  (LLU)leaf->header.numberOfEntriesInNode);
			printf("previous key is: %s\n", (char *)prev + sizeof(int32_t));
			printf("curr key is: %s\n", (char *)curr + sizeof(int32_t));
			raise(SIGINT);
			exit(EXIT_FAILURE);
		}
	}
}
#endif

void print_key(void *key)
{
	char tmp[32];
	memset(tmp, 0, 32);
	memcpy(tmp, ((char *)key) + sizeof(uint32_t), 16);
	printf("|%s|\n", tmp);
}

/**
 * gesalous 05/06/2014 17:30
 * added method for splitting an index node
 * @ struct btree_hanlde * handle: The handle of the B+ tree
 * @ node_header * req->node: Node to be splitted
 * @ void * key : pointer to key
 */
static bt_split_result split_index(node_header *node, bt_insert_req *ins_req)
{
	bt_split_result result;
	result.middle_kv_offt = 0;
	node_header *left_child;
	node_header *right_child;
	node_header *tmp_index;
	void *full_addr;
	uint64_t kv_offt;
	uint32_t i = 0;
	// assert_index_node(node);
	result.left_child = (node_header *)seg_get_index_node(
		ins_req->metadata.handle->volume_desc,
		&ins_req->metadata.handle->db_desc->levels[ins_req->metadata.level_id], ins_req->metadata.tree_id,
		INDEX_SPLIT);

	result.right_child = (node_header *)seg_get_index_node(
		ins_req->metadata.handle->volume_desc,
		&ins_req->metadata.handle->db_desc->levels[ins_req->metadata.level_id], ins_req->metadata.tree_id,
		INDEX_SPLIT);
	/*initialize*/
	full_addr = (void *)((uint64_t)node + (uint64_t)sizeof(node_header));
	/*set node heights*/
	result.left_child->height = node->height;
	result.right_child->height = node->height;

	for (i = 0; i < node->numberOfEntriesInNode; i++) {
		if (i < node->numberOfEntriesInNode / 2)
			tmp_index = result.left_child;
		else
			tmp_index = result.right_child;

		left_child = (node_header *)bt_get_real_address(*(uint64_t *)full_addr);
		full_addr += sizeof(uint64_t);
		kv_offt = *(uint64_t *)full_addr;
		full_addr += sizeof(uint64_t);
		right_child = (node_header *)bt_get_real_address(*(uint64_t *)full_addr);
		if (i == node->numberOfEntriesInNode / 2) {
			result.middle_kv_offt = kv_offt;
			continue; /*middle key not needed, is going to the upper level*/
		}
		insert_key_at_index(ins_req, (index_node *)tmp_index, left_child, right_child, kv_offt, KEY_LOG_SPLIT);
	}

	return result;
}

/**
 *  gesalous 26/05/2014 added method. Appends a key-value pair in a leaf node.
 *  returns 0 on success 1 on failure. Changed the default layout of leafs
 **/
static int bt_insert_kv_at_leaf(bt_insert_req *ins_req, node_header *leaf)
{
	uint64_t kv_offt = 0;
	int ret;
	uint8_t level_id;

	level_id = ins_req->metadata.level_id;
	struct bt_kv_log_address L = { .addr = NULL, .in_tail = 0, .tail_id = 0 };

	if (ins_req->metadata.append_to_log && ins_req->metadata.key_format == KV_FORMAT) {
		log_operation append_op = { .metadata = &ins_req->metadata,
					    .optype_tolog = insertOp,
					    .ins_req = ins_req };

		if (ins_req->metadata.is_tombstone)
			append_op.optype_tolog = deleteOp;
#if VALUE_LOG_EXPLICIT_IO
		kv_offt = append_key_value_to_log_direct_IO(&append_op);
#else
		kv_offt = append_key_value_to_log(&append_op);
#endif
		// log_info("KV offt %llu", kv_offt);
		// assert(kv_offt > 20000000);
		// construct the kv address

		if (!level_id) {
			L = bt_get_kv_log_address(ins_req->metadata.handle->db_desc, kv_offt);
		} else {
			L.addr = bt_get_real_address(kv_offt);
		}
	} else if (!ins_req->metadata.append_to_log && ins_req->metadata.key_format == KV_PREFIX) {
		L.addr = ins_req->key_value_buf;
		kv_offt = 0;
	} else {
		log_fatal("Wrong combination of key format / append_to_log option");
		exit(EXIT_FAILURE);
	}

	if (bt_update_leaf_index(ins_req, (leaf_node *)leaf, L.addr, kv_offt) != 0) {
		++leaf->numberOfEntriesInNode;
		__sync_fetch_and_add(
			&(ins_req->metadata.handle->db_desc->levels[level_id].level_size[ins_req->metadata.tree_id]),
			1);
		ret = 1;
	} else {
		/*if key already present at the leaf, must be an update or an append*/
		leaf->fragmentation++;
		ret = 0;
	}
	if (L.in_tail)
		bt_done_with_value_log_address(ins_req->metadata.handle->db_desc, &L);

	return ret;
}

static bt_split_result bt_split_leaf(bt_insert_req *req, leaf_node *node)
{
	leaf_node *node_copy;
	bt_split_result rep;
	rep.middle_kv_offt = 0;
	uint8_t level_id = req->metadata.level_id;
	/*cow check*/
	if (node->header.epoch <= req->metadata.handle->volume_desc->dev_catalogue->epoch) {
		level_id = req->metadata.level_id;
		node_copy = seg_get_leaf_node_header(req->metadata.handle->volume_desc,
						     &req->metadata.handle->db_desc->levels[level_id],
						     req->metadata.tree_id, COW_FOR_LEAF);

		memcpy(node_copy, node, LEAF_NODE_SIZE);
		node_copy->header.epoch = req->metadata.handle->volume_desc->mem_catalogue->epoch;
		node = node_copy;
	}

	rep.left_lchild = node;
	// rep.left_lchild->header.v1++;
	/*right leaf*/
	rep.right_lchild =
		seg_get_leaf_node(req->metadata.handle->volume_desc, &req->metadata.handle->db_desc->levels[level_id],
				  req->metadata.tree_id, LEAF_SPLIT);
	int split_point;
	int left_entries;
	int right_entries;
	if (req->metadata.special_split) {
		split_point = node->header.numberOfEntriesInNode - 1;
		left_entries = node->header.numberOfEntriesInNode - 1;
		right_entries = 1;
	} else {
		split_point = node->header.numberOfEntriesInNode / 2;
		left_entries = node->header.numberOfEntriesInNode / 2;
		right_entries = node->header.numberOfEntriesInNode - (node->header.numberOfEntriesInNode / 2);
	}

	rep.middle_kv_offt = node->kv_entry[split_point].device_offt;
	// log_info("Split point[%u] dev_offt = %llu", split_point,
	// rep.middle_kv_offt);
	/* pointers */
	memcpy(&(rep.right_lchild->kv_entry[0]), &node->kv_entry[split_point],
	       right_entries * sizeof(struct leaf_kv_pointer));

	/* prefixes */
	memcpy(&(rep.right_lchild->prefix[0]), &(node->prefix[split_point]), right_entries * PREFIX_SIZE);

	rep.right_lchild->header.numberOfEntriesInNode = right_entries;
	rep.right_lchild->header.type = leafNode;

	rep.right_lchild->header.height = node->header.height;
	/*left leaf*/
	rep.left_lchild->header.height = node->header.height;
	rep.left_lchild->header.numberOfEntriesInNode = left_entries;

	if (node->header.type == leafRootNode) {
		rep.left_lchild->header.type = leafNode;
		// printf("[%s:%s:%d] leafRoot node splitted\n",__FILE__,__func__,__LINE__);
		rep.stat = LEAF_ROOT_NODE_SPLITTED;
	} else
		rep.stat = KREON_OK;

	// rep.left_lchild->header.v2++; /*lamport counter*/
	// rep.right_lchild->header.v2++; /*lamport counter*/
	return rep;
}

/**
 *	gesalous added at 30/05/2014 14:00, performs a binary search at an
 *index(root, internal node) and returns the index. We have
 *  a separate search function for index and leaves due to their different
 *format
 *  Updated (26/10/2016 17:05) key_buf can be in two formats
 *
 **/
void *_index_node_binary_search(index_node *node, void *key_buf, char query_key_format)
{
	void *addr = NULL;
	void *index_key_buf;
	int64_t ret;
	int32_t middle = 0;
	int32_t start_idx = 0;
	int32_t end_idx = node->header.numberOfEntriesInNode - 1;
	int32_t numberOfEntriesInNode = node->header.numberOfEntriesInNode;

	while (numberOfEntriesInNode > 0) {
		middle = (start_idx + end_idx) / 2;

		if (numberOfEntriesInNode > index_order || middle < 0 || middle >= numberOfEntriesInNode)
			return NULL;

		addr = &(node->p[middle].pivot);
		index_key_buf = (void *)bt_get_real_address(*(uint64_t *)addr);
		ret = bt_key_cmp(index_key_buf, key_buf, KV_FORMAT, query_key_format);
		if (ret == 0) {
			// log_debug("I passed from this corner case1 %s",
			// (char*)(index_key_buf+4));
			addr = &(node->p[middle].right[0]);
			break;
		} else if (ret > 0) {
			end_idx = middle - 1;
			if (start_idx > end_idx) {
				// log_debug("I passed from this corner case2 %s",
				// (char*)(index_key_buf+4));
				addr = &(node->p[middle].left[0]);
				middle--;
				break;
			}
		} else { /* ret < 0 */
			start_idx = middle + 1;
			if (start_idx > end_idx) {
				// log_debug("I passed from this corner case3 %s",
				// (char*)(index_key_buf+4));
				addr = &(node->p[middle].right[0]);
				middle++;
				break;
			}
		}
	}

	if (middle < 0) {
		// log_debug("I passed from this corner case4 %s",
		// (char*)(index_key_buf+4));
		addr = &(node->p[0].left[0]);
	} else if (middle >= (int64_t)node->header.numberOfEntriesInNode) {
		// log_debug("I passed from this corner case5 %s",
		// (char*)(index_key_buf+4));
		/* log_debug("I passed from this corner case2 %s",
* (char*)(index_key_buf+4)); */
		addr = &(node->p[node->header.numberOfEntriesInNode - 1].right[0]);
	}
	// log_debug("END");
	return addr;
}

/*functions used for debugging*/
void assert_index_node(node_header *node)
{
	uint32_t k;
	void *key_tmp;
	void *key_tmp_prev = NULL;
	void *addr;
	node_header *child;
	addr = (void *)(uint64_t)node + (uint64_t)sizeof(node_header);
	if (node->numberOfEntriesInNode == 0)
		return;
	//	if(node->height > 1)
	//	log_info("Checking node of height %lu\n",node->height);
	for (k = 0; k < node->numberOfEntriesInNode; k++) {
		/*check child type*/
		child = (node_header *)bt_get_real_address(*(uint64_t *)addr);
		if (child->type != rootNode && child->type != internalNode && child->type != leafNode &&
		    child->type != leafRootNode) {
			log_fatal("corrupted child at index for child %llu type is %d\n", (LLU)(uint64_t)child - MAPPED,
				  child->type);
			raise(SIGINT);
			exit(EXIT_FAILURE);
		}
		addr += sizeof(uint64_t);
		key_tmp = (void *)MAPPED + *(uint64_t *)addr;
		// log_info("key %s\n", (char *)key_tmp + sizeof(int32_t));

		if (key_tmp_prev != NULL) {
			if (bt_key_cmp(key_tmp_prev, key_tmp, KV_FORMAT, KV_FORMAT) >= 0) {
				log_fatal("corrupted index %d:%s something else %d:%s\n", *(uint32_t *)key_tmp_prev,
					  key_tmp_prev + 4, *(uint32_t *)key_tmp, key_tmp + 4);
				raise(SIGINT);
				exit(EXIT_FAILURE);
			}
		}

		key_tmp_prev = key_tmp;
		addr += sizeof(uint64_t);
	}
	child = (node_header *)(MAPPED + *(uint64_t *)addr);
	if (child->type != rootNode && child->type != internalNode && child->type != leafNode &&
	    child->type != leafRootNode) {
		log_fatal("Corrupted last child at index");
		exit(EXIT_FAILURE);
	}
	// printf("\t\tpointer to last child %llu\n", (LLU)(uint64_t)child-MAPPED);
}

void print_node(node_header *node)
{
	printf("\n***Node synopsis***\n");
	if (node == NULL) {
		printf("NULL\n");
		return;
	}
	// printf("DEVICE OFFSET = %llu\n", (uint64_t)node - MAPPED);
	printf("type = %d\n", node->type);
	printf("total entries = %llu\n", (LLU)node->numberOfEntriesInNode);
	printf("epoch = %llu\n", (LLU)node->epoch);
	printf("height = %llu\n", (LLU)node->height);
	printf("fragmentation = %llu\n", (LLU)node->fragmentation);
}

uint64_t hash(uint64_t x)
{
	x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
	x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
	x = x ^ (x >> 31);
	return x;
}

lock_table *_find_position(lock_table **table, node_header *node)
{
	unsigned long position;
	lock_table *return_value;

	if (node->height < 0 || node->height >= MAX_HEIGHT) {
		log_fatal("MAX_HEIGHT exceeded %d rearrange values in size_per_height array ", node->height);
		raise(SIGINT);
		exit(EXIT_FAILURE);
	}

	position = hash((uint64_t)node) % size_per_height[node->height];
	// log_info("node %llu height %d position %lu size of height %d", node,
	// node->height, position, size_per_height[node->height]);
	return_value = table[node->height];
	return &return_value[position];
}

void _unlock_upper_levels(lock_table *node[], unsigned size, unsigned release)
{
	unsigned i;
	for (i = release; i < size; ++i)
		if (RWLOCK_UNLOCK(&node[i]->rx_lock) != 0) {
			log_fatal("ERROR unlocking");
			exit(EXIT_FAILURE);
		}
}

#if 0
node_header *allocate_root(insertKV_request *req, node_header *son)
{
	node_header *node_copy = (*req->allocator_desc.allocate_space)((void *)req->allocator_desc.handle, NODE_SIZE,
								       req->allocator_desc.level_id, NEW_ROOT);
	memcpy(node_copy, son, NODE_SIZE);
	node_copy->epoch = req->handle->volume_desc->soft_superindex->epoch;
	node_copy->v1 = 0;
	node_copy->v2 = 0;
	return node_copy;
}
node_header *rcuLock(node_header *node, db_descriptor *db_desc, insertKV_request *req)
{
	if (node && (node->type == leafRootNode || node->type == rootNode)) {
		MUTEX_LOCK(&db_desc->rcu_root);
		__sync_fetch_and_add(&db_desc->rcu_root_v1, 1);
		return (req->level_id != NUM_OF_TREES_PER_LEVEL) ? db_desc->root_w[db_desc->active_tree] :
								   db_desc->root_w[NUM_OF_TREES_PER_LEVEL];
	}

	return NULL;
}

void rcuUnlock(node_header *node, db_descriptor *db_desc, insertKV_request *req)
{
	int i = (req->level_id != NUM_OF_TREES_PER_LEVEL) ? db_desc->active_tree : NUM_OF_TREES_PER_LEVEL;
	if (node)
		db_desc->root_w[i] = node;

	__sync_fetch_and_add(&db_desc->rcu_root_v2, 1);
	assert(db_desc->rcu_root_v1 == db_desc->rcu_root_v2);
	MUTEX_UNLOCK(&db_desc->rcu_root);
}

int splitValidation(node_header *father, node_header *son, db_descriptor *db_desc, split_request *split_req,
		    uint32_t order, split_data *data, insertKV_request *req)
{
	node_header *flag = NULL;
	int flow_control = 0;
	uint32_t temp_order;
	data->son = data->father = NULL;

	if (son->type == leafRootNode || son->type == rootNode || (father && father->type == rootNode)) {
		flag = rcuLock(son, db_desc, req);
		if (!flag) {
			flag = rcuLock(father, db_desc, req);
			if (flag)
				flow_control = 1;
		} else {
			flow_control = 2;
		}

		if (flag->type == leafRootNode)
			temp_order = leaf_order;
		else
			temp_order = index_order;
		if (flow_control == 2) { // son = root
			if (son->numberOfEntriesInNode != flag->numberOfEntriesInNode || son->height != flag->height ||
			    flag->numberOfEntriesInNode < temp_order) {
				rcuUnlock(NULL, db_desc, req);
				data->son = data->father = NULL;
				return -1;
			}

			if (flag->type == leafRootNode || flag->type == rootNode) {
				data->son = flag;
				return 1;
			}
			assert(0);

		} else if (flow_control == 1) {
			if (father->numberOfEntriesInNode != flag->numberOfEntriesInNode ||
			    father->height != flag->height || flag->numberOfEntriesInNode >= index_order ||
			    (flag->height - son->height) != 1) {
				rcuUnlock(NULL, db_desc, req);
				data->son = data->father = NULL;
				return -1;
			}

			if (flag->type == rootNode) { // I am a root child and i should acquire
				// its lock in order to insert the pivot
				// after the split.
				data->father = flag;
				return 1;
			}
			assert(0);
		}
	}
	if (son->type == leafRootNode || son->type == rootNode)
		assert(0);
	if (father && father->type == rootNode)
		assert(0);
	return 0;
}
#endif

void init_leaf_node(leaf_node *node)
{
	node->header.fragmentation = 0;
	// node->header.v1 = 0;
	// node->header.v2 = 0;
	node->header.first_IN_log_header = NULL;
	node->header.last_IN_log_header = NULL;
	node->header.key_log_size = 0;
	node->header.height = 0;
	node->header.type = leafNode;
	node->header.numberOfEntriesInNode = 0;
}

static void init_index_node(index_node *node)
{
	node->header.fragmentation = 0;
	// node->header.v1 = 0;
	// node->header.v2 = 0;
	node->header.type = internalNode;
	node->header.numberOfEntriesInNode = 0;
}

uint8_t _concurrent_insert(bt_insert_req *ins_req)
{
	/*The array with the locks that belong to this thread from upper levels*/
	lock_table *upper_level_nodes[MAX_HEIGHT];
	bt_split_result split_res;

	lock_table *lock;
	void *next_addr;
	pr_system_catalogue *mem_catalogue;
	volume_descriptor *volume_desc;
	db_descriptor *db_desc;

	index_node *new_index_node;
	node_header *node_copy;
	node_header *father;
	node_header *son;
	uint64_t addr;
	int64_t ret;
	unsigned size; /*Size of upper_level_nodes*/
	unsigned release; /*Counter to know the position that releasing should begin
*/
	uint32_t order;

	// remove some warnings here
	(void)ret;
	(void)addr;

	lock_table *guard_of_level;
	int64_t *num_level_writers;
	uint32_t level_id;

	volume_desc = ins_req->metadata.handle->volume_desc;
	db_desc = ins_req->metadata.handle->db_desc;
	level_id = ins_req->metadata.level_id;
	guard_of_level = &(db_desc->levels[level_id].guard_of_level);
	num_level_writers = &db_desc->levels[level_id].active_writers;

	release = 0;
	size = 0;

	int retry = 0;
release_and_retry:
	if (retry) {
		// retry = 0;
		_unlock_upper_levels(upper_level_nodes, size, release);
		__sync_fetch_and_sub(num_level_writers, 1);
		if (ins_req->metadata.level_id == 0 && ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
			__sync_fetch_and_sub(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
		}
	}

	retry = 1;
	size = 0;
	release = 0;
	if (RWLOCK_WRLOCK(&guard_of_level->rx_lock)) {
		log_fatal("Failed to acquire guard lock for level %u", level_id);
		exit(EXIT_FAILURE);
	}
	/*now look which is the active_tree of L0*/
	if (ins_req->metadata.level_id == 0) {
		ins_req->metadata.tree_id = ins_req->metadata.handle->db_desc->levels[0].active_tree;
	}
	/*level's guard lock aquired*/
	upper_level_nodes[size++] = guard_of_level;
	/*mark your presence*/
	__sync_fetch_and_add(num_level_writers, 1);
	if (ins_req->metadata.level_id == 0 && ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
		__sync_fetch_and_add(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
	}

	mem_catalogue = ins_req->metadata.handle->volume_desc->mem_catalogue;

	father = NULL;

	/*cow logic follows*/
	if (db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] == NULL) {
		if (db_desc->levels[level_id].root_r[ins_req->metadata.tree_id] != NULL) {
			if (db_desc->levels[level_id].root_r[ins_req->metadata.tree_id]->type == rootNode) {
				index_node *t = seg_get_index_node_header(ins_req->metadata.handle->volume_desc,
									  &db_desc->levels[level_id],
									  ins_req->metadata.tree_id, NEW_ROOT);
				memcpy(t, db_desc->levels[level_id].root_r[ins_req->metadata.tree_id], INDEX_NODE_SIZE);
				t->header.epoch = mem_catalogue->epoch;
				db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] = (node_header *)t;
			} else {
				/*Tree too small consists only of 1 leafRootNode*/
				leaf_node *t = seg_get_leaf_node_header(ins_req->metadata.handle->volume_desc,
									&db_desc->levels[level_id],
									ins_req->metadata.tree_id, COW_FOR_LEAF);

				memcpy(t, db_desc->levels[level_id].root_r[ins_req->metadata.tree_id], LEAF_NODE_SIZE);
				t->header.epoch = mem_catalogue->epoch;
				db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] = (node_header *)t;
			}
		} else {
			/*we are allocating a new tree*/

			/*log_info("Allocating new active tree %d for level id %d epoch is at %llu",*/
			/*ins_req->metadata.tree_id, level_id, (LLU)mem_catalogue->epoch);*/

			leaf_node *t =
				seg_get_leaf_node(ins_req->metadata.handle->volume_desc, &db_desc->levels[level_id],
						  ins_req->metadata.tree_id, NEW_ROOT);
			init_leaf_node(t);
			t->header.type = leafRootNode;
			t->header.epoch = mem_catalogue->epoch;
			db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] = (node_header *)t;
		}
	}
	/*acquiring lock of the current root*/
	lock = _find_position(db_desc->levels[level_id].level_lock_table,
			      db_desc->levels[level_id].root_w[ins_req->metadata.tree_id]);
	if (RWLOCK_WRLOCK(&lock->rx_lock) != 0) {
		log_fatal("ERROR locking");
		exit(EXIT_FAILURE);
	}
	upper_level_nodes[size++] = lock;
	son = db_desc->levels[level_id].root_w[ins_req->metadata.tree_id];

	while (1) {
		if (son->type == leafNode || son->type == leafRootNode)
			order = leaf_order;
		else
			order = index_order;
		/*Check if father is safe it should be*/
		if (father) {
			unsigned int father_order;
			if (father->type == leafNode || father->type == leafRootNode)
				father_order = leaf_order;
			else
				father_order = index_order;
			assert(father->epoch > volume_desc->dev_catalogue->epoch);
			assert(father->numberOfEntriesInNode < father_order);
		}
		if (son->numberOfEntriesInNode >= order) {
			/*Overflow split*/
			if (son->height > 0) {
				// son->v1++;
				split_res = split_index(son, ins_req);
				/*node has splitted, free it*/
				seg_free_index_node(ins_req->metadata.handle->volume_desc, &db_desc->levels[level_id],
						    ins_req->metadata.tree_id, (index_node *)son);
				// son->v2++;
			} else {
				// son->v1++;
				split_res = bt_split_leaf(ins_req, (leaf_node *)son);
				if ((uint64_t)son != (uint64_t)split_res.left_child) {
					/*cow happened*/
					seg_free_leaf_node(ins_req->metadata.handle->volume_desc,
							   &ins_req->metadata.handle->db_desc->levels[level_id],
							   ins_req->metadata.tree_id, (leaf_node *)son);
					/*fix the dangling lamport*/
					// split_res.left_child->v2++;
				} // else
				// son->v2++;
			}
			/*Insert pivot at father*/
			if (father != NULL) {
				/*lamport counter*/
				// father->v1++;
				insert_key_at_index(ins_req, (index_node *)father, split_res.left_child,
						    split_res.right_child, split_res.middle_kv_offt, KEY_LOG_EXPANSION);

				// log_info("pivot Key is %d:%s\n", *(uint32_t
				// *)split_res.middle_key_buf,
				//	 split_res.middle_key_buf + 4);
				// log_info("key at root entries now %llu checking health now",
				//	 father->numberOfEntriesInNode);
				// if (split_res.left_child->type != leafNode) {
				//	assert_index_node(split_res.left_child);
				//	assert_index_node(split_res.right_child);
				//}
				// assert_index_node(father);
				// log_info("node healthy!");

				/*lamport counter*/
				// father->v2++;
			} else {
				/*Root was splitted*/
				// log_info("new root");
				new_index_node = seg_get_index_node(ins_req->metadata.handle->volume_desc,
								    &db_desc->levels[level_id],
								    ins_req->metadata.tree_id, NEW_ROOT);
				new_index_node->header.height =
					ins_req->metadata.handle->db_desc->levels[ins_req->metadata.level_id]
						.root_w[ins_req->metadata.tree_id]
						->height +
					1;

				init_index_node(new_index_node);
				new_index_node->header.type = rootNode;
				// new_index_node->header.v1++; /*lamport counter*/
				// son->v1++;
				insert_key_at_index(ins_req, new_index_node, split_res.left_child,
						    split_res.right_child, split_res.middle_kv_offt, KEY_LOG_EXPANSION);

				// new_index_node->header.v2++; /*lamport counter*/
				// son->v2++;
				/*new write root of the tree*/
				db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] =
					(node_header *)new_index_node;
			}
			goto release_and_retry;
		} else if (son->epoch <= volume_desc->dev_catalogue->epoch) {
			/*Cow*/
			if (son->height > 0) {
				node_copy = (node_header *)seg_get_index_node_header(
					ins_req->metadata.handle->volume_desc, &db_desc->levels[level_id],
					ins_req->metadata.tree_id, COW_FOR_INDEX);
				memcpy(node_copy, son, INDEX_NODE_SIZE);
				seg_free_index_node_header(ins_req->metadata.handle->volume_desc,
							   &db_desc->levels[level_id], ins_req->metadata.tree_id, son);

			} else {
				node_copy = (node_header *)seg_get_leaf_node_header(
					ins_req->metadata.handle->volume_desc, &db_desc->levels[level_id],
					ins_req->metadata.tree_id, COW_FOR_LEAF);
				memcpy(node_copy, son, LEAF_NODE_SIZE);
				seg_free_leaf_node(ins_req->metadata.handle->volume_desc, &db_desc->levels[level_id],
						   ins_req->metadata.tree_id, (leaf_node *)son);
			}
			node_copy->epoch = mem_catalogue->epoch;
			son = node_copy;
			/*Update father's pointer*/
			if (father != NULL) {
				// father->v1++; /*lamport counter*/
				*(uint64_t *)next_addr = (uint64_t)node_copy - MAPPED;
				// father->v2++; /*lamport counter*/
			} else { /*We COWED the root*/
				db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] = node_copy;
			}
			// log_info("son->epoch = %llu volume_desc->dev_catalogue->epoch %llu mem
			// "
			//        "epoch %llu",
			//         son->epoch, volume_desc->dev_catalogue->epoch,
			//         volume_desc->mem_catalogue->epoch);
			goto release_and_retry;
		}

		if (son->height == 0)
			break;
		/*Finding the next node to traverse*/
		next_addr = _index_node_binary_search((index_node *)son, ins_req->key_value_buf,
						      ins_req->metadata.key_format);
		father = son;
		/*Taking the lock of the next node before its traversal*/
		lock = _find_position(ins_req->metadata.handle->db_desc->levels[level_id].level_lock_table,
				      (node_header *)bt_get_real_address(*(uint64_t *)next_addr));
		upper_level_nodes[size++] = lock;
		if (RWLOCK_WRLOCK(&lock->rx_lock) != 0) {
			log_fatal("ERROR unlocking reason follows rc %d");
			exit(EXIT_FAILURE);
		}
		/*Node acquired */
		son = (node_header *)bt_get_real_address(*(uint64_t *)next_addr);
		if (son->type == leafNode || son->type == leafRootNode)
			order = leaf_order;
		else
			order = index_order;
		/*if the node is not safe hold its ancestor's lock else release locks from
* ancestors */
		if (!(son->epoch <= volume_desc->dev_catalogue->epoch || son->numberOfEntriesInNode >= order)) {
			_unlock_upper_levels(upper_level_nodes, size - 1, release);
			release = size - 1;
		}
	}
	/*Succesfully reached a bin (bottom internal node)*/
	if (son->type != leafRootNode)
		assert((size - 1) - release == 0);

	if (son->height != 0) {
		log_fatal("FATAL son corrupted");
		exit(EXIT_FAILURE);
	}

	// son->v1++;
	ret = bt_insert_kv_at_leaf(ins_req, son);
	// son->v2++;
	/*Unlock remaining locks*/
	_unlock_upper_levels(upper_level_nodes, size, release);
	// if (ins_req->metadata.level_id == 0 &&
	// ins_req->metadata.handle->db_desc->is_in_replicated_mode)
	//	return SUCCESS;
	// else {
	__sync_fetch_and_sub(num_level_writers, 1);
	return SUCCESS;
	//}
}

static uint8_t _writers_join_as_readers(bt_insert_req *ins_req)
{
	/*The array with the locks that belong to this thread from upper levels*/
	lock_table *upper_level_nodes[MAX_HEIGHT];
	void *next_addr;
	volume_descriptor *volume_desc;
	db_descriptor *db_desc;
	node_header *son;
	lock_table *lock;

	uint64_t addr;
	int64_t ret;
	unsigned size; /*Size of upper_level_nodes*/
	unsigned release; /*Counter to know the position that releasing should begin
*/
	uint32_t order;

	// remove some warnings here
	(void)ret;
	(void)addr;
	uint32_t level_id;
	lock_table *guard_of_level;
	int64_t *num_level_writers;

	volume_desc = ins_req->metadata.handle->volume_desc;
	db_desc = ins_req->metadata.handle->db_desc;
	level_id = ins_req->metadata.level_id;
	guard_of_level = &db_desc->levels[level_id].guard_of_level;
	num_level_writers = &db_desc->levels[level_id].active_writers;

	size = 0;
	release = 0;

	/*
* Caution no retry here, we just optimistically try to insert,
* if we donot succeed we try with concurrent_insert
*/
	/*Acquire read guard lock*/
	ret = RWLOCK_RDLOCK(&guard_of_level->rx_lock);
	if (ret) {
		log_fatal("Failed to acquire guard lock for db: %s", db_desc->db_name);
		perror("Reason: ");
		exit(EXIT_FAILURE);
	}
	/*now look which is the active_tree of L0*/
	if (ins_req->metadata.level_id == 0)
		ins_req->metadata.tree_id = ins_req->metadata.handle->db_desc->levels[0].active_tree;

	/*mark your presence*/
	__sync_fetch_and_add(num_level_writers, 1);
	if (ins_req->metadata.level_id == 0 && ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
		__sync_fetch_and_add(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
	}
	upper_level_nodes[size++] = guard_of_level;

	if (db_desc->levels[level_id].root_w[ins_req->metadata.tree_id] == NULL ||
	    db_desc->levels[level_id].root_w[ins_req->metadata.tree_id]->type == leafRootNode) {
		_unlock_upper_levels(upper_level_nodes, size, release);
		__sync_fetch_and_sub(num_level_writers, 1);
		if (ins_req->metadata.level_id == 0 && ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
			__sync_fetch_and_sub(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
		}
		return FAILURE;
	}

	/*acquire read lock of the current root*/
	lock = _find_position(db_desc->levels[level_id].level_lock_table,
			      db_desc->levels[level_id].root_w[ins_req->metadata.tree_id]);
	if (RWLOCK_RDLOCK(&lock->rx_lock) != 0) {
		log_fatal("ERROR locking");
		exit(EXIT_FAILURE);
	}
	upper_level_nodes[size++] = lock;
	son = db_desc->levels[level_id].root_w[ins_req->metadata.tree_id];
	while (1) {
		if (son->type == leafNode || son->type == leafRootNode)
			order = leaf_order;
		else
			order = index_order;
		if (son->numberOfEntriesInNode >= order) {
			/*failed needs split*/
			_unlock_upper_levels(upper_level_nodes, size, release);
			__sync_fetch_and_sub(num_level_writers, 1);
			if (ins_req->metadata.level_id == 0 &&
			    ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
				__sync_fetch_and_sub(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
			}
			return FAILURE;
		} else if (son->epoch <= volume_desc->dev_catalogue->epoch) {
			/*failed needs COW*/
			_unlock_upper_levels(upper_level_nodes, size, release);
			__sync_fetch_and_sub(num_level_writers, 1);
			if (ins_req->metadata.level_id == 0 &&
			    ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
				__sync_fetch_and_sub(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
			}
			return FAILURE;
		}

		/*Find the next node to traverse*/
		next_addr = _index_node_binary_search((index_node *)son, ins_req->key_value_buf,
						      ins_req->metadata.key_format);
		son = (node_header *)bt_get_real_address(*(uint64_t *)next_addr);
		if (son->height == 0)
			break;
		/*Acquire the lock of the next node before its traversal*/
		lock = _find_position(db_desc->levels[level_id].level_lock_table,
				      (node_header *)bt_get_real_address(*(uint64_t *)next_addr));
		upper_level_nodes[size++] = lock;
		if (RWLOCK_RDLOCK(&lock->rx_lock) != 0) {
			log_fatal("ERROR unlocking");
			exit(EXIT_FAILURE);
		}
		/*lock of node acquired */
		_unlock_upper_levels(upper_level_nodes, size - 1, release);
		release = size - 1;
	}

	lock = _find_position(db_desc->levels[level_id].level_lock_table,
			      (node_header *)bt_get_real_address(*(uint64_t *)next_addr));
	upper_level_nodes[size++] = lock;
	if (RWLOCK_WRLOCK(&lock->rx_lock) != 0) {
		log_fatal("ERROR unlocking");
		exit(EXIT_FAILURE);
	}

	if (son->numberOfEntriesInNode >= (uint32_t)leaf_order || son->epoch <= volume_desc->dev_catalogue->epoch) {
		_unlock_upper_levels(upper_level_nodes, size, release);
		__sync_fetch_and_sub(num_level_writers, 1);
		if (ins_req->metadata.level_id == 0 && ins_req->metadata.handle->db_desc->is_in_replicated_mode) {
			__sync_fetch_and_sub(&ins_req->metadata.handle->db_desc->pending_replica_operations, 1);
		}
		return FAILURE;
	}
	/*Succesfully reached a bin (bottom internal node)*/
	if (son->height != 0) {
		log_fatal("son corrupted");
		exit(EXIT_FAILURE);
	}
	// son->v1++;
	ret = bt_insert_kv_at_leaf(ins_req, son);
	// son->v2++;
	/*Unlock remaining locks*/
	_unlock_upper_levels(upper_level_nodes, size, release);
	// if (ins_req->metadata.level_id == 0 &&
	// ins_req->metadata.handle->db_desc->is_in_replicated_mode)
	//	return SUCCESS;
	// else {
	__sync_fetch_and_sub(num_level_writers, 1);
	//	return SUCCESS;
	//}
	return SUCCESS;
}
