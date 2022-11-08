#include "build_index.h"
#include "btree/kv_pairs.h"
#include "btree/lsn.h"
#include "log.h"
#include "metadata.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include "rdma_buffer_iterator.h"
#include <stdlib.h>
#include <unistd.h>

static void insert_and_proceed_iterator(rdma_buffer_iterator_t iterator, struct krm_region_desc *r_desc)
{
	par_handle *db = r_desc->db;
	const char *error_message = NULL;
	struct kv_splice *kv = rdma_buffer_iterator_get_kv(iterator);
	log_fatal("OH NO THIS IS BAD ");
	exit(EXIT_FAILURE);
	par_put_serialized(db, (char *)kv, &error_message);
	if (error_message) {
		log_fatal("Error uppon inserting key %s, key_size %u", get_key_offset_in_kv(kv), get_key_size(kv));
		_exit(EXIT_FAILURE);
	}

	rdma_buffer_iterator_next(iterator);
}

void rco_build_index(struct rco_build_index_task *task)
{
	rdma_buffer_iterator_t l0_recovery_iterator =
		rdma_buffer_iterator_init(task->l0_recovery_rdma_buffer, task->rdma_buffers_size);
	rdma_buffer_iterator_t big_recovery_iterator =
		rdma_buffer_iterator_init(task->big_recovery_rdma_buffer, task->rdma_buffers_size);

	while (rdma_buffer_iterator_is_valid(l0_recovery_iterator) == VALID &&
	       rdma_buffer_iterator_is_valid(big_recovery_iterator) == VALID) {
		struct lsn *curr_l0_recovery_lsn = rdma_buffer_iterator_get_lsn(l0_recovery_iterator);
		struct lsn *curr_big_recovery_lsn = rdma_buffer_iterator_get_lsn(big_recovery_iterator);

		int64_t ret = compare_lsns(curr_l0_recovery_lsn, curr_big_recovery_lsn);
		log_debug("Comparing l0_rec %ld, big %ld", curr_l0_recovery_lsn->id, curr_big_recovery_lsn->id);

		if (ret > 0) {
			log_debug("l0 recovery procceeding");
			insert_and_proceed_iterator(l0_recovery_iterator, task->r_desc);
		} else if (ret < 0) {
			log_debug("big recovery procceeding");
			insert_and_proceed_iterator(big_recovery_iterator, task->r_desc);
		} else {
			log_fatal("Found a duplicate lsn > 0 in both rdma buffers, this should never happen");
			_exit(EXIT_FAILURE);
		}
	}

	rdma_buffer_iterator_t unterminated_iterator = l0_recovery_iterator;
	if (rdma_buffer_iterator_is_valid(l0_recovery_iterator) == INVALID)
		unterminated_iterator = big_recovery_iterator;
	//proceed unterminated iterator until the end-of-buffer
	while (rdma_buffer_iterator_is_valid(unterminated_iterator) == VALID) {
		insert_and_proceed_iterator(unterminated_iterator, task->r_desc);
	}
#if 0
	struct rco_key {
		uint32_t size;
		char key[];
	};
	struct rco_value {
		uint32_t size;
		char value[];
	};

	// parse log entries
	char *kv = NULL;
	struct rco_key *key = NULL;
	struct rco_value *value = NULL;
	struct segment_header *curr_segment = task->segment;
	uint64_t log_offt = task->log_start;
	key = (struct rco_key *)((uint64_t)curr_segment + sizeof(struct segment_header));

	uint32_t remaining = SEGMENT_SIZE - sizeof(struct segment_header);
	while (1) {
		//db_desc->dirty = 0x01;
		struct bt_insert_req ins_req;
		//ins_req.metadata.handle = task->r_desc->db;
		ins_req.metadata.level_id = 0;
		ins_req.metadata.tree_id = 0; // will be filled properly by the engine
		ins_req.metadata.special_split = 0;
		ins_req.metadata.key_format = KV_FORMAT;
		ins_req.metadata.append_to_log = 1;
		kv = (char *)key;
		ins_req.key_value_buf = kv;
		//int active_tree = task->r_desc->db->db_desc->levels[0].active_tree;
		//if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
		//	pthread_mutex_lock(&db_desc->client_barrier_lock);
		//	active_tree = db_desc->levels[0].active_tree;

		//	if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
		//		sem_post(&db_desc->compaction_daemon_interrupts);
		//		if (pthread_cond_wait(&db_desc->client_barrier, &db_desc->client_barrier_lock) != 0) {
		//			log_fatal("failed to throttle");
		//			exit(EXIT_FAILURE);
		//		}
		//	}
		//	pthread_mutex_unlock(&db_desc->client_barrier_lock);
		//}
		/*log_info("Level 0 size %d", db_desc->levels[0].level_size[active_tree]);*/
		/*log_info("Adding index entry for key %u:%s offset %llu log end %llu",*/
		/*		key->size, key->key, log_offt, task->log_end);*/
		remaining -= (sizeof(struct rco_key) + key->size);
		_insert_key_value(&ins_req);
		value = (struct rco_value *)((uint64_t)key + sizeof(struct rco_key) + key->size);
		assert(value->size < 1200);
		remaining -= (sizeof(struct rco_value) + value->size);
		log_offt += (sizeof(struct rco_key) + key->size + sizeof(struct rco_value) + value->size);
		key = (struct rco_key *)((uint64_t)key + sizeof(struct rco_key) + key->size + sizeof(struct rco_value) +
					 value->size);
		if (task->log_end - log_offt < sizeof(uint32_t) || log_offt >= task->log_end) {
			//log_info("log_offt exceeded ok!");
			break;
		}
		if (remaining >= sizeof(struct rco_key) && key->size == 0) {
			break;
		}
		if (remaining < sizeof(struct rco_key))
			break;
	}
	//log_info("Done parsing segment");
#endif
}
