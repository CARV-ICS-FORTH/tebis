#include "send_index_rewriter.h"
#include "btree/conf.h"
#include "btree/level_write_appender.h"
#include "send_index.h"
#include "send_index_segment_iterator.h"
#include <assert.h>
#include <btree/dynamic_leaf.h>
#include <btree/index_node.h>
#include <btree/kv_pairs.h>
#include <errno.h>
#include <log.h>
#include <netdb.h>
#include <pthread.h>
#include <stdlib.h>
#include <strings.h>
#define SEGMENT_START(x) (x - (x % SEGMENT_SIZE))

struct send_index_rewriter {
	send_index_segment_iterator_t iterator;
	struct krm_region_desc *r_desc;
};

send_index_rewriter_t send_index_rewriter_init(struct krm_region_desc *r_desc)
{
	struct send_index_rewriter *rewriter =
		(struct send_index_rewriter *)calloc(1, sizeof(struct send_index_rewriter));
	rewriter->r_desc = r_desc;
	rewriter->iterator = NULL;

	return rewriter;
}

static void send_index_rewriter_rewrite_leaf_node(send_index_rewriter_t rewriter)
{
	struct leaf_iterator iter = { 0 };
	struct leaf_node *node = (struct leaf_node *)send_index_segment_iterator_get_node_addr(rewriter->iterator);

	for (dl_init_leaf_iterator(node, &iter, NULL, -1); dl_is_leaf_iterator_valid(&iter);
	     dl_leaf_iterator_next(&iter)) {
		struct kv_splice_base splice = dl_leaf_iterator_curr(&iter);
		if (splice.cat == SMALL_INPLACE)
			continue;

		//translate the seperated key value
		assert(splice.cat == BIG_INLOG || splice.cat == MEDIUM_INLOG);
		struct kv_seperation_splice2 *seperated_kv =
			(struct kv_seperation_splice2 *)kv_splice_base_get_reference(&splice);
		uint64_t old_kv_ptr = kv_sep2_get_value_offt(seperated_kv);
		uint64_t ptr_segment_offt = SEGMENT_START(old_kv_ptr);
		uint64_t ptr_shift = old_kv_ptr - ptr_segment_offt;

		struct krm_segment_entry *index_entry;
		pthread_rwlock_rdlock(&rewriter->r_desc->replica_log_map_lock);
		HASH_FIND_PTR(rewriter->r_desc->replica_log_map, &ptr_segment_offt, index_entry);
		pthread_rwlock_unlock(&rewriter->r_desc->replica_log_map_lock);

		if (!index_entry) {
			log_fatal("There is no chance we dont have a segment for you already");
			_exit(EXIT_FAILURE);
		}

		//translate now
		uint64_t replica_ptr_offt = index_entry->replica_segment_offt + ptr_shift;
		kv_sep2_set_value_offt(seperated_kv, replica_ptr_offt);
	}
}

static void send_index_rewriter_rewrite_index_node(send_index_rewriter_t rewriter, uint32_t level_id)
{
	struct index_node_iterator index_iterator = { 0 };
	struct index_node *node = (struct index_node *)send_index_segment_iterator_get_node_addr(rewriter->iterator);

	for (index_iterator_init(node, &index_iterator); index_iterator_is_valid(&index_iterator);
	     index_iterator_next(&index_iterator)) {
		struct pivot_pointer *pivot_ptr = index_iterator_get_pivot_pointer(&index_iterator);
		uint64_t old_pivot_ptr = pivot_ptr->child_offt;
		uint64_t child_segment_offt = SEGMENT_START(old_pivot_ptr);
		uint64_t pivot_shift = old_pivot_ptr - child_segment_offt;
		struct krm_segment_entry *index_entry;
		HASH_FIND_PTR(rewriter->r_desc->replica_index_map[level_id], &child_segment_offt, index_entry);
		if (!index_entry) {
			log_fatal("There is no chance we dont have a segment for you already");
			_exit(EXIT_FAILURE);
		}
		//translate now
		uint64_t replica_pivot_offt = index_entry->replica_segment_offt + pivot_shift;
		pivot_ptr->child_offt = replica_pivot_offt;
	}
}

void send_index_rewriter_rewrite_index(send_index_rewriter_t rewriter, struct krm_region_desc *r_desc,
				       struct segment_header *segment, uint32_t level_id)
{
	assert(!rewriter->iterator);
	rewriter->iterator = send_index_segment_iterator_init(segment);
	uint64_t next_segment_offt = (uint64_t)segment->next_segment;
	if (next_segment_offt) {
		//log_debug("Searching for next seg offt %lu", next_segment_offt);
		struct krm_segment_entry *index_entry;
		HASH_FIND_PTR(rewriter->r_desc->replica_index_map[level_id], &next_segment_offt, index_entry);
		uint64_t replica_next_segment_offt = 0;
		if (!index_entry) {
			replica_next_segment_offt = wappender_allocate_space(r_desc->r_state->wappender[level_id]);
			add_segment_to_index_HT(r_desc, next_segment_offt, replica_next_segment_offt, level_id);
		} else
			replica_next_segment_offt = index_entry->replica_segment_offt;

		segment->next_segment = (void *)replica_next_segment_offt;
	}
	while (send_index_segment_iterator_is_valid(rewriter->iterator)) {
		switch (send_index_segment_iterator_get_type(rewriter->iterator)) {
		case LEAF_SEGMENT_ITERATOR:
			send_index_rewriter_rewrite_leaf_node(rewriter);
			break;
		case INDEX_SEGMENT_ITERATOR:
			send_index_rewriter_rewrite_index_node(rewriter, level_id);
			break;
		default:
			log_fatal("Found a not valid iterator");
			_exit(EXIT_FAILURE);
		}

		send_index_segment_iterator_next(rewriter->iterator);
	}

	send_index_segment_iterator_destroy(&rewriter->iterator);
}

void send_index_rewriter_destroy(send_index_rewriter_t *rewriter)
{
	assert(*rewriter);
	free(*rewriter);
	*rewriter = NULL;
}
