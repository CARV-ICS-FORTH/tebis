#include "send_index_rewriter.h"
#include "btree/conf.h"
#include "btree/level_write_appender.h"
#include "parallax/structures.h"
#include "send_index.h"
#include "send_index_segment_iterator.h"
#include <allocator/persistent_operations.h>
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
#include <unistd.h>
#define SEGMENT_START(x) (x - (x % SEGMENT_SIZE))

struct send_index_rewriter {
	send_index_segment_iterator_t iterator;
	region_desc_t r_desc;
};

send_index_rewriter_t send_index_rewriter_init(region_desc_t r_desc)
{
	struct send_index_rewriter *rewriter = calloc(1UL, sizeof(struct send_index_rewriter));
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
		if (splice.kv_cat == SMALL_INPLACE || splice.kv_cat == MEDIUM_INPLACE)
			continue;

		//translate the seperated key value
		assert(splice.kv_cat == BIG_INLOG || splice.kv_cat == MEDIUM_INLOG);
		struct kv_seperation_splice2 *seperated_kv =
			(struct kv_seperation_splice2 *)kv_splice_base_get_reference(&splice);
		uint64_t old_kv_ptr = kv_sep2_get_value_offt(seperated_kv);
		uint64_t ptr_segment_offt = SEGMENT_START(old_kv_ptr);
		uint64_t ptr_shift = old_kv_ptr - ptr_segment_offt;

		uint64_t replica_seg_offt = region_desc_get_logmap_seg(rewriter->r_desc, ptr_segment_offt);

		if (!replica_seg_offt) {
			assert(splice.kv_cat == MEDIUM_INLOG);
			replica_seg_offt = region_desc_allocate_log_segment_offt(rewriter->r_desc, ptr_segment_offt);
		}

		//translate now
		uint64_t replica_ptr_offt = replica_seg_offt + ptr_shift;
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

		uint64_t replica_seg_offt =
			region_desc_get_indexmap_seg(rewriter->r_desc, child_segment_offt, level_id);
		if (!replica_seg_offt) {
			log_fatal("There is no chance we dont have a segment for you already");
			_exit(EXIT_FAILURE);
		}

		//translate now
		uint64_t replica_pivot_offt = replica_seg_offt + pivot_shift;
		pivot_ptr->child_offt = replica_pivot_offt;
	}
}

void send_index_rewriter_rewrite_index(send_index_rewriter_t rewriter, region_desc_t r_desc,
				       struct segment_header *segment, uint32_t level_id)
{
	assert(!rewriter->iterator);
	rewriter->iterator = send_index_segment_iterator_init(segment);
	uint64_t next_segment_offt = (uint64_t)segment->next_segment;
	if (next_segment_offt) {
		uint64_t replica_seg_offt = region_desc_get_indexmap_seg(r_desc, next_segment_offt, level_id);

		if (!replica_seg_offt) {
			struct ru_replica_state *r_state = region_desc_get_replica_state(r_desc);
			replica_seg_offt = wappender_allocate_space(r_state->wappender[level_id]);
			region_desc_add_to_indexmap(rewriter->r_desc, next_segment_offt, replica_seg_offt, level_id);
		}

		segment->next_segment = (void *)replica_seg_offt;
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
