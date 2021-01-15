#include <assert.h>
#include <stdint.h>

#include "metadata.h"
#include "../kreon_lib/btree/btree.h"
#include <log.h>

static void di_rewrite_leaf_node(struct krm_region_desc *r_desc, struct leaf_node *leaf)
{
	struct node_header *header = &leaf->header;
	header->epoch = r_desc->db->volume_desc->mem_catalogue->epoch;
	//header->v1 = 0;
	//header->v2 = 0;
	for (int i = 0; i < header->numberOfEntriesInNode; i++) {
		uint64_t offt_in_segment = leaf->kv_entry[i].device_offt % SEGMENT_SIZE;
		// log_info("offt = %llu leaf pointer[%d] =
		// %llu",offt_in_segment,i,leaf->pointer[i]);
		uint64_t primary_segment_offt = leaf->kv_entry[i].device_offt - offt_in_segment;
		/*do the lookup in the hash table, where have I stored the segment?*/
		struct krm_segment_entry *entry;

		HASH_FIND_PTR(r_desc->replica_log_map, &primary_segment_offt, entry);
		if (entry == NULL) {
			log_fatal("Cannot find mapping for primary's segment %llu of db %s", primary_segment_offt,
				  r_desc->db->db_desc->db_name);
			raise(SIGINT);
			exit(EXIT_FAILURE);
		}
		// log_info("Found translated!");
		leaf->kv_entry[i].device_offt = entry->my_seg + offt_in_segment;
		// void *s = MAPPED + leaf->pointer[i];
		// log_info("key is %s", s + 4);
	}
}

void di_rewrite_index(struct krm_region_desc *r_desc, uint8_t level_id, uint8_t tree_id)
{
	struct di_cursor *c = &r_desc->level_cursor[level_id];

	while (1) {
	main_loop:
		switch (c->state) {
		case DI_INIT: {
			// log_info("Initializing Cursor");
			c->offset = sizeof(struct segment_header);
			c->segment = r_desc->db->db_desc->levels[level_id].first_segment[tree_id];
			assert(c->segment != NULL);
			c->addr = (char *)((uint64_t)c->segment + sizeof(struct segment_header));
			c->curr_entry = 0;
			c->state = DI_CHECK_NEXT_ENTRY;
			break;
		}
		case DI_ADVANCE_CURSOR: {
			if (c->offset + c->inc >= c->max_offset) {
				log_info("Done rewriting index of level[%u][%u] reached offset %llu "
					 "max offset %llu",
					 level_id, tree_id, c->offset + c->inc, c->max_offset);
				c->state = DI_COMPLETE;
				return;
			}

			c->offset += c->inc;

			if (c->offset > 0 && c->offset % SEGMENT_SIZE == 0) {
				c->state = DI_CHANGE_SEGMENT;
				c->inc = 0;
				break;
			} else {
				c->addr = c->addr + c->inc;
				c->state = DI_CHECK_NEXT_ENTRY;
				c->inc = 0;
				break;
			}
		}
		case DI_CHANGE_SEGMENT: {
			if (c->offset == c->max_offset) {
				// log_info("Parsing index for level [%u][%u] done", level_id, tree_id);
				c->state = DI_COMPLETE;
				break;
			}
			if (c->segment->next_segment == NULL) {
				log_info("Next segment not here yet for level[%u][%u] ok next time", level_id, tree_id);
				return;
			}
			// log_info("Changing segment");
			c->segment = (struct segment_header *)(MAPPED + (uint64_t)c->segment->next_segment);
			c->offset += sizeof(struct segment_header);
			c->addr = (char *)((uint64_t)c->segment + sizeof(struct segment_header));
			c->state = DI_CHECK_NEXT_ENTRY;
			break;
		}
		case DI_CHECK_NEXT_ENTRY: {
			uint32_t type = *(uint32_t *)c->addr;
			switch (type) {
			case leafNode:
			case leafRootNode: {
				// log_info("We have a leaf");
				c->state = DI_LEAF_NODE;
				goto main_loop;
			}
			case internalNode:
			case rootNode: {
				// log_info("We have an internal node");
				c->state = DI_INDEX_NODE_FIRST_IN;
				goto main_loop;
			}

			case keyBlockHeader: {
				// log_info("We have a keyBlock");
				c->inc = KEY_BLOCK_SIZE;
				c->state = DI_ADVANCE_CURSOR;
				goto main_loop;
			}
			case paddedSpace: {
				uint32_t padded_space = SEGMENT_SIZE - (c->offset % SEGMENT_SIZE);
				//log_info("Found padded space %lu for level[%u][%u] of region %s cursor "
				//         "offset %llu",
				//         padded_space, level_id, tree_id, r_desc->region->id,
				//         c->offset);
				c->inc = padded_space;
				c->state = DI_ADVANCE_CURSOR;
				goto main_loop;
			}
			default:
				log_fatal("Corruption, unknown entry %u", type);
				assert(0);
				exit(EXIT_FAILURE);
			}
		}
		case DI_LEAF_NODE: {
			// log_info("Rewriting leaf node");
			di_rewrite_leaf_node(r_desc, (struct leaf_node *)c->addr);
			c->inc = LEAF_NODE_SIZE;
			c->state = DI_ADVANCE_CURSOR;
			goto main_loop;
		}
		case DI_INDEX_NODE_FIRST_IN: {
			// log_info("Rewriting FIRST IN of index node");
			/*first */
			struct index_node *index = (struct index_node *)c->addr;
			uint64_t primary_segment_offt = (uint64_t)index->header.first_IN_log_header % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)index->header.first_IN_log_header - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_warn("Cannot find mapping for primary's segment %llu of db %s that's "
					 "ok next time",
					 primary_segment, r_desc->db->db_desc->db_name);
				raise(SIGINT);
				return;
			}
			index->header.epoch = r_desc->db->volume_desc->mem_catalogue->epoch;
			//index->header.v1 = 0;
			//index->header.v2 = 0;
			index->header.first_IN_log_header =
				(struct IN_log_header *)index_entry->my_seg + primary_segment_offt;
			c->state = DI_INDEX_NODE_LAST_IN;
			break;
		}
		case DI_INDEX_NODE_LAST_IN: {
			// log_info("Rewriting LAST IN of index node");
			/*last */
			struct index_node *index = (struct index_node *)c->addr;
			uint64_t primary_segment_offt = (uint64_t)index->header.last_IN_log_header % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)index->header.last_IN_log_header - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_fatal("Cannot find mapping for primary's segment %llu of db %s that's "
					  "ok next time",
					  primary_segment_offt, r_desc->db->db_desc->db_name);
				return;
			}
			index->header.last_IN_log_header =
				(struct IN_log_header *)index_entry->my_seg + primary_segment_offt;
			c->state = DI_INDEX_NODE_LEFT_CHILD;
			c->curr_entry = 0;
			break;
		}
		case DI_INDEX_NODE_LEFT_CHILD: {
			// log_info("Rewriting LEFT_CHILD of index node");
			struct index_node *index = (struct index_node *)c->addr;
			uint64_t *left_child = &index->p[c->curr_entry].left[0];
			uint64_t primary_segment_offt = (uint64_t)*left_child % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)*left_child - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				//log_warn("Cannot find mapping for primary's segment %llu for entry %d "
				//          "of db %s that's "
				//          "ok next time",
				//          primary_segment, c->curr_entry, r_desc->db->db_desc->db_name);
				return;
			}
			*left_child = (uint64_t)index_entry->my_seg + primary_segment_offt;
			c->state = DI_INDEX_NODE_PIVOT;
			break;
		}
		case DI_INDEX_NODE_PIVOT: {
			// log_info("Rewriting PIVOT of index node");
			struct index_node *index = (struct index_node *)c->addr;
			uint64_t *pivot = &index->p[c->curr_entry].pivot;
			uint64_t primary_segment_offt = (uint64_t)*pivot % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)*pivot - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_warn("Cannot find mapping for primary's segment %llu of db %s that's "
					 "ok next time",
					 primary_segment_offt, r_desc->db->db_desc->db_name);
				raise(SIGINT);
				return;
			}
			*pivot = (uint64_t)index_entry->my_seg + primary_segment_offt;
			c->state = DI_INDEX_NODE_PIVOT;

			if (c->curr_entry == index->header.numberOfEntriesInNode - 1) {
				c->state = DI_INDEX_NODE_RIGHT_CHILD;
			} else {
				++c->curr_entry;
				c->state = DI_INDEX_NODE_LEFT_CHILD;
			}
			break;
		}
		case DI_INDEX_NODE_RIGHT_CHILD: {
			// log_info("Rewriting RIGHT CHILD  of index node");
			struct index_node *index = (struct index_node *)c->addr;
			uint64_t *right_child = &index->p[c->curr_entry].right[0];
			uint64_t primary_segment_offt = (uint64_t)*right_child % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)*right_child - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_warn("Cannot find mapping for primary's segment %llu of db %s that's "
					 "ok next time",
					 primary_segment_offt, r_desc->db->db_desc->db_name);
				return;
			}
			*right_child = (uint64_t)index_entry->my_seg + primary_segment_offt;
			c->inc = INDEX_NODE_SIZE;
			c->state = DI_ADVANCE_CURSOR;
			goto main_loop;
		}
		default:
			log_fatal("Unknown state");
			exit(EXIT_FAILURE);
		}
	}
}
