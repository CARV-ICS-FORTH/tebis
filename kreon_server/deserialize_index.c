#include <assert.h>
#include <stdint.h>

#include "metadata.h"
#include "../kreon_lib/btree/btree.h"
#include "../kreon_lib/btree/segment_allocator.h"
#include <log.h>

static void di_rewrite_leaf_node(struct krm_region_desc *r_desc, struct leaf_node *leaf)
{
	struct node_header *header = &leaf->header;
	header->epoch = r_desc->db->volume_desc->mem_catalogue->epoch;
	//header->v1 = 0;
	//header->v2 = 0;
	for (uint32_t i = 0; i < header->numberOfEntriesInNode; i++) {
		uint64_t offt_in_segment = leaf->kv_entry[i].device_offt % SEGMENT_SIZE;
		// log_info("offt = %llu leaf pointer[%d] =
		// %llu",offt_in_segment,i,leaf->pointer[i]);
		uint64_t primary_segment_offt = leaf->kv_entry[i].device_offt - offt_in_segment;
		//do the lookup in the hash table, where have I stored the segment?
		struct krm_segment_entry *entry;

		pthread_rwlock_rdlock(&r_desc->replica_log_map_lock);
		HASH_FIND_PTR(r_desc->replica_log_map, &primary_segment_offt, entry);
		pthread_rwlock_unlock(&r_desc->replica_log_map_lock);
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

#if RCO_EXPLICIT_IO
static void di_write_segment(struct krm_region_desc *r_desc, char *buffer, uint64_t primary_seg_offt, int fd,
			     uint8_t level_id)
{
	//do the lookup in the hash table, where have I stored the segment?
	struct krm_segment_entry *entry;
	HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_seg_offt, entry);
	if (entry == NULL) {
		log_fatal("Cannot find mapping for primary's segment %llu of db %s", primary_seg_offt,
			  r_desc->db->db_desc->db_name);
		raise(SIGINT);
		exit(EXIT_FAILURE);
	}
	ssize_t total_bytes_written = sizeof(struct segment_header);
	ssize_t bytes_written = sizeof(struct segment_header);
	while (total_bytes_written < SEGMENT_SIZE) {
		bytes_written = pwrite(fd, &buffer[total_bytes_written], SEGMENT_SIZE - total_bytes_written,
				       entry->my_seg + total_bytes_written);
		if (bytes_written == -1) {
			log_fatal("Failed to write segment for leaf nodes reason follows");
			perror("Reason");
			assert(0);
			exit(EXIT_FAILURE);
		}
		total_bytes_written += bytes_written;
	}

	return;
}

static int di_rewrite_index_node(struct di_buffer *buf)
{
	while (1) {
		switch (buf->state) {
		case DI_ADVANCE_CURSOR: {
			buf->offt = buf->offt + INDEX_NODE_SIZE + KEY_BLOCK_SIZE;
			if (buf->offt > buf->size) {
				log_info("This shouldn't happen");
				exit(EXIT_FAILURE);
			} else if (buf->offt == buf->size) {
				di_write_segment(buf->r_desc, buf->data, buf->primary_offt, buf->fd, buf->level_id);
				return 1;
			}
			uint32_t *type = (uint32_t *)&buf->data[buf->offt];
			if (*type == paddedSpace) {
				di_write_segment(buf->r_desc, buf->data, buf->primary_offt, buf->fd, buf->level_id);
				return 1;
			} else if (*type == internalNode || *type == rootNode) {
				if (*type == rootNode) {
					log_info("got a root");
				}
				buf->state = DI_INDEX_NODE_FIRST_IN;
				break;
			} else {
				log_fatal("Wrong type of node type %u buf offt is %llu", *type, buf->offt);
				assert(0);
				exit(EXIT_FAILURE);
			}
		}
		case DI_INDEX_NODE_FIRST_IN: {
			// log_info("Rewriting FIRST IN of index node");
			/*first */
			struct index_node *index = (struct index_node *)&buf->data[buf->offt];
			uint64_t primary_segment_offt = (uint64_t)index->header.first_IN_log_header % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)index->header.first_IN_log_header - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(buf->r_desc->replica_index_map[buf->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				//log_warn("Cannot find mapping for primary's segment %llu of db %s that's "
				//	 "ok next time",
				//	 primary_segment, buf->r_desc->db->db_desc->db_name);
				return 0;
			}
			index->header.epoch = buf->r_desc->db->volume_desc->mem_catalogue->epoch;
			index->header.first_IN_log_header =
				(struct IN_log_header *)index_entry->my_seg + primary_segment_offt;
			buf->state = DI_INDEX_NODE_LAST_IN;
			break;
		}
		case DI_INDEX_NODE_LAST_IN: {
			// log_info("Rewriting LAST IN of index node");
			/*last */
			struct index_node *index = (struct index_node *)&buf->data[buf->offt];
			uint64_t primary_segment_offt = (uint64_t)index->header.last_IN_log_header % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)index->header.last_IN_log_header - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(buf->r_desc->replica_index_map[buf->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_fatal("Cannot find mapping for primary's segment %llu of db %s that's "
					  "ok next time",
					  primary_segment_offt, buf->r_desc->db->db_desc->db_name);
				return 0;
			}
			index->header.last_IN_log_header =
				(struct IN_log_header *)index_entry->my_seg + primary_segment_offt;
			buf->state = DI_INDEX_NODE_LEFT_CHILD;
			buf->curr_entry = 0;
			break;
		}
		case DI_INDEX_NODE_LEFT_CHILD: {
			// log_info("Rewriting LEFT_CHILD of index node");
			struct index_node *index = (struct index_node *)&buf->data[buf->offt];
			uint64_t *left_child = &index->p[buf->curr_entry].left[0];
			uint64_t primary_segment_offt = (uint64_t)*left_child % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)*left_child - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(buf->r_desc->replica_index_map[buf->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				//log_warn("Cannot find mapping for primary's segment %llu for entry %d "
				//          "of db %s that's "
				//          "ok next time",
				//          primary_segment, c->curr_entry, r_desc->db->db_desc->db_name);
				return 0;
			}
			*left_child = (uint64_t)index_entry->my_seg + primary_segment_offt;
			buf->state = DI_INDEX_NODE_PIVOT;
			break;
		}
		case DI_INDEX_NODE_PIVOT: {
			// log_info("Rewriting PIVOT of index node");
			struct index_node *index = (struct index_node *)&buf->data[buf->offt];
			uint64_t *pivot = &index->p[buf->curr_entry].pivot;
			uint64_t primary_segment_offt = (uint64_t)*pivot % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)*pivot - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(buf->r_desc->replica_index_map[buf->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_warn("Cannot find mapping for primary's segment %llu of db %s that's "
					 "ok next time",
					 primary_segment_offt, buf->r_desc->db->db_desc->db_name);
				return 0;
			}
			*pivot = (uint64_t)index_entry->my_seg + primary_segment_offt;
			buf->state = DI_INDEX_NODE_PIVOT;

			if (buf->curr_entry == index->header.numberOfEntriesInNode - 1) {
				//log_info("Decoded idx %u entries last %u height %u", buf->curr_entry,
				//	 index->header.numberOfEntriesInNode - 1, index->header.height);
				buf->state = DI_INDEX_NODE_RIGHT_CHILD;
			} else {
				++buf->curr_entry;
				buf->state = DI_INDEX_NODE_LEFT_CHILD;
			}

			break;
		}
		case DI_INDEX_NODE_RIGHT_CHILD: {
			// log_info("Rewriting RIGHT CHILD  of index node");
			struct index_node *index = (struct index_node *)&buf->data[buf->offt];
			uint64_t *right_child = &index->p[buf->curr_entry].right[0];
			uint64_t primary_segment_offt = (uint64_t)*right_child % SEGMENT_SIZE;
			uint64_t primary_segment = (uint64_t)*right_child - primary_segment_offt;
			struct krm_segment_entry *index_entry;
			HASH_FIND_PTR(buf->r_desc->replica_index_map[buf->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_warn("Cannot find mapping for primary's segment %llu of db %s that's "
					 "ok next time",
					 primary_segment_offt, buf->r_desc->db->db_desc->db_name);
				return 0;
			}
			*right_child = (uint64_t)index_entry->my_seg + primary_segment_offt;
			buf->state = DI_ADVANCE_CURSOR;
			continue;
		}
		default:
			log_fatal("Unhandled state");
			exit(EXIT_FAILURE);
		}
	}
}

static void di_free_index_buffer(struct krm_region_desc *r_desc, uint32_t level_id, uint32_t height)
{
	if (r_desc->index_buffer[level_id][height]->allocated)
		free(r_desc->index_buffer[level_id][height]->data);
	memset(r_desc->index_buffer[level_id][height], 0x00, sizeof(struct di_buffer));
	free(r_desc->index_buffer[level_id][height]);
	r_desc->index_buffer[level_id][height] = NULL;
	return;
}

void di_rewrite_index_with_explicit_IO(struct segment_header *seg, struct krm_region_desc *r_desc,
				       uint64_t primary_seg_offt, uint8_t level_id)
{
	//First try to decode the segment, leaves can be done on the fly
	uint32_t *type = (uint32_t *)((uint64_t)seg + sizeof(struct segment_header));
	switch (*type) {
	case leafNode:
	case leafRootNode: {
		//Allocate a segment where we will place the segment after its decoding
		struct segment_header *seg =
			get_segment_for_explicit_IO(r_desc->db->volume_desc, &r_desc->db->db_desc->levels[level_id], 1);
		// add mapping to level's hash table
		struct krm_segment_entry *e = (struct krm_segment_entry *)malloc(sizeof(struct krm_segment_entry));
		e->master_seg = primary_seg_offt;
		e->my_seg = (uint64_t)seg - MAPPED;
		HASH_ADD_PTR(r_desc->replica_index_map[level_id], master_seg, e);

		struct leaf_node *l_node = (struct leaf_node *)((uint64_t)seg + sizeof(struct segment_header));
		uint32_t decoded_bytes = sizeof(struct segment_header);
		while (decoded_bytes < SEGMENT_SIZE) {
			//log_info("Decoding a leaf node");
			di_rewrite_leaf_node(r_desc, l_node);
			decoded_bytes += LEAF_NODE_SIZE;
			l_node = (struct leaf_node *)((uint64_t)l_node + LEAF_NODE_SIZE);
			if (l_node->header.type == paddedSpace) {
				decoded_bytes += (SEGMENT_SIZE - decoded_bytes);
				break;
			}
		}
		assert(decoded_bytes == SEGMENT_SIZE);
		/*Now write the buffer to storage and add the hash mapping*/
		di_write_segment(r_desc, (char *)seg, primary_seg_offt, FD, level_id);
		//check if we can decode previous halted tasks
		for (int i = 1; i < MAX_HEIGHT; i++) {
			if (r_desc->index_buffer[level_id][i]) {
				if (!di_rewrite_index_node(r_desc->index_buffer[level_id][i])) {
					log_warn(
						"Cannot decode pending indexing segment for DB %s level_id %u height %u",
						r_desc->db->db_desc->db_name, level_id, i);
					break;
				} else {
					di_free_index_buffer(r_desc, level_id, i);
					log_warn("Decoded !! pending indexing segment for DB %s level_id %u height %u",
						 r_desc->db->db_desc->db_name, level_id, i);
				}
			}
		}
		return;
	}
	case internalNode:
	case rootNode: {
		struct segment_header *disk_seg =
			get_segment_for_explicit_IO(r_desc->db->volume_desc, &r_desc->db->db_desc->levels[level_id], 1);
		// add mapping to level's hash table
		struct krm_segment_entry *e = (struct krm_segment_entry *)malloc(sizeof(struct krm_segment_entry));
		e->master_seg = primary_seg_offt;
		e->my_seg = (uint64_t)disk_seg - MAPPED;
		HASH_ADD_PTR(r_desc->replica_index_map[level_id], master_seg, e);

		struct index_node *idx = (struct index_node *)((uint64_t)seg + sizeof(struct segment_header));
		uint32_t height = idx->header.height;
		if (height < 1 || height >= MAX_HEIGHT) {
			log_fatal("Corrupted height %u", height);
			assert(0);
			exit(EXIT_FAILURE);
		}
		//Is there a pending decoding process for this level,height?
		if (r_desc->index_buffer[level_id][height]) {
			if (!di_rewrite_index_node(r_desc->index_buffer[level_id][height])) {
				log_fatal("Cannot decode pending indexing segment for DB %s level_id %u height %u",
					  r_desc->db->db_desc->db_name, level_id, height);
				assert(0);
				exit(EXIT_FAILURE);
			}
			di_free_index_buffer(r_desc, level_id, height);
			log_info("Decoded pending indexing segment for DB %s level_id %u height %u",
				 r_desc->db->db_desc->db_name, level_id, height);
		}

		r_desc->index_buffer[level_id][height] = (struct di_buffer *)calloc(1, sizeof(struct di_buffer));
		//if (posix_memalign((void **)&r_desc->index_buffer[level_id][height], SEGMENT_SIZE,
		//		   sizeof(struct di_buffer)) != 0) {
		//	log_fatal("Posix memalign failed");
		//	perror("Reason: ");
		//	exit(EXIT_FAILURE);
		//}

		r_desc->index_buffer[level_id][height]->r_desc = r_desc;

		r_desc->index_buffer[level_id][height]->primary_offt = primary_seg_offt;
		r_desc->index_buffer[level_id][height]->size = SEGMENT_SIZE;
		r_desc->index_buffer[level_id][height]->offt = sizeof(struct segment_header);
		r_desc->index_buffer[level_id][height]->curr_entry = 0;
		r_desc->index_buffer[level_id][height]->fd = FD;
		r_desc->index_buffer[level_id][height]->level_id = level_id;

		r_desc->index_buffer[level_id][height]->state = DI_INDEX_NODE_FIRST_IN;

		r_desc->index_buffer[level_id][height]->data = (char *)seg;
		r_desc->index_buffer[level_id][height]->allocated = 0;

		if (!di_rewrite_index_node(r_desc->index_buffer[level_id][height])) {
			log_warn(
				"Cannot decode pending indexing segment for DB %s level_id %u height %u, that's ok later",
				r_desc->db->db_desc->db_name, level_id, height);

			r_desc->index_buffer[level_id][height]->data = NULL;
			uint64_t *dst = NULL;
			if (posix_memalign((void **)&dst, ALIGNMENT, SEGMENT_SIZE) != 0) {
				log_fatal("Posix memalign failed");
				perror("Reason: ");
				exit(EXIT_FAILURE);
			}
			//uint64_t *src = (uint64_t*)seg;
			//uint32_t rounds = SEGMENT_SIZE/sizeof(uint64_t);
			//for(uint32_t i=rounds-1;i<=0;i--){
			//	dst[i] = src[i];
			//}
			memcpy(dst, seg, SEGMENT_SIZE);
			r_desc->index_buffer[level_id][height]->allocated = 1;
			r_desc->index_buffer[level_id][height]->data = (char *)dst;
			//raise(SIGINT);
		} else {
			//decoding success!
			di_free_index_buffer(r_desc, level_id, height);
			//check if we can decode previous halted tasks
			for (int i = height + 1; i < MAX_HEIGHT; i++) {
				if (r_desc->index_buffer[level_id][i]) {
					if (!di_rewrite_index_node(r_desc->index_buffer[level_id][i])) {
						log_warn(
							"Cannot decode pending indexing segment for DB %s level_id %u height %u",
							r_desc->db->db_desc->db_name, level_id, i);
						break;
					} else {
						di_free_index_buffer(r_desc, level_id, i);
						log_warn(
							"Decoded !! pending indexing segment for DB %s level_id %u height %u",
							r_desc->db->db_desc->db_name, level_id, i);
					}
				}
			}
		}
		return;
	}
	case paddedSpace:
		log_warn("Nothing to do plain padded space");
		return;
	case keyBlockHeader:
		log_fatal("This type shouldn't be first!");
		exit(EXIT_FAILURE);
	default:
		log_fatal("Unknown type! %u", *type);
		assert(0);
		exit(EXIT_FAILURE);
	}
}
#else
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
#endif
