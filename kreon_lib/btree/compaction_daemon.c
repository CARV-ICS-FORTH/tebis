#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#define COMPACTION
#include <signal.h>
#include <pthread.h>
#include <assert.h>
#include "../scanner/scanner.h"
#include "btree.h"
#include "segment_allocator.h"
#include <log.h>

struct comp_level_write_cursor {
	char segment_buf[MAX_HEIGHT][BUFFER_SEGMENT_SIZE];
	uint64_t segment_offt[MAX_HEIGHT];
	uint64_t dev_offt[MAX_HEIGHT];
	struct index_node *last_index[MAX_HEIGHT];
	struct leaf_node *last_leaf;
	uint64_t root_offt;
	db_handle *handle;
	uint32_t level_id;
	uint32_t tree_height;
	int fd;
};

enum comp_level_read_cursor_state {
	COMP_CUR_INIT,
	COMP_CUR_FIND_LEAF,
	COMP_CUR_FETCH_NEXT_SEGMENT,
	COMP_CUR_DECODE_KV,
	COMP_CUR_CHECK_OFFT
};

struct comp_level_read_cursor {
	char segment_buf[BUFFER_SEGMENT_SIZE];
	char last_ins_key[2 * PREFIX_SIZE];
	struct kv_prefix kvPrefix;
	int fd;
	uint64_t offset;
	db_handle *handle;
	segment_header *curr_segment;
	uint32_t level_id;
	uint32_t tree_id;
	uint32_t curr_leaf_entry;
	char end_of_level;
	char last_key_empty;
	enum KV_type kv_type;
	enum KV_type last_ins_key_type;
	enum comp_level_read_cursor_state state;
};

static void comp_init_read_cursor(struct comp_level_read_cursor *c, db_handle *handle, uint32_t level_id,
				  uint32_t tree_id, int fd)
{
	c->offset = 0;
	c->handle = handle;
	c->level_id = level_id;
	c->tree_id = tree_id;
	c->curr_leaf_entry = 0;
	c->curr_segment = NULL;
	c->end_of_level = 0;
	c->last_key_empty = 1;
	c->state = COMP_CUR_FETCH_NEXT_SEGMENT;
	memset(c->last_ins_key, 0x00, 2 * PREFIX_SIZE);
	c->last_ins_key_type = KV_PREFIX;
	c->fd = fd;
}

static void comp_get_next_key(struct comp_level_read_cursor *c)
{
	if (c == NULL) {
		log_fatal("NULL cursor!");
		assert(0);
		exit(EXIT_FAILURE);
	}
	if (c->end_of_level)
		return;
	while (1) {
	fsm_entry:
		switch (c->state) {
		case COMP_CUR_CHECK_OFFT: {
			if (c->offset >= c->handle->db_desc->levels[c->level_id].offset[c->tree_id]) {
				log_info("Done read level %u", c->level_id);
				c->end_of_level = 1;
				assert(c->offset == c->handle->db_desc->levels[c->level_id].offset[c->tree_id]);
				return;
			}
			if (c->offset % BUFFER_SEGMENT_SIZE == 0)
				c->state = COMP_CUR_FETCH_NEXT_SEGMENT;
			else
				c->state = COMP_CUR_FIND_LEAF;
			break;
		}

		case COMP_CUR_FETCH_NEXT_SEGMENT: {
			if (c->curr_segment == NULL) {
				c->curr_segment = c->handle->db_desc->levels[c->level_id].first_segment[c->tree_id];
			} else {
				if (c->curr_segment->next_segment == NULL) {
					assert((uint64_t)c->curr_segment ==
					       (uint64_t)c->handle->db_desc->levels[c->level_id]
						       .last_segment[c->tree_id]);
					log_info("Done parsing cursor offset %llu total offt %llu", c->offset,
						 c->handle->db_desc->levels[c->level_id].offset[c->tree_id]);
					c->state = COMP_CUR_CHECK_OFFT;
					goto fsm_entry;
				} else
					c->curr_segment =
						(segment_header *)(MAPPED + (uint64_t)c->curr_segment->next_segment);
			}

			//log_info("Fetching next segment id %llu", c->curr_segment->segment_id);
			/*read the segment*/
			off_t dev_offt = (uint64_t)c->curr_segment - MAPPED;
			ssize_t bytes_read = 0;
			ssize_t bytes = 0;
			while (bytes_read < BUFFER_SEGMENT_SIZE) {
				bytes = pread(c->fd, c->segment_buf, BUFFER_SEGMENT_SIZE - bytes_read,
					      dev_offt + bytes_read);
				if (bytes == -1) {
					log_fatal("Failed to read error code");
					perror("Error");
					assert(0);
					exit(EXIT_FAILURE);
				}
				bytes_read += bytes;
			}
			c->offset += sizeof(segment_header);
			c->state = COMP_CUR_FIND_LEAF;
			break;
		}

		case COMP_CUR_DECODE_KV: {
			// log_info("Decoding entry %u of leaf", c->curr_leaf_entry);
			struct leaf_node *leaf =
				(struct leaf_node *)((uint64_t)c->segment_buf + (c->offset % BUFFER_SEGMENT_SIZE));
			// slot array entry
			if (c->curr_leaf_entry >= leaf->header.numberOfEntriesInNode) {
				// done with this leaf
				c->curr_leaf_entry = 0;
				c->offset += LEAF_NODE_SIZE;
				c->state = COMP_CUR_CHECK_OFFT;
				break;
			} else {
				c->kv_type = KV_PREFIX;
				memcpy(c->kvPrefix.prefix, leaf->prefix[c->curr_leaf_entry], PREFIX_SIZE);
				c->kvPrefix.device_offt = leaf->kv_entry[c->curr_leaf_entry].device_offt;
				c->kvPrefix.tombstone = leaf->kv_entry[c->curr_leaf_entry].tombstone;
				++c->curr_leaf_entry;
				return;
			}
		}

		case COMP_CUR_FIND_LEAF: {
			/*read four bytes to check what is the node format*/
			nodeType_t type = *(uint32_t *)(c->segment_buf + (c->offset % BUFFER_SEGMENT_SIZE));
			switch (type) {
			case leafNode:
			case leafRootNode:
				// log_info("Found a leaf");
				c->state = COMP_CUR_DECODE_KV;
				goto fsm_entry;

			case rootNode:
			case internalNode:
				// log_info("Found an internal");
				c->offset += INDEX_NODE_SIZE;
				c->state = COMP_CUR_CHECK_OFFT;
				goto fsm_entry;

			case keyBlockHeader:
				// log_info("Found a keyblock header");
				c->offset += KEY_BLOCK_SIZE;
				c->state = COMP_CUR_CHECK_OFFT;
				goto fsm_entry;

			case paddedSpace:
				//log_info("Found padded space of size %llu",
				//	 (BUFFER_SEGMENT_SIZE - (c->offset % BUFFER_SEGMENT_SIZE)));
				c->offset += (BUFFER_SEGMENT_SIZE - (c->offset % BUFFER_SEGMENT_SIZE));
				c->state = COMP_CUR_CHECK_OFFT;
				goto fsm_entry;
			default:
				log_fatal("Wrong node type offset was %llu total level offset %llu", c->offset,
					  c->handle->db_desc->levels[c->level_id].offset[0]);
				assert(0);
				exit(EXIT_FAILURE);
			}

			break;
		}
		default:
			log_fatal("Error state");
			assert(0);
			exit(EXIT_FAILURE);
		}
	}
}

static void comp_init_write_cursor(struct comp_level_write_cursor *c, struct db_handle *handle, int level_id, int fd)
{
	c->level_id = level_id;
	c->tree_height = 0;

	c->fd = fd;
	c->handle = handle;

	for (int i = 0; i < MAX_HEIGHT; i++) {
		struct segment_header *seg = get_segment_for_explicit_IO(c->handle->volume_desc,
									 &c->handle->db_desc->levels[c->level_id], 1);

		c->dev_offt[i] = (uint64_t)seg - MAPPED;
		// log_info("Got dev_offt[%d] = %llu", i, c->dev_offt[i]);
		c->segment_offt[i] = sizeof(struct segment_header);
		if (i == 0) {
			c->last_index[0] = NULL;
			c->last_leaf = (struct leaf_node *)&c->segment_buf[0][c->segment_offt[0] % BUFFER_SEGMENT_SIZE];

			c->last_leaf->header.type = leafNode;
			c->last_leaf->header.epoch = c->handle->volume_desc->mem_catalogue->epoch;
			c->last_leaf->header.numberOfEntriesInNode = 0;
			c->last_leaf->header.fragmentation = 0;
			c->last_leaf->header.first_IN_log_header = NULL;
			c->last_leaf->header.last_IN_log_header = NULL;
			c->last_leaf->header.key_log_size = 0;
			c->last_leaf->header.height = 0;
			c->segment_offt[0] += LEAF_NODE_SIZE;
		} else {
			c->last_index[i] =
				(struct index_node *)&c->segment_buf[i][c->segment_offt[i] % BUFFER_SEGMENT_SIZE];

			/*initialization*/
			c->last_index[i]->header.type = internalNode;
			c->last_index[i]->header.epoch = c->handle->volume_desc->mem_catalogue->epoch;
			c->last_index[i]->header.numberOfEntriesInNode = 0;
			c->last_index[i]->header.fragmentation = 0;
			/*private key log for index nodes*/
			IN_log_header *bh = (IN_log_header *)((uint64_t)c->last_index[i] + INDEX_NODE_SIZE);
			bh->type = keyBlockHeader;
			bh->next = (void *)NULL;
			c->last_index[i]->header.first_IN_log_header =
				(IN_log_header *)((uint64_t)c->dev_offt[i] + ((uint64_t)bh % BUFFER_SEGMENT_SIZE));
			c->last_index[i]->header.last_IN_log_header = c->last_index[i]->header.first_IN_log_header;
			c->last_index[i]->header.key_log_size = sizeof(IN_log_header);
			c->segment_offt[i] += (INDEX_NODE_SIZE + KEY_BLOCK_SIZE);
		}
	}
	return;
}

static void comp_close_write_cursor(struct comp_level_write_cursor *c)
{
	for (uint32_t i = 0; i < MAX_HEIGHT; i++) {
		uint32_t *type;
		if (i <= c->tree_height) {
			if (i == 0 && c->segment_offt[i] % BUFFER_SEGMENT_SIZE != 0) {
				type = (uint32_t *)((uint64_t)c->last_leaf + LEAF_NODE_SIZE);
				//log_info("Marking padded space for %u segment offt %llu", i, c->segment_offt[0]);
				*type = paddedSpace;
			} else if (i > 0 && c->segment_offt[i] % BUFFER_SEGMENT_SIZE != 0) {
				type = (uint32_t *)((uint64_t)c->last_index[i] + INDEX_NODE_SIZE + KEY_BLOCK_SIZE);
				//log_info("Marking padded space for %u segment offt %llu", i, c->segment_offt[i]);
				*type = paddedSpace;
			}
		} else {
			type = (uint32_t *)&c->segment_buf[i][c->segment_offt[i]];
			*type = paddedSpace;
			// log_info("Marking full padded space for leaves segment offt %llu", i,
			// c->segment_offt[i]);
		}

		if (i == c->tree_height) {
			log_info("Merged level has a height off %u", c->tree_height);
			c->last_index[i]->header.type = rootNode;
			c->root_offt = c->dev_offt[i] + ((uint64_t)c->last_index[i] % BUFFER_SEGMENT_SIZE);
		}
		ssize_t total_bytes_written = sizeof(struct segment_header);
		ssize_t bytes_written = sizeof(struct segment_header);
		while (total_bytes_written < BUFFER_SEGMENT_SIZE) {
			bytes_written =
				pwrite(c->fd, &c->segment_buf[i][total_bytes_written],
				       BUFFER_SEGMENT_SIZE - total_bytes_written, c->dev_offt[i] + total_bytes_written);
			if (bytes_written == -1) {
				log_fatal("Failed to writed segment for leaf nodes reason follows");
				perror("Reason");
				exit(EXIT_FAILURE);
			}
			total_bytes_written += bytes_written;
		}
		// log_info("Dumped buffer %u at dev_offt %llu",i,c->dev_offt[i]);
	}
	if (syncfs(c->fd)) {
		log_fatal("Failed to sync file");
		exit(EXIT_FAILURE);
	}

	return;
}

/*mini allocator*/
static void comp_get_space(struct comp_level_write_cursor *c, uint32_t height, nodeType_t type)
{
	switch (type) {
	case leafNode:
	case leafRootNode: {
		uint32_t remaining_space;
		if (c->segment_offt[0] > 0 && c->segment_offt[0] % BUFFER_SEGMENT_SIZE == 0)
			remaining_space = 0;
		else
			remaining_space = BUFFER_SEGMENT_SIZE - (c->segment_offt[0] % BUFFER_SEGMENT_SIZE);

		if (remaining_space < LEAF_NODE_SIZE) {
			if (remaining_space > 0) {
				*(uint32_t *)(&c->segment_buf[0][c->segment_offt[0] % BUFFER_SEGMENT_SIZE]) =
					paddedSpace;
				c->segment_offt[0] += remaining_space;
			}
			ssize_t total_bytes_written = sizeof(struct segment_header);
			ssize_t bytes_written = sizeof(struct segment_header);
			while (total_bytes_written < BUFFER_SEGMENT_SIZE) {
				bytes_written = pwrite(c->fd, &c->segment_buf[0][total_bytes_written],
						       BUFFER_SEGMENT_SIZE - total_bytes_written,
						       c->dev_offt[0] + total_bytes_written);
				if (bytes_written == -1) {
					log_fatal("Failed to writed segment for leaf nodes reason follows");
					perror("Reason");
					exit(EXIT_FAILURE);
				}
				total_bytes_written += bytes_written;
			}
			// uint32_t *type = (uint32_t *)&c->segment_buf[0][4096];
			// assert(*type == leafNode || *type == leafRootNode);
			syncfs(FD);
			// type = (uint32_t *)(MAPPED + c->dev_offt[0] + 4096);
			// assert(*type == leafNode || *type == leafRootNode);
			// type = (uint32_t
			// *)((uint64_t)c->handle->db_desc->levels[1].first_segment[1] + 4096);
			// log_info("dev_offt %llu first segment %llu", c->dev_offt[0],
			//	 (uint64_t)c->handle->db_desc->levels[1].first_segment[1] -
			//MAPPED);
			// assert(*type == leafNode || *type == leafRootNode);

			// log_info("Dumped leaf segment buffer");
			/*get space from allocator*/
			struct segment_header *seg = get_segment_for_explicit_IO(
				c->handle->volume_desc, &c->handle->db_desc->levels[c->level_id], 1);
			c->dev_offt[0] = (uint64_t)seg - MAPPED;
			c->segment_offt[0] += sizeof(struct segment_header);
		}
		c->last_leaf = (struct leaf_node *)&c->segment_buf[0][c->segment_offt[0] % BUFFER_SEGMENT_SIZE];
		c->segment_offt[0] += LEAF_NODE_SIZE;

		c->last_leaf->header.type = type;
		c->last_leaf->header.epoch = c->handle->volume_desc->mem_catalogue->epoch;
		c->last_leaf->header.numberOfEntriesInNode = 0;
		c->last_leaf->header.fragmentation = 0;

		c->last_leaf->header.first_IN_log_header = NULL;
		c->last_leaf->header.last_IN_log_header = NULL;
		c->last_leaf->header.key_log_size = 0;
		c->last_leaf->header.height = 0;
		break;
	}
	case internalNode:
	case rootNode: {
		uint32_t remaining_space;
		if (c->segment_offt[height] > 0 && c->segment_offt[height] % BUFFER_SEGMENT_SIZE == 0)
			remaining_space = 0;
		else
			remaining_space = BUFFER_SEGMENT_SIZE - (c->segment_offt[height] % BUFFER_SEGMENT_SIZE);

		if (remaining_space < (INDEX_NODE_SIZE + KEY_BLOCK_SIZE)) {
			if (remaining_space > 0) {
				*(uint32_t *)(&c->segment_buf[height][c->segment_offt[height] % BUFFER_SEGMENT_SIZE]) =
					paddedSpace;
				c->segment_offt[height] += remaining_space;
			}
			ssize_t total_bytes_written = sizeof(struct segment_header);
			ssize_t bytes_written = sizeof(struct segment_header);
			while (total_bytes_written < BUFFER_SEGMENT_SIZE) {
				bytes_written = pwrite(c->fd, &c->segment_buf[height][total_bytes_written],
						       BUFFER_SEGMENT_SIZE - total_bytes_written,
						       c->dev_offt[height] + total_bytes_written);
				if (bytes_written == -1) {
					log_fatal("Failed to writed segment for leaf nodes reason follows");
					perror("Reason");
					exit(EXIT_FAILURE);
				}
				total_bytes_written += bytes_written;
			}
			log_info("Dumped index %d segment buffer", height);
			/*get space from allocator*/
			struct segment_header *seg = get_segment_for_explicit_IO(
				c->handle->volume_desc, &c->handle->db_desc->levels[c->level_id], 1);
			c->segment_offt[height] += sizeof(struct segment_header);
			c->dev_offt[height] = (uint64_t)seg - MAPPED;
		}
		c->last_index[height] =
			(struct index_node *)&c->segment_buf[height][c->segment_offt[height] % BUFFER_SEGMENT_SIZE];
		c->segment_offt[height] += (INDEX_NODE_SIZE + KEY_BLOCK_SIZE);
		/*initialization*/
		c->last_index[height]->header.type = type;
		c->last_index[height]->header.epoch = c->handle->volume_desc->mem_catalogue->epoch;
		c->last_index[height]->header.numberOfEntriesInNode = 0;
		c->last_index[height]->header.fragmentation = 0;
		/*private key log for index nodes*/
		IN_log_header *bh = (IN_log_header *)((uint64_t)c->last_index[height] + INDEX_NODE_SIZE);
		bh->type = keyBlockHeader;
		bh->next = (void *)NULL;
		c->last_index[height]->header.first_IN_log_header =
			(IN_log_header *)((uint64_t)c->dev_offt[height] + ((uint64_t)bh % BUFFER_SEGMENT_SIZE));
		c->last_index[height]->header.last_IN_log_header = c->last_index[height]->header.first_IN_log_header;
		c->last_index[height]->header.key_log_size = sizeof(IN_log_header);
		break;
	}
	default:
		log_fatal("Wrong type");
		exit(EXIT_FAILURE);
	}
}

static void comp_append_pivot_to_index(struct comp_level_write_cursor *c, uint64_t left_node_offt,
				       uint64_t right_node_offt, char *pivot, uint32_t height)
{
	uint32_t pivot_size = *(uint32_t *)pivot + sizeof(uint32_t);
	uint64_t left_index_offt;
	uint64_t right_index_offt;
	uint32_t new_index = 0;
	char *new_pivot = NULL;
	char *new_pivot_buf = NULL;
	if (c->tree_height < height)
		c->tree_height = height;

	if (c->last_index[height]->header.numberOfEntriesInNode >= (uint32_t)index_order ||
	    KEY_BLOCK_SIZE - (c->last_index[height]->header.key_log_size % KEY_BLOCK_SIZE) < pivot_size) {
		// node if full
		/*keep current aka left leaf offt*/

		left_index_offt = c->dev_offt[height] + ((uint64_t)c->last_index[height] % BUFFER_SEGMENT_SIZE);
		uint64_t offt = c->last_index[height]->p[c->last_index[height]->header.numberOfEntriesInNode - 1].pivot;

		new_pivot = &c->segment_buf[height][offt % BUFFER_SEGMENT_SIZE];

		assert(*(uint32_t *)(new_pivot) > 0);
		assert(*(uint32_t *)(new_pivot) < 26);
		new_pivot_buf = (char *)malloc(*(uint32_t *)new_pivot + sizeof(uint32_t));
		memcpy(new_pivot_buf, new_pivot, *(uint32_t *)new_pivot + sizeof(uint32_t));
		--c->last_index[height]->header.numberOfEntriesInNode;
		comp_get_space(c, height, internalNode);
		/*last leaf updated*/
		right_index_offt = c->dev_offt[height] + ((uint64_t)c->last_index[height] % BUFFER_SEGMENT_SIZE);
		new_index = 1;
	}
	/*copy pivot*/
	char *addr = (char *)(uint64_t)c->last_index[height]->header.last_IN_log_header +
		     (c->last_index[height]->header.key_log_size % KEY_BLOCK_SIZE);
	addr = &c->segment_buf[height][(uint64_t)addr % BUFFER_SEGMENT_SIZE];

	memcpy(addr, pivot, pivot_size);
	c->last_index[height]->header.key_log_size += pivot_size;
	assert(*(uint32_t *)(addr) > 0);
	assert(*(uint32_t *)(addr) < 26);
	++c->last_index[height]->header.numberOfEntriesInNode;
	uint32_t idx = c->last_index[height]->header.numberOfEntriesInNode - 1;
	c->last_index[height]->p[idx].left[0] = left_node_offt;
	c->last_index[height]->p[idx].pivot = c->dev_offt[height] + ((uint64_t)addr % BUFFER_SEGMENT_SIZE);
	c->last_index[height]->p[idx].right[0] = right_node_offt;

	if (new_index) {
		comp_append_pivot_to_index(c, left_index_offt, right_index_offt, new_pivot_buf, height + 1);
		free(new_pivot_buf);
	}
	return;
}

#define COMP_MAX_KEY_SIZE 8192
static void comp_append_entry_to_leaf_node(struct comp_level_write_cursor *c, struct kv_prefix *kvPrefix)
{
	int new_leaf = 0;
	uint64_t left_leaf_offt;
	uint64_t right_leaf_offt;
	uint32_t idx = c->last_leaf->header.numberOfEntriesInNode;
	if (idx >= (uint32_t)leaf_order) {
		/*keep current aka left leaf offt*/
		left_leaf_offt = c->dev_offt[0] + ((uint64_t)c->last_leaf % BUFFER_SEGMENT_SIZE);
		comp_get_space(c, 0, leafNode);
		/*last leaf updated*/
		right_leaf_offt = c->dev_offt[0] + ((uint64_t)c->last_leaf % BUFFER_SEGMENT_SIZE);
		new_leaf = 1;
	}
	// just append and leave
	++c->last_leaf->header.numberOfEntriesInNode;

	idx = c->last_leaf->header.numberOfEntriesInNode - 1;
	c->last_leaf->kv_entry[idx].device_offt = kvPrefix->device_offt;
	c->last_leaf->kv_entry[idx].tombstone = kvPrefix->tombstone;
	memcpy(c->last_leaf->prefix[idx], kvPrefix->prefix, PREFIX_SIZE);

	++c->handle->db_desc->levels[c->level_id].level_size[1];
	if (new_leaf) {
		// log_info("keys are %llu for level %u",
		// c->handle->db_desc->levels[c->level_id].level_size[1],
		//	 c->level_id);
		char *buf = (char *)MAPPED + kvPrefix->device_offt;
		//log_info("Pivot is %u:%s", *(uint32_t *)buf, buf + 4);
		comp_append_pivot_to_index(c, left_leaf_offt, right_leaf_offt, buf, 1);
	}
	return;
}

/*####################################################*/

/* Checks for pending compactions. It is responsible to check for dependencies
 * between two levels before triggering a compaction. */

struct compaction_request {
	db_descriptor *db_desc;
	volume_descriptor *volume_desc;
	uint64_t l0_start;
	uint8_t src_level;
	uint8_t src_tree;
	uint8_t dst_level;
	uint8_t dst_tree;
};

#ifdef COMPACTION
static void *compaction(void *_comp_req);
#else
static void *spill_buffer(void *_comp_req);
#endif

void *compaction_daemon(void *args)
{
	struct db_handle *handle = (struct db_handle *)args;
	struct db_descriptor *db_desc = handle->db_desc;
	struct compaction_request *comp_req = NULL;
	pthread_setname_np(pthread_self(), "compactiond");
	int next_L0_tree_to_compact = 0;
	while (1) {
		/*special care for Level 0 to 1*/
		sem_wait(&db_desc->compaction_daemon_interrupts);
		struct level_descriptor *level_0 = &handle->db_desc->levels[0];
		struct level_descriptor *level_1 = &handle->db_desc->levels[1];

		int L0_tree = next_L0_tree_to_compact;
		// is level-0 full and not already spilling?
		if (level_0->tree_status[L0_tree] == NO_SPILLING &&
		    level_0->level_size[L0_tree] >= level_0->max_level_size) {
			// Can I issue a spill to L1?
			int L1_tree = 0;
			if (level_1->tree_status[L1_tree] == NO_SPILLING &&
			    level_1->level_size[L1_tree] < level_1->max_level_size) {
				if (handle->db_desc->is_in_replicated_mode && handle->db_desc->fl != NULL) {
					(*handle->db_desc->fl)((void *)handle);
				}

				/*mark them as spilling L0*/
				level_0->tree_status[L0_tree] = SPILLING_IN_PROGRESS;
				/*mark them as spilling L1*/
				level_1->tree_status[L1_tree] = SPILLING_IN_PROGRESS;
				/*start a compaction*/
				comp_req = (struct compaction_request *)malloc(sizeof(struct compaction_request));
				comp_req->db_desc = handle->db_desc;
				comp_req->volume_desc = handle->volume_desc;
				comp_req->src_level = 0;
				comp_req->src_tree = L0_tree;
				comp_req->dst_level = 1;

#ifdef COMPACTION
				comp_req->dst_tree = 1;
#else
				comp_req->dst_tree = 0;
#endif
				if (++next_L0_tree_to_compact >= NUM_TREES_PER_LEVEL)
					next_L0_tree_to_compact = 0;
			}
		}
		/*can I set a different active tree for L0*/
		int active_tree = db_desc->levels[0].active_tree;
		if (db_desc->levels[0].tree_status[active_tree] == SPILLING_IN_PROGRESS) {
			int next_active_tree = active_tree + 1;
			if (next_active_tree >= NUM_TREES_PER_LEVEL)
				next_active_tree = 0;
			// for (int i = 0; i < NUM_TREES_PER_LEVEL; i++) {
			if (db_desc->levels[0].tree_status[next_active_tree] == NO_SPILLING) {
				/*Acquire guard lock and wait writers to finish*/
				if (RWLOCK_WRLOCK(&(handle->db_desc->levels[0].guard_of_level.rx_lock))) {
					log_fatal("Failed to acquire guard lock");
					exit(EXIT_FAILURE);
				}
				spin_loop(&(comp_req->db_desc->levels[0].active_writers), 0);

				db_desc->levels[0].active_tree = next_active_tree; // i

				/*Release guard lock*/
				if (RWLOCK_UNLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock)) {
					log_fatal("Failed to acquire guard lock");
					exit(EXIT_FAILURE);
				}

				pthread_mutex_lock(&db_desc->client_barrier_lock);
				if (pthread_cond_broadcast(&db_desc->client_barrier) != 0) {
					log_fatal("Failed to wake up stopped clients");
					exit(EXIT_FAILURE);
				}
				pthread_mutex_unlock(&db_desc->client_barrier_lock);
			}
			//}
		}

		/*Now fire up (if needed) the spill/compaction from L0 to L1*/
		if (comp_req) {
#ifdef COMPACTION
			comp_req->dst_tree = 1;
			assert(db_desc->levels[0].root_w[comp_req->src_tree] != NULL ||
			       db_desc->levels[0].root_r[comp_req->src_tree] != NULL);
			if (pthread_create(&db_desc->levels[0].compaction_thread[comp_req->src_tree], NULL, compaction,
					   comp_req) != 0) {
				log_fatal("Failed to start compaction");
				exit(EXIT_FAILURE);
			}
#else
			comp_req->dst_tree = 0;
			if (pthread_create(&db_desc->levels[0].compaction_thread[comp_req->src_tree], NULL,
					   spill_buffer, comp_req) != 0) {
				log_fatal("Failed to start compaction");
				exit(EXIT_FAILURE);
			}
#endif
			comp_req = NULL;
		}

		// rest of levels
		for (int level_id = 1; level_id < MAX_LEVELS - 1; ++level_id) {
			struct level_descriptor *level_1 = &handle->db_desc->levels[level_id];
			struct level_descriptor *level_2 = &handle->db_desc->levels[level_id + 1];
			uint8_t tree_1 = 0; // level_1->active_tree;
			uint8_t tree_2 = 0; // level_2->active_tree;

			// log_info("level[%u][%u] = %llu size max is: %llu level[%u][%u] = %llu
			// size", level_id, tree_1,
			//	 level_1->level_size[tree_1], level_1->max_level_size, level_id
			//+ 1, tree_2,
			//	 level_2->level_size[tree_2]);
			// log_info("level status = %u", level_1->tree_status[tree_1]);
			if (level_1->tree_status[tree_1] == NO_SPILLING &&
			    level_1->level_size[tree_1] >= level_1->max_level_size) {
				// log_info("Level %u is F U L L", level_id);
				// src ready is destination ok?
				if (level_2->tree_status[tree_2] == NO_SPILLING &&
				    level_2->level_size[tree_2] < level_2->max_level_size) {
					level_1->tree_status[tree_1] = SPILLING_IN_PROGRESS;
					level_2->tree_status[tree_2] = SPILLING_IN_PROGRESS;
					/*start a compaction*/
					struct compaction_request *comp_req_p =
						(struct compaction_request *)malloc(sizeof(struct compaction_request));
					comp_req_p->db_desc = handle->db_desc;
					comp_req_p->volume_desc = handle->volume_desc;
					comp_req_p->src_level = level_id;
					comp_req_p->src_tree = tree_1;
					comp_req_p->dst_level = level_id + 1;

#ifdef COMPACTION
					comp_req_p->dst_tree = 1;
					assert(db_desc->levels[level_id].root_w[0] != NULL ||
					       db_desc->levels[level_id].root_r[0] != NULL);
					if (pthread_create(&db_desc->levels[0].compaction_thread[tree_1], NULL,
							   compaction, comp_req_p) != 0) {
						log_fatal("Failed to start compaction");
						exit(EXIT_FAILURE);
					}
#else
					comp_req_p->dst_tree = 0;
					if (pthread_create(&db_desc->levels[level_id].compaction_thread[tree_1], NULL,
							   spill_buffer, comp_req_p) != 0) {
						log_fatal("Failed to start compaction");
						exit(EXIT_FAILURE);
					}
#endif
				}
			}
		}
	}
}

static void swap_levels(struct level_descriptor *src, struct level_descriptor *dst, int src_active_tree,
			int dst_active_tree)
{
	dst->first_segment[dst_active_tree] = src->first_segment[src_active_tree];
	src->first_segment[src_active_tree] = NULL;

	dst->last_segment[dst_active_tree] = src->last_segment[src_active_tree];
	src->last_segment[src_active_tree] = NULL;

	dst->offset[dst_active_tree] = src->offset[src_active_tree];
	src->offset[src_active_tree] = 0;

	dst->level_size[dst_active_tree] = src->level_size[src_active_tree];
	src->level_size[src_active_tree] = 0;

	while (!__sync_bool_compare_and_swap(&dst->root_w[dst_active_tree], dst->root_w[dst_active_tree],
					     src->root_w[src_active_tree])) {
	}
	// dst->root_w[dst_active_tree] = src->root_w[src_active_tree];
	src->root_w[src_active_tree] = NULL;

	while (!__sync_bool_compare_and_swap(&dst->root_r[dst_active_tree], dst->root_r[dst_active_tree],
					     src->root_r[src_active_tree])) {
	}
	// dst->root_r[dst_active_tree] = src->root_r[src_active_tree];
	src->root_r[src_active_tree] = NULL;

	return;
}

#if EXPLICIT_IO
static void comp_compact_with_explicit_IO(struct compaction_request *comp_req, struct node_header *src_root,
					  struct node_header *dst_root)
{
	/*used for L0 only as src*/
	struct level_scanner *level_src = NULL;
	struct comp_level_read_cursor *l_src = NULL;
	struct comp_level_read_cursor *l_dst = NULL;

	struct db_handle handle = { .db_desc = comp_req->db_desc, .volume_desc = comp_req->volume_desc };
	struct comp_level_write_cursor *merged_level =
		(struct comp_level_write_cursor *)malloc(sizeof(struct comp_level_write_cursor));
	comp_init_write_cursor(merged_level, &handle, comp_req->dst_level, FD);

	uint64_t local_spilled_keys = 0;

	if (comp_req->src_level == 0) {
		snapshot(comp_req->volume_desc);
		level_src = _init_spill_buffer_scanner(&handle, src_root, NULL);
	} else {
		l_src = (struct comp_level_read_cursor *)malloc(sizeof(struct comp_level_read_cursor));
		comp_init_read_cursor(l_src, &handle, comp_req->src_level, 0, FD);
		comp_get_next_key(l_src);
	}

	if (dst_root) {
		l_dst = (struct comp_level_read_cursor *)malloc(sizeof(struct comp_level_read_cursor));
		if (!l_dst) {
			log_fatal("Malloc failed!");
			exit(EXIT_FAILURE);
		}
		comp_init_read_cursor(l_dst, &handle, comp_req->dst_level, 0, FD);
		comp_get_next_key(l_dst);
		assert(!l_dst->end_of_level);
	}

	log_info("Src [%u][%u] size = %llu", comp_req->src_level, comp_req->src_tree,
		 handle.db_desc->levels[comp_req->src_level].level_size[comp_req->src_tree]);
	if (dst_root)
		log_info("Dst [%u][%u] size = %llu", comp_req->dst_level, 0,
			 handle.db_desc->levels[comp_req->dst_level].level_size[0]);
	else
		log_info("Empty dst [%u][%u]", comp_req->dst_level, 0);

	struct sh_min_heap *m_heap = (struct sh_min_heap *)malloc(sizeof(struct sh_min_heap));
	sh_init_heap(m_heap, comp_req->src_level);
	struct sh_heap_node nd_src;
	struct sh_heap_node nd_dst;
	struct sh_heap_node nd_min;

	memset(&nd_src, 0x00, sizeof(struct sh_heap_node));
	memset(&nd_dst, 0x00, sizeof(struct sh_heap_node));
	memset(&nd_min, 0x00, sizeof(struct sh_heap_node));

	if (level_src)
		nd_src.kv_prefix = level_src->kv_prefix;
	else if (l_src)
		nd_src.kv_prefix = l_src->kvPrefix;

	nd_src.level_id = comp_req->src_level;
	nd_src.active_tree = comp_req->src_tree;
	nd_src.type = KV_PREFIX;
	sh_insert_heap_node(m_heap, &nd_src);

	if (dst_root) {
		nd_dst.kv_prefix = l_dst->kvPrefix;
		nd_dst.level_id = comp_req->dst_level;
		nd_dst.active_tree = comp_req->dst_tree;
		nd_dst.type = KV_PREFIX;
		sh_insert_heap_node(m_heap, &nd_dst);
	}
	int32_t num_of_keys = COMPACTION_UNIT_OF_WORK;
	enum sh_heap_status stat = GOT_MIN_HEAP;
	do {
		// while (handle.volume_desc->snap_preemption == SNAP_INTERRUPT_ENABLE)
		// usleep(50000);

		handle.db_desc->dirty = 0x01;
		if (handle.db_desc->stat == DB_IS_CLOSING) {
			log_info("db %s is closing compaction thread exiting...", handle.db_desc->db_name);
			if (l_src)
				free(l_src);
			if (l_dst)
				free(l_dst);
			return;
		}

		for (int i = 0; i < num_of_keys; i++) {
			stat = sh_remove_min(m_heap, &nd_min);
			if (stat == EMPTY_MIN_HEAP)
				break;
			if (!nd_min.duplicate) {
				comp_append_entry_to_leaf_node(merged_level, &nd_min.kv_prefix);
				++local_spilled_keys;
			}
			// log_info("level size
			// %llu",comp_req->db_desc->levels[comp_req->dst_level].level_size[comp_req->dst_tree]);
			/*refill from the appropriate level*/
			if (nd_min.level_id == comp_req->src_level) {
				if (nd_min.level_id == 0) {
					int rc = _get_next_KV(level_src);
					if (rc != END_OF_DATABASE) {
						// log_info("Refilling from L0");
						nd_min.kv_prefix = level_src->kv_prefix;
						nd_min.level_id = comp_req->src_level;
						sh_insert_heap_node(m_heap, &nd_min);
					}
				} else {
					comp_get_next_key(l_src);
					if (!l_src->end_of_level) {
						nd_min.kv_prefix = l_src->kvPrefix;
						nd_min.level_id = comp_req->src_level;
						sh_insert_heap_node(m_heap, &nd_min);
					}
				}
			} else {
				comp_get_next_key(l_dst);
				if (!l_dst->end_of_level) {
					nd_min.kv_prefix = l_dst->kvPrefix;
					nd_min.level_id = comp_req->dst_level;
					sh_insert_heap_node(m_heap, &nd_min);
				}
			}
		}
	} while (stat != EMPTY_MIN_HEAP);

	if (comp_req->src_level == 0)
		_close_spill_buffer_scanner(level_src, src_root);
	else {
		free(l_src);
	}

	if (dst_root) {
		free(l_dst);
	}

	comp_close_write_cursor(merged_level);
	merged_level->handle->db_desc->levels[comp_req->dst_level].root_w[1] =
		(struct node_header *)(MAPPED + merged_level->root_offt);
	free(merged_level);

	//assert(local_spilled_keys == handle.db_desc->levels[comp_req->src_level].level_size[comp_req->src_tree] +
	//				     handle.db_desc->levels[comp_req->dst_level].level_size[0]);
	return;
}
#else
static void comp_compact_with_mmap_IO(struct compaction_request *comp_req, struct node_header *src_root,
				      struct node_header *dst_root)
{
	struct db_handle handle = { .db_desc = comp_req->db_desc, .volume_desc = comp_req->volume_desc };
	struct level_scanner *level_src = NULL;
	struct level_scanner *level_dst = NULL;
	level_src = _init_spill_buffer_scanner(&handle, src_root, NULL);
	if (dst_root)
		level_dst = _init_spill_buffer_scanner(&handle, dst_root, NULL);

	log_info("Src [%u][%u] size = %llu", comp_req->src_level, comp_req->src_tree,
		 handle.db_desc->levels[comp_req->src_level].level_size[comp_req->src_tree]);
	if (dst_root)
		log_info("Dst [%u][%u] size = %llu", comp_req->dst_level, 0,
			 handle.db_desc->levels[comp_req->dst_level].level_size[0]);
	else
		log_info("Empty dst [%u][%u]", comp_req->dst_level, 0);

	struct sh_min_heap *m_heap = (struct sh_min_heap *)malloc(sizeof(struct sh_min_heap));
	sh_init_heap(m_heap, comp_req->src_level);
	struct sh_heap_node nd_src;
	struct sh_heap_node nd_dst;
	struct sh_heap_node nd_min;

	memset(&nd_src, 0x00, sizeof(struct sh_heap_node));
	memset(&nd_dst, 0x00, sizeof(struct sh_heap_node));
	memset(&nd_min, 0x00, sizeof(struct sh_heap_node));
	nd_src.kv_prefix = level_src->kv_prefix;
	nd_src.level_id = comp_req->src_level;
	nd_src.active_tree = comp_req->src_tree;
	nd_src.type = KV_PREFIX;
	sh_insert_heap_node(m_heap, &nd_src);
	if (dst_root) {
		nd_dst.kv_prefix = level_dst->kv_prefix;
		nd_dst.level_id = comp_req->dst_level;
		nd_dst.active_tree = comp_req->dst_tree;
		nd_dst.type = KV_PREFIX;
		sh_insert_heap_node(m_heap, &nd_dst);
	}
	uint64_t local_spilled_keys = 0;
	int32_t num_of_keys = (SPILL_BUFFER_SIZE - (2 * sizeof(uint32_t))) / (PREFIX_SIZE + sizeof(uint64_t));
	enum sh_heap_status stat = GOT_MIN_HEAP;
	do {
		// while (handle.volume_desc->snap_preemption == SNAP_INTERRUPT_ENABLE)
		// usleep(50000);

		handle.db_desc->dirty = 0x01;
		if (handle.db_desc->stat == DB_IS_CLOSING) {
			log_info("db is closing bye bye from spiller");
			return;
		}
		struct bt_insert_req ins_req;
		ins_req.metadata.handle = &handle;
		ins_req.metadata.level_id = comp_req->dst_level;
		ins_req.metadata.tree_id = comp_req->dst_tree;
		ins_req.metadata.key_format = KV_PREFIX;
		ins_req.metadata.append_to_log = 0;
		ins_req.metadata.special_split = 1;
		ins_req.metadata.gc_request = 0;
		ins_req.metadata.recovery_request = 0;

		for (int i = 0; i < num_of_keys; i++) {
			stat = sh_remove_min(m_heap, &nd_min);
			if (stat != EMPTY_MIN_HEAP) {
				ins_req.key_value_buf = &nd_min.kv_prefix;
				ins_req.metadata.is_tombstone = nd_min.kv_prefix.tombstone;
			} else
				break;
			if (!nd_min.duplicate)
				_insert_key_value(&ins_req);
			// log_info("level size
			// %llu",comp_req->db_desc->levels[comp_req->dst_level].level_size[comp_req->dst_tree]);
			/*refill from the appropriate level*/
			struct level_scanner *curr_scanner = NULL;
			if (nd_min.level_id == comp_req->src_level)
				curr_scanner = level_src;
			else if (nd_min.level_id == comp_req->dst_level)
				curr_scanner = level_dst;
			else {
				log_fatal("corruption unknown level");
				exit(EXIT_FAILURE);
			}
			int rc = _get_next_KV(curr_scanner);
			if (rc != END_OF_DATABASE) {
				nd_min.kv_prefix = curr_scanner->kv_prefix;
				sh_insert_heap_node(m_heap, &nd_min);
			}
			++local_spilled_keys;
		}
	} while (stat != EMPTY_MIN_HEAP);

	_close_spill_buffer_scanner(level_src, src_root);
	if (dst_root)
		_close_spill_buffer_scanner(level_dst, dst_root);

	assert(local_spilled_keys == handle.db_desc->levels[comp_req->src_level].level_size[comp_req->src_tree] +
					     handle.db_desc->levels[comp_req->dst_level].level_size[0]);
	return;
}
#endif

#ifdef COMPACTION
void *compaction(void *_comp_req)
{
	struct compaction_request *comp_req = (struct compaction_request *)_comp_req;
	struct db_descriptor *db_desc;

	pthread_setname_np(pthread_self(), "comp_thread");
	log_info("starting compaction from level's tree [%u][%u] to level's "
		 "tree[%u][%u]",
		 comp_req->src_level, comp_req->src_tree, comp_req->dst_level, comp_req->dst_tree);
	/*Initialize a scan object*/
	db_desc = comp_req->db_desc;

	db_handle handle;
	handle.db_desc = comp_req->db_desc;
	handle.volume_desc = comp_req->volume_desc;

	struct node_header *src_root = NULL;

	if (handle.db_desc->levels[comp_req->src_level].root_w[comp_req->src_tree] != NULL)
		src_root = handle.db_desc->levels[comp_req->src_level].root_w[comp_req->src_tree];
	else if (handle.db_desc->levels[comp_req->src_level].root_r[comp_req->src_tree] != NULL)
		src_root = handle.db_desc->levels[comp_req->src_level].root_r[comp_req->src_tree];
	else {
		log_fatal("NULL src root for compaction from level's tree [%u][%u] to "
			  "level's tree[%u][%u] for db %s",
			  comp_req->src_level, comp_req->src_tree, comp_req->dst_level, comp_req->dst_tree,
			  handle.db_desc->db_name);
		exit(EXIT_FAILURE);
	}

	/*optimization check if level below is empty than spill is a metadata
* operation*/
	struct node_header *dst_root = NULL;
	if (handle.db_desc->levels[comp_req->dst_level].root_w[0] != NULL)
		dst_root = handle.db_desc->levels[comp_req->dst_level].root_w[0];
	else if (handle.db_desc->levels[comp_req->dst_level].root_r[0] != NULL)
		dst_root = handle.db_desc->levels[comp_req->dst_level].root_r[0];
	else {
		log_info("Empty destination level %d ", comp_req->dst_level);
		dst_root = NULL;
	}

	if (comp_req->src_level == 0 || dst_root) {
#if EXPLICIT_IO
		comp_compact_with_explicit_IO(comp_req, src_root, dst_root);
#else
		comp_compact_with_mmap_IO(comp_req, src_root, dst_root);
#endif
		struct db_handle hd = { .db_desc = comp_req->db_desc, .volume_desc = comp_req->volume_desc };

		if (RWLOCK_WRLOCK(&(comp_req->db_desc->levels[comp_req->src_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}

		if (RWLOCK_WRLOCK(&(comp_req->db_desc->levels[comp_req->dst_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}
		if (dst_root) {
			/*special care for dst level atomic switch tree 2 to tree 1 of dst*/
			struct segment_header *curr_segment =
				comp_req->db_desc->levels[comp_req->dst_level].first_segment[0];

			assert(curr_segment != NULL);
			uint64_t space_freed = 0;
			while (1) {
				free_block(comp_req->volume_desc, curr_segment, SEGMENT_SIZE);
				space_freed += SEGMENT_SIZE;
				if (curr_segment->next_segment == NULL)
					break;
				curr_segment = MAPPED + curr_segment->next_segment;
			}
			log_info("Freed space %llu MB from db:%s level %u", space_freed / (1024 * 1024),
				 comp_req->db_desc->db_name, comp_req->src_level);
		}
		/*do the switch for the destination level*/
		log_info("Switching tree[%u][%u] to tree[%u][%u]", comp_req->dst_level, 1, comp_req->dst_level, 0);
		struct level_descriptor *ld = &comp_req->db_desc->levels[comp_req->dst_level];

		ld->first_segment[0] = ld->first_segment[1];
		ld->first_segment[1] = NULL;
		ld->last_segment[0] = ld->last_segment[1];
		ld->last_segment[1] = NULL;
		ld->offset[0] = ld->offset[1];
		ld->offset[1] = 0;

		if (ld->root_w[1] != NULL)
			ld->root_r[0] = ld->root_w[1];

		else if (ld->root_r[1] != NULL)
			ld->root_r[0] = ld->root_r[1];
		else {
			log_fatal("Where is the root?");
			exit(EXIT_FAILURE);
		}
		ld->root_w[0] = NULL;
		ld->level_size[0] = ld->level_size[1];
		ld->level_size[1] = 0;
		ld->root_w[1] = NULL;
		ld->root_r[1] = NULL;

		/*free src level*/
		seg_free_level(&hd, comp_req->src_level, comp_req->src_tree);

		if (RWLOCK_UNLOCK(&(comp_req->db_desc->levels[comp_req->src_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}

		if (RWLOCK_UNLOCK(&(comp_req->db_desc->levels[comp_req->dst_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}
		log_info("After compaction tree[%d][%d] size is %llu", comp_req->dst_level, 0, ld->level_size[0]);

	} else {
		if (RWLOCK_WRLOCK(&(comp_req->db_desc->levels[comp_req->src_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}

		if (RWLOCK_WRLOCK(&(comp_req->db_desc->levels[comp_req->dst_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}
		struct level_descriptor *leveld_src = &comp_req->db_desc->levels[comp_req->src_level];
		struct level_descriptor *leveld_dst = &comp_req->db_desc->levels[comp_req->dst_level];

		swap_levels(leveld_src, leveld_dst, comp_req->src_tree, 0);
		if (RWLOCK_UNLOCK(&(comp_req->db_desc->levels[comp_req->src_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}

		if (RWLOCK_UNLOCK(&(comp_req->db_desc->levels[comp_req->dst_level].guard_of_level.rx_lock))) {
			log_fatal("Failed to acquire guard lock");
			exit(EXIT_FAILURE);
		}

		log_info("Swapped levels %d to %d successfully", comp_req->src_level, comp_req->dst_level);
		log_info("After swapping dst tree[%d][%d] size is %llu", comp_req->dst_level, 0,
			 leveld_dst->level_size[0]);
		assert(leveld_dst->first_segment != NULL);
	}

	/*Clean up code, Free the buffer tree was occupying. free_block() used
* intentionally*/
	log_info("DONE Compaction from level's tree [%u][%u] to level's tree[%u][%u]", comp_req->src_level,
		 comp_req->src_tree, comp_req->dst_level, comp_req->dst_tree);

	/*send index to replicas if needed*/
	if (comp_req->db_desc->t != NULL) {
		// log_info("Sending index to my replica group for db %s",
		// comp_req->db_desc->db_name);
		// Caution new level has been created
		struct bt_compaction_callback_args c = { .db_desc = comp_req->db_desc,
							 .src_level = comp_req->src_level,
							 .src_tree = comp_req->src_tree,
							 .dst_level = comp_req->dst_level,
							 .dst_local_tree = 0,
							 .dst_remote_tree = 1 };

		(*db_desc->t)(&c);
		log_info("Done sending to group for db %s", comp_req->db_desc->db_name);
	}

	snapshot(comp_req->volume_desc);
	db_desc->levels[comp_req->src_level].tree_status[comp_req->src_tree] = NO_SPILLING;
	db_desc->levels[comp_req->dst_level].tree_status[0] = NO_SPILLING;

	/*wake up clients*/
	if (comp_req->src_level == 0) {
		pthread_mutex_lock(&comp_req->db_desc->client_barrier_lock);
		if (pthread_cond_broadcast(&db_desc->client_barrier) != 0) {
			log_fatal("Failed to wake up stopped clients");
			exit(EXIT_FAILURE);
		}
	}
	pthread_mutex_unlock(&db_desc->client_barrier_lock);
	sem_post(&db_desc->compaction_daemon_interrupts);
	free(comp_req);
	return NULL;
}
#else
void *spill_buffer(void *_comp_req)
{
	struct bt_insert_req ins_req;
	struct compaction_request *comp_req = (struct compaction_request *)_comp_req;
	struct db_descriptor *db_desc;
	struct level_scanner *level_sc;

	int32_t local_spilled_keys = 0;
	int i, rc = 100;

	pthread_setname_np(pthread_self(), "comp_thread");
	log_info("starting compaction from level's tree [%u][%u] to level's "
		 "tree[%u][%u]",
		 comp_req->src_level, comp_req->src_tree, comp_req->dst_level, comp_req->dst_tree);
	/*Initialize a scan object*/
	db_desc = comp_req->db_desc;

	db_handle handle;
	handle.db_desc = comp_req->db_desc;
	handle.volume_desc = comp_req->volume_desc;
	struct node_header *src_root = NULL;
	if (handle.db_desc->levels[comp_req->src_level].root_w[comp_req->src_tree] != NULL)
		src_root = handle.db_desc->levels[comp_req->src_level].root_w[comp_req->src_tree];
	else if (handle.db_desc->levels[comp_req->src_level].root_r[comp_req->src_tree] != NULL)
		src_root = handle.db_desc->levels[comp_req->src_level].root_r[comp_req->src_tree];
	else {
		log_fatal("NULL src root for compaction?");
		exit(EXIT_FAILURE);
	}
	level_sc = _init_spill_buffer_scanner(&handle, src_root, NULL);
	if (!level_sc) {
		log_fatal("Failed to create a spill buffer scanner for level's tree[%u][%u]", comp_req->src_level,
			  comp_req->src_tree);
		exit(EXIT_FAILURE);
	}
	int32_t num_of_keys = (SPILL_BUFFER_SIZE - (2 * sizeof(uint32_t))) / (PREFIX_SIZE + sizeof(uint64_t));

	/*optimization check if level below is empty than spill is a metadata
* operation*/
	struct node_header *dst_root = NULL;
	if (handle.db_desc->levels[comp_req->dst_level].root_w[0] != NULL)
		dst_root = handle.db_desc->levels[comp_req->dst_level].root_w[0];
	else if (handle.db_desc->levels[comp_req->dst_level].root_r[0] != NULL)
		dst_root = handle.db_desc->levels[comp_req->dst_level].root_r[0];
	else {
		log_info("Empty level %d time for an optimization :-)");
		dst_root = NULL;
	}

	if (dst_root) {
		do {
			while (handle.volume_desc->snap_preemption == SNAP_INTERRUPT_ENABLE)
				usleep(50000);

			db_desc->dirty = 0x01;
			if (handle.db_desc->stat == DB_IS_CLOSING) {
				log_info("db is closing bye bye from spiller");
				return NULL;
			}

			ins_req.metadata.handle = &handle;
			ins_req.metadata.level_id = comp_req->dst_level;
			ins_req.metadata.tree_id = comp_req->dst_tree;
			ins_req.metadata.key_format = KV_PREFIX;
			ins_req.metadata.append_to_log = 0;
			ins_req.metadata.special_split = 0;

			ins_req.metadata.gc_request = 0;
			ins_req.metadata.recovery_request = 0;

			for (i = 0; i < num_of_keys; i++) {
				ins_req.key_value_buf = level_sc->keyValue;
				_insert_key_value(&ins_req);
				rc = _get_next_KV(level_sc);
				if (rc == END_OF_DATABASE)
					break;

				++local_spilled_keys;
			}
		} while (rc != END_OF_DATABASE);

		_close_spill_buffer_scanner(level_sc, src_root);

		log_info("local spilled keys %d", local_spilled_keys);

		struct db_handle hd = { .db_desc = comp_req->db_desc, .volume_desc = comp_req->volume_desc };
		seg_free_level(&hd, comp_req->src_level, comp_req->src_tree);
	} else {
		struct level_descriptor *level_src = &comp_req->db_desc->levels[comp_req->src_level];
		struct level_descriptor *level_dst = &comp_req->db_desc->levels[comp_req->dst_level];
		swap_levels(level_src, level_dst, comp_req->src_tree, comp_req->dst_tree);

		log_info("Swapped levels %d to %d successfully", comp_req->src_level, comp_req->dst_level);
	}

	/*Clean up code, Free the buffer tree was occupying. free_block() used
* intentionally*/
	log_info("DONE Compaction from level's tree [%u][%u] to level's tree[%u][%u] "
		 "cleaning src level",
		 comp_req->src_level, comp_req->src_tree, comp_req->dst_level, comp_req->dst_tree);

	/*assert check
if(db_desc->spilled_keys !=
db_desc->total_keys[comp_req->src_tree_id]){
printf("[%s:%s:%d] FATAL keys missing --- spilled keys %llu actual
%llu spiller
id
%d\n",__FILE__,__func__,__LINE__,(LLU)db_desc->spilled_keys,(LLU)db_desc->total_keys[comp_req->src_tree_id],
comp_req->src_tree_id);
exit(EXIT_FAILURE);
}*/

	db_desc->levels[comp_req->src_level].tree_status[comp_req->src_tree] = NO_SPILLING;
	db_desc->levels[comp_req->dst_level].tree_status[comp_req->dst_tree] = NO_SPILLING;
	if (comp_req->src_tree == 0)
		db_desc->L0_start_log_offset = comp_req->l0_end;

	// log_info("DONE Cleaning src level tree [%u][%u] snapshotting...",
	// comp_req->src_level, comp_req->src_tree);
	/*interrupt compaction daemon*/
	snapshot(comp_req->volume_desc);
	/*wake up clients*/
	if (comp_req->src_level == 0) {
		pthread_mutex_lock(&comp_req->db_desc->client_barrier_lock);
		if (pthread_cond_broadcast(&db_desc->client_barrier) != 0) {
			log_fatal("Failed to wake up stopped clients");
			exit(EXIT_FAILURE);
		}
	}
	pthread_mutex_unlock(&db_desc->client_barrier_lock);
	sem_post(&db_desc->compaction_daemon_interrupts);
	free(comp_req);
	return NULL;
}
#endif
