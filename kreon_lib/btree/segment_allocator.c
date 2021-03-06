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

#include "segment_allocator.h"
#include <assert.h>
#include <log.h>
#include <signal.h>
extern uint64_t MAPPED;

static void *get_space(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id, uint32_t size,
		       char reason)
{
	segment_header *new_segment = NULL;
	node_header *node = NULL;
	uint64_t available_space;
	uint64_t offset_in_segment = 0;
	uint64_t segment_id;

	MUTEX_LOCK(&level_desc->level_allocation_lock);

	/*check if we have enough space to satisfy the request*/
	if (level_desc->offset[tree_id] == 0) {
		available_space = 0;
		segment_id = 0;
	} else if (level_desc->offset[tree_id] % SEGMENT_SIZE != 0) {
		offset_in_segment = level_desc->offset[tree_id] % SEGMENT_SIZE;
		available_space = SEGMENT_SIZE - offset_in_segment;
		segment_id = level_desc->last_segment[tree_id]->segment_id;
	} else {
		available_space = 0;
		segment_id = level_desc->last_segment[tree_id]->segment_id;
	}

	if (available_space < size) {
		/*we need to go to the actual allocator to get space*/
		if (level_desc->level_id != 0) {
			MUTEX_LOCK(&volume_desc->bitmap_lock);
			new_segment = (segment_header *)allocate(volume_desc, SEGMENT_SIZE, -1, reason);
			MUTEX_UNLOCK(&volume_desc->bitmap_lock);
			assert(new_segment);
		} else {
			if (posix_memalign((void **)&new_segment, SEGMENT_SIZE, SEGMENT_SIZE) != 0) {
				log_fatal("MEMALIGN FAILED");
				exit(EXIT_FAILURE);
			}
			assert(new_segment);
		}

		if (level_desc->offset[tree_id]) {
			//log_info("Adding another index segmet for [%u] available_space = %d", tree_id, available_space);
			int *pad = (int *)((uint64_t)level_desc->last_segment[tree_id] +
					   (level_desc->offset[tree_id] % SEGMENT_SIZE));
			*pad = paddedSpace;
			/*chain segments*/
			new_segment->next_segment = NULL;
			new_segment->prev_segment =
				(segment_header *)((uint64_t)level_desc->last_segment[tree_id] - MAPPED);

			level_desc->last_segment[tree_id]->next_segment =
				(segment_header *)((uint64_t)new_segment - MAPPED);
			level_desc->last_segment[tree_id] = new_segment;
			level_desc->last_segment[tree_id]->segment_id = segment_id + 1;
			level_desc->offset[tree_id] += (available_space + sizeof(segment_header));
			level_desc->segments_allocated[tree_id]++;
		} else {
			//log_info("Adding first index segmet for [%u]",tree_id);
			/*special case for the first segment for this level*/
			new_segment->next_segment = NULL;
			new_segment->prev_segment = NULL;
			level_desc->first_segment[tree_id] = new_segment;
			level_desc->last_segment[tree_id] = new_segment;
			level_desc->last_segment[tree_id]->segment_id = 0;
			level_desc->offset[tree_id] = sizeof(segment_header);
			level_desc->segments_allocated[tree_id]++;
		}
		offset_in_segment = level_desc->offset[tree_id] % SEGMENT_SIZE;
	}
	node = (node_header *)((uint64_t)level_desc->last_segment[tree_id] + offset_in_segment);
	level_desc->offset[tree_id] += size;
	//level_desc->level_size += size;
	MUTEX_UNLOCK(&level_desc->level_allocation_lock);
	assert(node != NULL);
	//log_info("prt %llu",node);
	return node;
}

struct segment_header *get_segment_for_explicit_IO(volume_descriptor *volume_desc, level_descriptor *level_desc,
						   uint8_t tree_id)
{
	if (level_desc->level_id == 0) {
		log_warn("Not allowed this kind of allocations for L0!");
		return NULL;
	}
	MUTEX_LOCK(&volume_desc->bitmap_lock);
	struct segment_header *new_segment = (segment_header *)allocate(volume_desc, SEGMENT_SIZE, -1, 1);
	MUTEX_UNLOCK(&volume_desc->bitmap_lock);
	assert(new_segment);

	if (level_desc->offset[tree_id]) {
		uint64_t segment_id;
		segment_id = level_desc->last_segment[tree_id]->segment_id + 1;
		/*chain segments*/
		new_segment->next_segment = NULL;
		new_segment->prev_segment = (segment_header *)((uint64_t)level_desc->last_segment[tree_id] - MAPPED);

		level_desc->last_segment[tree_id]->next_segment = (segment_header *)((uint64_t)new_segment - MAPPED);
		level_desc->last_segment[tree_id] = new_segment;
		level_desc->last_segment[tree_id]->segment_id = segment_id;
		level_desc->offset[tree_id] += SEGMENT_SIZE;
	} else {
		//log_info("Adding first index segmet for [%u]",tree_id);
		/*special case for the first segment for this level*/
		new_segment->next_segment = NULL;
		new_segment->prev_segment = NULL;
		level_desc->first_segment[tree_id] = new_segment;
		level_desc->last_segment[tree_id] = new_segment;
		level_desc->last_segment[tree_id]->segment_id = 0;
		level_desc->offset[tree_id] = SEGMENT_SIZE;
	}
	return new_segment;
}

index_node *seg_get_index_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
			       char reason)
{
	index_node *ptr;
	IN_log_header *bh;

	ptr = (index_node *)get_space(volume_desc, level_desc, tree_id, INDEX_NODE_SIZE + KEY_BLOCK_SIZE, reason);

	if (reason == NEW_ROOT)
		ptr->header.type = rootNode;
	else
		ptr->header.type = internalNode;

	ptr->header.epoch = volume_desc->mem_catalogue->epoch;
	ptr->header.numberOfEntriesInNode = 0;
	ptr->header.fragmentation = 0;
	//ptr->header.v1 = 0;
	//ptr->header.v2 = 0;

	/*private key log for index nodes*/
	bh = (IN_log_header *)((uint64_t)ptr + INDEX_NODE_SIZE);
	bh->type = keyBlockHeader;
	bh->next = (void *)NULL;
	ptr->header.first_IN_log_header = (IN_log_header *)((uint64_t)bh - MAPPED);
	ptr->header.last_IN_log_header = ptr->header.first_IN_log_header;
	ptr->header.key_log_size = sizeof(IN_log_header);
	//if (ptr->header.type == rootNode) /*increase node height by 1*/
	//	ptr->header.height = level_desc->root_w[level_desc->active_tree]->height + 1;
	return ptr;
}

index_node *seg_get_index_node_header(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				      char reason)
{
	return (index_node *)get_space(volume_desc, level_desc, tree_id, INDEX_NODE_SIZE, reason);
}

IN_log_header *seg_get_IN_log_block(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				    char reason)
{
	return (IN_log_header *)get_space(volume_desc, level_desc, tree_id, KEY_BLOCK_SIZE, reason);
}

void seg_free_index_node_header(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				node_header *node)
{
	//leave for future use
	(void)level_desc;
	(void)tree_id;
	(void)volume_desc;
	(void)node;
	//free_block(volume_desc, node, INDEX_NODE_SIZE);
	return;
}

void seg_free_index_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
			 index_node *inode)
{
	//leave for future use
	return;
	(void)level_desc;
	(void)tree_id;
	if (inode->header.type == leafNode || inode->header.type == leafRootNode) {
		log_fatal("Faulty type of node!");
		exit(EXIT_FAILURE);
	}

	/*for IN, BIN, root nodes free the key log as well*/
	if (inode->header.first_IN_log_header == NULL) {
		log_fatal("NULL log for index?");
		exit(EXIT_FAILURE);
	}
	IN_log_header *curr = (IN_log_header *)(MAPPED + (uint64_t)inode->header.first_IN_log_header);
	IN_log_header *last = (IN_log_header *)(MAPPED + (uint64_t)inode->header.last_IN_log_header);
	IN_log_header *to_free;
	while ((uint64_t)curr != (uint64_t)last) {
		to_free = curr;
		curr = (IN_log_header *)((uint64_t)MAPPED + (uint64_t)curr->next);
		free_block(volume_desc, to_free, KEY_BLOCK_SIZE);
	}
	free_block(volume_desc, last, KEY_BLOCK_SIZE);
	/*finally node_header*/
	free_block(volume_desc, inode, INDEX_NODE_SIZE);
	return;
}

leaf_node *seg_get_leaf_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id, char reason)
{
	leaf_node *leaf = (leaf_node *)get_space(volume_desc, level_desc, tree_id, LEAF_NODE_SIZE, reason);

	leaf->header.type = leafNode;
	leaf->header.epoch = volume_desc->mem_catalogue->epoch;
	leaf->header.numberOfEntriesInNode = 0;
	leaf->header.fragmentation = 0;
	//leaf->header.v1 = 0;
	//leaf->header.v2 = 0;

	leaf->header.first_IN_log_header = NULL; /*unused field in leaves*/
	leaf->header.last_IN_log_header = NULL; /*unused field in leaves*/
	leaf->header.key_log_size = 0; /*unused also*/
	leaf->header.height = 0;
	return leaf;
}

leaf_node *seg_get_leaf_node_header(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				    char reason)
{
	return (leaf_node *)get_space(volume_desc, level_desc, tree_id, LEAF_NODE_SIZE, reason);
}

void seg_free_leaf_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id, leaf_node *leaf)
{
	//leave for future use
	(void)level_desc;
	(void)tree_id;
	return;
	free_block(volume_desc, leaf, LEAF_NODE_SIZE);
}

segment_header *seg_get_raw_index_segment(volume_descriptor *volume_desc, level_descriptor *level_desc, int tree_id)
{
	segment_header *sg;
	MUTEX_LOCK(&volume_desc->bitmap_lock);
	sg = (segment_header *)allocate(volume_desc, SEGMENT_SIZE, -1, KV_LOG_EXPANSION);
	if (level_desc->first_segment[tree_id] == NULL) {
		level_desc->first_segment[tree_id] = sg;
		level_desc->first_segment[tree_id]->segment_id = 0;
		level_desc->last_segment[tree_id] = sg;
		level_desc->last_segment[tree_id]->next_segment = NULL;
		level_desc->offset[tree_id] = SEGMENT_SIZE;
	} else {
		uint64_t id = level_desc->last_segment[tree_id]->segment_id + 1;
		level_desc->last_segment[tree_id]->next_segment = (void *)(uint64_t)sg - MAPPED;
		level_desc->last_segment[tree_id] = sg;
		level_desc->last_segment[tree_id]->next_segment = NULL;
		level_desc->offset[tree_id] += SEGMENT_SIZE;
		level_desc->last_segment[tree_id]->segment_id = id;
	}
	MUTEX_UNLOCK(&volume_desc->bitmap_lock);
	return sg;
}

segment_header *seg_get_raw_log_segment(volume_descriptor *volume_desc)
{
	segment_header *sg;
	MUTEX_LOCK(&volume_desc->bitmap_lock);
	sg = (segment_header *)allocate(volume_desc, SEGMENT_SIZE, -1, KV_LOG_EXPANSION);

	MUTEX_UNLOCK(&volume_desc->bitmap_lock);
	return sg;
}

void free_raw_segment(volume_descriptor *volume_desc, segment_header *segment)
{
	free_block(volume_desc, segment, SEGMENT_SIZE);
	return;
}

void *get_space_for_system(volume_descriptor *volume_desc, uint32_t size, int lock)
{
	void *addr;
	if (size % 4096 != 0) {
		log_fatal("faulty size %lu not a multiple of 4KB", size);
		raise(SIGINT);
		exit(EXIT_FAILURE);
	}

	segment_header *new_segment = NULL;
	segment_header *first_sys_segment;
	segment_header *last_sys_segment;
	uint64_t available_space;
	uint64_t offset_in_segment = 0;
	uint64_t segment_id;

	if (lock)
		MUTEX_LOCK(&volume_desc->bitmap_lock);

	first_sys_segment = (segment_header *)(MAPPED + volume_desc->mem_catalogue->first_system_segment);
	last_sys_segment = (segment_header *)(MAPPED + volume_desc->mem_catalogue->last_system_segment);
	/*check if we have enough space to satisfy the request*/

	if (volume_desc->mem_catalogue->offset == 0) {
		available_space = 0;
		segment_id = 0;
	} else if (volume_desc->mem_catalogue->offset % SEGMENT_SIZE != 0) {
		offset_in_segment = volume_desc->mem_catalogue->offset % SEGMENT_SIZE;
		available_space = SEGMENT_SIZE - offset_in_segment;
		segment_id = last_sys_segment->segment_id;
	} else {
		available_space = 0;
		segment_id = last_sys_segment->segment_id;
	}
	//log_info("available %llu volume offset %llu", available_space, volume_desc->mem_catalogue->offset);
	if (available_space < size) {
		/*we need to go to the actual allocator to get space*/

		new_segment = (segment_header *)allocate(volume_desc, SEGMENT_SIZE, -1, SYSTEM_ID);

		if (segment_id) {
			/*chain segments*/
			new_segment->next_segment = NULL;
			new_segment->prev_segment = (segment_header *)((uint64_t)last_sys_segment - MAPPED);
			last_sys_segment->next_segment = (segment_header *)((uint64_t)new_segment - MAPPED);
			last_sys_segment = new_segment;
			last_sys_segment->segment_id = segment_id + 1;
			volume_desc->mem_catalogue->offset += (available_space + sizeof(segment_header));
		} else {
			/*special case for the first segment for this level*/
			new_segment->next_segment = NULL;
			new_segment->prev_segment = NULL;
			first_sys_segment = new_segment;
			last_sys_segment = new_segment;
			last_sys_segment->segment_id = 1;
			volume_desc->mem_catalogue->offset = sizeof(segment_header);
		}
		offset_in_segment = volume_desc->mem_catalogue->offset % SEGMENT_SIZE;
		/*serialize the updated info of first, last system segments*/
		volume_desc->mem_catalogue->first_system_segment = (uint64_t)first_sys_segment - MAPPED;
		volume_desc->mem_catalogue->last_system_segment = (uint64_t)last_sys_segment - MAPPED;
	}

	addr = (void *)(uint64_t)last_sys_segment + offset_in_segment;
	volume_desc->mem_catalogue->offset += size;

	if (lock)
		MUTEX_UNLOCK(&volume_desc->bitmap_lock);
	return addr;
}

void free_system_space(volume_descriptor *volume_desc, void *addr, uint32_t length)
{
	(void)volume_desc;
	(void)addr;
	(void)length;
	//TODO
	return;
}

void seg_free_level(db_handle *handle, uint8_t level_id, uint8_t tree_id)
{
	segment_header *curr_segment;
	uint64_t space_freed = 0;
	/*log_info("Freeing tree [%u][%u] for db %s", level_id, tree_id, handle->db_desc->db_name);*/

	curr_segment = handle->db_desc->levels[level_id].first_segment[tree_id];
	//if (level_id == 0) {
	//	if (RWLOCK_WRLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock)) {
	//		exit(EXIT_FAILURE);
	//	}
	//}
	if (curr_segment == NULL) {
		log_warn("trying to free an empty level valid in case of replicas");
		return;
		//goto finish;
	}
	int freed_segments = 0;
	while (1) {
		uint64_t next_dev_offt = (uint64_t)curr_segment->next_segment;
		if (level_id == 0) {
			++freed_segments;
			free(curr_segment);
		} else
			free_block(handle->volume_desc, curr_segment, SEGMENT_SIZE);
		space_freed += SEGMENT_SIZE;
		if (!next_dev_offt)
			break;
		else
			curr_segment = (struct segment_header *)(MAPPED + next_dev_offt);
	}
	log_debug("Freed space %llu MB from db:%s level tree [%u][%u]", space_freed / (1024 * 1024),
		  handle->db_desc->db_name, level_id, tree_id);
	assert(handle->db_desc->levels[level_id].segments_allocated[tree_id] == freed_segments);
	/*buffered tree out*/
	handle->db_desc->levels[level_id].level_size[tree_id] = 0;
	handle->db_desc->levels[level_id].first_segment[tree_id] = NULL;
	handle->db_desc->levels[level_id].last_segment[tree_id] = NULL;
	handle->db_desc->levels[level_id].offset[tree_id] = 0;
	handle->db_desc->levels[level_id].root_r[tree_id] = NULL;
	handle->db_desc->levels[level_id].root_w[tree_id] = NULL;
	handle->db_desc->levels[level_id].segments_allocated[tree_id] = 0;
	//finish:
	//if (level_id == 0) {
	//	if (RWLOCK_UNLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock)) {
	//		exit(EXIT_FAILURE);
	//	}
	//}
}
