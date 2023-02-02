#include "send_index_segment_iterator.h"
#include <assert.h>
#include <btree/btree.h>
#include <btree/dynamic_leaf.h>
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

struct send_index_segment_iterator {
	struct segment_header *traversing_segment;
	uint64_t segment_end;
	uint64_t curr_offt;
	enum send_index_segment_iterator_type type;
};

static enum send_index_segment_iterator_type send_index_segment_iterator_init_type(struct node_header *node_type)
{
	if (node_type->type == leafNode || node_type->type == leafRootNode)
		return LEAF_SEGMENT_ITERATOR;
	else if (node_type->type == internalNode || node_type->type == rootNode)
		return INDEX_SEGMENT_ITERATOR;
	else if (node_type->type != paddedSpace) {
		log_fatal("Found an invalid node type, this should never happen");
		_exit(EXIT_FAILURE);
	}
	return INVALID;
}

send_index_segment_iterator_t send_index_segment_iterator_init(struct segment_header *segment)
{
	assert(segment);
	struct send_index_segment_iterator *iterator =
		(struct send_index_segment_iterator *)calloc(1, sizeof(struct send_index_segment_iterator));
	iterator->traversing_segment = segment;
	iterator->segment_end = (uint64_t)segment + SEGMENT_SIZE;
	iterator->curr_offt = (uint64_t)segment + sizeof(struct segment_header);
	struct node_header *node_type = (struct node_header *)iterator->curr_offt;
	iterator->type = send_index_segment_iterator_init_type(node_type);

	return iterator;
}

bool send_index_segment_iterator_is_valid(send_index_segment_iterator_t iterator)
{
	assert(iterator);
	if (iterator->type == INVALID)
		return false;
	return true;
}

void send_index_segment_iterator_next(send_index_segment_iterator_t iterator)
{
	assert(iterator);
	switch (iterator->type) {
	case LEAF_SEGMENT_ITERATOR:;
		struct leaf_node *curr_leaf = (struct leaf_node *)iterator->curr_offt;
		iterator->curr_offt += dl_leaf_get_node_size(curr_leaf);
		break;
	case INDEX_SEGMENT_ITERATOR:
		iterator->curr_offt += index_node_get_size();
		break;
	default:
		log_fatal("Invalid segment iterator type");
		_exit(EXIT_FAILURE);
	}

	if (iterator->curr_offt > iterator->segment_end)
		iterator->type = INVALID;

	struct node_header *node_type = (struct node_header *)iterator->curr_offt;
	if (node_type->type == paddedSpace)
		iterator->type = INVALID;
}

void send_index_segment_iterator_destroy(send_index_segment_iterator_t *iterator)
{
	assert(*iterator);
	free(*iterator);
	*iterator = NULL;
}

enum send_index_segment_iterator_type send_index_segment_iterator_get_type(send_index_segment_iterator_t iterator)
{
	assert(iterator);
	return iterator->type;
}

uint64_t send_index_segment_iterator_get_node_addr(send_index_segment_iterator_t iterator)
{
	assert(iterator);
	return iterator->curr_offt;
}

uint64_t send_index_segment_iterator_get_segment_start(send_index_segment_iterator_t iterator)
{
	assert(iterator);
	return (uint64_t)iterator->traversing_segment;
}
