#ifndef SEND_INDEX_SEGMENT_ITERATOR_H
#define SEND_INDEX_SEGMENT_ITERATOR_H
#include <btree/btree.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct send_index_segment_iterator *send_index_segment_iterator_t;

enum send_index_segment_iterator_type {
	LEAF_SEGMENT_ITERATOR = 0,
	INDEX_SEGMENT_ITERATOR,
	INVALID,
};

send_index_segment_iterator_t send_index_segment_iterator_init(struct segment_header *segment);

bool send_index_segment_iterator_is_valid(send_index_segment_iterator_t iterator);

void send_index_segment_iterator_next(send_index_segment_iterator_t iterator);

void send_index_segment_iterator_destroy(send_index_segment_iterator_t *iterator);

enum send_index_segment_iterator_type send_index_segment_iterator_get_type(send_index_segment_iterator_t iterator);

uint64_t send_index_segment_iterator_get_node_addr(send_index_segment_iterator_t iterator);

uint64_t send_index_segment_iterator_get_segment_start(send_index_segment_iterator_t iterator);

#endif // SEND_INDEX_SEGMENT_ITERATOR_H
