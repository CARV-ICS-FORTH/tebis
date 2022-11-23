#include "circular_buffer.h"
#include <assert.h>
#include <log.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#define BITS_PER_BITMAP_WORD 64
#define BIT_MASK(X) (1L << X)
#define INV_BIT_MASK(X) (~BIT_MASK(X))
#define BIT_ON(X, Y) (*(X) = *(X) | BIT_MASK(Y))
#define BIT_OFF(X, Y) (*(X) = *(X)&INV_BIT_MASK(Y))
#define GET_BIT(X, Y) ((X & (1L << Y)) >> Y)

static void mark_used_space_in_bitmap(circular_buffer *c, char *address, uint32_t size);
static int check_if_space_is_free(circular_buffer *c, char *addr, uint32_t size);

/*Note bit 1 unit free, 0 unit in use*/
circular_buffer *create_and_init_circular_buffer(char *memory_region, uint32_t memory_region_size,
						 uint32_t memory_size_represented_per_bit, circular_buffer_type type)
{
	assert(memory_region_size % (BITS_PER_BITMAP_WORD * memory_size_represented_per_bit) == 0);
	int bitmap_size = (memory_region_size / memory_size_represented_per_bit);
	assert(bitmap_size % BITS_PER_BITMAP_WORD == 0);
	bitmap_size = bitmap_size / BITS_PER_BITMAP_WORD;
	circular_buffer *c = (circular_buffer *)calloc(1, sizeof(circular_buffer) + (bitmap_size * sizeof(uint64_t)));
	c->bitmap_size = bitmap_size; // 4 words * 64 bit
	c->total_memory_size = memory_region_size;
	c->remaining_space = memory_region_size;
	c->memory_size_represented_per_bit = memory_size_represented_per_bit;
	c->memory_region = memory_region;
	c->last_addr = memory_region;

	c->type = type;
	memset((void *)c->bitmap, 0xFF, c->bitmap_size * sizeof(uint64_t));
	return c;
}

circular_buffer_op_status allocate_space_from_circular_buffer(circular_buffer *c, uint32_t size, char **addr)
{
	assert(size % c->memory_size_represented_per_bit == 0);
	assert(size <= c->total_memory_size);

	if (c->remaining_space == 0) {
		/*silently reset the buffer*/
		c->remaining_space = c->total_memory_size;
		c->last_addr = c->memory_region;
		*addr = NULL;
	}

	if (c->remaining_space >= size) {
		if (check_if_space_is_free(c, c->last_addr, size)) {
			mark_used_space_in_bitmap(c, c->last_addr, size);
			*addr = c->last_addr;
			c->remaining_space -= size;
			c->last_addr += size;
			return ALLOCATION_IS_SUCCESSFULL;
		}
		return SPACE_NOT_READY_YET;
	}
	/**
	 * space not enough, however for correctness we need to check if remaining space
	 * (although not sufficient) is free
	*/
	if (check_if_space_is_free(c, c->last_addr, c->remaining_space)) {
		*addr = NULL;
		return NOT_ENOUGH_SPACE_AT_THE_END;
	}
	return SPACE_NOT_READY_YET;
}

static int check_if_space_is_free(circular_buffer *c, char *tail_offt, uint32_t size)
{
	char *start_offt = tail_offt;
	char *end_offt = tail_offt + size;

	for (char *i = start_offt; i < end_offt; i += c->memory_size_represented_per_bit) {
		uint32_t distance_in_bits = (i - c->memory_region) / c->memory_size_represented_per_bit;

		uint32_t word_id = distance_in_bits / BITS_PER_BITMAP_WORD;
		uint32_t bit_inside_word = distance_in_bits % BITS_PER_BITMAP_WORD;
		//log_debug("Checking from word_id %lu if bit %lu is fre", word_id, bit_inside_word);
		if (!GET_BIT(c->bitmap[word_id], bit_inside_word))
			return 0;
		//log_debug("it is free");
	}

	return 1;
}

void free_space_from_circular_buffer(circular_buffer *c, char *tail_offt, uint32_t size)
{
	assert(size % c->memory_size_represented_per_bit == 0);
	char *start_offt = tail_offt;
	char *end_offt = tail_offt + size;

	for (char *i = start_offt; i < end_offt; i += c->memory_size_represented_per_bit) {
		uint32_t distance_in_bits = ((i - c->memory_region) / c->memory_size_represented_per_bit);
		uint32_t word_id = distance_in_bits / BITS_PER_BITMAP_WORD;

		uint32_t bit_inside_word = distance_in_bits % BITS_PER_BITMAP_WORD;
		//log_debug("Freeing from word_id %lu, bit %lu", word_id, bit_inside_word);
		BIT_ON(&c->bitmap[word_id], bit_inside_word);
	}
}

void mark_used_space_in_bitmap(circular_buffer *c, char *tail_offt, uint32_t size)
{
	assert(size % c->memory_size_represented_per_bit == 0);
	char *start_offt = tail_offt;
	char *end_offt = tail_offt + size;

	for (char *i = start_offt; i < end_offt; i += c->memory_size_represented_per_bit) {
		uint32_t distance_in_bits = (i - c->memory_region) / c->memory_size_represented_per_bit;
		uint32_t word_id = distance_in_bits / BITS_PER_BITMAP_WORD;
		uint32_t bit_inside_word = distance_in_bits % BITS_PER_BITMAP_WORD;

		//log_debug("Marking from word id %lu bit %lu off", word_id, bit_inside_word);
		BIT_OFF(&c->bitmap[word_id], bit_inside_word);
	}
}

void reset_circular_buffer(circular_buffer *c)
{
	c->remaining_space = c->total_memory_size;
	c->last_addr = c->memory_region;
}
