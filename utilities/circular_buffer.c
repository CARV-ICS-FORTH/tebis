#include "circular_buffer.h"
#include "macros.h"
#include <assert.h>
#include <log.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#define BITS_PER_BITMAP_WORD 64

static void mark_used_space_in_bitmap(circular_buffer *c, char *address, uint32_t size);
static int check_if_space_is_free(circular_buffer *c, char *addr, uint32_t size);
static circular_buffer_op_status __allocate_space_from_send_circular_buffer(circular_buffer *c, uint32_t size,
									    char **addr);
static circular_buffer_op_status __allocate_space_from_recv_circular_buffer(circular_buffer *c, uint32_t size,
									    char **addr);
/*Note bit 1 unit free, 0 unit in use*/

circular_buffer *create_and_init_circular_buffer(char *memory_region, uint32_t memory_region_size,
						 uint32_t memory_size_represented_per_bit, circular_buffer_type type)
{
	assert(memory_region_size % (BITS_PER_BITMAP_WORD * memory_size_represented_per_bit) == 0);
	int bitmap_size = (memory_region_size / memory_size_represented_per_bit);
	assert(bitmap_size % BITS_PER_BITMAP_WORD == 0);
	bitmap_size = bitmap_size / BITS_PER_BITMAP_WORD;
	circular_buffer *c = (circular_buffer *)calloc(1, sizeof(circular_buffer) + (bitmap_size * sizeof(uint64_t)));
	c->bitmap_size = bitmap_size;
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
	switch (c->type) {
	case SEND_BUFFER:
	case SC_SEND_BUFFER:
		return __allocate_space_from_send_circular_buffer(c, size, addr);
	case RECEIVE_BUFFER:
	case SC_RECEIVE_BUFFER:
		return __allocate_space_from_recv_circular_buffer(c, size, addr);
	default:
		log_fatal("Unknown %d buffer type", c->type);
		exit(EXIT_FAILURE);
	}
}

static circular_buffer_op_status __allocate_space_from_send_circular_buffer(circular_buffer *c, uint32_t size,
									    char **addr)
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
		} else {
			/*space not freed yet*/
			return SPACE_NOT_READY_YET;
		}
	} else {
		/*
		 * space not enough, however for correctness we need to check if remaining space
		 * (although not sufficient) is free
		 */
		if (check_if_space_is_free(c, c->last_addr, c->remaining_space)) {
			*addr = NULL;
			return NOT_ENOUGH_SPACE_AT_THE_END;
		} else {
			return SPACE_NOT_READY_YET;
		}
	}
}

static circular_buffer_op_status __allocate_space_from_recv_circular_buffer(circular_buffer *c, uint32_t size,
									    char **addr)
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
		} else {
			/*space not freed yet*/
			//log_warn("Space not ready size requested %u remaining %u preallocated control space %u\n", size,
			//	 c->remaining_space, preallocated_control_space);
			return SPACE_NOT_READY_YET;
		}
	} else {
		/*
		 * space not enough, however for correctness we need to check if remaining space
		 * (although not sufficient) is free
		 */
		if (check_if_space_is_free(c, c->last_addr, c->remaining_space)) {
			*addr = NULL;
			return NOT_ENOUGH_SPACE_AT_THE_END;
		} else {
			return SPACE_NOT_READY_YET;
		}
	}
}

static int check_if_space_is_free(circular_buffer *c, char *address, uint32_t size)
{
	char *end;
	char *addr;

	uint32_t word_id;
	uint32_t bit_inside_word;
	uint64_t mask;

	addr = address;
	end = address + size;

	while (addr < end) {
		mask = 0x01;
		word_id = ((addr - c->memory_region) / c->memory_size_represented_per_bit) / BITS_PER_BITMAP_WORD;
		bit_inside_word =
			((addr - c->memory_region) / c->memory_size_represented_per_bit) % BITS_PER_BITMAP_WORD;

		mask = mask << bit_inside_word;
		mask = mask & c->bitmap[word_id];
		if (mask == 0) {
			return 0;
		}
		addr += c->memory_size_represented_per_bit;
	}
	return 1;
}

void free_space_from_circular_buffer(circular_buffer *c, char *address, uint32_t size)
{
	char *start;
	char *end;
	char *addr;
	uint32_t word_id;
	uint32_t bit_inside_word;
	uint64_t mask;
	uint64_t old_val;
	uint64_t new_val;
	uint64_t update_val;
	assert(size % c->memory_size_represented_per_bit == 0);
	assert(address >= c->memory_region && address < c->memory_region + c->total_memory_size);
	start = address;
	end = address + size;
	addr = start;

	while (addr < end) {
		word_id = ((addr - c->memory_region) / c->memory_size_represented_per_bit) / BITS_PER_BITMAP_WORD;
		bit_inside_word =
			((addr - c->memory_region) / c->memory_size_represented_per_bit) % BITS_PER_BITMAP_WORD;
		new_val = 0;
		do {
			mask = 0x01;
			mask = mask << bit_inside_word;
			new_val = new_val | mask;
			addr += c->memory_size_represented_per_bit;
			bit_inside_word =
				((addr - c->memory_region) / c->memory_size_represented_per_bit) % BITS_PER_BITMAP_WORD;
		} while (bit_inside_word != 0 && addr < end);

		while (1) {
			old_val = c->bitmap[word_id];
			update_val = new_val | old_val;
			if (__sync_bool_compare_and_swap(&(c->bitmap[word_id]), old_val, update_val))
				break;
		}
	}
}

void mark_used_space_in_bitmap(circular_buffer *c, char *address, uint32_t size)
{
	char *start;
	char *end;
	char *addr;

	uint32_t word_id;
	uint32_t bit_inside_word;
	uint64_t mask;
	uint64_t old_val;
	uint64_t new_val;
	uint64_t update_val;

	assert(size % c->memory_size_represented_per_bit == 0);
	start = address;
	end = address + size;
	addr = start;

	while (addr < end) {
		word_id = ((addr - c->memory_region) / c->memory_size_represented_per_bit) / BITS_PER_BITMAP_WORD;
		bit_inside_word =
			((addr - c->memory_region) / c->memory_size_represented_per_bit) % BITS_PER_BITMAP_WORD;
		new_val = 0xFFFFFFFFFFFFFFFF;
		do {
			mask = 0x01;
			mask = mask << bit_inside_word;
			mask = ~mask;
			new_val = new_val & mask;
			addr += c->memory_size_represented_per_bit;
			bit_inside_word =
				((addr - c->memory_region) / c->memory_size_represented_per_bit) % BITS_PER_BITMAP_WORD;
		} while (bit_inside_word != 0 && addr < end);

		while (1) {
			old_val = c->bitmap[word_id];
			update_val = new_val & old_val;
			if (__sync_bool_compare_and_swap(&(c->bitmap[word_id]), old_val, update_val))
				break;
		}
	}
}

void reset_circular_buffer(circular_buffer *c)
{
	c->remaining_space = c->total_memory_size;
	c->last_addr = c->memory_region;
}
