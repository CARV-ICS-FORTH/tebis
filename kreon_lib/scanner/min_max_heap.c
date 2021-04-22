/*
        File:   minHeap.c
        Desc:   Program showing various operations on a binary min heap
        Author: Robin Thomas <robinthomas2591@gmail.com>
        Edited by Giorgos Saloustros (gesalous@ics.forth.gr) 21/07/2017
*/

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>
#include <log.h>
#include <assert.h>
#include "scanner.h"
#include "../allocator/allocator.h"
#include "../btree/btree.h"
#define LCHILD(x) ((2 * x) + 1)
#define RCHILD(x) ((2 * x) + 2)
#define PARENT(x) ((x - 1) / 2)

static int sh_cmp_heap_nodes(struct sh_min_heap *hp, struct sh_heap_node *nd_1, struct sh_heap_node *nd_2)
{
	(void)hp;
	// this is used only for compactions, so there is no chance for things to not
	// be
	// in storage. So every time we use bt_get_real_address(...);

	struct bt_kv_log_address L1 = { .addr = NULL, .in_tail = 0, .tail_id = UINT8_MAX };
	struct bt_kv_log_address L2 = { .addr = NULL, .in_tail = 0, .tail_id = UINT8_MAX };

	if (nd_1->type == KV_FORMAT) {
		switch (nd_1->level_id) {
		case 0:
			L1 = bt_get_kv_log_address(nd_1->db_desc, nd_1->key_value.kv_log_offt);
			break;
		default:
			L1.addr = bt_get_real_address(nd_1->key_value.kv_log_offt);
			break;
		}
	}

	if (nd_2->type == KV_FORMAT) {
		switch (nd_2->level_id) {
		case 0:
			L2 = bt_get_kv_log_address(nd_2->db_desc, nd_2->key_value.kv_log_offt);
			break;
		default:
			L2.addr = bt_get_real_address(nd_2->key_value.kv_log_offt);
			break;
		}
	}

	int64_t ret;
	if (nd_1->type == KV_FORMAT && nd_2->type == KV_FORMAT) {
		ret = bt_key_cmp(L1.addr, L2.addr, KV_FORMAT, KV_FORMAT);
	} else if (nd_1->type == KV_FORMAT && nd_2->type == KV_PREFIX) {
		// kv 2 will work as usual:)
		ret = bt_key_cmp(L1.addr, &nd_2->kv_prefix, KV_FORMAT, KV_PREFIX);
	} else if (nd_1->type == KV_PREFIX && nd_2->type == KV_FORMAT) {
		// kv 1 will work as usual:)
		ret = bt_key_cmp(&nd_1->kv_prefix, L2.addr, KV_PREFIX, KV_FORMAT);
	} else if (nd_1->type == KV_PREFIX && nd_2->type == KV_PREFIX)
		ret = bt_key_cmp(&nd_1->kv_prefix, &nd_2->kv_prefix, KV_PREFIX, KV_PREFIX);

	else {
		log_fatal("unknown combination");
		raise(SIGINT);
		exit(EXIT_FAILURE);
	}

	if (L1.in_tail)
		bt_done_with_value_log_address(nd_1->db_desc, &L1);

	if (L2.in_tail)
		bt_done_with_value_log_address(nd_2->db_desc, &L2);

	if (ret == 0) {
		/* duplicate detected smallest level_id wins, others are duplicates*/
		if (nd_1->level_id < nd_2->level_id) {
			nd_2->duplicate = 1;
			return -1;
		} else if (nd_1->level_id > nd_2->level_id) {
			nd_1->duplicate = 1;
			return 1;
		} else {
			log_fatal("cannot resolve tie");
			assert(0);
			exit(EXIT_FAILURE);
		}
	}
	return ret;
}

/*
 * Function to initialize the min heap with size = 0
 */
void sh_init_heap(struct sh_min_heap *heap, int active_tree)
{
	(void)active_tree;
	heap->size = 0;
	// heap->active_tree = active_tree;
}

/*
    Heapify function is used to make sure that the heap property is never
   violated
    In case of deletion of a heap_node, or creating a min heap from an array,
   heap property
    may be violated. In such cases, heapify function can be called to make sure
   that
    heap property is never violated
*/
static inline void heapify(struct sh_min_heap *hp, int i)
{
	int smallest = i;
	if (LCHILD(i) < hp->size && sh_cmp_heap_nodes(hp, &hp->elem[LCHILD(i)], &hp->elem[i]) < 0)
		smallest = LCHILD(i);
	if (RCHILD(i) < hp->size && sh_cmp_heap_nodes(hp, &hp->elem[RCHILD(i)], &hp->elem[smallest]) < 0)
		smallest = RCHILD(i);

	if (smallest != i) {
		// swap(&(hp->elem[i]), &(hp->elem[smallest]))
		struct sh_heap_node temp = hp->elem[i];
		hp->elem[i] = hp->elem[smallest];
		hp->elem[smallest] = temp;
		heapify(hp, smallest);
	}
}

/*
    Function to insert a heap_node into the min heap, by allocating space for
   that heap_node in the
    heap and also making sure that the heap property and shape propety are never
   violated.
*/
void sh_insert_heap_node(struct sh_min_heap *hp, struct sh_heap_node *nd)
{
	int i;

	nd->duplicate = 0;
	if (hp->size > HEAP_SIZE) {
		log_fatal("min max heap out of space resize heap accordingly");
		exit(EXIT_FAILURE);
	}

	i = hp->size++;
	while (i && sh_cmp_heap_nodes(hp, nd, &(hp->elem[PARENT(i)])) < 0) {
		hp->elem[i] = hp->elem[PARENT(i)];
		// hp->elem[i].data = hp->elem[PARENT(i)].data;
		// hp->elem[i].level_id = hp->elem[PARENT(i)].level_id;
		// hp->elem[i].duplicate = hp->elem[PARENT(i)].duplicate;
		i = PARENT(i);
	}

	hp->elem[i] = *nd;
}

enum sh_heap_status sh_remove_min(struct sh_min_heap *hp, struct sh_heap_node *heap_node)
{
	if (hp->size) {
		*heap_node = hp->elem[0];
		// log_info("key is %s",heap_node->data+4);

		if (hp->size == 1) { // fast path
			hp->size = 0;
		} else {
			hp->elem[0] = hp->elem[--(hp->size)];
			heapify(hp, 0);
		}
		return GOT_MIN_HEAP;
	} else
		return EMPTY_MIN_HEAP;
}
