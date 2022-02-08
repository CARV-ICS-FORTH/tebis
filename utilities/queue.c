/**
 * @brief
 * The Dynamic circular work-stealing deque WITHOUT the dynamic part ;)
 * (https://dl.acm.org/citation.cfm?id=1073974)
 *
 * NOTE: Also WITHOUT pop-front
 *
 * Removes the need for dynamic reallocation and constantly increasing
 * bottom and top
 *
 * @author  Foivos Zakkak <zakkak@ics.forth.gr>
 */

#include "queue.h"
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>

#define COMPILER_BARRIER() asm volatile("" : : : "memory")

#ifdef __GNUC__
#define UNLIKELY(cond) __builtin_expect(cond, 0)
#define LIKELY(cond) __builtin_expect(cond, 1)
#else /* ifdef __GNUC__ */
#define UNLIKELY(cond) (cond)
#define LIKELY(cond) (cond)
#endif /* ifdef __GNUC__ */

#ifndef UTILS_QUEUE_MPMC
// No spinlock, so define spin functions to nop
#define utils_spinlock_init(V)
#define utils_spinlock_lock(V)
#define utils_spinlock_unlock(V)
#endif

utils_queue_s *utils_queue_init(void *buff)
{
	assert(!(UTILS_QUEUE_CAPACITY & (UTILS_QUEUE_CAPACITY - 1)));
	/* Zero memory */
	memset(buff, 0, sizeof(struct queue));

	//utils_spinlock_init(&(((utils_queue_s *)buff)->lock));
	pthread_spin_init(&(((utils_queue_s *)buff)->lock), PTHREAD_PROCESS_PRIVATE);

	return (utils_queue_s *)buff;
}

unsigned int utils_queue_used_slots(utils_queue_s *q)
{
	register int used_slots;

	used_slots = q->bottom - q->top;

	if (used_slots < 0)
		used_slots += UINT16_MAX + 1;

	return (unsigned int)used_slots;
}

void *utils_queue_pop(utils_queue_s *q)
{
	register uint16_t t, b;
	register int i;
	void *ret_val = 0;

	//utils_spinlock_lock(&(q->lock));
	pthread_spin_lock(&(q->lock));

	/* Only one thief can succeed in the following critical section */
	t = q->top;
	b = q->bottom;

	/* If it is empty */
	if (b == t)
		goto RETURN;

	/* Get the top element */
	i = t & (UTILS_QUEUE_CAPACITY - 1);
	ret_val = q->entries[i];
	if (!__sync_bool_compare_and_swap(&q->top, t, t + 1))
		ret_val = 0;

RETURN:
	//utils_spinlock_unlock(&(q->lock));
	pthread_spin_unlock(&(q->lock));

	return ret_val;
}

void *utils_queue_push(utils_queue_s *q, void *data)
{
	uint16_t b, t;
	int i;

	assert(data);
	assert(q);

	//utils_spinlock_lock(&(q->lock));
	pthread_spin_lock(&(q->lock));

	b = q->bottom;
	t = q->top;

	int used_slots = b - t;

	if (used_slots < 0)
		used_slots += UINT16_MAX + 1;

	/* If there is no more space */
	if (used_slots == UTILS_QUEUE_CAPACITY) {
		data = 0;
		goto RETURN;
	}

	i = b & (UTILS_QUEUE_CAPACITY - 1);
	q->entries[i] = data;
	__sync_synchronize();
	q->bottom = b + 1;

	/* printf("b=%u t=%u\n", ++b, t);
     * vine_assert(((b >> 7) == (t >> 7)) || ((b & (UTILS_QUEUE_CAPACITY-1)) <= (t & (UTILS_QUEUE_CAPACITY)))); */

RETURN:
	//utils_spinlock_unlock(&(q->lock));
	pthread_spin_unlock(&(q->lock));
	return data;
} /* utils_queue_push */

void *utils_queue_peek(utils_queue_s *q)
{
	register uint16_t t, b;
	register int i;

	/* Only one thief can succeed in the following critical section */
	t = q->top;
	b = q->bottom;

	/* If it is empty */
	if (b == t)
		return 0;

	i = t & (UTILS_QUEUE_CAPACITY - 1);

	return q->entries[i];
}
