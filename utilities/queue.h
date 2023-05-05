// Copyright [2019] [FORTH-ICS]
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
#ifndef QUEUE_H
#define QUEUE_H
#include <pthread.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif /* ifdef __cplusplus */
#define CONF_CACHE_LINE 64
#define UTILS_QUEUE_CAPACITY 128
#define UTILS_QUEUE_MPMC
/**
 * Internal structure of queue.
 */

struct queue {
	/** Pointers to data. */
	void *entries[UTILS_QUEUE_CAPACITY];

#ifdef UTILS_QUEUE_MPMC
	//utils_spinlock lock;
	pthread_spinlock_t lock;
#endif
	/** Push here  */
	volatile uint16_t bottom __attribute__((aligned(CONF_CACHE_LINE)));

	/** Pop here */
	volatile uint16_t top __attribute__((aligned(CONF_CACHE_LINE)));
} __attribute__((aligned(CONF_CACHE_LINE)));

typedef struct queue utils_queue_s;

/**
 * Initialize a queue at the memory pointed by buff.
 *
 * @param buff Allocated buffer.
 * @return queue instance.NULL on failure.
 */
utils_queue_s *utils_queue_init(void *buff);

/**
 * Return number of used slots in the queue.
 *
 * NOTE: Since this is a concurrent queue the value returned by this
 * function may not always reflect the true state of the queue
 *
 * @param q Valid queue instance pointer.
 * @return Number of used slots in queue.
 */
unsigned int utils_queue_used_slots(utils_queue_s *q);

/**
 * Add data to an queue
 *
 * @param q Valid queue instance pointer.
 * @param data Non NULL pointer to data.
 * @return Equal to data, NULL on failure.
 */
void *utils_queue_push(utils_queue_s *q, void *data);

/**
 * Pop data from queue.
 *
 * @param q Valid queue instance pointer.
 * @return Data pointer, NULL on failure.
 */
void *utils_queue_pop(utils_queue_s *q);

/**
 * Peek first element from queue if any
 *
 * @param q Valid queue instance pointer.
 * @return Data pointer, NULL on failure.
 */
void *utils_queue_peek(utils_queue_s *q);
#ifdef __cplusplus
}
#endif /* ifdef __cplusplus */

#endif /* ifndef UTILS_QUEUE_HEADER */
