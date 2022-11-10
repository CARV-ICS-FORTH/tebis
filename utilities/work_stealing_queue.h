#ifndef __QUEUE_H__
#define __QUEUE_H__

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define UTILS_QUEUE_CAPACITY 128

typedef struct _queue_t queue_t;

void new_queue(queue_t **);
void init_queue(queue_t *);
void release_queue(queue_t *);
//__inline__ uint8_t isEmpty_Queue(queue_t *);
//__inline__ uint8_t isFull_Queue(queue_t *);
int enqueue(void *, queue_t *);
void *dequeue_front(queue_t *);
void *dequeue_back(queue_t *);
#endif
