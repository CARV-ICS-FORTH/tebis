#define _GNU_SOURCE
#include "stats.h"

#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define OUT_FILE "ops.txt"

static pthread_t stats_reporter_thread_id;
static const struct timespec STATS_SLEEP_DURATION_TIMESPEC = { 5, 0 }; // sec = usec * 10^6
static int Stats_threads;
static int Stats_numa_nodes;
static FILE *Stats_output_file;
// (Performance) statistics
volatile uint32_t *Stats_operations;
volatile char stat_reporter_thread_exit = 0;

static void *stats_reporter_thread(void *);

void stats_init(int numa_nodes, int max_worker_threads_per_numa)
{
	Stats_threads = max_worker_threads_per_numa;
	Stats_numa_nodes = numa_nodes;
	int size = numa_nodes * max_worker_threads_per_numa * sizeof(uint32_t);
	Stats_operations = (uint32_t *)malloc(size);
	memset((void *)Stats_operations, 0, size);
	Stats_output_file = fopen(OUT_FILE, "w");
	pthread_create(&stats_reporter_thread_id, NULL, stats_reporter_thread, NULL);
}

void stats_update(int numa_node, int thread_id)
{
	++Stats_operations[numa_node * Stats_threads + thread_id];
}

void stats_notify_stop_reporter_thread(void)
{
	stat_reporter_thread_exit = 1;
}

static uint32_t sum_operations(void)
{
	uint32_t sum = 0;
	for (int i = 0; i < Stats_numa_nodes * Stats_threads; ++i) {
		sum += Stats_operations[i];
	}
	return sum;
}

#ifdef DEBUG_RESET_RENDEZVOUS
unsigned detected_operations = 0;
#endif /* DEBUG_RESET_RENDEZVOUS */
static void *stats_reporter_thread(void *args)
{
	(void)args;
	pthread_setname_np(pthread_self(), "stats_reporter");
	uint32_t ops_at_last_second = sum_operations();
	uint32_t ops_at_curr_second;
	struct timespec rem;
	size_t seconds_passed = 0;

	while (!sum_operations())
		nanosleep(&STATS_SLEEP_DURATION_TIMESPEC, &rem);

	do {
		nanosleep(&STATS_SLEEP_DURATION_TIMESPEC, &rem);
		seconds_passed += STATS_SLEEP_DURATION_TIMESPEC.tv_sec;

		ops_at_curr_second = sum_operations();
#ifdef DEBUG_RESET_RENDEZVOUS
		printf("%lu Sec %u Detected %u Completed %.2f Ops/sec\n", seconds_passed, detected_operations,
		       ops_at_curr_second,
		       (ops_at_curr_second - ops_at_last_second) / (double)STATS_SLEEP_DURATION_TIMESPEC.tv_sec);
#else
		printf("%lu Sec %u Completed %.2f Ops/sec\n", seconds_passed, ops_at_curr_second,
		       (ops_at_curr_second - ops_at_last_second) / (double)STATS_SLEEP_DURATION_TIMESPEC.tv_sec);
#endif /* DEBUG_RESET_RENDEZVOUS */
		ops_at_last_second = ops_at_curr_second;
	} while (!stat_reporter_thread_exit);

	fclose(Stats_output_file);

	return NULL;
}
