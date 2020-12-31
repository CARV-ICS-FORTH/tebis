#pragma once

/* Initialize stastistics module and starts the reporter thread
 * @arg numa_nodes: number of numa nodes used
 * @arg max_worker_threads_per_numa: maximum number of worker threadd per numa node
 */
void stats_init(int numa_nodes, int max_worker_threads_per_numa);

/* Called when a new request has been received by a worker thread
 * @arg numa_node: id of the numa node
 * @arg thread_id: id of the calling thread
 */
void stats_update(int numa_node, int thread_id);

/* Stop the reporter thread */
void stats_notify_stop_reporter_thread(void);
