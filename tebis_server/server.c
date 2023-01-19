// Copyright [2020] [FORTH-ICS]
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
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#include "allocator/log_structures.h"
#include "btree/conf.h"
#include "btree/kv_pairs.h"
#include "btree/level_write_appender.h"
#include "btree/level_write_cursor.h"
#include "btree/lsn.h"
#include "conf.h"
#include "parallax/structures.h"
#include "region_server.h"
#include "send_index/send_index_rewriter.h"
#include "send_index/send_index_uuid_checker/send_index_uuid_checker.h"
#include <include/parallax/parallax.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#endif
#include <alloca.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "../tebis_rdma/rdma.h"
#include "../tebis_rdma_client/msg_factory.h"
#include "../utilities/queue.h"
#include "../utilities/spin_loop.h"
#include "build_index/build_index.h"
#include "djb2.h"
#include "globals.h"
#include "messages.h"
#include "metadata.h"
#include "send_index/send_index.h"
#include "stats.h"
#include <btree/btree.h>
#include <log.h>
#include <scanner/scanner.h>

#ifdef CHECKSUM_DATA_MESSAGES
#include "djb2.h"
#endif

#define LOG_SEGMENT_CHUNK (32 * 1024)
#define MY_MAX_THREADS 2048

#define WORKER_THREAD_PRIORITIES_NUM 4
#define WORKER_THREAD_HIGH_PRIORITY_TASKS_PER_TURN 1
#define WORKER_THREAD_NORMAL_PRIORITY_TASKS_PER_TURN 1
#define MAX_OUTSTANDING_REQUESTS (UTILS_QUEUE_CAPACITY / 2)

#define DS_CLIENT_QUEUE_SIZE (UTILS_QUEUE_CAPACITY / 2)

struct ds_task_buffer_pool {
	pthread_mutex_t tbp_lock;
	utils_queue_s task_buffers;
};

#define MAX_WORKERS 32
#define MAX_NUMA_SERVERS 8

struct ds_worker_thread {
	utils_queue_s work_queue;
	sem_t sem;
	struct channel_rdma *channel;
	pthread_t context;
	/*for affinity purposes*/
	int cpu_core_id;
	int worker_id;
	/*my parent*/
	int root_server_id;
	volatile int idle_time;
	volatile worker_status status;
};

struct ds_spinning_thread {
	struct ds_task_buffer_pool resume_task_pool;
	pthread_mutex_t conn_list_lock;
	SIMPLE_CONCURRENT_LIST *conn_list;
	struct krm_work_task *server_tasks;
	uint64_t num_of_outstanding_client_req;
	int num_workers;
	int next_worker_to_submit_job;
	/*for affinity purposes*/
	int spinner_id;
	/*entry in the root table of my dad (numa_server)*/
	int root_server_id;
	// my workers follow
	struct ds_worker_thread worker[MAX_WORKERS];
};

struct ds_numa_server {
	// position in the numa_servers table of root
	int server_id;
	int rdma_port;
	// context of the socket thread
	pthread_t socket_thread_cnxt;
	// context of the poll_cq threwad
	pthread_t poll_cq_cnxt;
	// context of spinning thread
	pthread_t spinner_cnxt;
	pthread_t meta_server_cnxt;
	// our rdma channel
	struct channel_rdma *channel;

	struct regs_server_desc *region_server;
	// spinner manages its workers
	struct ds_spinning_thread spinner;
};

struct ds_root_server {
	int num_of_numa_servers;
	struct ds_numa_server *numa_servers[MAX_NUMA_SERVERS];
};

/*root of everything*/
static struct ds_root_server *root_server = NULL;

static void handle_task(struct regs_server_desc const *mydesc, struct krm_work_task *task);
static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct krm_work_task *task);

static void crdma_server_create_connection_inuse(struct connection_rdma *conn, struct channel_rdma *channel,
						 connection_type type)
{
	/*gesalous, This is the path where it creates the useless memory queues*/
	memset(conn, 0, sizeof(struct connection_rdma));
	conn->type = type;
	conn->channel = channel;
}

struct channel_rdma *ds_get_channel(struct regs_server_desc const *my_desc)
{
	return root_server->numa_servers[my_desc->root_server_id]->channel;
}

/*the tail of the kv_payload is a uint8_t at the end of the buffer.
	 *kv payloads follow |key_size|key|value_size|value| format */
void set_tail_for_kv_payload(char *kv_payload, uint32_t key_size, uint32_t value_size, uint8_t value)
{
	char *tail = kv_payload + key_size + value_size;
	*(uint8_t *)tail = value;
}

uint8_t get_tail_of_kv_payload(char *kv_payload)
{
	uint32_t key_size = *(uint32_t *)kv_payload;
	uint32_t value_size = *(uint32_t *)(kv_payload + key_size + sizeof(uint32_t));
	return *(uint8_t *)(kv_payload + sizeof(uint32_t) + key_size + sizeof(uint32_t) + value_size);
}

void on_completion_notify(struct rdma_message_context *msg_ctx)
{
	sem_post(&msg_ctx->wait_for_completion);
}

void *socket_thread(void *args)
{
	struct ds_numa_server *my_server = (struct ds_numa_server *)args;
	struct channel_rdma *channel = my_server->channel;
	int rdma_port = my_server->rdma_port;
	connection_rdma *conn;

	pthread_setname_np(pthread_self(), "rdma_conn_thread");

	log_info("Starting listener for new rdma connections thread at port %d", rdma_port);

	struct ibv_qp_init_attr qp_init_attr;
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.cap.max_send_wr = qp_init_attr.cap.max_recv_wr = MAX_WR;
	qp_init_attr.cap.max_send_sge = qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.cap.max_inline_data = 16;
	qp_init_attr.sq_sig_all = 1;
	qp_init_attr.qp_type = IBV_QPT_RC;

	struct rdma_addrinfo hints, *res;
	char port[16];
	sprintf(port, "%d", rdma_port);

	memset(&hints, 0, sizeof hints);
	hints.ai_flags = RAI_PASSIVE; // Passive side, awaiting incoming connections
	hints.ai_port_space = RDMA_PS_TCP; // Supports Reliable Connections
	int ret = rdma_getaddrinfo(NULL, port, &hints, &res);
	if (ret) {
		log_fatal("rdma_getaddrinfo: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	struct rdma_cm_id *listen_id = NULL;
	ret = rdma_create_ep(&listen_id, res, NULL, NULL);
	if (ret) {
		log_fatal("rdma_create_ep: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	// Listen for incoming connections on available RDMA devices
	ret = rdma_listen(listen_id, 0); // called with backlog = 0
	if (ret) {
		log_fatal("rdma_listen: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	while (1) {
		// Block until a new connection request arrives
		struct rdma_cm_id *request_id, *new_conn_id;
		ret = rdma_get_request(listen_id, &request_id);
		if (ret) {
			log_fatal("rdma_get_request: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
		new_conn_id = request_id->event->id;
		conn = (connection_rdma *)malloc(sizeof(connection_rdma));
		qp_init_attr.send_cq = qp_init_attr.recv_cq =
			ibv_create_cq(channel->context, MAX_WR, (void *)conn, channel->comp_channel, 0);
		ibv_req_notify_cq(qp_init_attr.send_cq, 0);
		assert(qp_init_attr.send_cq);

		ret = rdma_create_qp(new_conn_id, NULL, &qp_init_attr);
		if (ret) {
			log_fatal("rdma_create_qp: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}

		connection_type incoming_connection_type = -1;
		struct ibv_mr *recv_mr = rdma_reg_msgs(new_conn_id, &incoming_connection_type, sizeof(connection_type));
		struct rdma_message_context msg_ctx;
		client_rdma_init_message_context(&msg_ctx, NULL);
		msg_ctx.on_completion_callback = on_completion_notify;
		ret = rdma_post_recv(new_conn_id, &msg_ctx, &incoming_connection_type, sizeof(connection_type),
				     recv_mr);
		if (ret) {
			log_fatal("rdma_post_recv: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}

		// Accept incomming connection TODO look into rdma_conn_param a bit more
		ret = rdma_accept(new_conn_id, NULL);
		if (ret) {
			log_fatal("rdma_accept: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}

		// Block until client sends connection type
		sem_wait(&msg_ctx.wait_for_completion);
		rdma_dereg_mr(recv_mr);

		if (incoming_connection_type == CLIENT_TO_SERVER_CONNECTION) {
			incoming_connection_type = SERVER_TO_CLIENT_CONNECTION;
			log_info("We have a new client connection request");
		} else if (incoming_connection_type == MASTER_TO_REPLICA_CONNECTION) {
			incoming_connection_type = REPLICA_TO_MASTER_CONNECTION;
			log_debug("We have a new replica connection request");
		} else {
			log_fatal("bad connection type");
			exit(EXIT_FAILURE);
		}
		/*
* Important note to future self: klist.h used for keeping
* channels and connections used by spinning thread is NOT thread
* safe. Patch: all operations adding new connections and
* removing connections take place from the context of the
* spinning thread
*/

		/* I assume global state for the connections is already kept in the
* system?*/
		/*!!! follow this path to add this connection to the appropriate connection
* list !!!*/
		crdma_server_create_connection_inuse(
			conn, channel,
			incoming_connection_type); // TODO not sure if it's needed with rdma_cm
		conn->rdma_cm_id = new_conn_id;

		switch (conn->type) {
		case SERVER_TO_CLIENT_CONNECTION:
		case REPLICA_TO_MASTER_CONNECTION:
			conn->rdma_memory_regions = mrpool_allocate_memory_region(channel->dynamic_pool, new_conn_id);
			break;
		case MASTER_TO_REPLICA_CONNECTION:
			assert(0);
			break;
		default:
			log_fatal("bad connection type %d", conn->type);
			exit(EXIT_FAILURE);
		}
		assert(conn->rdma_memory_regions);

		struct ibv_mr *send_mr = rdma_reg_msgs(new_conn_id, conn->rdma_memory_regions->remote_memory_region,
						       sizeof(struct ibv_mr));

		// Receive memory region information
		conn->peer_mr = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
		memset(conn->peer_mr, 0, sizeof(struct ibv_mr));
		recv_mr = rdma_reg_msgs(new_conn_id, conn->peer_mr, sizeof(struct ibv_mr));
		ret = rdma_post_recv(new_conn_id, &msg_ctx, conn->peer_mr, sizeof(struct ibv_mr), recv_mr);
		if (ret) {
			log_fatal("rdma_post_recv: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}

		// Send memory region information
		ret = rdma_post_send(new_conn_id, NULL, conn->rdma_memory_regions->remote_memory_region,
				     sizeof(struct ibv_mr), send_mr, 0);
		if (ret) {
			log_fatal("rdma_post_send: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
		// Block until client sends memory region information
		sem_wait(&msg_ctx.wait_for_completion);
		rdma_dereg_mr(send_mr);
		rdma_dereg_mr(recv_mr);

		conn->status = CONNECTION_OK;
		conn->qp = conn->rdma_cm_id->qp;
		conn->rendezvous = conn->rdma_memory_regions->remote_memory_buffer;
		/*zerop all rdma memory*/
		memset(conn->rdma_memory_regions->local_memory_buffer, 0x00,
		       conn->rdma_memory_regions->memory_region_length);

		if (sem_init(&conn->congestion_control, 0, 0) != 0) {
			log_fatal("failed to initialize semaphore reason follows");
			perror("Reason: ");
		}
		conn->sleeping_workers = 0;

		conn->offset = 0;
		conn->worker_id = -1;
#ifdef CONNECTION_BUFFER_WITH_MUTEX_LOCK
		pthread_mutex_init(&conn->buffer_lock, NULL);
#else
		pthread_spin_init(&conn->buffer_lock, PTHREAD_PROCESS_PRIVATE);
#endif
		// Add connection to spinner's connection list
		pthread_mutex_lock(&my_server->spinner.conn_list_lock);
		add_last_in_simple_concurrent_list(my_server->spinner.conn_list, conn);
		conn->responsible_spin_list = my_server->spinner.conn_list;
		conn->responsible_spinning_thread_id = my_server->spinner.spinner_id;
		pthread_mutex_unlock(&my_server->spinner.conn_list_lock);

		log_debug("Built new connection successfully for Server at port %d", rdma_port);
	}
	return NULL;
}

/*
 * Each spin thread has a group of worker threads. Spin threads detects that a
 * request has arrived and it assigns the task to one of its worker threads
 * */
static inline size_t diff_timespec_usec(struct timespec *start, struct timespec *stop)
{
	struct timespec result;
	if ((stop->tv_nsec - start->tv_nsec) < 0) {
		result.tv_sec = stop->tv_sec - start->tv_sec - 1;
		result.tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000;
	} else {
		result.tv_sec = stop->tv_sec - start->tv_sec;
		result.tv_nsec = stop->tv_nsec - start->tv_nsec;
	}
	return result.tv_sec * 1000000 + (size_t)(result.tv_nsec / (double)1000) + 1;
}
uint32_t no_ops_acks_send = 0;
void *worker_thread_kernel(void *args)
{
	struct krm_work_task *job = NULL;
	struct ds_worker_thread *worker;
	const size_t spin_time_usec = globals_get_worker_spin_time_usec();

	worker = (struct ds_worker_thread *)args;
	char name[16];
	strcpy(name, "ds_worker");
	int idx = strlen(name);
	sprintf(&name[idx], "%d", worker->worker_id);
	pthread_setname_np(pthread_self(), name);

	while (1) {
		// Get the next task from one of the task queues
		// If there are tasks pending, rotate between all queues
		// Try to get a task

		if (!(job = utils_queue_pop(&worker->work_queue))) {
			// Try for a few more usecs
			struct timespec start;
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &start);
			while (1) {
				// I could have a for loop with a few iterations to avoid constantly
				// calling clock_gettime
				if ((job = utils_queue_pop(&worker->work_queue)))
					break;
				clock_gettime(CLOCK_MONOTONIC, &end);
				size_t local_idle_time = diff_timespec_usec(&start, &end);
				if (local_idle_time > spin_time_usec)
					worker->idle_time = local_idle_time;
				if (worker->status == IDLE_SLEEPING) {
					worker->idle_time = 0;
					sem_wait(&worker->sem);
					job = utils_queue_pop(&worker->work_queue);
					break;
				}
			}
			if (!job) // Which case would result here?
				continue;
		}
		/*process task*/
		enum message_type type = job->msg->msg_type;
		handle_task(root_server->numa_servers[worker->root_server_id]->region_server, job);
		switch (job->kreon_operation_status) {
		case TASK_COMPLETE:

			zero_rendezvous_locations(job->msg);
			// Replica index flush requests have already rdma writed there response to the status their according status fields
			if (type != REPLICA_INDEX_FLUSH_REQ) {
				__send_rdma_message(job->conn, job->reply_msg, NULL);
			}
			switch (job->task_type) {
			case KRM_CLIENT_TASK:
				__sync_fetch_and_sub(&root_server->numa_servers[worker->root_server_id]
							      ->spinner.num_of_outstanding_client_req,
						     1);
				free(job);
				break;
			case KRM_SERVER_TASK:
				free(job);
				break;
			default:
				log_fatal("Unkown pool type!");
				_exit(EXIT_FAILURE);
			}
			break;

		default:
			ds_put_resume_task(&(root_server->numa_servers[worker->root_server_id]->spinner), job);
			break;
		}
		job = NULL;
	}

	log_warn("worker thread exited");
	return NULL;
}

static inline int worker_queued_jobs(struct ds_worker_thread *worker)
{
	return utils_queue_used_slots(&worker->work_queue);
}

static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct krm_work_task *task)
{
	pthread_mutex_lock(&(spinner->resume_task_pool.tbp_lock));
	if (utils_queue_push(&(spinner->resume_task_pool.task_buffers), task) == NULL) {
		log_fatal("failed to add to resumed task queue");
		_exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&spinner->resume_task_pool.tbp_lock);
}

static int ds_is_server2server_job(struct msg_header *msg)
{
	switch (msg->msg_type) {
	case REPLICA_INDEX_GET_BUFFER_REQ:
	case REPLICA_INDEX_GET_BUFFER_REP:
	case REPLICA_INDEX_FLUSH_REQ:
	case REPLICA_INDEX_FLUSH_REP:
	case FLUSH_COMMAND_REQ:
	case FLUSH_COMMAND_REP:
	case FLUSH_L0_REQUEST:
	case FLUSH_L0_REPLY:
	case GET_RDMA_BUFFER_REQ:
	case GET_RDMA_BUFFER_REP:
	case CLOSE_COMPACTION_REQUEST:
	case CLOSE_COMPACTION_REPLY:
	case REPLICA_INDEX_SWAP_LEVELS_REQUEST:
	case REPLICA_INDEX_SWAP_LEVELS_REPLY:
		return 1;
	default:
		return 0;
	}
}

/** Functions assigning tasks to workers
 *  TODO refactor task mechanism. Now when task is NULL means that we have to insert the task in a workers queue
 *  while when task is not NULL we have to resume a task */
static int assign_job_to_worker(struct ds_spinning_thread *spinner, struct connection_rdma *conn, msg_header *msg,
				struct krm_work_task *task)
{
	struct krm_work_task *job = NULL;
	int is_server_message = ds_is_server2server_job(msg);
	uint8_t is_task_resumed = 0;

	/*allocate or resume task*/
	/*we have to schedule a new task*/
	if (task == NULL) {
		is_task_resumed = 0;
		if (is_server_message) {
			/*TODO XXX check this again*/
			job = (struct krm_work_task *)calloc(1, sizeof(struct krm_work_task));
			job->task_type = KRM_SERVER_TASK;
		} else {
			if (spinner->num_of_outstanding_client_req > MAX_OUTSTANDING_REQUESTS) {
				//log_debug("I am full, can not serve more");
				return TEBIS_FAILURE;
			}
			__sync_fetch_and_add(&spinner->num_of_outstanding_client_req, 1);
			job = (struct krm_work_task *)calloc(1, sizeof(struct krm_work_task));
			job->task_type = KRM_CLIENT_TASK;
		}
	} else { /*we have to resume a task*/
		job = task;
		is_task_resumed = 1;
		if (is_server_message) {
			log_fatal("server2server messages are not resumable");
			_exit(EXIT_FAILURE);
		}
	}

	int max_queued_jobs = globals_get_job_scheduling_max_queue_depth(); // TODO [mvard] Tune this
	int worker_id = spinner->next_worker_to_submit_job;

	/* Regular tasks scheduling policy
* Assign tasks to one worker until he is swamped, then start assigning
* to the next one. Once all workers are swamped it will essentially
* become a round robin policy since the worker_id will be incremented
* at for every task.
*/

	if (is_server_message) {
		uint64_t hash = djb2_hash((unsigned char *)&msg->session_id, sizeof(msg->session_id));
		int bound = spinner->num_workers / 2;
		worker_id = (hash % bound) + bound;
		//log_warn("fifo worker id %d chosen for session id %u, %u", worker_id, msg->session_id, msg->msg_type);
	} else {
		// 1. Round robin with threshold
		if (worker_queued_jobs(&spinner->worker[worker_id]) >= max_queued_jobs) {
			/* Find an active worker with used_slots < max_queued_jobs
* If there's none, wake up a sleeping worker
* If all worker's are running, pick the one with least load
* NOTE a worker's work can only increase through this function call, which
* is only called by the spinning
* thread. Each worker is assigned to one spinning thread, therefore a
* worker can't wake up or have its
* work increased during the duration of a single call of this function
*/

			// Find active worker with min worker_queued_jobs
			int current_choice = worker_id; // worker_id is most likely not sleeping
			int a_sleeping_worker_id = -1;
			int bound = spinner->num_workers / 2;
			for (int i = 0; i < bound; ++i) {
				// Keep note of a sleeping worker in case we need to wake him up for
				// task
				if (spinner->worker[i].status == IDLE_SLEEPING) {
					if (a_sleeping_worker_id == -1)
						a_sleeping_worker_id = i;
					continue;
				}
				if (worker_queued_jobs(&spinner->worker[i]) <
				    worker_queued_jobs(&spinner->worker[current_choice]))
					current_choice = i;
			}
			worker_id = current_choice;
			if (a_sleeping_worker_id != -1 &&
			    worker_queued_jobs(&spinner->worker[worker_id]) >= max_queued_jobs)
				worker_id = a_sleeping_worker_id;
		}
	}

	if (!is_task_resumed) {
		job->channel = root_server->numa_servers[spinner->root_server_id]->channel;
		job->conn = conn;
		job->msg = msg;
		job->kreon_operation_status = TASK_START;
		/*initialization of various fsm*/
		job->server_id = spinner->root_server_id;
		job->thread_id = worker_id;
		job->triggering_msg_offset = job->msg->triggering_msg_offset_in_send_buffer;
	}

	if (utils_queue_push(&(spinner->worker[worker_id].work_queue), (void *)job) == NULL) {
		log_fatal("Cannot put task in workers queue is full!");
		_exit(EXIT_FAILURE);
	}

	if (spinner->worker[worker_id].status == IDLE_SLEEPING) {
		spinner->worker[worker_id].status = BUSY;
		sem_post(&spinner->worker[worker_id].sem);
	}

	return TEBIS_SUCCESS;
}

static void ds_check_idle_workers(struct ds_spinning_thread *spinner)
{
	int max_idle_time_usec = globals_get_worker_spin_time_usec();
	for (int i = 0; i < spinner->num_workers; ++i) {
		uint32_t queue_size = utils_queue_used_slots(&spinner->worker[i].work_queue);
		if (spinner->worker[i].idle_time > max_idle_time_usec && queue_size == 0 &&
		    spinner->worker[i].status == BUSY)
			spinner->worker[i].status = IDLE_SLEEPING;
		else if (queue_size > 0 && spinner->worker[i].status == IDLE_SLEEPING) {
			spinner->worker[i].status = BUSY;
			sem_post(&spinner->worker[i].sem);
		}
	}
}

static void ds_resume_halted_tasks(struct ds_spinning_thread *spinner)
{
	/*check for resumed tasks to be rescheduled*/
	uint32_t total_halted_tasks = 0;
	uint32_t b_detection = 0;
	pthread_mutex_lock(&spinner->resume_task_pool.tbp_lock);

	struct krm_work_task *task = utils_queue_pop(&spinner->resume_task_pool.task_buffers);
	while (task) {
		// log_info("Rescheduling task");

		if (0 == ++task->rescheduling_counter % 1000000) {
			log_warn(
				"Suspicious task %lu for region %s has been rescheduled %lu times pending region tasks are: %lu state is %u",
				(uint64_t)task, task->r_desc->region->id, task->rescheduling_counter,
				task->r_desc->pending_region_tasks, task->kreon_operation_status);
			b_detection = 1;
		}

		++total_halted_tasks;
		int rc = assign_job_to_worker(spinner, task->conn, task->msg, task);
		if (rc == TEBIS_FAILURE) {
			log_fatal("Failed to reschedule task");
			_exit(EXIT_FAILURE);
		}
		task = utils_queue_pop(&spinner->resume_task_pool.task_buffers);
	}
	pthread_mutex_unlock(&spinner->resume_task_pool.tbp_lock);
	while (b_detection) {
		log_warn("Total halted tasks are: %u", utils_queue_used_slots(&spinner->resume_task_pool.task_buffers));
		ds_check_idle_workers(spinner);
		log_debug("Client tasks state: *****");
		for (int i = 0; i < UTILS_QUEUE_CAPACITY / 2; ++i) {
			log_debug("krm work task status[%d]: rescheduling counter: %lu", i,
				  spinner->num_of_outstanding_client_req);
		}
		log_debug("Finally workers queue sizes");
		for (int i = 0; i < 8; ++i) {
			log_debug("Worker[%d] has %d tasks and its status is %d", i,
				  utils_queue_used_slots(&spinner->worker[i].work_queue), spinner->worker[i].status);
		}
		log_debug("Client tasks state: ***** DONE");
		sleep(4);
	}
}

static void *server_spinning_thread_kernel(void *args)
{
	struct msg_header *hdr = NULL;

	SIMPLE_CONCURRENT_LIST_NODE *node = NULL;
	SIMPLE_CONCURRENT_LIST_NODE *prev_node = NULL;
	SIMPLE_CONCURRENT_LIST_NODE *next_node = NULL;

	struct sigaction sa;
	struct ds_spinning_thread *spinner = (struct ds_spinning_thread *)args;
	struct connection_rdma *conn = NULL;

	uint32_t message_size = 0;
	uint8_t recv = 0;

	int spinning_thread_id = 0; // = spinner->spinner_id;
	int rc = 0;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = ec_sig_handler;

	pthread_t self = pthread_self();
	pthread_setname_np(self, "spinner");

	pthread_mutex_init(&spinner->resume_task_pool.tbp_lock, NULL);
	utils_queue_init(&spinner->resume_task_pool.task_buffers);

	spinner->num_of_outstanding_client_req = 0;

	/*Init my worker threads*/
	for (int i = 0; i < spinner->num_workers; ++i) {
		/*init worker group vars*/
		spinner->worker[i].idle_time = 0;
		spinner->worker[i].status = IDLE_SLEEPING;
		sem_init(&spinner->worker[i].sem, 0, 0);
		utils_queue_init(&spinner->worker[i].work_queue);
		spinner->worker[i].root_server_id = spinner->root_server_id;
	}

	/*set the proper affinity for my workers*/
	cpu_set_t worker_threads_affinity_mask;
	CPU_ZERO(&worker_threads_affinity_mask);
	/*pin and create my workers*/
	for (int i = 0; i < spinner->num_workers; i++)
		CPU_SET(spinner->worker[i].cpu_core_id, &worker_threads_affinity_mask);

	for (int i = 0; i < spinner->num_workers; i++) {
		pthread_create(&spinner->worker[i].context, NULL, worker_thread_kernel, &spinner->worker[i]);

		/*set affinity for this group*/
		log_info("Spinning thread %d Started worker %d", spinner->spinner_id, i);
		int status = pthread_setaffinity_np(spinner->worker[i].context, sizeof(cpu_set_t),
						    &worker_threads_affinity_mask);
		if (status != 0) {
			log_fatal("failed to pin workers for spinning thread %d", spinner->spinner_id);
			_exit(EXIT_FAILURE);
		}
	}

	// int max_idle_time_usec = globals_get_worker_spin_time_usec();

	while (1) {
		// gesalous, iterate the connection list of this channel for new messages
		node = spinner->conn_list->first;

		prev_node = NULL;
		ds_resume_halted_tasks(spinner);
		ds_check_idle_workers(spinner);

		while (node != NULL) {
			conn = (connection_rdma *)node->data;

			if (conn->status != CONNECTION_OK)
				goto iterate_next_element;

			hdr = (msg_header *)conn->rendezvous;
			recv = hdr->receive;

			/*messages belonging to data path category*/
			if (recv == TU_RDMA_REGULAR_MSG) {
				message_size = wait_for_payload_arrival(hdr);
				if (message_size == 0) {
					/*payload have not arrived yet check next connection*/
					goto iterate_next_element;
				}
				/*normal messages*/
				hdr->receive = 0;
				rc = assign_job_to_worker(spinner, conn, hdr, NULL);
				if (rc == TEBIS_FAILURE) {
					// all workers are busy let's see messages from other connections
					// Caution! message not consumed leave the rendezvous points as is
					hdr->receive = recv;
					goto iterate_next_element;
				}

				/* Set the new rendezvous point, be careful for the case that the rendezvous is outsize of the
				 * rdma_memory_regions->remote_memory_buffer
				 */
				update_rendezvous_location(conn, message_size);
			} else if (recv == CONNECTION_PROPERTIES) {
				message_size = wait_for_payload_arrival(hdr);
				if (message_size == 0) {
					/*payload have not arrived yet check next connection*/
					goto iterate_next_element;
				}

				if (hdr->msg_type == DISCONNECT) {
					/* Warning! the guy that consumes/handles the message is responsible for zeroing the message's
					 * segments for possible future rendezvous points. This is done inside free_rdma_received_message
					 * function
					 */

					log_info("Disconnect operation bye bye mr Client garbage collection "
						 "follows");
					// NOTE these operations might need to be atomic with more than one spinning threads
					// (we currently only support a single spinning thread per worker)
					struct channel_rdma *channel = conn->channel;
					// Decrement spinning thread's connections and total connections
					--channel->spin_num[channel->spinning_conn % channel->spinning_num_th];
					--channel->spinning_conn;
					zero_rendezvous_locations(hdr);
					update_rendezvous_location(conn, message_size);
					close_and_free_RDMA_connection(channel, conn);
					goto iterate_next_element;
				} else {
					log_fatal("unknown message type for connetion properties unknown "
						  "type is %d\n",
						  hdr->msg_type);
					_exit(EXIT_FAILURE);
				}
			}

		iterate_next_element:
			if (node->marked_for_deletion) {
				log_warn("garbage collection");
				pthread_mutex_lock(&spinner->conn_list_lock);
				next_node = node->next; /*Caution prev_node remains intact*/
				delete_element_from_simple_concurrent_list(spinner->conn_list, prev_node, node);
				node = next_node;
				pthread_mutex_unlock(&spinner->conn_list_lock);
			} else {
				prev_node = node;
				node = node->next;
			}
		}
	}
	log_warn("Server Spinning thread %d exiting", spinning_thread_id);
	return NULL;
}

extern void on_completion_client(struct rdma_message_context *);

struct recover_log_context {
	struct msg_header header;
	uint8_t num_of_replies_needed;
	void *memory;
	struct ibv_mr *mr;
};

void recover_log_context_completion(struct rdma_message_context *msg_ctx)
{
	struct recover_log_context *cnxt = (struct recover_log_context *)msg_ctx->args;
	__sync_fetch_and_sub(&cnxt->num_of_replies_needed, 1);
}

static int krm_enter_parallax(struct krm_region_desc *r_desc, struct krm_work_task *task)
{
	if (r_desc == NULL) {
		log_fatal("NULL region?");
		_exit(EXIT_FAILURE);
	}
	if (r_desc->region->num_of_backup == 0)
		return 1;

	pthread_rwlock_rdlock(&r_desc->kreon_lock);
	if (r_desc->status == KRM_HALTED) {
		int ret;
		switch (task->kreon_operation_status) {
		case TASK_GET_KEY:
		case TASK_MULTIGET:
		case TASK_DELETE_KEY:
		case INS_TO_KREON:
			// log_info("Do not enter Kreon task status is %d region status =
			// %d",task->kreon_operation_status,r_desc->status);
			ret = 0;
			break;
		case REPLICATE:
		case WAIT_FOR_REPLICATION_COMPLETION:
		case ALL_REPLICAS_ACKED:
		case SEND_FLUSH_COMMANDS:
		case WAIT_FOR_FLUSH_REPLIES:
			ret = 1;
			break;
		default:
			log_fatal("Unhandled state");
			_exit(EXIT_FAILURE);
		}
		pthread_rwlock_unlock(&r_desc->kreon_lock);
		return ret;
	} else {
		switch (task->kreon_operation_status) {
		case TASK_GET_KEY:
		case TASK_MULTIGET:
		case TASK_DELETE_KEY:
		case INS_TO_KREON:
			__sync_fetch_and_add(&r_desc->pending_region_tasks, 1);
			break;
		case REPLICATE:
		case WAIT_FOR_REPLICATION_TURN:
		case WAIT_FOR_REPLICATION_COMPLETION:
		case ALL_REPLICAS_ACKED:
		case SEND_FLUSH_COMMANDS:
		case WAIT_FOR_FLUSH_REPLIES:
			break;
		default:
			log_fatal("Unhandled state");
			_exit(EXIT_FAILURE);
		}
		pthread_rwlock_unlock(&r_desc->kreon_lock);
		return 1;
	}
}

static void krm_leave_parallax(struct krm_region_desc *r_desc)
{
	if (r_desc->region->num_of_backup == 0)
		return;
	__sync_fetch_and_sub(&r_desc->pending_region_tasks, 1);
}

int64_t lsn_to_be_replicated = 1;

/** Function filling reply headers from server to clients
 *  payload and msg_type must be provided as they defer from msg to msg
 *  */
static void fill_reply_header(msg_header *reply_msg, struct krm_work_task *task, uint32_t payload_size,
			      uint16_t msg_type)
{
	uint32_t reply_size = sizeof(struct msg_header) + payload_size + TU_TAIL_SIZE;
	uint32_t padding = MESSAGE_SEGMENT_SIZE - (reply_size % MESSAGE_SEGMENT_SIZE);

	reply_msg->padding_and_tail_size = 0;
	reply_msg->payload_length = payload_size;
	if (reply_msg->payload_length != 0)
		reply_msg->padding_and_tail_size = padding + TU_TAIL_SIZE;

	reply_msg->offset_reply_in_recv_buffer = UINT32_MAX;
	reply_msg->reply_length_in_recv_buffer = UINT32_MAX;
	reply_msg->offset_in_send_and_target_recv_buffers = task->msg->offset_reply_in_recv_buffer;
	reply_msg->triggering_msg_offset_in_send_buffer = task->msg->triggering_msg_offset_in_send_buffer;
	reply_msg->session_id = task->msg->session_id;
	reply_msg->msg_type = msg_type;
	reply_msg->op_status = 0;
	reply_msg->receive = TU_RDMA_REGULAR_MSG;
}

static void execute_get_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	assert(task->msg->msg_type == GET_REQUEST);
	struct msg_data_get_request request_data = get_request_get_msg_data(task->msg);
	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, request_data.key, request_data.key_size);

	if (r_desc == NULL) {
		log_fatal("Region not found for key %s", request_data.key);
		_exit(EXIT_FAILURE);
	}

	task->kreon_operation_status = TASK_GET_KEY;
	task->r_desc = r_desc;
	if (!krm_enter_parallax(r_desc, task)) {
		// later...
		return;
	}
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);

	par_handle par_hd = (par_handle)task->r_desc->db;
	struct par_value lookup_value = { .val_buffer = get_reply_get_kv_offset(task->reply_msg),
					  .val_buffer_size = request_data.bytes_to_read };
	const char *error_message = NULL;
	par_get_serialized(par_hd, get_msg_get_key_slice_t(task->msg), &lookup_value, &error_message);
	krm_leave_parallax(r_desc);

	if (error_message) {
		log_warn("key not found key %s : length %u", request_data.key, request_data.key_size);

		struct msg_data_get_reply reply_data = { 0 };
		create_get_reply_msg(reply_data, task->reply_msg);
		goto exit;
	}
	uint32_t offset = request_data.offset;
	uint32_t msg_bytes_to_read = request_data.bytes_to_read;
	int32_t fetch_value = request_data.fetch_value;
	// tranlate now
	if (offset > lookup_value.val_size) {
		struct msg_data_get_reply reply_data = { .key_found = 1,
							 .offset_too_large = 1,
							 .value_size = 0,
							 .value = NULL,
							 .bytes_remaining = lookup_value.val_size };
		create_get_reply_msg(reply_data, task->reply_msg);
		goto exit;
	}

	if (!fetch_value) {
		struct msg_data_get_reply reply_data = { .key_found = 1,
							 .offset_too_large = 0,
							 .value_size = 0,
							 .value = NULL,
							 .bytes_remaining = lookup_value.val_size - offset };
		create_get_reply_msg(reply_data, task->reply_msg);
		goto exit;
	}
	uint32_t value_bytes_remaining = lookup_value.val_size - offset;
	uint32_t bytes_to_read = value_bytes_remaining;
	bytes_to_read = value_bytes_remaining;
	int32_t bytes_remaining = 0;
	if (msg_bytes_to_read <= value_bytes_remaining) {
		bytes_to_read = msg_bytes_to_read;
		bytes_remaining = lookup_value.val_size - (offset + bytes_to_read);
	}

	struct msg_data_get_reply reply_data = { .key_found = 1,
						 .offset_too_large = 0,
						 .value_size = bytes_to_read,
						 .value = NULL,
						 .bytes_remaining = bytes_remaining };
	create_get_reply_msg(reply_data, task->reply_msg);

exit:;
	//finally fix the header
	uint32_t payload_length = get_reply_get_payload_size(task->reply_msg);

	fill_reply_header(task->reply_msg, task, payload_length, GET_REPLY);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_multi_get_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_debug("Close scans since we dont use them for now (the tebis-parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
}

static void execute_delete_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_debug("Closing delete ops since we dont use them for now (tebis-parallax) replication");
	assert(0);
	_exit(EXIT_FAILURE);
}

/*zero both RDMA buffers*/
static void zero_rdma_buffer(struct krm_region_desc *r_desc, enum log_category log_type)
{
	if (log_type == L0_RECOVERY)
		memset(r_desc->r_state->l0_recovery_rdma_buf.mr->addr, 0x00,
		       r_desc->r_state->l0_recovery_rdma_buf.rdma_buf_size);
	else
		memset(r_desc->r_state->big_recovery_rdma_buf.mr->addr, 0x00,
		       r_desc->r_state->big_recovery_rdma_buf.rdma_buf_size);
}

static int8_t is_segment_in_HT_mappings(struct krm_region_desc *r_desc, uint64_t primary_segment_offt)
{
	struct krm_segment_entry *index_entry;

	pthread_rwlock_rdlock(&r_desc->replica_log_map_lock);
	HASH_FIND_PTR(r_desc->replica_log_map, &primary_segment_offt, index_entry);
	pthread_rwlock_unlock(&r_desc->replica_log_map_lock);

	if (!index_entry)
		return 0;
	return 1;
}

static uint64_t get_segment_in_indexHT_mappings(struct krm_region_desc *r_desc, uint64_t primary_segment_offt,
						uint32_t level_id)
{
	struct krm_segment_entry *index_entry;

	HASH_FIND_PTR(r_desc->replica_index_map[level_id], &primary_segment_offt, index_entry);

	if (!index_entry)
		return 0;
	return index_entry->replica_segment_offt;
}

static void add_segment_to_logmap_HT(struct krm_region_desc *r_desc, uint64_t primary_segment_offt,
				     uint64_t replica_segment_offt)
{
	//log_debug("Inserting primary seg offt %lu replica seg offt %lu", primary_segment_offt, replica_segment_offt);
	struct krm_segment_entry *entry = (struct krm_segment_entry *)calloc(1, sizeof(struct krm_segment_entry));
	entry->primary_segment_offt = primary_segment_offt;
	entry->replica_segment_offt = replica_segment_offt;
	pthread_rwlock_wrlock(&r_desc->replica_log_map_lock);
	HASH_ADD_PTR(r_desc->replica_log_map, primary_segment_offt, entry);
	pthread_rwlock_unlock(&r_desc->replica_log_map_lock);
}

static void execute_flush_command_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)region_server_desc;
	assert(task->msg->msg_type == FLUSH_COMMAND_REQ);
	// log_debug("Primary orders a flush!");
	struct s2s_msg_flush_cmd_req *flush_req =
		(struct s2s_msg_flush_cmd_req *)((char *)task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc =
		regs_get_region(region_server_desc, flush_req->region_key, flush_req->region_key_size);
	if (r_desc->r_state == NULL) {
		log_fatal("No state for backup region %s", r_desc->region->id);
		_exit(EXIT_FAILURE);
	}

	enum log_category log_type_to_flush = flush_req->log_type;

	if (!globals_get_send_index()) {
		build_index_procedure(r_desc, log_type_to_flush);
		zero_rdma_buffer(r_desc, log_type_to_flush);
	} else {
		uint64_t replica_new_segment_offt = send_index_flush_rdma_buffer(r_desc, log_type_to_flush);
		int8_t segment_exist_in_HT = is_segment_in_HT_mappings(r_desc, flush_req->primary_segment_offt);
		if (!segment_exist_in_HT) {
			add_segment_to_logmap_HT(r_desc, flush_req->primary_segment_offt, replica_new_segment_offt);
			zero_rdma_buffer(r_desc, log_type_to_flush);
		}
	}

	//time for reply :-)
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_flush_cmd_rep *flush_rep =
		(struct s2s_msg_flush_cmd_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	flush_rep->status = TEBIS_SUCCESS;
	flush_rep->uuid = flush_req->uuid;
	fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_flush_cmd_rep), FLUSH_COMMAND_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
}

static struct ru_replica_rdma_buffer initialize_rdma_buffer(uint32_t size, struct rdma_cm_id *rdma_cm_id)
{
	void *addr = NULL;
	if (posix_memalign(&addr, ALIGNMENT, size)) {
		log_fatal("Failed to allocate aligned RDMA buffer");
		perror("Reason\n");
		_exit(EXIT_FAILURE);
	}
	/*Zero RDMA buffer*/
	memset(addr, 0, size);
	/*initialize the replicas rdma buffer*/
	struct ru_replica_rdma_buffer rdma_buf = { .rdma_buf_size = size,
						   .mr = rdma_reg_write(rdma_cm_id, addr, size) };
	return rdma_buf;
}

static void execute_get_rdma_buffer_req(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	assert(task->msg->msg_type == GET_RDMA_BUFFER_REQ);
	struct s2s_msg_get_rdma_buffer_req *get_log =
		(struct s2s_msg_get_rdma_buffer_req *)((char *)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc =
		regs_get_region(region_server_desc, get_log->region_key, get_log->region_key_size);
	if (r_desc == NULL) {
		log_fatal("No region found for min key %s", get_log->region_key);
		_exit(EXIT_FAILURE);
	}

	log_debug("Region-master wants to initialize rdma buffers for region %s", r_desc->region->id);
	pthread_mutex_lock(&r_desc->region_mgmnt_lock);
	if (r_desc->r_state == NULL) {
		r_desc->r_state = (struct ru_replica_state *)calloc(1, sizeof(struct ru_replica_state));
		uint32_t size = get_log->buffer_size;
		struct rdma_cm_id *rdma_cm_id = task->conn->rdma_cm_id;
		/*initalize l0_recovery_buffer*/
		r_desc->r_state->l0_recovery_rdma_buf = initialize_rdma_buffer(size, rdma_cm_id);
		/*initialize big recovery buffer*/
		r_desc->r_state->big_recovery_rdma_buf = initialize_rdma_buffer(size, rdma_cm_id);
	} else {
		log_fatal("remote buffers already initialized, what?");
		_exit(EXIT_FAILURE);
	}

	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
	task->reply_msg = (void *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
				   task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_get_rdma_buffer_rep *rep =
		(struct s2s_msg_get_rdma_buffer_rep *)((char *)task->reply_msg + sizeof(msg_header));
	rep->status = TEBIS_SUCCESS;
	rep->l0_recovery_mr = *r_desc->r_state->l0_recovery_rdma_buf.mr;
	rep->big_recovery_mr = *r_desc->r_state->big_recovery_rdma_buf.mr;

	fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_get_rdma_buffer_rep) + (sizeof(struct ibv_mr)),
			  GET_RDMA_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	log_debug("Region master wants rdma buffers...DONE");
	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_replica_index_get_buffer_req(struct regs_server_desc const *region_server_desc,
						 struct krm_work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_GET_BUFFER_REQ);
	assert(globals_get_send_index());

	struct s2s_msg_replica_index_get_buffer_req *req =
		(struct s2s_msg_replica_index_get_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", req->region_key);
		_exit(EXIT_FAILURE);
	}

	log_debug("Starting compaction for level %u at region %s", req->level_id, r_desc->region->id);
	uint32_t dst_level_id = req->level_id + 1;

	//initialize and reg write buffers for recieving the primary's segments ready to be flushed.
	//The buffers follow Parallax compaction index
	struct send_index_create_compactions_rdma_buffer_params create_buffers_params = {
		.level_id = req->level_id,
		.tree_id = req->tree_id,
		.conn = task->conn,
		.r_desc = r_desc,
		.number_of_rows = req->num_rows,
		.number_of_columns = req->num_cols,
		.size_of_entry = req->entry_size
	};
	send_index_create_compactions_rdma_buffer(create_buffers_params);
	// also initialize a buffer for sending the flush replies to the primary
	// we need to reg write this memory region since replicas rdma write segment flush replies into primary's status buffers
	struct send_index_create_mr_for_segment_replies_params create_flush_reply_mr_params = {
		.conn = task->conn, .level_id = req->level_id, .r_desc = r_desc
	};
	send_index_create_mr_for_segment_replies(create_flush_reply_mr_params);
	r_desc->r_state->index_rewriter[dst_level_id] = send_index_rewriter_init(r_desc);

	//time for reply
	task->reply_msg = (struct msg_header *)((char *)task->conn->rdma_memory_regions->local_memory_buffer +
						task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_replica_index_get_buffer_rep *reply =
		(struct s2s_msg_replica_index_get_buffer_rep *)((char *)task->reply_msg + sizeof(msg_header));
	reply->mr = *r_desc->r_state->index_buffer[dst_level_id];
	reply->uuid = req->uuid;

	fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_replica_index_get_buffer_rep),
			  REPLICA_INDEX_GET_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	log_debug("Registed a new memory region in region %s at offt %lu", r_desc->region->id,
		  (uint64_t)reply->mr.addr);
	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_replica_index_flush_req(struct regs_server_desc const *region_server_desc,
					    struct krm_work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_FLUSH_REQ);
	assert(globals_get_send_index());

	struct s2s_msg_replica_index_flush_req *req =
		(struct s2s_msg_replica_index_flush_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", req->region_key);
		_exit(EXIT_FAILURE);
	}
	//TODO send dst_level_id
	uint32_t dst_level_id = req->level_id + 1;
	uint64_t segment_offt = get_segment_in_indexHT_mappings(r_desc, req->primary_segment_offt, dst_level_id);
	log_debug("Searching for index segment %lu", segment_offt);
	if (!segment_offt) {
		segment_offt = wappender_allocate_space(r_desc->r_state->wappender[dst_level_id]);
		add_segment_to_index_HT(r_desc, req->primary_segment_offt, segment_offt, dst_level_id);
	}

	char *rdma_buffer = r_desc->r_state->index_buffer[dst_level_id]->addr;
	uint32_t row_size = req->entry_size * req->number_of_columns;
	struct segment_header *inmem_segment =
		(struct segment_header *)&rdma_buffer[req->height * row_size + req->clock * req->entry_size];

	send_index_rewriter_rewrite_index(r_desc->r_state->index_rewriter[dst_level_id], r_desc, inmem_segment,
					  dst_level_id);

	struct wappender_append_index_segment_params flush_index_segment = { .buffer = (char *)inmem_segment,
									     .buffer_size = req->entry_size,
									     .segment_offt = segment_offt };
	wappender_append_index_segment(r_desc->r_state->wappender[dst_level_id], flush_index_segment);
	//rdma write to primary's status
	uint64_t reply_value = WCURSOR_STATUS_OK;
	char *reply_address = (char *)r_desc->r_state->index_segment_flush_replies[dst_level_id]->addr;
	memcpy(reply_address, &reply_value, sizeof(uint64_t));
	//rdma write it back to the primary's write cursor status buffers
	while (1) {
		int ret = rdma_post_write(task->conn->rdma_cm_id, NULL, reply_address, WCURSOR_ALIGNMNENT,
					  r_desc->r_state->index_segment_flush_replies[dst_level_id], IBV_SEND_SIGNALED,
					  (uint64_t)req->reply_offt, req->mr_of_primary.rkey);
		if (!ret)
			break;
	}

	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_test_req(struct regs_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	assert(mydesc);
	assert(task->msg->msg_type == TEST_REQUEST);
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	/*initialize message*/
	if (task->msg->reply_length_in_recv_buffer < TU_HEADER_SIZE) {
		log_fatal("CLIENT reply space not enough  size %" PRIu32 " FIX XXX TODO XXX\n",
			  task->msg->reply_length_in_recv_buffer);
		_exit(EXIT_FAILURE);
	}
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	fill_reply_header(task->reply_msg, task, task->msg->payload_length, TEST_REPLY);
	task->kreon_operation_status = TASK_COMPLETE;
}

void execute_test_req_fetch_payload(struct regs_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	assert(task->msg->msg_type == TEST_REQUEST_FETCH_PAYLOAD);
	log_fatal("This message is not supported yet...");
	_exit(EXIT_FAILURE);
}

/** Function that acks a NO_OP operation. Client spins for server's reply.
 *  This operation happens only when there is no space in server's recv circular buffer for a client to
 * allocate and send its msg */
void execute_no_op(struct regs_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	assert(mydesc && task);
	assert(task->msg->msg_type == NO_OP);

	task->kreon_operation_status = TASK_NO_OP;
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];

	fill_reply_header(task->reply_msg, task, 0, NO_OP_ACK);
	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_flush_L0_op(struct regs_server_desc const *region_server_desc, struct krm_work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == FLUSH_L0_REQUEST);
	assert(globals_get_send_index());
	struct s2s_msg_flush_L0_req *flush_req =
		(struct s2s_msg_flush_L0_req *)((char *)task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc =
		regs_get_region(region_server_desc, flush_req->region_key, flush_req->region_key_size);
	if (r_desc->r_state == NULL) {
		log_fatal("No state for backup region %s", r_desc->region->id);
		_exit(EXIT_FAILURE);
	}

	task->kreon_operation_status = TASK_FLUSH_L0;
	// persist the buffers
	uint64_t new_small_log_tail = send_index_flush_rdma_buffer(r_desc, L0_RECOVERY);
	uint64_t new_big_small_log_tail = send_index_flush_rdma_buffer(r_desc, BIG);
	par_flush_superblock(r_desc->db);

	// append new segments to logmap
	add_segment_to_logmap_HT(r_desc, flush_req->small_log_tail_dev_offt, new_small_log_tail);
	add_segment_to_logmap_HT(r_desc, flush_req->big_log_tail_dev_offt, new_big_small_log_tail);

	// create and send the reply
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];
	fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_flush_L0_rep), FLUSH_L0_REPLY);

	// for debugging purposes
	struct s2s_msg_flush_L0_rep *reply_payload =
		(struct s2s_msg_flush_L0_rep *)((char *)task->reply_msg + sizeof(struct msg_header));
	reply_payload->uuid = flush_req->uuid;
	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_send_index_close_compaction(struct regs_server_desc const *region_server_desc,
						struct krm_work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == CLOSE_COMPACTION_REQUEST);
	assert(globals_get_send_index());

	// get the region descriptor
	struct s2s_msg_close_compaction_request *req =
		(struct s2s_msg_close_compaction_request *)((char *)task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc->r_state == NULL) {
		log_fatal("No state for backup region %s", r_desc->region->id);
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_CLOSE_COMPACTION;
	uint32_t dst_level_id = req->level_id + 1;
	log_debug("Closing compaction for region %s level %u", r_desc->region->id, req->level_id);
	send_index_close_compactions_rdma_buffer(r_desc, req->level_id);
	send_index_close_mr_for_segment_replies(r_desc, req->level_id);
	send_index_free_index_HT(r_desc, req->level_id);
	send_index_rewriter_destroy(&r_desc->r_state->index_rewriter[dst_level_id]);

	// create and send the reply
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];
	struct s2s_msg_close_compaction_reply *reply =
		(struct s2s_msg_close_compaction_reply *)((char *)task->reply_msg + sizeof(struct msg_header));
	reply->uuid = req->uuid;
	fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_close_compaction_reply), CLOSE_COMPACTION_REPLY);

	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

static void execute_replica_index_swap_levels(struct regs_server_desc const *region_server_desc,
					      struct krm_work_task *task)
{
	assert(region_server_desc && task);
	assert(task->msg->msg_type == REPLICA_INDEX_SWAP_LEVELS_REQUEST);
	assert(globals_get_send_index());

	// get the region descriptor
	struct s2s_msg_swap_levels_request *req =
		(struct s2s_msg_swap_levels_request *)((char *)task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc = regs_get_region(region_server_desc, req->region_key, req->region_key_size);
	if (r_desc->r_state == NULL) {
		log_fatal("No state for backup region %s", r_desc->region->id);
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_CLOSE_COMPACTION;

	log_debug("Swap levels for region %s level %u", r_desc->region->id, req->level_id);

	// create and send the reply
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];

	struct s2s_msg_swap_levels_reply *reply =
		(struct s2s_msg_swap_levels_reply *)((char *)task->reply_msg + sizeof(struct msg_header));
	reply->uuid = req->uuid;
	fill_reply_header(task->reply_msg, task, sizeof(struct s2s_msg_swap_levels_reply),
			  REPLICA_INDEX_SWAP_LEVELS_REPLY);

	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

typedef void execute_task(struct regs_server_desc const *mydesc, struct krm_work_task *task);

execute_task *const task_dispatcher[NUMBER_OF_TASKS] = { execute_replica_index_get_buffer_req,
							 execute_replica_index_flush_req,
							 execute_get_rdma_buffer_req,
							 execute_flush_command_req,
							 regs_execute_put_req,
							 execute_delete_req,
							 execute_get_req,
							 execute_multi_get_req,
							 execute_test_req,
							 execute_test_req_fetch_payload,
							 execute_no_op,
							 execute_flush_L0_op,
							 execute_send_index_close_compaction,
							 execute_replica_index_swap_levels };

/*
 * Tebis main processing function of networkrequests.
 * Each network processing request must be resumable.
 * For each message type Tebis process it via a specific data path.
 * We treat all tasks related to network  as paths that may fail, and we can resume later.
 */
static void handle_task(struct regs_server_desc const *mydesc, struct krm_work_task *task)
{
	enum message_type type;
	if (task->msg->msg_type == PUT_IF_EXISTS_REQUEST)
		type = PUT_REQUEST; /*will handle the IF_EXIST internally*/
	else
		type = task->msg->msg_type;

	task_dispatcher[type](mydesc, task);

	if (task->kreon_operation_status == TASK_COMPLETE)
		stats_update(task->server_id, task->thread_id);
}

sem_t exit_main = { 0 };
static void sigint_handler(int signo)
{
	(void)signo;
	/*pid_t tid = syscall(__NR_gettid);*/
	printf("caught signal closing server, sorry gracefull shutdown not yet "
	       "supported. Contace <gesalous,geostyl>@ics.forth.gr");
	stats_notify_stop_reporter_thread();
	sem_post(&exit_main);
}

#define MAX_CORES_PER_NUMA 64
int main(int argc, char *argv[])
{
#if CREATE_TRACE_FILE
	globals_open_trace_file("tracefile.txt");
#endif

#ifdef DEBUG_BUILD_TYPE
	log_set_level(LOG_DEBUG);
#elif RELEASE_BUILD_TYPE
	log_set_level(LOG_INFO);
#else
	log_fatal(
		"Build must be in release or debug mode, if not please update the corresponding flags in /path/to/tebis/CMakeLists.txt");
	_exit(EXIT_FAILURE);
#endif

	int num_of_numa_servers = 1;

	if (argc != 8) {
		log_fatal(
			"Error args are %d! usage: ./kreon_server <device name>"
			" <zk_host:zk_port>  <RDMA subnet> <TEBIS_L0 size in keys> <growth factor> <send_index or build_index> <server(s) vector>\n "
			" where server(s) vector is \"<RDMA_PORT>,<Spinning thread core "
			"id>,<worker id 1>,<worker id 2>,...,<worker id N>\"",
			argc);
		_exit(EXIT_FAILURE);
	}
	// argv[0] program name don't care
	// dev name
	int next_argv = 1;
	char *device_name = argv[next_argv++];
	globals_set_dev(device_name);
	// zookeeper
	globals_set_zk_host(argv[next_argv++]);
	// RDMA subnet
	globals_set_RDMA_IP_filter(argv[next_argv++]);
	//TEBIS_L0 size
	uint32_t TEBIS_L0_size = strtoul(argv[next_argv++], NULL, 10);
	globals_set_l0_size(TEBIS_L0_size);
	//growth factor
	uint32_t growth_factor = strtoul(argv[next_argv++], NULL, 10);
	globals_set_growth_factor(growth_factor);

	if (strcmp(argv[next_argv], "send_index") == 0)
		globals_set_send_index(1);
	else if (strcmp(argv[next_argv], "build_index") == 0)
		globals_set_send_index(0);
	else {
		log_fatal("what do you want send or build index?");
		_exit(EXIT_FAILURE);
	}
	++next_argv;

	/*time to allocate the root server*/
	root_server = (struct ds_root_server *)calloc(1, sizeof(struct ds_root_server));
	root_server->num_of_numa_servers = num_of_numa_servers;
	int server_idx = 0;
	// now servers <RDMA port, spinning thread, workers>

	char *strtok_saveptr = NULL;
	char *rest = argv[next_argv];
	// RDMA port of server
	char *rdma_port_token = strtok_r(rest, ",", &strtok_saveptr);
	if (rdma_port_token == NULL) {
		log_fatal("RDMA port missing in server configuration");
		_exit(EXIT_FAILURE);
	}
	char *ptr = NULL;
	int rdma_port = strtol(rdma_port_token, &ptr, 10);
	log_info("Staring server no %d rdma port: %d", server_idx, rdma_port);
	// Spinning thread of server
	char *token = strtok_r(NULL, ",", &strtok_saveptr);
	if (token == NULL) {
		log_fatal("Spinning thread id missing in server configuration");
		_exit(EXIT_FAILURE);
	}
	int spinning_thread_id = strtol(token, &ptr, 10);
	log_info("Server %d spinning_thread id: %d", server_idx, spinning_thread_id);

	// now the worker ids
	int num_workers = 0;
	int workers_id[MAX_CORES_PER_NUMA] = { 0 };

	for (token = strtok_r(NULL, ",", &strtok_saveptr); token != NULL; token = strtok_r(NULL, ",", &strtok_saveptr))
		workers_id[num_workers++] = strtol(token, &ptr, 10);

	if (num_workers == 0) {
		log_fatal("No workers specified for Server %d", server_idx);
		_exit(EXIT_FAILURE);
	}
	// Double the workers, mirror workers are for server tasks
	num_workers *= 2;

	// now we have all info to allocate ds_numa_server, pin,
	// and inform the root server
	struct ds_numa_server *server = (struct ds_numa_server *)calloc(1, sizeof(struct ds_numa_server));

	/*But first let's build each numa server's RDMA channel*/
	/*RDMA channel staff*/
	server->channel = (struct channel_rdma *)calloc(1, sizeof(struct channel_rdma));
	if (server->channel == NULL) {
		log_fatal("malloc failed could do not get memory for channel");
		_exit(EXIT_FAILURE);
	}
	server->channel->sockfd = 0;
	server->channel->context = get_rdma_device_context(NULL);

	server->channel->comp_channel = ibv_create_comp_channel(server->channel->context);
	if (server->channel->comp_channel == 0) {
		log_fatal("building context reason follows:");
		perror("Reason: \n");
		_exit(EXIT_FAILURE);
	}

	server->channel->pd = ibv_alloc_pd(server->channel->context);
	server->channel->nconn = 0;
	server->channel->nused = 0;
	server->channel->connection_created = NULL;

	/*Creating the thread in charge of the completion channel*/
	if (pthread_create(&server->poll_cq_cnxt, NULL, poll_cq, server->channel) != 0) {
		log_fatal("Failed to create poll_cq thread reason follows:");
		perror("Reason: \n");
		_exit(EXIT_FAILURE);
	}

	server->channel->dynamic_pool = mrpool_create(server->channel->pd, -1, DYNAMIC, MEM_REGION_BASE_SIZE);
	server->channel->spinning_th = 0; // what?
	server->channel->spinning_conn = 0; // what?
	server->channel->spinning_num_th = num_of_spinning_threads; // what?
	// Lock for the conn_list what?
	pthread_mutex_init(&server->channel->spin_conn_lock, NULL);
	// channels done

	server->spinner.num_workers = num_workers;
	server->spinner.spinner_id = spinning_thread_id;
	server->spinner.root_server_id = server_idx;
	// server->meta_server.root_server_id = server_idx;
	server->rdma_port = rdma_port;
	// server->meta_server.ld_regions = NULL;
	// server->meta_server.dataservers_map = NULL;

	// server->meta_server.RDMA_port = rdma_port;

	for (int j = 0; j < num_workers; j++) {
		server->spinner.worker[j].worker_id = j;
		server->spinner.worker[j].cpu_core_id = workers_id[j];
	}
	root_server->numa_servers[server_idx] = server;

	/*
	 * list of chain reaction: main fires socket threads (one per server) and
	 * then spinners. Spinners finally fire up their workers
	 */
	server = root_server->numa_servers[0];
	pthread_mutex_init(&server->spinner.conn_list_lock, NULL);
	server->spinner.conn_list = init_simple_concurrent_list();

	server->spinner.next_worker_to_submit_job = 0;

	if (pthread_create(&server->socket_thread_cnxt, NULL, socket_thread, (void *)server) != 0) {
		log_fatal("failed to spawn socket thread  for Server: %d reason follows:", server->server_id);
		perror("Reason: \n");
		_exit(EXIT_FAILURE);
	}

	// Now pin it in the numa node! Important step so allocations used by spinner and workers to be in the same numa node

	cpu_set_t numa_node_affinity;
	CPU_ZERO(&numa_node_affinity);
	CPU_SET(server->spinner.spinner_id, &numa_node_affinity);
	for (int k = 0; k < server->spinner.num_workers; k++)
		CPU_SET(server->spinner.worker[k].worker_id, &numa_node_affinity);
	int status = pthread_setaffinity_np(server->socket_thread_cnxt, sizeof(cpu_set_t), &numa_node_affinity);
	if (status != 0) {
		log_fatal("failed to pin socket thread for server %d at port %d", server->server_id, server->rdma_port);
		_exit(EXIT_FAILURE);
	}

	log_info("Started socket thread successfully for Server %d", server->server_id);

	log_info("Starting spinning thread for Server %d", server->server_id);
	if (pthread_create(&server->spinner_cnxt, NULL, server_spinning_thread_kernel, &server->spinner) != 0) {
		log_fatal("Failed to create spinner thread for Server: %d", server->server_id);
		_exit(EXIT_FAILURE);
	}

	// spinning thread pin staff
	log_info("Pinning spinning thread of Server: %d at port %d at core %d", server->server_id, server->rdma_port,
		 server->spinner.spinner_id);
	cpu_set_t spinning_thread_affinity_mask;
	CPU_ZERO(&spinning_thread_affinity_mask);
	CPU_SET(server->spinner.spinner_id, &spinning_thread_affinity_mask);
	status = pthread_setaffinity_np(server->spinner_cnxt, sizeof(cpu_set_t), &spinning_thread_affinity_mask);
	if (status != 0) {
		log_fatal("failed to pin spinning thread");
		_exit(EXIT_FAILURE);
	}
	log_info("Pinned successfully spinning thread of Server: %d at port %d at "
		 "core %d",
		 server->server_id, server->rdma_port, server->spinner.spinner_id);

	if (pthread_create(&server->meta_server_cnxt, NULL, run_master, &rdma_port)) {
		log_fatal("Failed to start metadata_server");
		_exit(EXIT_FAILURE);
	}

	server->region_server = regs_create_server();
	regs_set_group_id(server->region_server, server_idx);
	regs_set_rdma_port(server->region_server, rdma_port);
	regs_start_server(server->region_server);
	stats_init(root_server->num_of_numa_servers, server->spinner.num_workers);
	sem_init(&exit_main, 0, 0);

	log_debug("Server(S) ready");
	if (signal(SIGINT, sigint_handler) == SIG_ERR) {
		log_fatal("can't catch SIGINT");
		_exit(EXIT_FAILURE);
	}
	sem_wait(&exit_main);
	log_warn("kreonR server exiting");
	return 0;
}
