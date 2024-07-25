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

#define _GNU_SOURCE
#include "../tebis_rdma/memory_region_pool.h"
#include "../utilities/simple_concurrent_list.h"
#include "conf.h"
#include "multi_get.h"
#include "region_desc.h"
#include "region_server.h"
#include "request_factory.h"
#include "send_index/send_index_rewriter.h"
#include "send_index/send_index_uuid_checker/send_index_uuid_checker.h"
#include "work_task.h"
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <log.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <sched.h>
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../tebis_rdma/rdma.h"
#include "../utilities/queue.h"
#include "djb2.h"
#include "globals.h"
#include "messages.h"
#include "metadata.h"
#include "server_config.h"
#include "stats.h"
#// IWYU pragma: no_forward_declare timespec

#define GB_TO_BYTES(gb) ((uint64_t)(gb) * 1024 * 1024 * 1024)

#ifdef CHECKSUM_DATA_MESSAGES
#include "djb2.h"
#endif

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
	struct work_task *server_tasks;
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

static void handle_task(struct regs_server_desc const *mydesc, struct work_task *task);
static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct work_task *task);

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
	//sem_post(&msg_ctx->wait_for_completion);
	msg_ctx->completion_flag = 1;
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
			_exit(EXIT_FAILURE);
		}
		new_conn_id = request_id->event->id;
		conn = calloc(1UL, sizeof(connection_rdma));
		qp_init_attr.send_cq = qp_init_attr.recv_cq =
			ibv_create_cq(channel->context, MAX_WR, (void *)conn, channel->comp_channel, 0);
		ibv_req_notify_cq(qp_init_attr.send_cq, 0);
		assert(qp_init_attr.send_cq);

		ret = rdma_create_qp(new_conn_id, NULL, &qp_init_attr);
		if (ret) {
			log_fatal("rdma_create_qp: %s", strerror(errno));
			_exit(EXIT_FAILURE);
		}

		connection_type incoming_connection_type = -1;
		struct ibv_mr *recv_mr = rdma_reg_msgs(new_conn_id, &incoming_connection_type, sizeof(connection_type));
		struct rdma_message_context msg_ctx = { 0 };
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
			_exit(EXIT_FAILURE);
		}

		// Block until client sends connection type
		//sem_wait(&msg_ctx.wait_for_completion);
		while (0 == msg_ctx.completion_flag)
			;
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
			_exit(EXIT_FAILURE);
		}
		assert(conn->rdma_memory_regions);

		struct ibv_mr *send_mr = rdma_reg_msgs(new_conn_id, conn->rdma_memory_regions->remote_memory_region,
						       sizeof(struct ibv_mr));

		// Receive memory region information
		conn->peer_mr = calloc(1UL, sizeof(struct ibv_mr));
		recv_mr = rdma_reg_msgs(new_conn_id, conn->peer_mr, sizeof(struct ibv_mr));
		msg_ctx.completion_flag = 0;
		ret = rdma_post_recv(new_conn_id, &msg_ctx, conn->peer_mr, sizeof(struct ibv_mr), recv_mr);
		if (ret) {
			log_fatal("rdma_post_recv: %s", strerror(errno));
			_exit(EXIT_FAILURE);
		}
		// Send memory region information
		ret = rdma_post_send(new_conn_id, NULL, conn->rdma_memory_regions->remote_memory_region,
				     sizeof(struct ibv_mr), send_mr, 0);
		if (ret) {
			log_fatal("rdma_post_send: %s", strerror(errno));
			_exit(EXIT_FAILURE);
		}
		// Block until client sends memory region information
		//sem_wait(&msg_ctx.wait_for_completion);
		while (0 == msg_ctx.completion_flag)
			;
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
	struct work_task *job = NULL;
	struct ds_worker_thread *worker = NULL;
	const size_t spin_time_usec = globals_get_worker_spin_time_usec();

	worker = (struct ds_worker_thread *)args;
	char name[16] = { 0 };
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

			if (job->task_type == KRM_CLIENT_TASK)
				__sync_fetch_and_sub(&root_server->numa_servers[worker->root_server_id]
							      ->spinner.num_of_outstanding_client_req,
						     1);
			free(job);
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

static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct work_task *task)
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
	case REPLICA_FLUSH_MEDIUM_LOG_REQUEST:
	case REPLICA_FLUSH_MEDIUM_LOG_REP:
		return 1;
	default:
		return 0;
	}
}

/** Functions assigning tasks to workers
 *  TODO refactor task mechanism. Now when task is NULL means that we have to insert the task in a workers queue
 *  while when task is not NULL we have to resume a task */
static int assign_job_to_worker(struct ds_spinning_thread *spinner, struct connection_rdma *conn, msg_header *msg,
				struct work_task *task)
{
	struct work_task *job = NULL;
	int is_server_message = ds_is_server2server_job(msg);
	uint8_t is_task_resumed = 0;

	/*allocate or resume task*/
	/*we have to schedule a new task*/
	if (task == NULL) {
		is_task_resumed = 0;
		if (is_server_message) {
			/*TODO XXX check this again*/
			job = (struct work_task *)calloc(1, sizeof(struct work_task));
			job->task_type = KRM_SERVER_TASK;
		} else {
			if (spinner->num_of_outstanding_client_req > MAX_OUTSTANDING_REQUESTS) {
				//log_debug("I am full, can not serve more");
				return TEBIS_FAILURE;
			}
			__sync_fetch_and_add(&spinner->num_of_outstanding_client_req, 1);
			job = (struct work_task *)calloc(1, sizeof(struct work_task));
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
	// uint32_t total_halted_tasks = 0;
	uint32_t b_detection = 0;
	pthread_mutex_lock(&spinner->resume_task_pool.tbp_lock);

	struct work_task *task = utils_queue_pop(&spinner->resume_task_pool.task_buffers);
	while (task) {
		// log_info("Rescheduling task");

		if (0 == ++task->rescheduling_counter % 1000000) {
			log_warn(
				"Suspicious task %lu for region %s has been rescheduled %lu times pending region tasks are: %lu state is %u",
				(uint64_t)task, region_desc_get_id(task->r_desc), task->rescheduling_counter,
				region_desc_get_pending_tasks(task->r_desc), task->kreon_operation_status);
			b_detection = 1;
		}

		// ++total_halted_tasks;
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

	/*create my workers*/

	for (int i = 0; i < spinner->num_workers; i++) {
		pthread_create(&spinner->worker[i].context, NULL, worker_thread_kernel, &spinner->worker[i]);

		log_info("Spinning thread %d Started worker %d", spinner->spinner_id, i);
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

typedef void execute_task(struct regs_server_desc const *mydesc, struct work_task *task);

execute_task *const task_dispatcher[NUMBER_OF_TASKS] = { regs_execute_replica_index_get_buffer_req,
							 regs_execute_replica_index_flush_req,
							 regs_execute_get_rdma_buffer_req,
							 regs_execute_flush_command_req,
							 regs_execute_put_req,
							 regs_execute_delete_req,
							 regs_execute_get_req,
							 regs_execute_multi_get_req,
							 regs_execute_test_req,
							 regs_execute_test_req_fetch_payload,
							 regs_execute_no_op,
							 regs_execute_flush_L0_op,
							 regs_execute_send_index_close_compaction,
							 regs_execute_replica_index_swap_levels,
							 regs_execute_flush_medium_log,
							 regs_execute_compact_L0 };

/*
 * Tebis main processing function of networkrequests.
 * Each network processing request must be resumable.
 * For each message type Tebis process it via a specific data path.
 * We treat all tasks related to network  as paths that may fail, and we can resume later.
 */
static void handle_task(struct regs_server_desc const *region_server, struct work_task *task)
{
	/*handle multigets here until all is fixed*/
	enum message_type type = task->msg->msg_type;

	/*XXX TODO XXX in next versions all request will follow the factory pattern*/
	if (task->msg->msg_type == MULTI_GET_REQUEST && NULL == task->request)
		task->request = factory_create_req(factory_get_instance(), region_server, task->msg);

	if (task->msg->msg_type == PUT_IF_EXISTS_REQUEST)
		type = PUT_REQUEST;

	/*XXX TODO XXX new*/
	if (task->msg->msg_type == MULTI_GET_REQUEST && task->request)
		task->kreon_operation_status = task->request->execute(task->request, task);
	else
		task_dispatcher[type](region_server, task);

	/*XXX TODO XXX new*/
	if (task->msg->msg_type == MULTI_GET_REQUEST && task->kreon_operation_status == TASK_COMPLETE)
		task->request->destruct(task->request);

	if (task->kreon_operation_status == TASK_COMPLETE)
		stats_update(task->server_id, task->thread_id);
}

sem_t exit_main = { 0 };
static void sigint_handler(int signo)
{
	(void)signo;
	const char *msg = "caught signal closing server, sorry gracefull shutdown not yet "
			  "supported. Contact <gesalous>@ics.forth.gr";
	write(STDERR_FILENO, msg, strlen(msg));
	stats_notify_stop_reporter_thread();
	sem_post(&exit_main);
}

static void allocate_device(const char *device_name, int device_size)
{
	struct stat st;

	if (stat(device_name, &st) == 0) {
		return;
	} else if (errno != ENOENT) {
		perror("stat failed");
		exit(EXIT_FAILURE);
	}

	int fd = open(device_name, O_CREAT | O_RDWR, 0666);
	if (fd < 0) {
		perror("Failed to create device file");
		exit(EXIT_FAILURE);
	}

	if (fallocate(fd, 0, 0, GB_TO_BYTES(device_size)) != 0) {
		perror("Failed to allocate space for device file");
		close(fd);
		exit(EXIT_FAILURE);
	}

	close(fd);
	log_info("Created device file: %s\n", device_name);
}

#define MAX_CORES_PER_NUMA 64
int main(int argc, char *argv[])
{
	factory_register(factory_get_instance(), MULTI_GET_REQUEST, mget_constructor);
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

	SCONF_server_config_t s_config = SCONF_create_server_config();
	SCONF_parse_arguments(argc, argv, s_config);

	allocate_device(SCONF_get_device_name(s_config), SCONF_get_device_size(s_config));

	int num_of_numa_servers = 1;

	// dev name
	globals_set_dev(SCONF_get_device_name(s_config));

	// zookeeper
	globals_set_zk_host(SCONF_get_zk_host(s_config));

	// RDMA subnet
	globals_set_RDMA_IP_filter(SCONF_get_rdma_subnet(s_config));

	//TEBIS_L0 size
	globals_set_l0_size(SCONF_get_tebisl0_size(s_config));

	//growth factor
	globals_set_growth_factor(SCONF_get_growth_factor(s_config));

	//send_index
	globals_set_send_index(SCONF_get_index(s_config));

	/*time to allocate the root server*/
	root_server = (struct ds_root_server *)calloc(1, sizeof(struct ds_root_server));
	root_server->num_of_numa_servers = num_of_numa_servers;
	int server_idx = 0;
	// now servers <RDMA port, spinning thread, workers>

	int rdma_port = SCONF_get_server_port(s_config);
	log_info("Staring server no %d rdma port: %d", server_idx, rdma_port);

	// Spinning thread of server
	int spinning_thread_id = 0;
	log_info("Server %d spinning_thread id: %d", server_idx, spinning_thread_id);

	// now the worker ids
	int num_workers = SCONF_get_num_threads(s_config) - 1;

	if (num_workers == 0) {
		log_fatal("No workers specified for Server %d", server_idx);
		_exit(EXIT_FAILURE);
	}

	// Double the workers, mirror workers are for server tasks
	num_workers *= 2;

	// now we have all info to allocate ds_numa_server, pin,
	// and inform the root server
	struct ds_numa_server *server = calloc(1, sizeof(struct ds_numa_server));
	if (server == NULL) {
		log_fatal("malloc failed: could not get memory for server");
		_exit(EXIT_FAILURE);
	}

	/*But first let's build each numa server's RDMA channel*/
	/*RDMA channel staff*/
	server->channel = (struct channel_rdma *)calloc(1UL, sizeof(struct channel_rdma));
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

	for (int worker_id = 0; worker_id < num_workers; worker_id++) {
		server->spinner.worker[worker_id].worker_id = worker_id;
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
		log_fatal("failed to spawn socket thread for Server: %d reason follows:", server->server_id);
		perror("Reason: \n");
		_exit(EXIT_FAILURE);
	}

	// Now pin it in the numa node! Important step so allocations used by spinner and workers to be in the same numa node
	log_info("Started socket thread successfully for Server %d", server->server_id);

	log_info("Starting spinning thread for Server %d", server->server_id);
	if (pthread_create(&server->spinner_cnxt, NULL, server_spinning_thread_kernel, &server->spinner) != 0) {
		log_fatal("Failed to create spinner thread for Server: %d", server->server_id);
		_exit(EXIT_FAILURE);
	}

	log_info("Pinned successfully spinning thread of Server: %d at port %d at core %d", server->server_id,
		 server->rdma_port, server->spinner.spinner_id);

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
	log_warn("Tebis Region Server exiting");
	return 0;
}
