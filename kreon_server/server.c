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
#include "conf.h"
#include <include/parallax.h>
#include <stdint.h>
#include <stdlib.h>
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

#include "../kreon_rdma/rdma.h"
#include "../utilities/queue.h"
#include "../utilities/spin_loop.h"
#include "djb2.h"
#include "globals.h"
#include "messages.h"
#include "metadata.h"
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
	SIMPLE_CONCURRENT_LIST *idle_conn_list;
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
	// our rdma channel
	struct channel_rdma *channel;
	// context of the metadata thread
	pthread_t meta_server_cnxt;
	struct krm_server_desc meta_server;
	// spinner manages its workers
	struct ds_spinning_thread spinner;
};

struct ds_root_server {
	int num_of_numa_servers;
	struct ds_numa_server *numa_servers[MAX_NUMA_SERVERS];
};

/*root of everything*/
static struct ds_root_server *root_server = NULL;

static void handle_task(struct krm_server_desc const *mydesc, struct krm_work_task *task);
static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct krm_work_task *task);

static void crdma_server_create_connection_inuse(struct connection_rdma *conn, struct channel_rdma *channel,
						 connection_type type)
{
	/*gesalous, This is the path where it creates the useless memory queues*/
	memset(conn, 0, sizeof(struct connection_rdma));
	conn->type = type;
	conn->channel = channel;
}

struct channel_rdma *ds_get_channel(struct krm_server_desc const *my_desc)
{
	return root_server->numa_servers[my_desc->root_server_id]->channel;
}

/** Function filling replies from server to clients
 *  payload and msg_type must be provided as they defer from msg to msg
 *  */
static void fill_reply_msg(msg_header *reply_msg, struct krm_work_task *task, uint32_t payload_size, uint16_t msg_type)
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

	struct rdma_cm_id *listen_id;
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
 * gesalous new staff
 * each spin thread has a group of worker threads. Spin threads detects that a
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
		handle_task(&root_server->numa_servers[worker->root_server_id]->meta_server, job);
		switch (job->kreon_operation_status) {
		case TASK_COMPLETE:

			zero_rendezvous_locations(job->msg);
			__send_rdma_message(job->conn, job->reply_msg, NULL);
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
	case GET_LOG_BUFFER_REQ:
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
				return KREON_FAILURE;
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
		uint64_t hash = djb2_hash((unsigned char *)&msg->session_id, sizeof(uint64_t));
		int bound = spinner->num_workers / 2;
		worker_id = (hash % bound) + bound;
		// log_warn("fifo worker id %d chosen for session id %llu", worker_id,
		// msg->session_id);
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

	return KREON_SUCCESS;
}

static void print_task(struct krm_work_task *task)
{
	switch (task->kreon_operation_status) {
	case SEGMENT_BARRIER:
		log_debug("SEGMENT_BARRIER state details follow");
		for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
			uint32_t remaining =
				task->r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].replicated_bytes;
			remaining = SEGMENT_SIZE - (remaining + task->ins_req.metadata.log_padding);
			log_debug(
				"Sorry segment with replica %u not ready bytes remaining to replicate %u task %lu seg if %u key size is %lu",
				i, remaining, (uint64_t)task, task->seg_id_to_flush, task->kv_size);
		}
		break;
	default:
		log_debug("Sorry canoot provide additional info");
		return;
	}
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
			print_task(task);
			b_detection = 1;
		}

		++total_halted_tasks;
		int rc = assign_job_to_worker(spinner, task->conn, task->msg, task);
		if (rc == KREON_FAILURE) {
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
				if (rc == KREON_FAILURE) {
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
	return;
}

// This function is called by the poll_cq thread every time a notification
// arrives
static void wait_for_replication_completion_callback(struct rdma_message_context *r_cnxt)
{
	if (r_cnxt->__is_initialized != 1) {
		log_debug("replication completion callback %u", r_cnxt->__is_initialized);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	sem_post(&r_cnxt->wait_for_completion);
}

static int init_replica_connections(struct krm_server_desc const *server, struct krm_work_task *task)
{
	while (1) {
		switch (task->kreon_operation_status) {
		case TASK_START: {
			task->kreon_operation_status = GET_RSTATE;
			break;
		}
		case GET_RSTATE: {
			if (task->r_desc->region->num_of_backup) {
				if (task->r_desc->replica_buf_status == KRM_BUFS_READY) {
					task->kreon_operation_status = INS_TO_KREON;
					return 1;
				}

				pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
				if (task->r_desc->replica_buf_status == KRM_BUFS_INITIALIZING) {
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					return 0;
				} else if (task->r_desc->replica_buf_status == KRM_BUFS_UNINITIALIZED) {
					/*log_info("Initializing log buffers with replicas for DB %s",
						 task->r_desc->db->db_desc->db_name);*/
					task->r_desc->replica_buf_status = KRM_BUFS_INITIALIZING;
					task->kreon_operation_status = INIT_LOG_BUFFERS;
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					break;
				} else {
					task->kreon_operation_status = INS_TO_KREON;
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					return 1;
				}
			} else {
				task->kreon_operation_status = INS_TO_KREON;
				return 1;
			}
		}
		case INIT_LOG_BUFFERS: {
			struct krm_region_desc *r_desc = task->r_desc;
			if (r_desc->m_state == NULL) {
				r_desc->m_state = (struct ru_master_state *)calloc(1, sizeof(struct ru_master_state));

				//sizeof(struct ru_master_state) + (r_desc->region->num_of_backup *
				//				  (sizeof(struct ru_master_log_buffer) +
				//				   (RU_REPLICA_NUM_SEGMENTS *
				//				    sizeof(struct ru_master_log_buffer_seg)))));
				// we need to dive into Kreon to check what in the current end of
				// log. Since for this region we are the first to do this there is
				// surely no
				// concurrent access
				log_debug("Tebis-Parallax no replication should not care about this");
				uint64_t range = 0;
				/*if (r_desc->db->db_desc->KV_log_size > 0) {
					range = r_desc->db->db_desc->KV_log_size -
						(r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE);
				}
				*/
				for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
					r_desc->m_state->r_buf[i].stat = RU_BUFFER_UNINITIALIZED;
					for (int j = 0; j < RU_REPLICA_NUM_SEGMENTS; j++) {
						r_desc->m_state->r_buf[i].segment[j].start = range;
						range += SEGMENT_SIZE;

						r_desc->m_state->r_buf[i].segment[j].end = range;
						r_desc->m_state->r_buf[i].segment[j].lc1 = 0;
						r_desc->m_state->r_buf[i].segment[j].lc2 = 0;
						r_desc->m_state->r_buf[i].segment[j].replicated_bytes =
							sizeof(segment_header);

						r_desc->m_state->r_buf[i].segment[j].flush_cmd_stat =
							RU_BUFFER_UNINITIALIZED;
						memset(&r_desc->m_state->r_buf[i].segment[j].mr, 0x00,
						       sizeof(struct ibv_mr));
					}
				}
			}

			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
				struct connection_rdma *conn =
					sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);

				if (r_desc->m_state->r_buf[i].stat == RU_BUFFER_UNINITIALIZED) {
					log_debug("Sending GET_LOG_BUFFER req to Server %s for DB %s",
						  r_desc->region->backups[i].kreon_ds_hostname,
						  r_desc->db->volume_desc->volume_name);

					r_desc->m_state->r_buf[i].p = sc_allocate_rpc_pair(
						conn,
						sizeof(struct s2s_msg_get_log_buffer_req) +
							r_desc->region->min_key_size,
						sizeof(struct s2s_msg_get_log_buffer_rep) +
							(RU_REPLICA_NUM_SEGMENTS * sizeof(struct ibv_mr)),
						GET_LOG_BUFFER_REQ);

					if (r_desc->m_state->r_buf[i].p.stat != ALLOCATION_IS_SUCCESSFULL)
						continue;

					/*inform the req about its buddy*/
					msg_header *req_header = r_desc->m_state->r_buf[i].p.request;
					msg_header *rep_header = r_desc->m_state->r_buf[i].p.reply;
					req_header->triggering_msg_offset_in_send_buffer =
						real_address_to_triggering_msg_offt(conn, req_header);
					/*location where server should put the reply*/
					req_header->offset_reply_in_recv_buffer =
						(uint64_t)rep_header - (uint64_t)conn->recv_circular_buf->memory_region;
					req_header->reply_length_in_recv_buffer = sizeof(msg_header) +
										  rep_header->payload_length +
										  rep_header->padding_and_tail_size;
					/*time to send the message*/
					req_header->session_id = (uint64_t)r_desc->region;
					struct s2s_msg_get_log_buffer_req *g_req =
						(struct s2s_msg_get_log_buffer_req *)((uint64_t)req_header +
										      sizeof(struct msg_header));
					g_req->num_buffers = RU_REPLICA_NUM_SEGMENTS;
					g_req->buffer_size = SEGMENT_SIZE;
					g_req->region_key_size = r_desc->region->min_key_size;
					strcpy(g_req->region_key, r_desc->region->min_key);
					__send_rdma_message(conn, req_header, NULL);

					r_desc->m_state->r_buf[i].stat = RU_BUFFER_REQUESTED;
				}
			}
			// log_info("Checking log buffer replies num of replicas are %d",
			// r_desc->region->num_of_backup);
			// check replies
			uint32_t ready_buffers = 0;
			for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
				if (r_desc->m_state->r_buf[i].stat == RU_BUFFER_REQUESTED) {
					/*check reply and process*/
					// log_info("Waiting tail at offset: %d",
					//	 (sizeof(struct msg_header) +
					//	  r_desc->m_state->r_buf[i].p.reply->payload_length +
					//	  r_desc->m_state->r_buf[i].p.reply->padding_and_tail_size) -
					//		 TU_TAIL_SIZE);

					/*wait first for the header and then the payload*/
					if (r_desc->m_state->r_buf[i].p.reply->receive != TU_RDMA_REGULAR_MSG)
						continue;

					/*Check arrival of payload*/
					uint8_t tail = get_receive_field(r_desc->m_state->r_buf[i].p.reply);
					if (tail != TU_RDMA_REGULAR_MSG)
						continue;

					struct s2s_msg_get_log_buffer_rep *rep =
						(struct s2s_msg_get_log_buffer_rep
							 *)(((uint64_t)r_desc->m_state->r_buf[i].p.reply) +
							    sizeof(struct msg_header));
					assert(rep->status == KREON_SUCCESS);

					r_desc->m_state->r_buf[i].segment_size = SEGMENT_SIZE;
					r_desc->m_state->r_buf[i].num_buffers = RU_REPLICA_NUM_SEGMENTS;
					/*TODO tebis parallax no replication should not care about this*/
					/*uint64_t seg_offt = r_desc->db->db_desc->KV_log_size -
							    (r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE);
					*/
					uint64_t seg_offt = 0;
					r_desc->next_segment_to_flush = seg_offt;

					for (int j = 0; j < RU_REPLICA_NUM_SEGMENTS; ++j) {
						r_desc->m_state->r_buf[i].segment[j].start = seg_offt;
						seg_offt += SEGMENT_SIZE;
						r_desc->m_state->r_buf[i].segment[j].end = seg_offt;
						r_desc->m_state->r_buf[i].segment[j].mr = rep->mr;
						//log_info("Remote memory for server segment [%u][%u] = %llu key %lu", i,
						//	 j, r_desc->m_state->r_buf[i].segment[j].mr.addr,
						//	 r_desc->m_state->r_buf[i].segment[j].mr.rkey);
						assert(r_desc->m_state->r_buf[i].segment[j].mr.length == SEGMENT_SIZE);
					}

					r_desc->m_state->r_buf[i].stat = RU_BUFFER_OK;
					/*finally free the message*/
					sc_free_rpc_pair(&r_desc->m_state->r_buf[i].p);
				}
				if (r_desc->m_state->r_buf[i].stat == RU_BUFFER_OK)
					++ready_buffers;
			}

			if (ready_buffers != r_desc->region->num_of_backup) {
				// log_info("Not all replicas ready waiting status %d",
				// task->kreon_operation_status);
				return 0;
			}

			pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++)
				r_desc->m_state->r_buf[i].stat = RU_BUFFER_UNINITIALIZED;

			log_debug("Remote buffers ready initialize remote segments with current state");

			/* Prepare the context for the poller to later free the staff needed*/
			for (uint32_t i = 0; i < r_desc->region->num_of_backup; ++i) {
				struct recover_log_context *context =
					(struct recover_log_context *)calloc(1, sizeof(struct recover_log_context));
				context->num_of_replies_needed = 1;
				context->memory = calloc(1, SEGMENT_SIZE);
				task->msg_ctx[0].msg = NULL;
				client_rdma_init_message_context(&task->msg_ctx[0], NULL);
				task->msg_ctx[0].on_completion_callback = recover_log_context_completion;
				task->msg_ctx[0].args = (void *)context;
				/* copy last segment to a register buffer */
				struct segment_header *last_segment = (struct segment_header *)context->memory;
				/*TODO tebis-parallax no replication should not care about this*/
				/*memcpy(last_segment, (const char *)r_desc->db->db_desc->KV_log_last_segment,
				       SEGMENT_SIZE);
				       */
				struct connection_rdma *r_conn =
					sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
				context->mr = rdma_reg_write(r_conn->rdma_cm_id, last_segment, SEGMENT_SIZE);

				if (context->mr == NULL) {
					log_fatal("Failed to reg memory");
					exit(EXIT_FAILURE);
				}

				log_debug("Sending last segment to server: %s",
					  r_desc->region->backups[i].kreon_ds_hostname);
				// 2. rdma it to the remote
				log_debug("Sending last segment to %lu with rkey %lu",
					  (uint64_t)r_desc->m_state->r_buf[i].segment[0].mr.addr,
					  (uint64_t)r_desc->m_state->r_buf[i].segment[0].mr.rkey);
				while (1) {
					int ret =
						rdma_post_write(r_conn->rdma_cm_id, &task->msg_ctx[0], last_segment,
								SEGMENT_SIZE, context->mr, IBV_SEND_SIGNALED,
								(uint64_t)r_desc->m_state->r_buf[i].segment[0].mr.addr,
								r_desc->m_state->r_buf[i].segment[0].mr.rkey);
					if (!ret) {
						break;
					}
					if (r_conn->status == CONNECTION_ERROR) {
						log_fatal("connection failed !: %s\n", strerror(errno));
						exit(EXIT_FAILURE);
					}
				}

				/* Wait for the completion of the rdma operation above*/
				log_debug("Waiting for RDMA completion of the SEGMENT with server: %s",
					  r_desc->region->backups[i].kreon_ds_hostname);
				field_spin_for_value(&context->num_of_replies_needed, 0);
				task->msg_ctx[0].__is_initialized = 0;
				task->msg_ctx[0].on_completion_callback = NULL;
				// destroy context
				rdma_dereg_mr(context->mr);
				free(context->memory);
				free(context);

				log_debug("Successfully sent the last segment to server: %s for region %s",
					  r_desc->region->backups[i].kreon_ds_hostname, r_desc->region->id);
				/*TODO tebis-parallax no replication should not care aboyt this*/
				/* r_desc->next_segment_to_flush = r_desc->db->db_desc->KV_log_size -
								(r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE);*/
			}

			log_info("Success RDMA buffers initialized for all replicas of region %s", r_desc->region->id);
			task->r_desc->replica_buf_status = KRM_BUFS_READY;
			pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
			task->kreon_operation_status = INS_TO_KREON;
			return 1;
		}
		default:
			return 1;
		}
	}
}

static int krm_enter_kreon(struct krm_region_desc *r_desc, struct krm_work_task *task)
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
		case SEGMENT_BARRIER:
		case FLUSH_REPLICA_BUFFERS:
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
		case WAIT_FOR_REPLICATION_COMPLETION:
		case ALL_REPLICAS_ACKED:
		case SEGMENT_BARRIER:
		case FLUSH_REPLICA_BUFFERS:
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

static void krm_leave_kreon(struct krm_region_desc *r_desc)
{
	if (r_desc->region->num_of_backup == 0)
		return;
	__sync_fetch_and_sub(&r_desc->pending_region_tasks, 1);
}

static void fill_flush_request(struct krm_region_desc *r_desc, struct s2s_msg_flush_cmd_req *flush_request,
			       struct krm_work_task *task)
{
	/*where primary has stored its segment*/
	flush_request->is_partial = 0;
	flush_request->log_buffer_id = task->seg_id_to_flush;
	flush_request->master_segment = task->ins_req.metadata.log_segment_addr;
	// log_info("Sending flush command for segment %llu",
	// flush_request->master_segment);
	flush_request->segment_id = task->ins_req.metadata.segment_id;
	flush_request->end_of_log = task->ins_req.metadata.end_of_log;
	flush_request->log_padding = task->ins_req.metadata.log_padding;
	flush_request->region_key_size = r_desc->region->min_key_size;
	strcpy(flush_request->region_key, r_desc->region->min_key);
}

static void insert_kv_pair(struct krm_server_desc const *server, struct krm_work_task *task)
{
	//############## fsm state logic follows ###################
	while (1) {
		switch (task->kreon_operation_status) {
		case INS_TO_KREON: {
			task->ins_req.metadata.handle = task->r_desc->db;
			task->ins_req.metadata.kv_size = 0;
			task->ins_req.key_value_buf = (char *)task->key;
			task->ins_req.metadata.level_id = 0;
			task->ins_req.metadata.key_format = KV_FORMAT;
			task->ins_req.metadata.append_to_log = 1;
			task->ins_req.metadata.gc_request = 0;
			task->ins_req.metadata.recovery_request = 0;
			task->ins_req.metadata.segment_full_event = 0;
			task->ins_req.metadata.special_split = 0;

			if (_insert_key_value(&task->ins_req) == PARALLAX_FAILURE) {
				krm_leave_kreon(task->r_desc);
				return;
			}

			if (task->r_desc->region->num_of_backup > 0) {
				if (task->ins_req.metadata.segment_full_event) {
					task->last_replica_to_ack = 0;
					task->kreon_operation_status = FLUSH_REPLICA_BUFFERS;
				} else
					task->kreon_operation_status = REPLICATE;
			} else
				task->kreon_operation_status = TASK_COMPLETE;

			break;
		}

		case FLUSH_REPLICA_BUFFERS: {
			struct krm_region_desc *r_desc = task->r_desc;

			pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
			uint64_t next_segment_to_flush = r_desc->next_segment_to_flush;
			// pthread_mutex_lock(&task->r_desc->region_lock);
			/*Is it my turn to flush or can I resume?*/
			if (task->ins_req.metadata.log_offset_full_event > next_segment_to_flush &&
			    task->ins_req.metadata.log_offset_full_event <= next_segment_to_flush + SEGMENT_SIZE) {
				// log_info("Ok my turn to flush proceeding");
				// task->r_desc->region_halted = 1;
				// pthread_mutex_unlock(&task->r_desc->region_lock);
			} else {
				// pthread_mutex_unlock(&task->r_desc->region_lock);
				/*sorry not my turn*/
				pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
				return;
			}
			/*find out the idx of the buffer that needs flush*/

			uint64_t lc1;
			uint64_t lc2;
		retry:

			//task->seg_id_to_flush = -1;
			for (uint32_t m = task->last_replica_to_ack; m < task->r_desc->region->num_of_backup; ++m) {
				task->seg_id_to_flush = -1;
				lc1 = r_desc->m_state->r_buf[m].segment[0].lc1;
				if (task->ins_req.metadata.log_offset_full_event >
					    r_desc->m_state->r_buf[m].segment[0].start &&
				    task->ins_req.metadata.log_offset_full_event <=
					    r_desc->m_state->r_buf[m].segment[0].end) {
					task->seg_id_to_flush = 0;
				}
				lc2 = r_desc->m_state->r_buf[m].segment[0].lc2;
				if (lc1 != lc2)
					goto retry;

				if (task->seg_id_to_flush == -1) {
					task->last_replica_to_ack = m;
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					return;
				}
			}

			if (task->seg_id_to_flush != 0) {
				log_fatal("No appropriate remote segment id found for flush, what?");
				_exit(EXIT_FAILURE);
			}
			/*sent flush command to all motherfuckers*/
			task->kreon_operation_status = SEGMENT_BARRIER;
			task->last_replica_to_ack = 0;
			pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
			break;
		}
		case SEGMENT_BARRIER: {
			for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; ++i) {
				uint64_t remaining =
					task->r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].replicated_bytes;
				remaining = SEGMENT_SIZE - (remaining + task->ins_req.metadata.log_padding);
				if (remaining > 0) {
					//log_info("Sorry segment not ready bytes remaining to replicate %llu "
					//	 "task %p seg if %u",
					//	 remaining, task, task->seg_id_to_flush);
					task->last_replica_to_ack = i;
					return;
				}
			}
			task->last_replica_to_ack = 0;
			task->kreon_operation_status = SEND_FLUSH_COMMANDS;
			break;
		}
		case SEND_FLUSH_COMMANDS: {
			struct krm_region_desc *r_desc = task->r_desc;
			for (uint32_t i = task->last_replica_to_ack; i < r_desc->region->num_of_backup; ++i) {
				struct connection_rdma *r_conn = NULL;
				/*allocate and send command*/
				if (r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd_stat ==
				    RU_BUFFER_UNINITIALIZED) {
					r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
					/*send flush req and piggyback it with the seg id num*/
					uint32_t req_size =
						sizeof(struct s2s_msg_flush_cmd_req) + r_desc->region->min_key_size;
					uint32_t rep_size = sizeof(struct s2s_msg_flush_cmd_rep);
					r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd =
						sc_allocate_rpc_pair(r_conn, req_size, rep_size, FLUSH_COMMAND_REQ);

					if (r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd.stat !=
					    ALLOCATION_IS_SUCCESSFULL) {
						task->last_replica_to_ack = i;
						return;
					}

					msg_header *req_header = r_desc->m_state->r_buf[i]
									 .segment[task->seg_id_to_flush]
									 .flush_cmd.request;

					/*time to send the message*/
					req_header->session_id = (uint64_t)task->r_desc->region;
					struct s2s_msg_flush_cmd_req *f_req =
						(struct s2s_msg_flush_cmd_req *)((uint64_t)req_header +
										 sizeof(struct msg_header));

					fill_flush_request(r_desc, f_req, task);
					__send_rdma_message(r_conn, req_header, NULL);
					r_desc->m_state->r_buf[i].stat = RU_BUFFER_REQUESTED;
					// log_info("Sent flush command req_header %llu", req_header);
				}
			}
			task->last_replica_to_ack = 0;
			task->kreon_operation_status = WAIT_FOR_FLUSH_REPLIES;
			break;
		}

		case WAIT_FOR_FLUSH_REPLIES: {
			struct krm_region_desc *r_desc = task->r_desc;
			for (uint32_t i = task->last_replica_to_ack; i < r_desc->region->num_of_backup; ++i) {
				/*check if header is there*/
				msg_header *reply =
					r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd.reply;

				if (reply->receive != TU_RDMA_REGULAR_MSG) {
					task->last_replica_to_ack = i;
					return;
				}
				/*check if payload is there*/
				uint8_t tail = get_receive_field(reply);
				if (tail != TU_RDMA_REGULAR_MSG) {
					task->last_replica_to_ack = i;
					return;
				}
			}
			/*got all replies motherfuckers*/
			pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
			r_desc->next_segment_to_flush += SEGMENT_SIZE;
			// pthread_mutex_unlock(&task->r_desc->region_lock);

			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
				++r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].lc2;

				r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].end += (uint64_t)SEGMENT_SIZE;
				r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].start +=
					(uint64_t)SEGMENT_SIZE;
				r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].replicated_bytes =
					sizeof(segment_header);
				sc_free_rpc_pair(&r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd);
				++r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].lc1;
			}
			// log_info("Resume possible halted tasks after flush");
			// pthread_mutex_lock(&r_desc->region_lock);
			pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
			/*count how many replication requests we have send*/
			task->last_replica_to_ack = 0;
			task->kreon_operation_status = REPLICATE;
			break;
		}

		case REPLICATE: {
			struct krm_region_desc *r_desc = task->r_desc;
			task->kv_size = task->key->key_size + sizeof(struct msg_put_key);
			task->kv_size = task->kv_size + task->value->value_size + sizeof(struct msg_put_value);
			uint32_t remote_offset = 0;
			if (task->ins_req.metadata.log_offset > 0)
				remote_offset = task->ins_req.metadata.log_offset % (uint64_t)SEGMENT_SIZE;
			for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; ++i) {
				uint32_t replicate_success = 0;
				/*find appropriate seg buffer to rdma the mutation*/
				pthread_mutex_lock(&task->r_desc->region_mgmnt_lock);
				for (uint32_t j = 0; j < RU_REPLICA_NUM_SEGMENTS; ++j) {
					uint64_t lc1;
					uint64_t lc2;
				retry_1:
					lc1 = r_desc->m_state->r_buf[i].segment[j].lc1;
					if (task->ins_req.metadata.log_offset >=
						    r_desc->m_state->r_buf[i].segment[j].start &&
					    task->ins_req.metadata.log_offset <
						    r_desc->m_state->r_buf[i].segment[j].end) {
						lc2 = r_desc->m_state->r_buf[i].segment[j].lc2;
						if (lc1 != lc2) {
							j = 0;
							goto retry_1;
						}
						/*Correct choice rdma the fucking thing*/
						struct connection_rdma *r_conn = sc_get_data_conn(
							server, r_desc->region->backups[i].kreon_ds_hostname);

						client_rdma_init_message_context(&task->msg_ctx[i], NULL);

						task->msg_ctx[i].args = task;
						task->msg_ctx[i].on_completion_callback =
							wait_for_replication_completion_callback;
						task->replicated_bytes[i] =
							&r_desc->m_state->r_buf[i].segment[j].replicated_bytes;
						/*BUG Is this necessery gesalous? I think this is a logical error because
						 *a task may go for replication wait without sending its rdma write properly */
						//task->kreon_operation_status = WAIT_FOR_REPLICATION_COMPLETION;
						while (1) {
							int ret = rdma_post_write(
								r_conn->rdma_cm_id, &task->msg_ctx[i], task->key,
								task->kv_size,
								task->conn->rdma_memory_regions->remote_memory_region,
								IBV_SEND_SIGNALED,
								(uint64_t)r_desc->m_state->r_buf[i].segment[j].mr.addr +
									remote_offset,
								r_desc->m_state->r_buf[i].segment[j].mr.rkey);

							if (ret == 0)
								break;
						}
						++task->last_replica_to_ack;
						replicate_success = 1;
						break;
					}
				}
				if (!replicate_success) {
					pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
					return;
				}
				pthread_mutex_unlock(&task->r_desc->region_mgmnt_lock);
			}

			if (task->r_desc->region->num_of_backup != task->last_replica_to_ack)
				return;
			/*Reset the counter to count replies*/
			task->last_replica_to_ack = 0;
			task->kreon_operation_status = WAIT_FOR_REPLICATION_COMPLETION;
			break;
		}
		case WAIT_FOR_REPLICATION_COMPLETION: {
#if 1
			for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; ++i) {
				if (sem_trywait(&task->msg_ctx[i].wait_for_completion) != 0) {
					task->last_replica_to_ack = i;
					return;
				}

				if (task->msg_ctx[i].wc.status != IBV_WC_SUCCESS &&
				    task->msg_ctx[i].wc.status != IBV_WC_WR_FLUSH_ERR) {
					log_fatal("Replication RDMA write error: %s",
						  ibv_wc_status_str(task->msg_ctx[i].wc.status));
					_exit(EXIT_FAILURE);
				}

				/*count bytes replicated for this segment*/
				//assert(task->kv_size < 1200);
				__sync_fetch_and_add(task->replicated_bytes[i], task->kv_size);
				//log_info(" key is %u:%s Bytes now %llu i =%u kv size was %u full event? %u",
				//	 *(uint32_t *)task->ins_req.key_value_buf, task->ins_req.key_value_buf + 4,
				//	 *task->replicated_bytes[i], i, task->kv_size,
				//	 task->ins_req.metadata.segment_full_event);
				assert(*task->replicated_bytes[i] <= SEGMENT_SIZE);
			}
			task->kreon_operation_status = ALL_REPLICAS_ACKED;
#endif
			break;
		}
		case ALL_REPLICAS_ACKED:
			task->kreon_operation_status = TASK_COMPLETE;
			break;
		case TASK_COMPLETE:
			// bt_decrease_level0_writers(task->r_desc->db);
			return;

		default:
			log_fatal("Ended up in faulty state %u", task->kreon_operation_status);
			assert(0);
			return;
		}
	}
}
/*TODO closing this for now since it is used at flush log procedure*/
/*
static int write_segment_with_explicit_IO(char *buf, ssize_t num_bytes, ssize_t dev_offt, int fd)
{
	ssize_t total_bytes_written = 0;
	while (total_bytes_written < num_bytes) {
		ssize_t bytes_written =
			pwrite(fd, buf, num_bytes - total_bytes_written, dev_offt + total_bytes_written);
		if (bytes_written == -1) {
			log_fatal("Failed to writed segment for leaf nodes reason follows");
			perror("Reason");
			_exit(EXIT_FAILURE);
		}
		total_bytes_written += bytes_written;
	}
	return 1;
}
*/
static void execute_put_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	assert(task->msg->msg_type == PUT_REQUEST || task->msg->msg_type == PUT_IF_EXISTS_REQUEST);
	//retrieve region handle for the corresponding key, find_region
	//initiates internally rdma connections if needed
	if (task->key == NULL) {
		task->key = (struct msg_put_key *)((uint64_t)task->msg + sizeof(struct msg_header));
		task->value = (struct msg_put_value *)((uint64_t)task->key + sizeof(msg_put_key) + task->key->key_size);
		uint32_t key_length = task->key->key_size;
		assert(key_length != 0);
		struct krm_region_desc *r_desc = krm_get_region(mydesc, task->key->key, task->key->key_size);

		if (r_desc == NULL) {
			log_fatal("Region not found for key size %u:%s", task->key->key_size, task->key->key);
			_exit(EXIT_FAILURE);
		}
		task->r_desc = (void *)r_desc;

		par_handle par_hd = (par_handle)task->r_desc->db;
		struct par_key pkey;
		pkey.data = task->key->key;
		pkey.size = task->key->key_size;
		if (par_exists(par_hd, &pkey) == PAR_KEY_NOT_FOUND) {
			log_warn("Key %s in update_if_exists for region %s not found!", task->key->key,
				 r_desc->region->id);
			_exit(EXIT_FAILURE);
		}
	}

	if (!init_replica_connections(mydesc, task))
		return;

	if (!krm_enter_kreon(task->r_desc, task))
		return;
	insert_kv_pair(mydesc, task);
	if (task->kreon_operation_status == TASK_COMPLETE) {
		krm_leave_kreon(task->r_desc);

		/*prepare the reply*/
		task->reply_msg = (struct msg_header *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
							task->msg->offset_reply_in_recv_buffer);

		uint32_t actual_reply_size = sizeof(msg_header) + sizeof(msg_put_rep) + TU_TAIL_SIZE;
		if (task->msg->reply_length_in_recv_buffer >= actual_reply_size) {
			fill_reply_msg(task->reply_msg, task, sizeof(msg_put_rep), PUT_REPLY);
			msg_put_rep *put_rep = (msg_put_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
			put_rep->status = KREON_SUCCESS;
			set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
		} else {
			log_fatal("SERVER: mr CLIENT reply space not enough  size %" PRIu32 " FIX XXX TODO XXX\n",
				  task->msg->reply_length_in_recv_buffer);
			_exit(EXIT_FAILURE);
		}
	}
}

static void execute_get_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	/*TODO IMPORTANT*/
	(void)mydesc;
	(void)task;
	log_debug(
		"closing get req for now to avoid compile errors, this SHOULD BE implemented in tebis-parallax no replications scheme");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	msg_get_req *get_req = NULL;
	msg_get_rep *get_rep = NULL;
	assert(task->msg->msg_type == GET_REQUEST);
	struct bt_kv_log_address L = { .addr = NULL, .in_tail = 0, .tail_id = UINT8_MAX };
	int level_id;
	//kreon phase
	get_req = (struct msg_get_req *)((uint64_t)task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc = krm_get_region(mydesc, get_req->key, get_req->key_size);

	if (r_desc == NULL) {
		log_fatal("Region not found for key %s", get_req->key);
		_exit(EXIT_FAILURE);
	}

	task->kreon_operation_status = TASK_GET_KEY;
	task->r_desc = r_desc;
	if (!krm_enter_kreon(r_desc, task)) {
		// later...
		return;
	}
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	get_rep = (msg_get_rep *)((uint64_t)task->reply_msg + sizeof(struct msg_header));
	uint64_t kv_offt = find_kv_offt(r_desc->db, &get_req->key_size, &level_id);
	krm_leave_kreon(r_desc);

	if (kv_offt == 0) {
		log_warn("key not found key %s : length %u", get_req->key, get_req->key_size);

		get_rep->key_found = 0;
		get_rep->bytes_remaining = 0;
		get_rep->value_size = 0;
		get_rep->offset_too_large = 0;
		goto exit;

	} else {
		get_rep->key_found = 1;
		// tranlate now
		if (level_id)
			L.addr = bt_get_real_address(kv_offt);
		else
			L = bt_get_kv_log_address(r_desc->db->db_desc, kv_offt);
		char *value_p = (char *)L.addr + *(uint32_t *)L.addr + sizeof(uint32_t);
		if (get_req->offset > *(uint32_t *)value_p) {
			get_rep->offset_too_large = 1;
			get_rep->value_size = 0;
			get_rep->bytes_remaining = *(uint32_t *)value_p;
			goto exit;
		} else
			get_rep->offset_too_large = 0;
		if (!get_req->fetch_value) {
			get_rep->bytes_remaining = *(uint32_t *)value_p - get_req->offset;
			get_rep->value_size = 0;
			goto exit;
		}
		uint32_t value_bytes_remaining = *(uint32_t *)value_p - get_req->offset;
		uint32_t bytes_to_read = 0;
		if (get_req->bytes_to_read <= value_bytes_remaining) {
			bytes_to_read = get_req->bytes_to_read;
			get_rep->bytes_remaining = *(uint32_t *)value_p - (get_req->offset + bytes_to_read);
		} else {
			bytes_to_read = value_bytes_remaining;
			get_rep->bytes_remaining = 0;
		}
		get_rep->value_size = bytes_to_read;
		// log_info("Client wants to read %u will read
		// %u",get_req->bytes_to_read,bytes_to_read);
		memcpy(get_rep->value, value_p + sizeof(uint32_t) + get_req->offset, bytes_to_read);
	}

exit:
	if (L.in_tail)
		bt_done_with_value_log_address(r_desc->db->db_desc, &L);

	//finally fix the header
	uint32_t payload_length = sizeof(msg_get_rep) + get_rep->value_size;

	fill_reply_msg(task->reply_msg, task, payload_length, GET_REPLY);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
*/
}

static void execute_multi_get_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	log_debug("Close scans since we dont use them for now (the tebis-parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	msg_multi_get_req *multi_get;
	scannerHandle *sc;
	msg_value zero_value;
	uint32_t actual_reply_size = 0;
	uint32_t padding;
	zero_value.size = 0;
	assert(task->msg->msg_type == MULTI_GET_REQUEST);
	multi_get = (msg_multi_get_req *)(task->msg + sizeof(struct msg_header));
	struct krm_region_desc *r_desc = krm_get_region(mydesc, multi_get->seek_key, multi_get->seek_key_size);

	if (r_desc == NULL) {
		log_fatal("Region not found for key size %u:%s", multi_get->seek_key_size, multi_get->seek_key);
		_exit(EXIT_FAILURE);
	}
	task->kreon_operation_status = TASK_MULTIGET;
	task->r_desc = r_desc;
	if (!krm_enter_kreon(r_desc, task)) {
		// later...
		return;
	}
	//create an internal scanner object
	sc = (scannerHandle *)malloc(sizeof(scannerHandle));

	if (multi_get->seek_mode != FETCH_FIRST) {
		// log_info("seeking at key %u:%s",
		// multi_get->seek_key_size,multi_get->seek_key);
		init_dirty_scanner(sc, r_desc->db, &multi_get->seek_key_size, multi_get->seek_mode);
	} else {
		// log_info("seeking at key first key of region");
		init_dirty_scanner(sc, r_desc->db, NULL, FETCH_FIRST);
	}

	//put the data in the buffer
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	msg_multi_get_rep *buf = (msg_multi_get_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	buf->curr_entry = 0;
	buf->end_of_region = 0;
	buf->buffer_overflow = 0;

	buf->capacity = task->msg->reply_length_in_recv_buffer -
			(sizeof(msg_header) + sizeof(msg_multi_get_rep) + TU_TAIL_SIZE);
	buf->remaining = buf->capacity;
	buf->pos = 0;
	buf->num_entries = 0;
	if (isValid(sc)) {
		void *kv_pointer = get_kv_pointer(sc);
		// msg_key *key = (struct msg_key *)(getKeyPtr(sc) - sizeof(uint32_t));
		msg_key *key = kv_pointer;
		struct msg_value *value = NULL;

		if (multi_get->fetch_keys_only)
			value = (msg_value *)&zero_value;
		else
			// value = (struct msg_value *)(getValuePtr(sc) - sizeof(uint32_t));
			value = (struct msg_value *)((char *)kv_pointer + key->size + sizeof(struct msg_key));
		int rc = msg_push_to_multiget_buf(key, value, buf);
		done_with_kv_pointer(sc);

		if (rc == KREON_SUCCESS) {
			while (buf->num_entries <= multi_get->max_num_entries) {
				if (getNext(sc) == END_OF_DATABASE) {
					buf->end_of_region = 1;
					break;
				}
				// key = (struct msg_key *)(getKeyPtr(sc) - sizeof(uint32_t));
				kv_pointer = get_kv_pointer(sc);
				key = kv_pointer;
				if (multi_get->fetch_keys_only)
					value = (msg_value *)&zero_value;
				else
					// value = (struct msg_value *)(getValuePtr(sc) - sizeof(uint32_t));
					value = (struct msg_value *)((char *)kv_pointer + key->size +
								     sizeof(struct msg_key));

				rc = msg_push_to_multiget_buf(key, value, buf);
				done_with_kv_pointer(sc);
				if (rc == KREON_FAILURE) {
					break;
				}
			}
		}
	} else {
		// log_info("Scanner not valid");
		buf->end_of_region = 1;
	}

	closeScanner(sc);
	free(sc);
	krm_leave_kreon(r_desc);

	//finally fix the header
	//set now the actual capacity
	buf->capacity = buf->capacity - buf->remaining;
	buf->remaining = buf->capacity;

	actual_reply_size = sizeof(msg_header) + task->reply_msg->payload_length + TU_TAIL_SIZE;
	if (actual_reply_size % MESSAGE_SEGMENT_SIZE == 0)
		padding = 0;
	else
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);

	//set tail to the proper value
	*(uint32_t *)((uint64_t)task->reply_msg + (actual_reply_size - TU_TAIL_SIZE) + padding) = TU_RDMA_REGULAR_MSG;

	// assert((actual_reply_size + padding) % MESSAGE_SEGMENT_SIZE == 0);
	// assert((actual_reply_size + padding) <= task->msg->reply_length);

	fill_reply_msg(task->reply_msg, task, sizeof(msg_multi_get_rep) + (buf->capacity - buf->remaining),
		       MULTI_GET_REPLY);
	task->kreon_operation_status = TASK_COMPLETE;
*/
}

static void execute_delete_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	log_debug("Closing delete ops since we dont use them for now (tebis-parallax) replication");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	uint32_t actual_reply_size = 0;
	uint32_t padding;
	//GESALOUS is this a bug???
	msg_delete_req *del_req = (msg_delete_req *)(task->msg + sizeof(msg_header));
	assert(task->msg->msg_type == DELETE_REQUEST);
	struct krm_region_desc *r_desc = krm_get_region(mydesc, del_req->key, del_req->key_size);

	if (r_desc == NULL) {
		log_fatal("Region not found for key %s", del_req->key);
		exit(EXIT_FAILURE);
	}
	task->r_desc = r_desc;
	task->kreon_operation_status = TASK_DELETE_KEY;
	if (!krm_enter_kreon(r_desc, task)) {
		// later...
		return;
	}

	krm_leave_kreon(r_desc);
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);

	//piggyback info for use with the client
	actual_reply_size = sizeof(msg_header) + sizeof(msg_delete_rep) + TU_TAIL_SIZE;
	padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
	//set tail to the proper value
	*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) = TU_RDMA_REGULAR_MSG;

	msg_delete_rep *del_rep = (msg_delete_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));

	fill_reply_msg(task->reply_msg, task, sizeof(msg_delete_rep), DELETE_REPLY);
	task->kreon_operation_status = TASK_COMPLETE;
	// caution delete key needs KV_FORMAT!
	if (delete_key(r_desc->db, &del_req->key_size) == SUCCESS) {
		del_rep->status = KREON_SUCCESS;
		// log_info("Deleted key %s successfully", del_req->key);
	} else {
		del_rep->status = KREON_FAILURE;
		// log_info("Deleted key %s not found!", del_req->key);
	}
*/
}

static void execute_flush_command_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	assert(task->msg->msg_type == FLUSH_COMMAND_REQ);
	log_debug("Temporary closing flush command for Parallax no replciation porting");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	// log_info("Master orders a flush, obey your master!");
	struct s2s_msg_flush_cmd_req *flush_req =
		(struct s2s_msg_flush_cmd_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = krm_get_region(mydesc, flush_req->region_key, flush_req->region_key_size);
	if (r_desc->r_state == NULL) {
		log_fatal("No state for backup region %s", r_desc->region->id);
		_exit(EXIT_FAILURE);
	}

	struct segment_header *seg = (struct segment_header *)r_desc->r_state->seg[flush_req->log_buffer_id].mr->addr;
	seg->segment_id = flush_req->segment_id;
	if (flush_req->log_padding)
		memset((void *)((uint64_t)seg + (SEGMENT_SIZE - flush_req->log_padding)), 0x00, flush_req->log_padding);

	if (!globals_get_send_index()) {
		struct rco_build_index_task t;
		t.r_desc = r_desc;
		t.segment = (struct segment_header *)seg;
		t.log_start = 0; // r_desc->db->db_desc->KV_log_size - (SEGMENT_SIZE -
		// sizeof(struct segment_header));
		t.log_end = SEGMENT_SIZE; // r_desc->db->db_desc->KV_log_size;
		rco_build_index(&t);
	} else {
		pthread_mutex_lock(&r_desc->db->db_desc->lock_log);
		//Now take a segment from the allocator and copy the buffer
		volatile segment_header *last_log_segment = r_desc->db->db_desc->KV_log_last_segment;
		// log_info("Last log segment id %llu id to flush %llu",
		// last_log_segment->segment_id,
		//	 flush_req->segment_id);

		uint64_t diff = flush_req->end_of_log - r_desc->db->db_desc->KV_log_size;
		int exists = 0;
		if (last_log_segment->segment_id == flush_req->segment_id) {
			// log_info("Forced value log flush due to compaction id %llu",
			// last_log_segment->segment_id);
			if (r_desc->r_state->next_segment_id_to_flush == 0)
				++r_desc->r_state->next_segment_id_to_flush;

			// This op should follow after a partial write
			if (flush_req->end_of_log - r_desc->db->db_desc->KV_log_size >= SEGMENT_SIZE) {
				log_fatal("Corruption");
				_exit(EXIT_FAILURE);
			}
			exists = 1;
		} else if (r_desc->r_state->next_segment_id_to_flush != flush_req->segment_id) {
			log_fatal("Corruption non-contiguous segment ids: expected %llu  got "
				  "flush_req id is %llu",
				  r_desc->r_state->next_segment_id_to_flush, flush_req->segment_id);
			log_fatal("last segment in database is %llu", last_log_segment->segment_id);
			assert(0);
			_exit(EXIT_FAILURE);
		}

		//log_info("Flushing segment %llu padding is %llu primary offset %llu local diff is %llu ",
		//	 flush_req->segment_id, flush_req->log_padding, flush_req->end_of_log, diff);

		if (!exists) {
			++r_desc->r_state->next_segment_id_to_flush;
			segment_header *disk_segment = seg_get_raw_log_segment(r_desc->db->volume_desc);
			seg->next_segment = NULL;
			seg->prev_segment = (segment_header *)((uint64_t)last_log_segment - MAPPED);
			if (!write_segment_with_explicit_IO((char *)(uint64_t)seg, SEGMENT_SIZE,
							    (uint64_t)disk_segment - MAPPED, FD)) {
				log_fatal("Failed to write segment with explicit I/O");
				_exit(EXIT_FAILURE);
			}
			if (r_desc->db->db_desc->KV_log_first_segment == NULL)
				r_desc->db->db_desc->KV_log_first_segment = disk_segment;
			r_desc->db->db_desc->KV_log_last_segment = disk_segment;
			// add mapping to level's hash table
			struct krm_segment_entry *e =
				(struct krm_segment_entry *)malloc(sizeof(struct krm_segment_entry));
			assert(flush_req->master_segment % SEGMENT_SIZE == 0);
			e->master_seg = flush_req->master_segment;
			e->my_seg = (uint64_t)disk_segment - MAPPED;
			pthread_rwlock_wrlock(&r_desc->replica_log_map_lock);
			HASH_ADD_PTR(r_desc->replica_log_map, master_seg, e);
			pthread_rwlock_unlock(&r_desc->replica_log_map_lock);
			// log_info("Added mapping for index for log %llu replica %llu",
			// e->master_seg, e->my_seg);
		} else {
			// check if we have the mapping
			struct krm_segment_entry *index_entry;
			pthread_rwlock_rdlock(&r_desc->replica_log_map_lock);
			HASH_FIND_PTR(r_desc->replica_log_map, &flush_req->master_segment, index_entry);
			pthread_rwlock_unlock(&r_desc->replica_log_map_lock);
			if (index_entry == NULL) {
				// add mapping to level's hash table
				struct krm_segment_entry *e =
					(struct krm_segment_entry *)malloc(sizeof(struct krm_segment_entry));
				e->master_seg = flush_req->master_segment;
				e->my_seg = (uint64_t)r_desc->db->db_desc->KV_log_last_segment - MAPPED;
				pthread_rwlock_wrlock(&r_desc->replica_log_map_lock);
				HASH_ADD_PTR(r_desc->replica_log_map, master_seg, e);
				pthread_rwlock_unlock(&r_desc->replica_log_map_lock);
				// log_info("Added mapping for index for log %llu replica %llu",
				// e->master_seg, e->my_seg);
			}
			if (!write_segment_with_explicit_IO(
				    (char *)(uint64_t)seg + sizeof(struct segment_header),
				    SEGMENT_SIZE - sizeof(struct segment_header),
				    ((uint64_t)last_log_segment - MAPPED) + sizeof(struct segment_header), FD)) {
				log_fatal("Failed to write segment with explicit I/O");
				_exit(EXIT_FAILURE);
			}
		}

		r_desc->db->db_desc->KV_log_size += diff;
		pthread_mutex_unlock(&r_desc->db->db_desc->lock_log);
	}
	//time for reply :-)
	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);

	struct s2s_msg_flush_cmd_rep *flush_rep =
		(struct s2s_msg_flush_cmd_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	flush_rep->status = KREON_SUCCESS;

	fill_reply_msg(task->reply_msg, task, sizeof(struct s2s_msg_flush_cmd_rep), FLUSH_COMMAND_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
	// log_info("Responded to server!");
*/
}

static void execute_get_log_buffer_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	assert(task->msg->msg_type == GET_LOG_BUFFER_REQ);
	log_debug("Closing get_log_buffer req for parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	void *addr;
	struct s2s_msg_get_log_buffer_req *get_log =
		(struct s2s_msg_get_log_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = krm_get_region(mydesc, get_log->region_key, get_log->region_key_size);
	if (r_desc == NULL) {
		log_fatal("No region found for min key %s", get_log->region_key);
		_exit(EXIT_FAILURE);
	}

	log_debug("Region-master wants %d log buffer(s) for region %s", get_log->num_buffers, r_desc->region->id);

	pthread_mutex_lock(&r_desc->region_mgmnt_lock);
	if (r_desc->r_state == NULL) {
		r_desc->r_state = (struct ru_replica_state *)calloc(1, sizeof(struct ru_replica_state));
		//(get_log->num_buffers * sizeof(struct ru_replica_log_buffer_seg)));
		r_desc->r_state->num_buffers = get_log->num_buffers;
		for (int i = 0; i < get_log->num_buffers; i++) {
			if (posix_memalign(&addr, ALIGNMENT, get_log->buffer_size)) {
				log_fatal("Failed to allocate aligned RDMA buffer");
				perror("Reason\n");
				_exit(EXIT_FAILURE);
			}
			r_desc->r_state->seg[i].segment_size = get_log->buffer_size;
			r_desc->r_state->seg[i].mr = rdma_reg_write(task->conn->rdma_cm_id, addr, get_log->buffer_size);
		}
		// what is the next segment id that we should expect (for correctness reasons)
		if (r_desc->db->db_desc->KV_log_size > 0 && r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE == 0)
			r_desc->r_state->next_segment_id_to_flush =
				r_desc->db->db_desc->KV_log_last_segment->segment_id + 1;
		else
			r_desc->r_state->next_segment_id_to_flush =
				r_desc->db->db_desc->KV_log_last_segment->segment_id;
	} else {
		log_fatal("remote buffers already initialized, what?");
		_exit(EXIT_FAILURE);
	}

	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);

	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_get_log_buffer_rep *rep =
		(struct s2s_msg_get_log_buffer_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	rep->status = KREON_SUCCESS;
	rep->num_buffers = get_log->num_buffers;
	for (int i = 0; i < rep->num_buffers; i++) {
		rep->mr = *r_desc->r_state->seg[i].mr;
	}

	//piggyback info for use with the client
	fill_reply_msg(task->reply_msg, task,
		       sizeof(struct s2s_msg_get_log_buffer_rep) + (get_log->num_buffers * sizeof(struct ibv_mr)),
		       GET_LOG_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	log_debug("Region master wants a log buffer...DONE");
	task->kreon_operation_status = TASK_COMPLETE;
*/
}

static void execute_replica_index_get_buffer_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	assert(task->msg->msg_type == REPLICA_INDEX_GET_BUFFER_REQ);
	log_debug("Closing index_get_buffer req for parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	struct s2s_msg_replica_index_get_buffer_req *g_req =
		(struct s2s_msg_replica_index_get_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = krm_get_region(mydesc, g_req->region_key, g_req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", g_req->region_key);
		exit(EXIT_FAILURE);
	}
	// log_info("REPLICA: Master wants %d rdma buffers to start index tranfer of
	// "
	//	 "level %d of region %s",
	// g_req->num_buffers, g_req->level_id, r_desc->region->id);

	log_debug("DB %s Allocating %u and registering with RDMA buffers for remote "
		  "compaction",
		  r_desc->region->id, g_req->num_buffers);
	for (int i = 0; i < g_req->num_buffers; i++) {
		char *addr = NULL;
		if (posix_memalign((void **)&addr, ALIGNMENT, SEGMENT_SIZE) != 0) {
			log_fatal("Posix memalign failed");
			perror("Reason: ");
			_exit(EXIT_FAILURE);
		}
		if (r_desc->r_state->index_buffers[g_req->level_id][i] == NULL) {
			r_desc->r_state->index_buffers[g_req->level_id][i] =
				rdma_reg_write(task->conn->rdma_cm_id, addr, SEGMENT_SIZE);
		} else {
			log_fatal("Remote compaction for regions %s level %d still pending", r_desc->region->id,
				  g_req->level_id);
			assert(0);
			_exit(EXIT_FAILURE);
		}
	}

	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	struct s2s_msg_replica_index_get_buffer_rep *g_rep =
		(struct s2s_msg_replica_index_get_buffer_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	g_rep->status = KREON_SUCCESS;
	g_rep->num_buffers = g_req->num_buffers;
	for (int i = 0; i < g_rep->num_buffers; i++)
		g_rep->mr = *r_desc->r_state->index_buffers[g_req->level_id][i];

	// log_info("REPLICA: DONE registering %d buffer for index transfer for
	// region %s", g_rep->num_buffers,
	// r_desc->region->id);
	fill_reply_msg(task->reply_msg, task, sizeof(struct s2s_msg_replica_index_get_buffer_rep),
		       REPLICA_INDEX_GET_BUFFER_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
*/
}

static void execute_replica_index_flush_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	(void)mydesc;
	(void)task;
	assert(task->msg->msg_type == REPLICA_INDEX_FLUSH_REQ);
	log_debug("Closing index flush req for parallax no replication porting");
	assert(0);
	_exit(EXIT_FAILURE);
	/*
	struct s2s_msg_replica_index_flush_req *f_req =
		(struct s2s_msg_replica_index_flush_req *)((uint64_t)task->msg + sizeof(struct msg_header));

	struct krm_region_desc *r_desc = krm_get_region(mydesc, f_req->region_key, f_req->region_key_size);
	if (r_desc == NULL) {
		log_fatal("no hosted region found for min key %s", f_req->region_key);
		_exit(EXIT_FAILURE);
	}
	if (f_req->seg_id > 0)
		if (f_req->seg_id !=
		    r_desc->db->db_desc->levels[f_req->level_id].last_segment[f_req->tree_id]->segment_id + 1) {
			log_fatal(
				"freq segid = %d should have been %u", f_req->seg_id,
				r_desc->db->db_desc->levels[f_req->level_id].last_segment[f_req->tree_id]->segment_id +
					1);
			assert(0);
		}
	uint64_t s = djb2_hash((unsigned char *)r_desc->r_state->index_buffers[f_req->level_id][0]->addr, SEGMENT_SIZE);
	assert(s == f_req->seg_hash);

	// log_info("Primary hash %llu mine %llu", s, f_req->seg_hash);
	di_rewrite_index_with_explicit_IO(r_desc->r_state->index_buffers[f_req->level_id][0]->addr, r_desc,
					  f_req->primary_segment_offt, f_req->level_id);
	if (f_req->is_last) {
		//translate root
		if (!f_req->root_w && !f_req->root_r) {
			log_fatal("Both roots can't be NULL");
			_exit(EXIT_FAILURE);
		}
		struct krm_segment_entry *index_entry;
		uint64_t primary_segment_offt;
		uint64_t primary_segment;
		if (f_req->root_w) {
			primary_segment_offt = f_req->root_w % SEGMENT_SIZE;
			primary_segment = f_req->root_w - primary_segment_offt;
			HASH_FIND_PTR(r_desc->replica_index_map[f_req->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_fatal("Cannot translate root_w for primary's segment %llu of db %s",
					  primary_segment_offt, r_desc->db->db_desc->db_name);
				_exit(EXIT_FAILURE);
			}
			r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id] =
				(struct node_header *)(MAPPED + index_entry->my_seg + primary_segment_offt);
			assert(r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id]->type ==
				       leafRootNode ||
			       r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id]->type == rootNode);
			// log_info("Ok translated root_w[%u][%u]", f_req->level_id,
			// f_req->tree_id);
		} else {
			r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id] = NULL;
			// log_info("Ok primary says root_w[%u][%u] is NULL, ok",
			// f_req->level_id, f_req->tree_id);
		}
		if (f_req->root_r) {
			primary_segment_offt = f_req->root_r % SEGMENT_SIZE;
			primary_segment = f_req->root_r - primary_segment_offt;
			HASH_FIND_PTR(r_desc->replica_index_map[f_req->level_id], &primary_segment, index_entry);
			if (index_entry == NULL) {
				log_fatal("Cannot translate root_r for primary's segment %llu of db %s",
					  primary_segment_offt, r_desc->db->db_desc->db_name);
				//raise(SIGINT);
				_exit(EXIT_FAILURE);
			}
			r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id] =
				(struct node_header *)(MAPPED + index_entry->my_seg + primary_segment_offt);
			// assert(r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id]->type
			// ==
			//	       leafRootNode ||
			//       r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id]->type
			// ==
			//	       rootNode);
			// log_info("Ok translated root_r[%u][%u]", f_req->level_id,
			// f_req->tree_id);
		} else {
			r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id] = NULL;
			// log_info("Ok primary says root_r[%u][%u] is NULL, ok",
			// f_req->level_id, f_req->tree_id);
		}
		// deregistering buffers
		free(r_desc->r_state->index_buffers[f_req->level_id][0]->addr);
		if (rdma_dereg_mr(r_desc->r_state->index_buffers[f_req->level_id][0])) {
			log_fatal("Failed to deregister rdma buffer");
			_exit(EXIT_FAILURE);
		}
		r_desc->r_state->index_buffers[f_req->level_id][0] = NULL;
		if (f_req->level_id > 1) {
			seg_free_level(r_desc->db, f_req->level_id - 1, 0);
		}
		seg_free_level(r_desc->db, f_req->level_id, 0);
		// log_info("REPLICA: Setting new level as new");
		struct level_descriptor *l = &r_desc->db->db_desc->levels[f_req->level_id];
		l->first_segment[0] = l->first_segment[1];
		l->first_segment[1] = NULL;

		l->last_segment[0] = l->last_segment[1];
		l->last_segment[1] = NULL;

		l->offset[0] = l->offset[1];
		l->offset[1] = 0;

		l->level_size[0] = l->level_size[1];
		l->level_size[1] = 0;

		while (!__sync_bool_compare_and_swap(&l->root_w[0], l->root_w[0], l->root_w[1])) {
		}
		// dst->root_w[dst_active_tree] = src->root_w[src_active_tree];
		l->root_w[1] = NULL;

		while (!__sync_bool_compare_and_swap(&l->root_r[0], l->root_r[0], l->root_r[1])) {
		}
		l->root_r[1] = NULL;
		log_debug("Destroying mappings for index level %d useless now", f_req->level_id);

		//iterate over regions
		struct krm_segment_entry *current, *tmp;

		HASH_ITER(hh, r_desc->replica_index_map[f_req->level_id], current, tmp)
		{
			HASH_DEL(r_desc->replica_index_map[f_req->level_id], current);
			free(current);
		}
	}

	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);

	struct s2s_msg_replica_index_flush_rep *rep =
		(struct s2s_msg_replica_index_flush_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
	rep->status = KREON_SUCCESS;
	//GESALOUS check this
	rep->seg_id = f_req->seg_id;

	fill_reply_msg(task->reply_msg, task, sizeof(struct s2s_msg_replica_index_flush_rep), REPLICA_INDEX_FLUSH_REP);
	set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	task->kreon_operation_status = TASK_COMPLETE;
	// log_info("REPLICA: Successfully flushed index segment id %d",
	// f_req->seg_id);
*/
}

static void execute_test_req(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
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
	fill_reply_msg(task->reply_msg, task, task->msg->payload_length, TEST_REPLY);
	task->kreon_operation_status = TASK_COMPLETE;
}

void execute_test_req_fetch_payload(struct krm_server_desc const *mydesc, struct krm_work_task *task)
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
void execute_no_op(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	assert(mydesc && task);
	assert(task->msg->msg_type == NO_OP);

	//log_info("A NO OP HAS COME");
	task->kreon_operation_status = TASK_NO_OP;
	task->reply_msg = (struct msg_header *)&task->conn->rdma_memory_regions
				  ->local_memory_buffer[task->msg->offset_reply_in_recv_buffer];

	fill_reply_msg(task->reply_msg, task, 0, NO_OP_ACK);
	if (task->reply_msg->payload_length != 0)
		set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);

	task->kreon_operation_status = TASK_COMPLETE;
}

typedef void execute_task(struct krm_server_desc const *mydesc, struct krm_work_task *task);

execute_task *const task_dispatcher[NUMBER_OF_TASKS] = { execute_replica_index_get_buffer_req,
							 execute_replica_index_flush_req,
							 execute_get_log_buffer_req,
							 execute_flush_command_req,
							 execute_put_req,
							 execute_delete_req,
							 execute_get_req,
							 execute_multi_get_req,
							 execute_test_req,
							 execute_test_req_fetch_payload,
							 execute_no_op };

/*
   * KreonR main processing function of networkrequests.
   * Each network processing request must be resumable. For each message type
   * KreonR process it via
   * a specific data path. We treat all taks related to network  as paths that
   * may
   * fail, that we can resume later. The idea
   * behind this
   * */
uint64_t requests_handled = 0;
static void handle_task(struct krm_server_desc const *mydesc, struct krm_work_task *task)
{
	enum message_type type;
	if (task->msg->msg_type == PUT_IF_EXISTS_REQUEST)
		type = PUT_REQUEST; /*will handle the IF_EXIST internally*/
	else
		type = task->msg->msg_type;

	task_dispatcher[type](mydesc, task);

	if (task->kreon_operation_status == TASK_COMPLETE) {
		stats_update(task->server_id, task->thread_id);
		if (task->msg->msg_type == PUT_REQUEST)
			__sync_fetch_and_add(&requests_handled, 1);
	}
}

sem_t exit_main;
static void sigint_handler(int signo)
{
	(void)signo;
	/*pid_t tid = syscall(__NR_gettid);*/
	log_warn("caught signal closing server, sorry gracefull shutdown not yet "
		 "supported. Contace <gesalous,mvard>@ics.forth.gr");
	stats_notify_stop_reporter_thread();
	sem_post(&exit_main);
}

#define MAX_CORES_PER_NUMA 64
int main(int argc, char *argv[])
{
	char *device_name;
	int num_of_numa_servers = 1;
	int next_argv;
	if (argc != 8) {
		log_fatal(
			"Error args are %d! usage: ./kreon_server <device name>"
			" <zk_host:zk_port>  <RDMA subnet> <L0 size in keys> <growth factor> <send_index or build_index> <server(s) vector>\n "
			" where server(s) vector is \"<RDMA_PORT>,<Spinning thread core "
			"id>,<worker id 1>,<worker id 2>,...,<worker id N>\"",
			argc);
		_exit(EXIT_FAILURE);
	}
	// argv[0] program name don't care
	// dev name
	next_argv = 1;
	device_name = argv[next_argv];
	globals_set_dev(device_name);
	++next_argv;
	// zookeeper
	globals_set_zk_host(argv[next_argv]);
	++next_argv;
	// RDMA subnet
	globals_set_RDMA_IP_filter(argv[next_argv]);
	++next_argv;
	//L0 size
	uint32_t L0_size = strtoul(argv[next_argv], NULL, 10);
	globals_set_l0_size(L0_size);
	++next_argv;
	//growth factor
	uint32_t growth_factor = strtoul(argv[next_argv], NULL, 10);
	globals_set_growth_factor(growth_factor);
	++next_argv;

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
	//root_server = (struct ds_root_server *)calloc(
	//	1, sizeof(struct ds_root_server) + (num_of_numa_servers * sizeof(struct ds_root_server *)));
	root_server->num_of_numa_servers = num_of_numa_servers;
	int server_idx = 0;
	// now servers <RDMA port, spinning thread, workers>
	int rdma_port;
	int spinning_thread_id;
	int workers_id[MAX_CORES_PER_NUMA];
	int num_workers;
	char *token;
	char *strtok_saveptr = NULL;
	char *rest = argv[next_argv];
	// RDMA port of server
	token = strtok_r(rest, ",", &strtok_saveptr);
	if (token == NULL) {
		log_fatal("RDMA port missing in server configuration");
		exit(EXIT_FAILURE);
	}
	char *ptr;
	rdma_port = strtol(token, &ptr, 10);
	log_info("Server %d rdma port: %d", server_idx, rdma_port);
	// Spinning thread of server
	token = strtok_r(NULL, ",", &strtok_saveptr);
	if (token == NULL) {
		log_fatal("Spinning thread id missing in server configuration");
		exit(EXIT_FAILURE);
	}
	spinning_thread_id = strtol(token, &ptr, 10);
	log_info("Server %d spinning_thread id: %d", server_idx, spinning_thread_id);

	// now the worker ids
	num_workers = 0;
	token = strtok_r(NULL, ",", &strtok_saveptr);
	while (token != NULL) {
		workers_id[num_workers++] = strtol(token, &ptr, 10);
		token = strtok_r(NULL, ",", &strtok_saveptr);
	}
	if (num_workers == 0) {
		log_fatal("No workers specified for Server %d", server_idx);
		exit(EXIT_FAILURE);
	}
	// Double the workers, mirror workers are for server tasks
	num_workers *= 2;

	// now we have all info to allocate ds_numa_server, pin,
	// and inform the root server

	//struct ds_numa_server *server = (struct ds_numa_server *)calloc(
	//	1, sizeof(struct ds_numa_server) + (num_workers * sizeof(struct ds_worker_thread)));
	struct ds_numa_server *server = (struct ds_numa_server *)calloc(1, sizeof(struct ds_numa_server));

	/*But first let's build each numa server's RDMA channel*/
	/*RDMA channel staff*/
	server->channel = (struct channel_rdma *)malloc(sizeof(struct channel_rdma));
	if (server->channel == NULL) {
		log_fatal("malloc failed could do not get memory for channel");
		exit(EXIT_FAILURE);
	}
	server->channel->sockfd = 0;
	server->channel->context = get_rdma_device_context(NULL);

	server->channel->comp_channel = ibv_create_comp_channel(server->channel->context);
	if (server->channel->comp_channel == 0) {
		log_fatal("building context reason follows:");
		perror("Reason: \n");
		exit(EXIT_FAILURE);
	}

	server->channel->pd = ibv_alloc_pd(server->channel->context);
	server->channel->nconn = 0;
	server->channel->nused = 0;
	server->channel->connection_created = NULL;

	/*Creating the thread in charge of the completion channel*/
	if (pthread_create(&server->poll_cq_cnxt, NULL, poll_cq, server->channel) != 0) {
		log_fatal("Failed to create poll_cq thread reason follows:");
		perror("Reason: \n");
		exit(EXIT_FAILURE);
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
	server->meta_server.root_server_id = server_idx;
	server->rdma_port = rdma_port;
	server->meta_server.ld_regions = NULL;
	server->meta_server.dataservers_map = NULL;

	server->meta_server.RDMA_port = rdma_port;

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
	// unused
	server->spinner.idle_conn_list = init_simple_concurrent_list();

	server->spinner.next_worker_to_submit_job = 0;

	if (pthread_create(&server->socket_thread_cnxt, NULL, socket_thread, (void *)server) != 0) {
		log_fatal("failed to spawn socket thread  for Server: %d reason follows:", server->server_id);
		perror("Reason: \n");
		exit(EXIT_FAILURE);
	}
	// Now pin it in the numa node! Important step so allocations used by
	// spinner and workers to be
	// in the same numa node

	cpu_set_t numa_node_affinity;
	CPU_ZERO(&numa_node_affinity);
	CPU_SET(server->spinner.spinner_id, &numa_node_affinity);
	for (int k = 0; k < server->spinner.num_workers; k++)
		CPU_SET(server->spinner.worker[k].worker_id, &numa_node_affinity);
	int status = pthread_setaffinity_np(server->socket_thread_cnxt, sizeof(cpu_set_t), &numa_node_affinity);
	if (status != 0) {
		log_fatal("failed to pin socket thread for server %d at port %d", server->server_id, server->rdma_port);
		exit(EXIT_FAILURE);
	}

	log_info("Started socket thread successfully for Server %d", server->server_id);

	log_info("Starting spinning thread for Server %d", server->server_id);
	if (pthread_create(&server->spinner_cnxt, NULL, server_spinning_thread_kernel, &server->spinner) != 0) {
		log_fatal("Failed to create spinner thread for Server: %d", server->server_id);
		exit(EXIT_FAILURE);
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
		exit(EXIT_FAILURE);
	}
	log_info("Pinned successfully spinning thread of Server: %d at port %d at "
		 "core %d",
		 server->server_id, server->rdma_port, server->spinner.spinner_id);

	server->meta_server.root_server_id = server_idx;
	log_info("Initializing kreonR metadata server");
	if (pthread_create(&server->meta_server_cnxt, NULL, krm_metadata_server, &server->meta_server)) {
		log_fatal("Failed to start metadata_server");
		exit(EXIT_FAILURE);
	}

	stats_init(root_server->num_of_numa_servers, server->spinner.num_workers);
	// A long long
	sem_init(&exit_main, 0, 0);

	log_debug("Kreon server(S) ready");
	if (signal(SIGINT, sigint_handler) == SIG_ERR) {
		log_fatal("can't catch SIGINT");
		exit(EXIT_FAILURE);
	}
	sem_wait(&exit_main);
	log_info("kreonR server exiting");
	return 0;
}
