/**
 *  kreon_server.c
 * Created 28/07/16.
 * Authors
 * Giorgos Saloustros <gesalous@ics.forth.gr>
 * Michalis Vardoulakis <mvard@ics.forth.gr>
 *
 **/
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <limits.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <alloca.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

//#include "regions.h"
//#include "prototype.h"
//#include "storage_devices.h"
#include "messages.h"
#include "globals.h"
#include "metadata.h"
#include "../kreon_lib/btree/btree.h"
#include "../kreon_lib/btree/segment_allocator.h"
#include "../kreon_lib/scanner/scanner.h"
#include "../kreon_rdma/rdma.h"
#include "../kreon_lib/scanner/scanner.h"
#include "../kreon_lib/btree/conf.h"
#include "../utilities/queue.h"
#include "djb2.h"
#include <log.h>
#include "stats.h"

#ifdef CHECKSUM_DATA_MESSAGES
#include "djb2.h"
#endif

#define LOG_SEGMENT_CHUNK 32 * 1024
#define MY_MAX_THREADS 2048

#define WORKER_THREAD_PRIORITIES_NUM 4
#define WORKER_THREAD_HIGH_PRIORITY_TASKS_PER_TURN 1
#define WORKER_THREAD_NORMAL_PRIORITY_TASKS_PER_TURN 1

/*block the socket thread if there's no
                                  * available memory to allocate to an
                                  * incoming connection*/
sem_t memory_steal_sem;
volatile memory_region *backup_region = NULL;

extern char *DB_NO_SPILLING;

typedef struct prefix_table {
	char prefix[PREFIX_SIZE];
} prefix_table;

#define DS_CLIENT_QUEUE_SIZE (UTILS_QUEUE_CAPACITY / 2)
#define DS_POOL_NUM 4

struct ds_task_buffer_pool {
	pthread_mutex_t tbp_lock;
	utils_queue_s task_buffers;
};

struct ds_worker_thread {
	utils_queue_s work_queue;
	sem_t sem;
	struct channel_rdma *channel;
	pthread_t context;
	/*for affinity purposes*/
	/*my parent*/
	int worker_id;
	int root_server_id;
	volatile int idle_time;
	volatile worker_status status;
};

struct ds_spinning_thread {
	struct ds_task_buffer_pool ctb_pool[DS_POOL_NUM];
	struct ds_task_buffer_pool stb_pool[DS_POOL_NUM];
	struct ds_task_buffer_pool resume_task_pool[DS_POOL_NUM];
	pthread_mutex_t conn_list_lock;
	SIMPLE_CONCURRENT_LIST *conn_list;
	SIMPLE_CONCURRENT_LIST *idle_conn_list;
	int num_workers;
	int next_worker_to_submit_job;
	int c_last_pool;
	int s_last_pool;
	/*for affinity purposes*/
	int spinner_id;
	/*entry in the root table of my dad (numa_server)*/
	int root_server_id;
	// my workers follow
	struct ds_worker_thread worker[];
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
	struct ds_numa_server *numa_servers[];
};

/*root of everything*/
static struct ds_root_server *root_server = NULL;

static void handle_task(struct krm_server_desc *my_server, struct krm_work_task *task);
static void ds_put_server_task_buffer(struct ds_spinning_thread *spinner, struct krm_work_task *task);
static void ds_put_client_task_buffer(struct ds_spinning_thread *spinner, struct krm_work_task *task);
static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct krm_work_task *task);
/*inserts to Kreon and implements the replication logic*/
void insert_kv_pair(struct krm_server_desc *my_server, struct krm_work_task *task);

static void crdma_server_create_connection_inuse(struct connection_rdma *conn, struct channel_rdma *channel,
						 connection_type type)
{
	/*gesalous, This is the path where it creates the useless memory queues*/
	tu_rdma_init_connection(conn);
	conn->type = type;
	conn->channel = channel;
	return;
}

struct channel_rdma *ds_get_channel(struct krm_server_desc *my_desc)
{
	return root_server->numa_servers[my_desc->root_server_id]->channel;
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
	sem_init(&memory_steal_sem, 0, 1); // sem_wait when using backup_region,
	// spinning_thread will sem_post
	/*backup_region = mrpool_allocate_memory_region(channel->static_pool);*/
	/*assert(backup_region);*/

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

	struct rdma_cm_id *rdma_cm_id;
	ret = rdma_create_ep(&rdma_cm_id, res, NULL, NULL);
	if (ret) {
		log_fatal("rdma_create_ep: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	// Listen for incoming connections on available RDMA devices
	ret = rdma_listen(rdma_cm_id, 0); // called with backlog = 0
	if (ret) {
		log_fatal("rdma_listen: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	while (1) {
		// Block until a new connection request arrives
		struct rdma_cm_id *request_id, *new_conn_id;
		ret = rdma_get_request(rdma_cm_id, &request_id);
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
			log_info("We have a new replica connection request");
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
#if 0
		memory_region * mr, *other_half, *halved_mr;
		connection_rdma *candidate = NULL;
		if (conn->type == SERVER_TO_CLIENT_CONNECTION && !conn->rdma_memory_regions) { /*{{{*/
			// Run out of memory, need to steal from a connection
			// FIXME Need to figure out how destroying these half memory regions will work since
			// calling free on the second half buffer will fail. I'm not sure ibv_dereg_mr will
			// work either
			DPRINT("Run out of memory regions!\n");
			candidate = find_memory_steal_candidate(channel->spin_list[0]);
			assert(candidate);

			sem_wait(&memory_steal_sem);
			// changed from NULL to something != NULL
			assert(backup_region);
			mr = (struct memory_region *)backup_region;
			backup_region = NULL;

			halved_mr = (memory_region *)malloc(sizeof(memory_region));
			halved_mr->mrpool = mr->mrpool;
			halved_mr->memory_region_length = mr->memory_region_length / 2;

			halved_mr->local_memory_region = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
			memcpy(halved_mr->local_memory_region, mr->local_memory_region, sizeof(memory_region));
			halved_mr->local_memory_region->length = halved_mr->memory_region_length;
			halved_mr->local_memory_buffer = mr->local_memory_buffer;
			assert(halved_mr->local_memory_buffer == halved_mr->local_memory_region->addr);

			halved_mr->remote_memory_region = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
			memcpy(halved_mr->remote_memory_region, mr->remote_memory_region, sizeof(memory_region));
			halved_mr->remote_memory_region->length = halved_mr->memory_region_length;
			halved_mr->remote_memory_buffer = mr->remote_memory_buffer;
			assert(halved_mr->remote_memory_buffer == halved_mr->remote_memory_region->addr);

			candidate->next_rdma_memory_regions = halved_mr;

			other_half = (memory_region *)malloc(sizeof(memory_region));
			other_half->mrpool = mr->mrpool;
			other_half->memory_region_length = mr->memory_region_length / 2;

			other_half->local_memory_region = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
			memcpy(other_half->local_memory_region, mr->local_memory_region, sizeof(memory_region));
			other_half->local_memory_region->addr += other_half->memory_region_length;
			other_half->local_memory_region->length = other_half->memory_region_length;
			other_half->local_memory_buffer = mr->local_memory_buffer + other_half->memory_region_length;
			assert(other_half->local_memory_buffer == other_half->local_memory_region->addr);

			other_half->remote_memory_region = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
			memcpy(other_half->remote_memory_region, mr->remote_memory_region, sizeof(memory_region));
			other_half->remote_memory_region->addr += other_half->memory_region_length;
			other_half->remote_memory_region->length = other_half->memory_region_length;
			other_half->remote_memory_buffer = mr->remote_memory_buffer + other_half->memory_region_length;
			assert(other_half->remote_memory_buffer == other_half->remote_memory_region->addr);
			DPRINT("SERVER: length = %llu\n", (LLU)other_half->memory_region_length);

			conn->rdma_memory_regions = other_half;

			// TODO replace assign_job_to_worker with __stop_client. Add new mr info in the
			// stop client message's payload
			// assign_job_to_worker(candidate->channel, candidate, (msg_header*)577, 0, -1);
			__stop_client(candidate);
			candidate = NULL;
		} /*}}}*/
#endif
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

		// conn->pending_sent_messages = 0;
		// conn->pending_received_messages = 0;
		conn->offset = 0;
		conn->worker_id = -1;
#ifdef CONNECTION_BUFFER_WITH_MUTEX_LOCK
		pthread_mutex_init(&conn->buffer_lock, NULL);
#else
		pthread_spin_init(&conn->buffer_lock, PTHREAD_PROCESS_PRIVATE);
#endif
		/*add connection to its spinner*/
		// conn->idconn = -1; // what?
		// if (next_spinner_to_submit_conn >= my_server->spinner)
		//	next_spinner_to_submit_conn = 0;
		// struct ds_spinning_thread *spinner =
		// &my_server->spinner[next_spinner_to_submit_conn];
		pthread_mutex_lock(&my_server->spinner.conn_list_lock);
		/*gesalous new policy*/
		add_last_in_simple_concurrent_list(my_server->spinner.conn_list, conn);
		conn->responsible_spin_list = my_server->spinner.conn_list;
		conn->responsible_spinning_thread_id = my_server->spinner.spinner_id;

		pthread_mutex_unlock(&my_server->spinner.conn_list_lock);
		log_info("Built new connection successfully for Server %d at port %d", my_server->server_id, rdma_port);
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

void *worker_thread_kernel(void *args)
{
	struct krm_work_task *job = NULL;
	struct ds_worker_thread *worker;
	const int spin_time_usec = globals_get_worker_spin_time_usec();

	pthread_setname_np(pthread_self(), "ds_worker");
	worker = (struct ds_worker_thread *)args;

	while (1) {
		// Get the next task from one of the task queues
		// If there are tasks pending, rotate between all queues
		// Try to get a task
		if (!(job = utils_queue_pop(&worker->work_queue))) {
			// Try for a few more usecs
			struct timespec start, end;
			int local_idle_time;
			clock_gettime(CLOCK_MONOTONIC, &start);
			while (1) {
				// I could have a for loop with a few iterations to avoid constantly
				// calling clock_gettime
				if ((job = utils_queue_pop(&worker->work_queue)))
					break;
				clock_gettime(CLOCK_MONOTONIC, &end);
				local_idle_time = diff_timespec_usec(&start, &end);
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
		if (!job->suspended) {
			switch (job->kreon_operation_status) {
			case TASK_COMPLETE:
				_zero_rendezvous_locations(job->msg);
				__send_rdma_message(job->conn, job->reply_msg, NULL);
				switch (job->pool_type) {
				case KRM_CLIENT_POOL:
					ds_put_client_task_buffer(
						&root_server->numa_servers[worker->root_server_id]->spinner, job);
					break;
				case KRM_SERVER_POOL:
					ds_put_server_task_buffer(
						&root_server->numa_servers[worker->root_server_id]->spinner, job);
					break;
				}
				break;

			default:
				/*send it to spinning thread*/
				// log_info("Putting task %p away to be resumed pool id %d pool type %d
				// spinner id %d",
				//	 job, job->pool_id, job->pool_type, worker->spinner_id);
				ds_put_resume_task(&root_server->numa_servers[worker->root_server_id]->spinner, job);
			}
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

static struct krm_work_task *ds_get_server_task_buffer(struct ds_spinning_thread *spinner)
{
	struct krm_work_task *job = NULL;
	int idx = spinner->s_last_pool + 1;
	if (spinner->s_last_pool >= DS_POOL_NUM)
		idx = 0;

	int i = idx;
	while (1) {
		job = (struct krm_work_task *)utils_queue_pop(&spinner->stb_pool[i].task_buffers);
		if (job != NULL)
			break;
		++i;
		if (i >= DS_POOL_NUM)
			i = 0;
		if (i == idx || (DS_POOL_NUM == 1))
			// nothing found after a full round
			break;
	}
	// reset task struct
	if (job) {
		spinner->c_last_pool = i;
		int job_idx = job->pool_id;
		int pool_type = job->pool_type;
		memset(job, 0x00, sizeof(struct krm_work_task));

		job->pool_id = job_idx;
		job->pool_type = pool_type;
		assert(job->pool_id < DS_POOL_NUM);
	}
	assert(job != NULL);
	return job;
}

static void ds_put_server_task_buffer(struct ds_spinning_thread *spinner, struct krm_work_task *task)
{
	uint32_t pool_id = task->pool_id;
	pthread_mutex_lock(&spinner->stb_pool[pool_id].tbp_lock);
	if (utils_queue_push(&spinner->stb_pool[pool_id].task_buffers, task) == NULL) {
		log_fatal("Failed to add task buffer in pool id %d, this should not happen", pool_id);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&spinner->stb_pool[pool_id].tbp_lock);
	return;
}

static void ds_put_resume_task(struct ds_spinning_thread *spinner, struct krm_work_task *task)
{
	int pool_id = task->pool_id;
	assert(pool_id < DS_POOL_NUM);
	pthread_mutex_lock(&spinner->resume_task_pool[pool_id].tbp_lock);
	if (utils_queue_push(&spinner->resume_task_pool[pool_id].task_buffers, task) == NULL) {
		log_fatal("failed to add to resumed task queue");
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&spinner->resume_task_pool[pool_id].tbp_lock);
}

static struct krm_work_task *ds_get_client_task_buffer(struct ds_spinning_thread *spinner)
{
	struct krm_work_task *job = NULL;
	int idx = spinner->c_last_pool + 1;
	if (idx >= DS_POOL_NUM)
		idx = 0;
	int i = idx;
	while (1) {
		job = (struct krm_work_task *)utils_queue_pop(&spinner->ctb_pool[i].task_buffers);
		if (job != NULL)
			break;
		++i;
		if (i >= DS_POOL_NUM)
			i = 0;
		if (i == idx || (DS_POOL_NUM == 1))
			// nothing found after a full round
			break;
	}

	// reset task struct
	if (job) {
		spinner->c_last_pool = i;
		int job_idx = job->pool_id;
		int pool_type = job->pool_type;
		memset(job, 0x00, sizeof(struct krm_work_task));

		job->pool_id = job_idx;
		job->pool_type = pool_type;
		assert(job->pool_id < DS_POOL_NUM);
	}
	return job;
}

static void ds_put_client_task_buffer(struct ds_spinning_thread *spinner, struct krm_work_task *task)
{
	uint32_t pool_id = task->pool_id;
	pthread_mutex_lock(&spinner->ctb_pool[pool_id].tbp_lock);
	if (utils_queue_push(&spinner->ctb_pool[pool_id].task_buffers, task) == NULL) {
		log_fatal("Failed to add task buffer in pool id %d, this should not happen", pool_id);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&spinner->ctb_pool[pool_id].tbp_lock);
	return;
}

static int assign_job_to_worker(struct ds_spinning_thread *spinner, struct connection_rdma *conn, msg_header *msg,
				struct krm_work_task *task)
{
	struct krm_work_task *job = NULL;
	int fifo_ordering = 0;
	uint8_t is_task_resumed;
	if (task == NULL) {
		is_task_resumed = 0;
		switch (msg->type) {
		case FLUSH_COMMAND_REP:
		case GET_LOG_BUFFER_REP:
			job = (struct krm_work_task *)ds_get_server_task_buffer(spinner);
			if (job == NULL)
				assert(0);
			break;
		/*remote compaction related*/
		case REPLICA_INDEX_GET_BUFFER_REQ:
		case REPLICA_INDEX_GET_BUFFER_REP:
		case REPLICA_INDEX_FLUSH_REQ:
		case REPLICA_INDEX_FLUSH_REP:
		case FLUSH_COMMAND_REQ:
		case GET_LOG_BUFFER_REQ:
			/*log replication related*/
			fifo_ordering = 1;
			job = (struct krm_work_task *)ds_get_server_task_buffer(spinner);
			if (job == NULL)
				assert(0);
			break;
		default:
			job = (struct krm_work_task *)ds_get_client_task_buffer(spinner);
			break;
		}
	} else {
		job = task;
		is_task_resumed = 1;
	}
	if (!job) {
		// log_info("assign_job_to_worker failed!");
		return KREON_FAILURE;
	}

	int max_queued_jobs = globals_get_job_scheduling_max_queue_depth(); // TODO [mvard] Tune this
	int worker_id = spinner->next_worker_to_submit_job;

	/* Regular tasks scheduling policy
* Assign tasks to one worker until he is swamped, then start assigning
* to the next one. Once all workers are swamped it will essentially
* become a round robin policy since the worker_id will be incremented
* at for every task.
*/

	/* Regular tasks scheduling policy
* Assign tasks to one worker until he is swamped, then start assigning
* to the next one. Once all workers are swamped it will essentially
* become a round robin policy since the worker_id will be incremented
* at for every task.
*/
	if (fifo_ordering) {
		uint64_t hash = djb2_hash((unsigned char *)&msg->session_id, sizeof(uint64_t));
		worker_id = hash % spinner->num_workers;
		//log_warn("fifo worker id %d chosen for session id %llu", worker_id, msg->session_id);
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
			for (int i = 0; i < spinner->num_workers; ++i) {
				// Keep note of a sleeping worker in case we need to wake him up for
				// this
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

#if 0
	for (int i = 0; i < spinner->num_workers; i++) {
		load = worker_queued_jobs(&spinner->worker[i]);
		if (load < min_load) {
			min_load = load;
			min_loaded_worker = i;
		}
		if (worker_queued_jobs(&spinner->worker[i]) < max_queued_jobs) {
			worker_id = i;
		}
	}
	if (worker_id == -1) {
		worker_id = min_loaded_worker;
	}
	// assertion
	if (worker_id == -1) {
		log_fatal("Failed to queue request");
		exit(EXIT_FAILURE);
	}


	// 3. Round robin
  int worker_id;
	worker_id = spinner->next_server_worker_to_submit_job++;
	if (spinner->next_server_worker_to_submit_job == spinner->num_workers)
		spinner->next_server_worker_to_submit_job = 0;
#endif

	if (!is_task_resumed) {
		job->channel = root_server->numa_servers[spinner->root_server_id]->channel;
		job->conn = conn;
		job->msg = msg;
		job->kreon_operation_status = TASK_START;
		/*initialization of various fsm*/
		job->server_id = spinner->root_server_id;
		job->thread_id = worker_id;
		job->notification_addr = (void *)job->msg->request_message_local_addr;
	}
	assert(job->kreon_operation_status != 0);
	if (utils_queue_push(&spinner->worker[worker_id].work_queue, (void *)job) == NULL) {
		assert(0);
		// Give back the allocated job buffer
		switch (job->pool_type) {
		case KRM_SERVER_POOL:
			ds_put_server_task_buffer(spinner, job);
			break;
		case KRM_CLIENT_POOL:
			ds_put_client_task_buffer(spinner, job);
			log_info("Boom");
			break;
		default:
			log_fatal("Corrupted pool type of job");
			exit(EXIT_FAILURE);
		}
		return KREON_FAILURE;
	}

	if (spinner->worker[worker_id].status == IDLE_SLEEPING) {
		spinner->worker[worker_id].status = BUSY;
		sem_post(&spinner->worker[worker_id].sem);
	}

	return KREON_SUCCESS;
}

void _update_connection_score(int spinning_list_type, connection_rdma *conn)
{
	if (spinning_list_type == HIGH_PRIORITY)
		conn->idle_iterations = 0;
	else
		++conn->idle_iterations;
}

static void ds_resume_halted_tasks(struct ds_spinning_thread *spinner)
{
	/*check for resumed tasks to be rescheduled*/
	for (int i = 0; i < DS_POOL_NUM; i++) {
		struct krm_work_task *task;
		task = utils_queue_pop(&spinner->resume_task_pool[i].task_buffers);
		if (task != NULL) {
			assert(task->r_desc != NULL);
			// log_info("Rescheduling task");
			int rc = assign_job_to_worker(spinner, task->conn, task->msg, task);
			if (rc == KREON_FAILURE) {
				log_fatal("Failed to reschedule task");
				assert(0);
				exit(EXIT_FAILURE);
			}
		}
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

static void *server_spinning_thread_kernel(void *args)
{
	struct msg_header *hdr;

	SIMPLE_CONCURRENT_LIST_NODE *node;
	SIMPLE_CONCURRENT_LIST_NODE *prev_node;
	SIMPLE_CONCURRENT_LIST_NODE *next_node;

	struct sigaction sa;
	struct ds_spinning_thread *spinner = (struct ds_spinning_thread *)args;
	struct connection_rdma *conn;

	uint32_t message_size;
	volatile uint32_t recv;

	int spinning_thread_id = spinner->spinner_id;
	int spinning_list_type;
	int rc;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = ec_sig_handler;

	pthread_t self;
	self = pthread_self();
	pthread_setname_np(self, "spinner");
	log_info("Spinning thread %d initializing empty task buffers pool size %d, "
		 "putting %d buffers per pool",
		 spinner->spinner_id, DS_POOL_NUM, DS_CLIENT_QUEUE_SIZE / DS_POOL_NUM);
	spinner->s_last_pool = DS_POOL_NUM;
	spinner->c_last_pool = DS_POOL_NUM;
	for (int i = 0; i < DS_POOL_NUM; i++) {
		pthread_mutex_init(&spinner->ctb_pool[i].tbp_lock, NULL);
		utils_queue_init(&spinner->ctb_pool[i].task_buffers);

		pthread_mutex_init(&spinner->stb_pool[i].tbp_lock, NULL);
		utils_queue_init(&spinner->stb_pool[i].task_buffers);

		pthread_mutex_init(&spinner->resume_task_pool[i].tbp_lock, NULL);
		utils_queue_init(&spinner->resume_task_pool[i].task_buffers);

		int size = DS_CLIENT_QUEUE_SIZE / DS_POOL_NUM;
		for (int j = 0; j < size; j++) {
			/*adding buffer to the server/client pool*/
			struct krm_work_task *work_task = (struct krm_work_task *)malloc(sizeof(struct krm_work_task));
			memset(work_task, 0x00, sizeof(struct krm_work_task));
			work_task->pool_id = i;
			work_task->pool_type = KRM_CLIENT_POOL;
			utils_queue_push(&spinner->ctb_pool[i].task_buffers, (void *)work_task);
			work_task = (struct krm_work_task *)malloc(sizeof(struct krm_work_task));
			memset(work_task, 0x00, sizeof(struct krm_work_task));
			work_task->pool_id = i;
			work_task->pool_type = KRM_SERVER_POOL;
			utils_queue_push(&spinner->stb_pool[i].task_buffers, (void *)work_task);
		}
	}
	/*Init my worker threads*/
	for (int i = 0; i < spinner->num_workers; i++) {
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
		CPU_SET(spinner->worker[i].worker_id, &worker_threads_affinity_mask);

	for (int i = 0; i < spinner->num_workers; i++) {
		pthread_create(&spinner->worker[i].context, NULL, worker_thread_kernel, &spinner->worker[i]);

		/*set affinity for this group*/
		log_info("Spinning thread %d Started worker %d", spinner->spinner_id, i);
		int status = pthread_setaffinity_np(spinner->worker[i].context, sizeof(cpu_set_t),
						    &worker_threads_affinity_mask);
		if (status != 0) {
			log_fatal("failed to pin workers for spinning thread %d", spinner->spinner_id);
			exit(EXIT_FAILURE);
		}
	}

	int count = 0;

	// int max_idle_time_usec = globals_get_worker_spin_time_usec();

	while (1) {
		// gesalous, iterate the connection list of this channel for new messages
		if (count < 10) {
			node = spinner->conn_list->first;
			spinning_list_type = HIGH_PRIORITY;
		} else {
			node = spinner->idle_conn_list->first;
			spinning_list_type = LOW_PRIORITY;
			count = 0;
		}

		prev_node = NULL;
		ds_resume_halted_tasks(spinner);
		ds_check_idle_workers(spinner);

		while (node != NULL) {
			/*check for resumed tasks to be rescheduled*/
			// for (int i = 0; i < DS_POOL_NUM; i++) {
			//	struct krm_work_task *task;
			//	task =
			//utils_queue_pop(&spinner->resume_task_pool[i].task_buffers);
			//	if (task != NULL) {
			//		assert(task->r_desc != NULL);
			// log_info("Rescheduling task");
			//		rc = assign_job_to_worker(spinner, task->conn,
			//task->msg, task);
			//		if (rc == KREON_FAILURE) {
			//			log_fatal("Failed to reschedule task");
			//			assert(0);
			//			exit(EXIT_FAILURE);
			//		}
			//	}
			//}
			// for (int i = 0; i < spinner->num_workers; ++i) {
			//	if (spinner->worker[i].idle_time > max_idle_time_usec &&
			//	    utils_queue_used_slots(&spinner->worker[i].work_queue) == 0
			//&&
			//	    spinner->worker[i].status == BUSY)
			//		spinner->worker[i].status = IDLE_SLEEPING;
			//}

			conn = (connection_rdma *)node->data;

			if (conn->status != CONNECTION_OK)
				goto iterate_next_element;

			hdr = (msg_header *)conn->rendezvous;
			recv = hdr->receive;

			/*messages belonging to data path category*/
			if (recv == TU_RDMA_REGULAR_MSG) {
				_update_connection_score(spinning_list_type, conn);
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
					//__sync_fetch_and_sub(&conn->pending_received_messages, 1);
					// Caution! message not consumed leave the rendezvous points as is
					hdr->receive = recv;
					goto iterate_next_element;
				}

				/**
* Set the new rendezvous point, be careful for the case that the
* rendezvous is
* outsize of the rdma_memory_regions->remote_memory_buffer
* */
				_update_rendezvous_location(conn, message_size);
			} else if (recv == CONNECTION_PROPERTIES) {
				message_size = wait_for_payload_arrival(hdr);
				if (message_size == 0) {
					/*payload have not arrived yet check next connection*/
					goto iterate_next_element;
				}

				if (hdr->type == DISCONNECT) {
					// Warning! the guy that consumes/handles the message is
					// responsible
					// for zeroing
					// the message's segments for possible future rendezvous points.
					// This
					// is done
					// inside free_rdma_received_message function

					log_info("Disconnect operation bye bye mr Client garbage collection "
						 "follows");
					// FIXME these operations might need to be atomic with more than
					// one
					// spinning threads
					struct channel_rdma *channel = conn->channel;
					// Decrement spinning thread's connections and total connections
					--channel->spin_num[channel->spinning_conn % channel->spinning_num_th];
					--channel->spinning_conn;
					_zero_rendezvous_locations(hdr);
					_update_rendezvous_location(conn, message_size);
					close_and_free_RDMA_connection(channel, conn);
					goto iterate_next_element;
				} else if (hdr->type == CHANGE_CONNECTION_PROPERTIES_REQUEST) {
					log_warn("Remote side wants to change its connection properties\n");
					set_connection_property_req *req = (set_connection_property_req *)hdr->data;

					if (req->desired_priority_level == HIGH_PRIORITY) {
						log_warn("Remote side wants to pin its connection\n");
						/*pin this conn bitches!*/
						conn->priority = HIGH_PRIORITY;
						msg_header *reply = allocate_rdma_message(
							conn, 0, CHANGE_CONNECTION_PROPERTIES_REPLY);
						reply->request_message_local_addr = hdr->request_message_local_addr;
						send_rdma_message(conn, reply);

						if (spinning_list_type == LOW_PRIORITY) {
							log_warn("Upgrading its connection\n");
							_zero_rendezvous_locations(hdr);
							_update_rendezvous_location(conn, message_size);
							goto upgrade_connection;
						} else {
							_zero_rendezvous_locations(hdr);
							_update_rendezvous_location(conn, message_size);
						}
					}
				} else if (hdr->type == CHANGE_CONNECTION_PROPERTIES_REPLY) {
					assert(0);
					((msg_header *)hdr->request_message_local_addr)->ack_arrived = KR_REP_ARRIVED;
					/*do nothing for now*/
					_zero_rendezvous_locations(hdr);
					_update_rendezvous_location(conn, message_size);
					goto iterate_next_element;
				} else {
					log_fatal("unknown message type for connetion properties unknown "
						  "type is %d\n",
						  hdr->type);
					assert(0);
					exit(EXIT_FAILURE);
				}
			} else if (recv == RESET_RENDEZVOUS) {
				// log_info("SERVER: Clients wants a reset ... D O N E");
				_zero_rendezvous_locations(hdr);
				conn->rendezvous = conn->rdma_memory_regions->remote_memory_buffer;
				goto iterate_next_element;
			} else if (recv == I_AM_CLIENT) {
				assert(conn->type == SERVER_TO_CLIENT_CONNECTION ||
				       conn->type == REPLICA_TO_MASTER_CONNECTION);
				conn->control_location = hdr->reply;
				conn->control_location_length = hdr->reply_length;
				hdr->receive = 0;
				log_info("SERVER: We have a new client control location %llu",
					 (LLU)conn->control_location);
				_zero_rendezvous_locations(hdr);
				_update_rendezvous_location(conn, MESSAGE_SEGMENT_SIZE);
				goto iterate_next_element;
			} else if (recv == SERVER_I_AM_READY) {
				conn->status = CONNECTION_OK;
				hdr->receive = 0;
				conn->control_location = hdr->data;

				log_info("Received SERVER_I_AM_READY at %llu\n", (LLU)conn->rendezvous);
				if (!backup_region) {
					backup_region = conn->rdma_memory_regions;
					conn->rdma_memory_regions = NULL;
					sem_post(&memory_steal_sem);
				} else {
					mrpool_free_memory_region(&conn->rdma_memory_regions);
				}
				assert(backup_region);

				conn->rdma_memory_regions = conn->next_rdma_memory_regions;
				conn->next_rdma_memory_regions = NULL;
				conn->rendezvous = conn->rdma_memory_regions->remote_memory_buffer;

				msg_header *msg =
					(msg_header *)((uint64_t)conn->rdma_memory_regions->local_memory_buffer +
						       (uint64_t)conn->control_location);
				msg->pay_len = 0;
				msg->padding_and_tail = 0;
				msg->data = NULL;
				msg->next = NULL;
				msg->type = CLIENT_RECEIVED_READY;
				msg->receive = TU_RDMA_REGULAR_MSG;
				msg->local_offset = (uint64_t)conn->control_location;
				msg->remote_offset = (uint64_t)conn->control_location;
				msg->ack_arrived = KR_REP_PENDING;
				msg->request_message_local_addr = NULL;
				__send_rdma_message(conn, msg, NULL);

				// DPRINT("SERVER: Client I AM READY reply will be send at %llu
				// length
				// %d type %d message size %d id %llu\n",
				//(LLU)hdr->reply,hdr->reply_length,
				// hdr->type,message_size,hdr->MR);
				goto iterate_next_element;
			} else {
				if (spinning_list_type == HIGH_PRIORITY)
					++conn->idle_iterations;
				else if (conn->idle_iterations > 0)
					--conn->idle_iterations;
				goto iterate_next_element;
			}

		iterate_next_element:
			if (node->marked_for_deletion) {
				log_warn("garbage collection");
				pthread_mutex_lock(&spinner->conn_list_lock);
				next_node = node->next; /*Caution prev_node remains intact*/
				if (spinning_list_type == HIGH_PRIORITY)
					delete_element_from_simple_concurrent_list(spinner->conn_list, prev_node, node);
				else
					delete_element_from_simple_concurrent_list(spinner->idle_conn_list, prev_node,
										   node);
				node = next_node;
				pthread_mutex_unlock(&spinner->conn_list_lock);
			} else if (0
                 /*spinning_list_type == HIGH_PRIORITY &&
						conn->priority != HIGH_PRIORITY &&//we don't touch high priority connections
						conn->idle_iterations > MAX_IDLE_ITERATIONS*/) {
				log_warn("Downgrading connection...");
				pthread_mutex_lock(&spinner->conn_list_lock);
				next_node = node->next; /*Caution prev_node remains intact*/
				remove_element_from_simple_concurrent_list(spinner->conn_list, prev_node, node);
				add_node_in_simple_concurrent_list(spinner->idle_conn_list, node);
				conn->responsible_spin_list = spinner->idle_conn_list;
				conn->idle_iterations = 0;
				node = next_node;
				pthread_mutex_unlock(&spinner->conn_list_lock);
				log_warn("Downgrading connection...D O N E ");
			} else if (spinning_list_type == LOW_PRIORITY && conn->idle_iterations > MAX_IDLE_ITERATIONS) {
			upgrade_connection:
				log_warn("Upgrading connection...");
				pthread_mutex_lock(&spinner->conn_list_lock);
				next_node = node->next; /*Caution prev_node remains intact*/
				remove_element_from_simple_concurrent_list(spinner->idle_conn_list, prev_node, node);
				add_node_in_simple_concurrent_list(spinner->conn_list, node);
				conn->responsible_spin_list = spinner->conn_list;
				conn->idle_iterations = 0;
				node = next_node;
				pthread_mutex_unlock(&spinner->conn_list_lock);
				log_warn("Upgrading connection...D O N E");
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
	uint32_t num_of_replies_needed;
	void *memory;
	struct ibv_mr *mr;
};

void recover_log_context_completion(struct rdma_message_context *msg_ctx)
{
	struct recover_log_context *cnxt = (struct recover_log_context *)msg_ctx->args;
	if (--cnxt->num_of_replies_needed == 0) {
		rdma_dereg_mr(cnxt->mr);
		free(cnxt->memory);
		free(msg_ctx->args);
		log_info("Recovering log Done");
	}
}

//This function is called by the poll_cq thread every time a notification arrives
static void wait_for_replication_completion_callback(struct rdma_message_context *r_cnxt)
{
	if (r_cnxt->__is_initialized != 1) {
		log_info("replication completion callback %u", r_cnxt->__is_initialized);
		assert(0);
		exit(EXIT_FAILURE);
	}
	sem_post(&r_cnxt->wait_for_completion);
	struct krm_work_task *task = (struct krm_work_task *)r_cnxt->args;
	for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; i++) {
		if (sem_trywait(&task->msg_ctx[i].wait_for_completion) != 0) {
			task->last_replica_to_ack = i;
			return;
		}

		if (task->msg_ctx[i].wc.status != IBV_WC_SUCCESS && task->msg_ctx[i].wc.status != IBV_WC_WR_FLUSH_ERR) {
			log_fatal("Replication RDMA write error: %s", ibv_wc_status_str(task->msg_ctx[i].wc.status));
			exit(EXIT_FAILURE);
		}
		/*count bytes replicated for this segment*/
		__sync_fetch_and_add(task->replicated_bytes[i], task->kv_size);
		r_cnxt->__is_initialized = 0;
		r_cnxt->on_completion_callback = NULL;
	}

	bt_decrease_level0_writers(task->r_desc->db);
	while (1) {
		enum krm_work_task_status old_val = task->kreon_operation_status;
		if (__sync_bool_compare_and_swap(&task->kreon_operation_status, old_val, ALL_REPLICAS_ACKED))
			break;
	}

	return;
}

void insert_kv_pair(struct krm_server_desc *server, struct krm_work_task *task)
{
	//assert(task->kreon_operation_status!=0);
	/*############## fsm state logic follows ###################*/
	while (1) {
		switch (task->kreon_operation_status) {
		case TASK_START:
		case TASK_SUSPENDED: {
			task->kreon_operation_status = GET_RSTATE;
			break;
		}

		case GET_RSTATE: {
			if (task->r_desc->region->num_of_backup) {
				if (task->r_desc->replica_bufs_initialized)
					task->kreon_operation_status = INS_TO_KREON;
				else {
					pthread_mutex_lock(&task->r_desc->region_lock);
					if (task->r_desc->region_halted) {
						/*suspend and return*/
						// log_info("Suspending task %p key %s", task, task->key->key);
						task->suspended = 1;
						utils_queue_push(&task->r_desc->halted_tasks, task);
						pthread_mutex_unlock(&task->r_desc->region_lock);
						return;
					} else if (!task->r_desc->replica_bufs_initialized) {
						log_info("initializing log buffers with replicas");
						task->r_desc->region_halted = 1;
						task->kreon_operation_status = INIT_LOG_BUFFERS;

					} else
						task->kreon_operation_status = INS_TO_KREON;
					pthread_mutex_unlock(&task->r_desc->region_lock);
				}
			} else {
				/*log_info("No replicated region");*/
				task->kreon_operation_status = INS_TO_KREON;
			}
			break;
		}
		case INIT_LOG_BUFFERS: {
			struct krm_region_desc *r_desc = task->r_desc;
			if (r_desc->m_state == NULL) {
				r_desc->m_state = (struct ru_master_state *)malloc(
					sizeof(struct ru_master_state) +
					(r_desc->region->num_of_backup *
					 (sizeof(struct ru_master_log_buffer) +
					  (RU_REPLICA_NUM_SEGMENTS * sizeof(struct ru_master_log_buffer_seg)))));
				/*we need to dive into Kreon to check what in the current end of
* log.
* Since for this region we are the first to do this there is surely no
* concurrent access*/
				uint64_t range;
				if (r_desc->db->db_desc->KV_log_size > 0) {
					range = r_desc->db->db_desc->KV_log_size -
						(r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE);
				} else
					range = 0;
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
					log_info("Sending get_log_buffer req to %s",
						 r_desc->region->backups[i].kreon_ds_hostname);
					r_desc->m_state->r_buf[i].p = sc_allocate_rpc_pair(
						conn,
						sizeof(struct msg_get_log_buffer_req) + r_desc->region->min_key_size,
						sizeof(struct msg_get_log_buffer_rep) +
							(RU_REPLICA_NUM_SEGMENTS * sizeof(struct ibv_mr)),
						GET_LOG_BUFFER_REQ);

					if (r_desc->m_state->r_buf[i].p.stat != ALLOCATION_IS_SUCCESSFULL)
						continue;
					else {
						/*inform the req about its buddy*/
						msg_header *req_header = r_desc->m_state->r_buf[i].p.request;
						msg_header *rep_header = r_desc->m_state->r_buf[i].p.reply;
						req_header->request_message_local_addr = req_header;
						req_header->ack_arrived = KR_REP_PENDING;
						/*location where server should put the reply*/
						req_header->reply =
							(char *)((uint64_t)rep_header -
								 (uint64_t)conn->recv_circular_buf->memory_region);
						req_header->reply_length = sizeof(msg_header) + rep_header->pay_len +
									   rep_header->padding_and_tail;
						/*time to send the message*/
						req_header->session_id = (uint64_t)task->r_desc->region;
						struct msg_get_log_buffer_req *g_req =
							(struct msg_get_log_buffer_req *)((uint64_t)req_header +
											  sizeof(struct msg_header));
						g_req->num_buffers = RU_REPLICA_NUM_SEGMENTS;
						g_req->buffer_size = SEGMENT_SIZE;
						g_req->region_key_size = r_desc->region->min_key_size;
						strcpy(g_req->region_key, r_desc->region->min_key);
						__send_rdma_message(conn, req_header, NULL);

						log_info("DONE Sending get_log_buffer req to %s",
							 r_desc->region->backups[i].kreon_ds_hostname);
						r_desc->m_state->r_buf[i].stat = RU_BUFFER_REQUESTED;
					}
				}
			}
			// log_info("Checking log buffer replies num of replicas are %d",
			// r_desc->region->num_of_backup);
			/*check possible replies*/
			uint32_t ready_buffers = 0;
			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
				if (r_desc->m_state->r_buf[i].stat == RU_BUFFER_REQUESTED) {
					/*check reply and process*/
					// log_info("Waiting tail at offset: %d",
					//	 (sizeof(struct msg_header) +
					//	  r_desc->m_state->r_buf[i].p.reply->pay_len +
					//	  r_desc->m_state->r_buf[i].p.reply->padding_and_tail) -
					//		 TU_TAIL_SIZE);
					/*wait first for the header and then the payload*/
					if (r_desc->m_state->r_buf[i].p.reply->receive != TU_RDMA_REGULAR_MSG)
						continue;
					uint32_t *tail =
						(uint32_t *)(((uint64_t)r_desc->m_state->r_buf[i].p.reply +
							      sizeof(struct msg_header) +
							      r_desc->m_state->r_buf[i].p.reply->pay_len +
							      r_desc->m_state->r_buf[i].p.reply->padding_and_tail) -
							     TU_TAIL_SIZE);

					if (*tail == TU_RDMA_REGULAR_MSG) {
						struct msg_get_log_buffer_rep *rep =
							(struct msg_get_log_buffer_rep
								 *)(((uint64_t)r_desc->m_state->r_buf[i].p.reply) +
								    sizeof(struct msg_header));
						assert(rep->status == KREON_SUCCESS);
						r_desc->m_state->r_buf[i].segment_size = SEGMENT_SIZE;
						r_desc->m_state->r_buf[i].num_buffers = RU_REPLICA_NUM_SEGMENTS;
						uint64_t seg_offt = r_desc->db->db_desc->KV_log_size -
								    (r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE);
						task->r_desc->next_segment_to_flush = seg_offt;
						for (int j = 0; j < RU_REPLICA_NUM_SEGMENTS; j++) {
							r_desc->m_state->r_buf[i].segment[j].start = seg_offt;
							seg_offt += SEGMENT_SIZE;
							r_desc->m_state->r_buf[i].segment[j].end = seg_offt;
							r_desc->m_state->r_buf[i].segment[j].mr = rep->mr[j];

							assert(r_desc->m_state->r_buf[i].segment[j].mr.length ==
							       SEGMENT_SIZE);
						}
						r_desc->m_state->r_buf[i].stat = RU_BUFFER_OK;
						/*finally free the message*/
						sc_free_rpc_pair(&r_desc->m_state->r_buf[i].p);
					}
				}
				if (r_desc->m_state->r_buf[i].stat == RU_BUFFER_OK)
					++ready_buffers;
			}
			if (ready_buffers == r_desc->region->num_of_backup) {
				pthread_mutex_lock(&task->r_desc->region_lock);
				for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++)
					r_desc->m_state->r_buf[i].stat = RU_BUFFER_UNINITIALIZED;

				// log_info("Remote buffers ready initialize remote segments with
				// current state");

				// 1.prepare the context for the poller to later free the staff
				// needed*/
				struct recover_log_context *context =
					(struct recover_log_context *)malloc(sizeof(struct recover_log_context));
				context->num_of_replies_needed = r_desc->region->num_of_backup;
				context->memory = malloc(SEGMENT_SIZE);
				task->msg_ctx[0].msg = NULL;
				task->msg_ctx[0].on_completion_callback = recover_log_context_completion;
				task->msg_ctx[0].args = (void *)context;

				// 2. copy last segment to a register buffer
				struct segment_header *last_segment = (struct segment_header *)context->memory;
				memcpy(last_segment, (const char *)r_desc->db->db_desc->KV_log_last_segment,
				       SEGMENT_SIZE);
				struct connection_rdma *r_conn =
					sc_get_data_conn(server, r_desc->region->backups[0].kreon_ds_hostname);
				context->mr = rdma_reg_write(r_conn->rdma_cm_id, last_segment, SEGMENT_SIZE);
				if (context->mr == NULL) {
					log_fatal("Failed to reg memory");
					exit(EXIT_FAILURE);
				}
				/*setup the context*/
				for (uint32_t j = 0; j < r_desc->region->num_of_backup; j++) {
					r_conn = sc_get_data_conn(server, r_desc->region->backups[j].kreon_ds_hostname);
					// 2. rdma it to the remote
					while (1) {
						int ret = rdma_post_write(
							r_conn->rdma_cm_id, &task->msg_ctx[0], last_segment,
							SEGMENT_SIZE, context->mr, IBV_SEND_SIGNALED,
							(uint64_t)r_desc->m_state->r_buf[j].segment[0].mr.addr,
							r_desc->m_state->r_buf[j].segment[0].mr.rkey);
						if (!ret) {
							break;
						}
						if (r_conn->status == CONNECTION_ERROR) {
							log_fatal("connection failed !: %s\n", strerror(errno));
							exit(EXIT_FAILURE);
						}
					}
				}
				r_desc->next_segment_to_flush = r_desc->db->db_desc->KV_log_size -
								(r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE);
				log_info("Successfully sent the last segment to all the group");

				/*resume halted tasks*/
				log_info("Resuming halted tasks");
				struct krm_work_task *halted_task = utils_queue_pop(&r_desc->halted_tasks);
				while (halted_task != NULL) {
					halted_task->suspended = 0;
					// log_info("Resuming task pool %d key is %s", halted_task->pool_id,
					//        halted_task->key->key);
					ds_put_resume_task(&root_server->numa_servers[task->server_id]->spinner,
							   halted_task);
					halted_task = utils_queue_pop(&r_desc->halted_tasks);
				}
				// log_info("*******************");

				task->r_desc->region_halted = 0;
				task->r_desc->replica_bufs_initialized = 1;
				pthread_mutex_unlock(&task->r_desc->region_lock);
				task->kreon_operation_status = INS_TO_KREON;
			} else {
				// log_info("Not all replicas ready waiting status %d suspended %d",
				//	 task->kreon_operation_status, task->suspended);
				return;
			}

			break;
		}
		case REGION_HALTED:
			break;
		case INS_TO_KREON: {
			task->ins_req.metadata.handle = task->r_desc->db;
			task->ins_req.metadata.kv_size = 0;
			task->ins_req.key_value_buf = task->key;
			task->ins_req.metadata.level_id = 0;
			task->ins_req.metadata.key_format = KV_FORMAT;
			task->ins_req.metadata.append_to_log = 1;
			task->ins_req.metadata.gc_request = 0;
			task->ins_req.metadata.recovery_request = 0;
			task->ins_req.metadata.segment_full_event = 0;
			task->ins_req.metadata.special_split = 0;

			/*now Level-0 check (if it needs compaction)*/
			struct db_descriptor *db_desc = task->r_desc->db->db_desc;
			int active_tree = db_desc->levels[0].active_tree;
			if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
				pthread_mutex_lock(&db_desc->client_barrier_lock);
				active_tree = db_desc->levels[0].active_tree;

				if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
					sem_post(&db_desc->compaction_daemon_interrupts);
					pthread_mutex_unlock(&db_desc->client_barrier_lock);
					return;
					// if (pthread_cond_wait(&db_desc->client_barrier,
					//		      &db_desc->client_barrier_lock) != 0) {
					//	log_fatal("failed to throttle");
					//	exit(EXIT_FAILURE);
					//}
				}
				pthread_mutex_unlock(&db_desc->client_barrier_lock);
			}
			/********************************************/
			_insert_key_value(&task->ins_req);
			if (task->r_desc->region->num_of_backup > 0) {
				if (task->ins_req.metadata.segment_full_event)
					task->kreon_operation_status = FLUSH_REPLICA_BUFFERS;
				else
					task->kreon_operation_status = REPLICATE;
				break;
			} else {
				task->kreon_operation_status = TASK_COMPLETE;
				break;
			}
		}

		case FLUSH_REPLICA_BUFFERS: {
			struct krm_region_desc *r_desc = task->r_desc;

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
				return;
			}
			/*find out the idx of the buffer that needs flush*/

			uint64_t lc1;
			uint64_t lc2;
		retry:
			task->seg_id_to_flush = -1;

			for (int i = 0; i < RU_REPLICA_NUM_SEGMENTS; i++) {
				lc1 = r_desc->m_state->r_buf[0].segment[i].lc1;
				if (task->ins_req.metadata.log_offset_full_event >
					    r_desc->m_state->r_buf[0].segment[i].start &&
				    task->ins_req.metadata.log_offset_full_event <=
					    r_desc->m_state->r_buf[0].segment[i].end) {
					task->seg_id_to_flush = i;
				}
				lc2 = r_desc->m_state->r_buf[0].segment[i].lc2;
				if (lc1 != lc2)
					goto retry;
				if (task->seg_id_to_flush != -1)
					break;
			}

			if (task->seg_id_to_flush == -1) {
				log_fatal("No appropriate remote segment id found for flush, what?");
				exit(EXIT_FAILURE);
			}
			/*sent flush command to all motherfuckers*/
			task->kreon_operation_status = SEGMENT_BARRIER;
			break;
		}
		case SEGMENT_BARRIER: {
			for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
				/*find appropriate seg buffer to rdma the mutation*/
				uint32_t remaining =
					task->r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].replicated_bytes;
				remaining = SEGMENT_SIZE - (remaining + task->ins_req.metadata.log_padding);
				if (remaining > 0) {
					// log_info("Sorry segment not ready bytes remaining to replicate
					// %lu", remaining);
					return;
				}
			}
			// pthread_mutex_lock(&task->r_desc->region_lock);
			// task->r_desc->region_halted = 1;
			// pthread_mutex_unlock(&task->r_desc->region_lock);
			task->kreon_operation_status = SEND_FLUSH_COMMANDS;
			break;
		}
		case SEND_FLUSH_COMMANDS: {
			struct krm_region_desc *r_desc = task->r_desc;
			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
				struct connection_rdma *r_conn = NULL;
				/*allocate and send command*/
				if (r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd_stat ==
				    RU_BUFFER_UNINITIALIZED) {
					r_conn = sc_get_data_conn(server, r_desc->region->backups[i].kreon_ds_hostname);
					uint32_t req_size =
						sizeof(struct msg_flush_cmd_req) + r_desc->region->min_key_size;
					uint32_t rep_size = sizeof(struct msg_flush_cmd_rep);
					r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd =
						sc_allocate_rpc_pair(r_conn, req_size, rep_size, FLUSH_COMMAND_REQ);

					if (r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd.stat !=
					    ALLOCATION_IS_SUCCESSFULL)
						return;

					msg_header *req_header = r_desc->m_state->r_buf[i]
									 .segment[task->seg_id_to_flush]
									 .flush_cmd.request;
					msg_header *rep_header =
						r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd.reply;
					req_header->request_message_local_addr = req_header;
					req_header->ack_arrived = KR_REP_PENDING;
					/*location where server should put the reply*/
					req_header->reply =
						(char *)((uint64_t)rep_header -
							 (uint64_t)r_conn->recv_circular_buf->memory_region);
					req_header->reply_length =
						sizeof(msg_header) + rep_header->pay_len + rep_header->padding_and_tail;
					/*time to send the message*/
					req_header->session_id = (uint64_t)task->r_desc->region;
					struct msg_flush_cmd_req *f_req =
						(struct msg_flush_cmd_req *)((uint64_t)req_header +
									     sizeof(struct msg_header));

					/*where primary has stored its segment*/
					f_req->is_partial = 0;
					f_req->log_buffer_id = task->seg_id_to_flush;
					f_req->master_segment = task->ins_req.metadata.log_segment_addr;
					// log_info("Sending flush command for segment %llu",
					// f_req->master_segment);
					f_req->segment_id = task->ins_req.metadata.segment_id;
					f_req->end_of_log = task->ins_req.metadata.end_of_log;
					f_req->log_padding = task->ins_req.metadata.log_padding;
					f_req->region_key_size = r_desc->region->min_key_size;
					strcpy(f_req->region_key, r_desc->region->min_key);

					__send_rdma_message(r_conn, req_header, NULL);
					r_desc->m_state->r_buf[i].stat = RU_BUFFER_REQUESTED;
					// log_info("Sent flush command req_header %llu", req_header);
				}
			}
			task->kreon_operation_status = WAIT_FOR_FLUSH_REPLIES;
			break;
		}

		case WAIT_FOR_FLUSH_REPLIES: {
			struct krm_region_desc *r_desc = task->r_desc;
			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
				/*check if header is there*/
				msg_header *reply =
					r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd.reply;

				if (reply->receive != TU_RDMA_REGULAR_MSG)
					return;
				/*check if payload is there*/

				uint32_t *tail = (uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) +
							       reply->pay_len + reply->padding_and_tail) -
							      TU_TAIL_SIZE);

				if (*tail != TU_RDMA_REGULAR_MSG)
					return;
			}
			/*got all replies motherfuckers*/
			pthread_mutex_lock(&task->r_desc->region_lock);
			r_desc->next_segment_to_flush += SEGMENT_SIZE;
			// pthread_mutex_unlock(&task->r_desc->region_lock);

			for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
				++r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].lc2;
				r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].end +=
					(RU_REPLICA_NUM_SEGMENTS * SEGMENT_SIZE);
				r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].start +=
					(RU_REPLICA_NUM_SEGMENTS * SEGMENT_SIZE);
				r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].replicated_bytes =
					sizeof(segment_header);
				sc_free_rpc_pair(&r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].flush_cmd);
				++r_desc->m_state->r_buf[i].segment[task->seg_id_to_flush].lc1;
			}
			// log_info("Resume possible halted tasks after flush");
			// pthread_mutex_lock(&r_desc->region_lock);
			r_desc->region_halted = 0;
			struct krm_work_task *halted_task = utils_queue_pop(&task->r_desc->halted_tasks);
			while (halted_task != NULL) {
				halted_task->suspended = 0;
				ds_put_resume_task(&root_server->numa_servers[task->server_id]->spinner, halted_task);
				halted_task = utils_queue_pop(&task->r_desc->halted_tasks);
			}
			pthread_mutex_unlock(&r_desc->region_lock);
			task->kreon_operation_status = REPLICATE;
			break;
		}

		case REPLICATE: {
			struct krm_region_desc *r_desc = task->r_desc;
			task->kv_size = task->key->key_size + sizeof(struct msg_put_key);
			task->kv_size = task->kv_size + task->value->value_size + sizeof(struct msg_put_value);
			uint32_t remote_offset;
			if (task->ins_req.metadata.log_offset > 0)
				remote_offset = task->ins_req.metadata.log_offset % SEGMENT_SIZE;
			else
				remote_offset = 0;
			uint32_t done = 0;
			for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
				/*find appropriate seg buffer to rdma the mutation*/

				for (int j = 0; j < RU_REPLICA_NUM_SEGMENTS; j++) {
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

						int ret;

						client_rdma_init_message_context(&task->msg_ctx[i], NULL);
						task->last_replica_to_ack = 0;
						task->msg_ctx[i].args = task;
						task->msg_ctx[i].on_completion_callback =
							wait_for_replication_completion_callback;
						task->replicated_bytes[i] =
							&r_desc->m_state->r_buf[i].segment[j].replicated_bytes;
						task->kreon_operation_status = WAIT_FOR_REPLICATION_COMPLETION;
						while (1) {
							ret = rdma_post_write(
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
						done = 1;
						break;
					}
				}
			}
			if (!done) {
				//seems some segment flushes at the replicas..., check if region is
				//halted otherwise retry
				pthread_mutex_lock(&r_desc->region_lock);
				if (r_desc->region_halted) {
					log_info("halting task region waits for flush");
					assert(0);
					task->suspended = 1;
					if (utils_queue_push(&r_desc->halted_tasks, task) == NULL) {
						log_fatal("failed to add task to halt queue");
						exit(EXIT_FAILURE);
					}
					pthread_mutex_unlock(&r_desc->region_lock);
					return;
				} else {
					pthread_mutex_unlock(&r_desc->region_lock);
					return;
				}
			}
			//task->kreon_operation_status = WAIT_FOR_REPLICATION_COMPLETION;
			break;
		}
		case WAIT_FOR_REPLICATION_COMPLETION: {
			//poll cq calls wait_for_replication_completion callback
#if 0
			for (uint32_t i = task->last_replica_to_ack; i < task->r_desc->region->num_of_backup; i++) {
				if (sem_trywait(&task->msg_ctx[i].wait_for_completion) != 0) {
					task->last_replica_to_ack = i;
					return;
				}

				if (task->msg_ctx[i].wc.status != IBV_WC_SUCCESS &&
				    task->msg_ctx[i].wc.status != IBV_WC_WR_FLUSH_ERR) {
					log_fatal("Replication RDMA write error: %s",
						  ibv_wc_status_str(task->msg_ctx[i].wc.status));
					exit(EXIT_FAILURE);
				}
				/*count bytes replicated for this segment*/
				__sync_fetch_and_add(task->replicated_bytes[i], task->kv_size);
			}
			task->kreon_operation_status = TASK_COMPLETE;
#endif
			return;
		}
		case ALL_REPLICAS_ACKED:
			task->kreon_operation_status = TASK_COMPLETE;
			break;
		case TASK_COMPLETE:
			//bt_decrease_level0_writers(task->r_desc->db);
			return;

		default:
			log_fatal("Ended up in faulty state %u", task->kreon_operation_status);
			assert(0);
			return;
		}
	}
}

static int write_segment_with_explicit_IO(char *buf, ssize_t num_bytes, ssize_t dev_offt, int fd)
{
	ssize_t total_bytes_written = 0;
	ssize_t bytes_written = 0;
	while (total_bytes_written < num_bytes) {
		bytes_written = pwrite(fd, buf, num_bytes - total_bytes_written, dev_offt + total_bytes_written);
		if (bytes_written == -1) {
			log_fatal("Failed to writed segment for leaf nodes reason follows");
			perror("Reason");
			exit(EXIT_FAILURE);
		}
		total_bytes_written += bytes_written;
	}
	return 1;
}

/*
   * KreonR main processing function of networkrequests.
   * Each network processing request must be resumable. For each message type
   * KreonR process it via
   * a specific data path. We treat all taks related to network  as paths that
   * may
   * fail, that we can resume later. The idea
   * behind this
   * */
static void handle_task(struct krm_server_desc *mydesc, struct krm_work_task *task)
{
	// struct connection_rdma *rdma_conn = NULL;
	struct krm_region_desc *r_desc;
	void *value;
	scannerHandle *sc;
	msg_multi_get_req *multi_get;
	msg_get_req *get_req;
	msg_get_rep *get_rep;
	uint32_t key_length = 0;
	uint32_t actual_reply_size = 0;
	uint32_t padding;
	/*unboxing the arguments*/
	r_desc = NULL;
	//task->reply_msg = NULL;

	switch (task->msg->type) {
	case REPLICA_INDEX_GET_BUFFER_REQ: {
		struct msg_replica_index_get_buffer_req *g_req =
			(struct msg_replica_index_get_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));

		struct krm_region_desc *r_desc =
			krm_get_region_based_on_id(mydesc, g_req->region_key, g_req->region_key_size);
		if (r_desc == NULL) {
			log_fatal("no hosted region found for min key %s", g_req->region_key);
			exit(EXIT_FAILURE);
		}
		log_info("REPLICA: Master wants %d rdma buffers to start index tranfer of "
			 "level %d of region %s",
			 g_req->num_buffers, g_req->level_id, r_desc->region->id);

		pthread_mutex_lock(&r_desc->region_lock);
		if (r_desc->r_state == NULL) {
			log_fatal("replica state should not be NULL at this point");
			exit(EXIT_FAILURE);
		}
		pthread_mutex_unlock(&r_desc->region_lock);
#if !RCO_EXPLICIT_IO
		r_desc->level_cursor[g_req->level_id].state = DI_INIT;
		r_desc->level_cursor[g_req->level_id].max_offset = g_req->index_offset;
		log_info("Index offset set at %llu", r_desc->level_cursor[g_req->level_id].max_offset);
#endif
		log_info("DB %s Allocating %u and registering with RDMA buffers for remote compaction",
			 r_desc->region->id, g_req->num_buffers);
		for (int i = 0; i < g_req->num_buffers; i++) {
			char *addr = NULL;
			if (posix_memalign((void **)&addr, ALIGNMENT, SEGMENT_SIZE) != 0) {
				log_fatal("Posix memalign failed");
				perror("Reason: ");
				exit(EXIT_FAILURE);
			}
			if (r_desc->r_state->index_buffers[g_req->level_id][i] == NULL) {
				r_desc->r_state->index_buffers[g_req->level_id][i] =
					rdma_reg_write(task->conn->rdma_cm_id, addr, SEGMENT_SIZE);
			} else {
				log_fatal("Remote compaction for level %d still pending", g_req->level_id);
				exit(EXIT_FAILURE);
			}
		}

		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);
		/*initialize message*/
		task->reply_msg->pay_len = sizeof(struct msg_replica_index_get_buffer_rep);

		actual_reply_size = sizeof(msg_header) + task->reply_msg->pay_len + TU_TAIL_SIZE;
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
		/*set tail to the proper value*/
		// log_info("Setting tail to offset %d", actual_reply_size + (padding -
		// TU_TAIL_SIZE));
		*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;
		task->reply_msg->data = (void *)((uint64_t)task->reply_msg + sizeof(msg_header));
		task->reply_msg->next = task->reply_msg->data;

		task->reply_msg->type = REPLICA_INDEX_GET_BUFFER_REP;

		task->reply_msg->ack_arrived = KR_REP_PENDING;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;

		struct msg_replica_index_get_buffer_rep *g_rep =
			(struct msg_replica_index_get_buffer_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		g_rep->status = KREON_SUCCESS;
		g_rep->num_buffers = g_req->num_buffers;
		for (int i = 0; i < g_rep->num_buffers; i++)
			g_rep->mr[i] = *r_desc->r_state->index_buffers[g_req->level_id][i];

		/*piggyback info for use with the client*/
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		assert(task->reply_msg->request_message_local_addr != NULL);
		log_info("REPLICA: DONE registering %d buffer for index transfer for region %s", g_rep->num_buffers,
			 r_desc->region->id);
		task->kreon_operation_status = TASK_COMPLETE;
		break;
	}

	case REPLICA_INDEX_FLUSH_REQ: {
		struct msg_replica_index_flush_req *f_req =
			(struct msg_replica_index_flush_req *)((uint64_t)task->msg + sizeof(struct msg_header));

		struct krm_region_desc *r_desc =
			krm_get_region_based_on_id(mydesc, f_req->region_key, f_req->region_key_size);
		if (r_desc == NULL) {
			log_fatal("no hosted region found for min key %s", f_req->region_key);
			exit(EXIT_FAILURE);
		}
		if (f_req->seg_id > 0)
			if (f_req->seg_id !=
			    r_desc->db->db_desc->levels[f_req->level_id].last_segment[f_req->tree_id]->segment_id + 1) {
				log_fatal("freq segid = %d should have been %u", f_req->seg_id,
					  r_desc->db->db_desc->levels[f_req->level_id]
							  .last_segment[f_req->tree_id]
							  ->segment_id +
						  1);
				assert(0);
			}
		uint64_t s = djb2_hash((unsigned char *)r_desc->r_state->index_buffers[f_req->level_id][0]->addr,
				       SEGMENT_SIZE);
		assert(s == f_req->seg_hash);

		//log_info("Primary hash %llu mine %llu", s, f_req->seg_hash);
#if RCO_EXPLICIT_IO
		di_rewrite_index_with_explicit_IO(r_desc->r_state->index_buffers[f_req->level_id][0]->addr, r_desc,
						  f_req->primary_segment_offt, f_req->level_id);
#else
		struct segment_header *seg = seg_get_raw_index_segment(
			r_desc->db->volume_desc, &r_desc->db->db_desc->levels[f_req->level_id], f_req->tree_id);
		seg->next_segment = NULL;

		memcpy((char *)((uint64_t)seg + sizeof(segment_header)),
		       r_desc->r_state->index_buffers[f_req->level_id][f_req->seg_id % MAX_REPLICA_INDEX_BUFFERS]->addr +
			       sizeof(struct segment_header),
		       SEGMENT_SIZE - sizeof(segment_header));
		// add mapping to level's hash table
		struct krm_segment_entry *e = (struct krm_segment_entry *)malloc(sizeof(struct krm_segment_entry));
		e->master_seg = f_req->primary_segment_offt;
		e->my_seg = (uint64_t)seg - MAPPED;
		HASH_ADD_PTR(r_desc->replica_index_map[f_req->level_id], master_seg, e);

		di_rewrite_index(r_desc, f_req->level_id, f_req->tree_id);
#endif
		if (f_req->is_last) {
#if !RCO_EXPLICIT_IO
			if (r_desc->level_cursor[f_req->level_id].state != DI_COMPLETE) {
				log_fatal("Failed to rewrite index");
				assert(0);
				exit(EXIT_FAILURE);
			}
#endif
			/*translate root*/
			if (!f_req->root_w && !f_req->root_r) {
				log_fatal("Both roots can't be NULL");
				exit(EXIT_FAILURE);
			}
			struct krm_segment_entry *index_entry;
			uint64_t primary_segment_offt;
			uint64_t primary_segment;
			if (f_req->root_w) {
				primary_segment_offt = f_req->root_w % SEGMENT_SIZE;
				primary_segment = f_req->root_w - primary_segment_offt;
				HASH_FIND_PTR(r_desc->replica_index_map[f_req->level_id], &primary_segment,
					      index_entry);
				if (index_entry == NULL) {
					log_fatal("Cannot translate root_w for primary's segment %llu of db %s",
						  primary_segment_offt, r_desc->db->db_desc->db_name);
					exit(EXIT_FAILURE);
				}
				r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id] =
					(struct node_header *)(MAPPED + index_entry->my_seg + primary_segment_offt);
				assert(r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id]->type ==
					       leafRootNode ||
				       r_desc->db->db_desc->levels[f_req->level_id].root_w[f_req->tree_id]->type ==
					       rootNode);
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
				HASH_FIND_PTR(r_desc->replica_index_map[f_req->level_id], &primary_segment,
					      index_entry);
				if (index_entry == NULL) {
					log_fatal("Cannot translate root_r for primary's segment %llu of db %s",
						  primary_segment_offt, r_desc->db->db_desc->db_name);
					raise(SIGINT);
					exit(EXIT_FAILURE);
				}
				r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id] =
					(struct node_header *)(MAPPED + index_entry->my_seg + primary_segment_offt);
				//assert(r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id]->type ==
				//	       leafRootNode ||
				//       r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id]->type ==
				//	       rootNode);
				// log_info("Ok translated root_r[%u][%u]", f_req->level_id,
				// f_req->tree_id);
			} else {
				r_desc->db->db_desc->levels[f_req->level_id].root_r[f_req->tree_id] = NULL;
				// log_info("Ok primary says root_r[%u][%u] is NULL, ok",
				// f_req->level_id, f_req->tree_id);
			}
			// deregistering buffers
#if RCO_EXPLICIT_IO
			free(r_desc->r_state->index_buffers[f_req->level_id][0]->addr);
			if (rdma_dereg_mr(r_desc->r_state->index_buffers[f_req->level_id][0])) {
				log_fatal("Failed to deregister rdma buffer");
				exit(EXIT_FAILURE);
			}
			r_desc->r_state->index_buffers[f_req->level_id][0] = NULL;
#else
			for (int i = 0; i < MAX_REPLICA_INDEX_BUFFERS; i++) {
				free(r_desc->r_state->index_buffers[f_req->level_id][i]->addr);
				if (rdma_dereg_mr(r_desc->r_state->index_buffers[f_req->level_id][i])) {
					log_fatal("Failed to deregister rdma buffer");
					exit(EXIT_FAILURE);
				}
				r_desc->r_state->index_buffers[f_req->level_id][i] = NULL;
			}
#endif
			if (f_req->level_id > 1) {
				log_info("REPLICA: After index transfer freeing  level %d", f_req->level_id - 1);
				seg_free_level(r_desc->db, f_req->level_id - 1, 0);
			}
			log_info("REPLICA: After index transfer freeing level %d", f_req->level_id);
			seg_free_level(r_desc->db, f_req->level_id, 0);
			log_info("REPLICA: Setting new level as new");
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
			log_info("Destroying mappings for index level %d useless now", f_req->level_id);

			/*iterate over regions*/
			struct krm_segment_entry *current, *tmp;

			HASH_ITER(hh, r_desc->replica_index_map[f_req->level_id], current, tmp)
			{
				HASH_DEL(r_desc->replica_index_map[f_req->level_id], current);
				free(current);
			}
			if (fsync(FD_explicit_IO) != 0) {
				log_fatal("Failed to sync file!");
				perror("Reason:\n");
				exit(EXIT_FAILURE);
			}
			snapshot(r_desc->db->volume_desc);
		}

		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);
		/*initialize message*/
		task->reply_msg->pay_len = sizeof(struct msg_replica_index_flush_rep);

		actual_reply_size = sizeof(msg_header) + task->reply_msg->pay_len + TU_TAIL_SIZE;
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
		/*set tail to the proper value*/
		// log_info("Setting tail to offset %d", actual_reply_size + (padding -
		// TU_TAIL_SIZE));
		*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;
		task->reply_msg->data = (void *)((uint64_t)task->reply_msg + sizeof(msg_header));
		task->reply_msg->next = task->reply_msg->data;

		task->reply_msg->type = REPLICA_INDEX_FLUSH_REP;

		task->reply_msg->ack_arrived = KR_REP_PENDING;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;

		struct msg_replica_index_flush_rep *rep =
			(struct msg_replica_index_flush_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		rep->status = KREON_SUCCESS;
		rep->seg_id = f_req->seg_id;

		/*piggyback info for use with the client*/
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		assert(task->reply_msg->request_message_local_addr != NULL);
		task->kreon_operation_status = TASK_COMPLETE;
		//log_info("REPLICA: Successfully flushed index segment id %d", f_req->seg_id);
		break;
	}
	case GET_LOG_BUFFER_REQ: {
		void *addr;
		struct msg_get_log_buffer_req *get_log =
			(struct msg_get_log_buffer_req *)((uint64_t)task->msg + sizeof(struct msg_header));
		log_info("Region master wants a log buffer for region %s key size %d", get_log->region_key,
			 get_log->region_key_size);
		struct krm_region_desc *r_desc =
			krm_get_region_based_on_id(mydesc, get_log->region_key, get_log->region_key_size);
		if (r_desc == NULL) {
			log_fatal("no hosted region found for min key %s", get_log->region_key);
			exit(EXIT_FAILURE);
		}
		pthread_mutex_lock(&r_desc->region_lock);
		if (r_desc->r_state == NULL) {
			r_desc->r_state = (struct ru_replica_state *)malloc(
				sizeof(struct ru_replica_state) +
				(get_log->num_buffers * sizeof(struct ru_replica_log_buffer_seg)));
			r_desc->r_state->num_buffers = get_log->num_buffers;
			for (int i = 0; i < get_log->num_buffers; i++) {
#if RCO_EXPLICIT_IO
				if (posix_memalign(&addr, ALIGNMENT, get_log->buffer_size)) {
					log_fatal("Failed to allocate aligned RDMA buffer");
					perror("Reason\n");
					exit(EXIT_FAILURE);
				}
#else
				addr = malloc(get_log->buffer_size);
#endif
				r_desc->r_state->seg[i].segment_size = get_log->buffer_size;
				r_desc->r_state->seg[i].mr =
					rdma_reg_write(task->conn->rdma_cm_id, addr, get_log->buffer_size);
			}
			/*what is the next segment id that we should expect (for correctness
* reasons)*/
			if (r_desc->db->db_desc->KV_log_size > 0 &&
			    r_desc->db->db_desc->KV_log_size % SEGMENT_SIZE == 0)
				r_desc->r_state->next_segment_id_to_flush =
					r_desc->db->db_desc->KV_log_last_segment->segment_id + 1;
			else
				r_desc->r_state->next_segment_id_to_flush =
					r_desc->db->db_desc->KV_log_last_segment->segment_id;
		} else {
			log_fatal("remote buffers already initialized, what?");
			exit(EXIT_FAILURE);
		}

		pthread_mutex_unlock(&r_desc->region_lock);

		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);
		/*initialize message*/
		task->reply_msg->pay_len =
			sizeof(struct msg_get_log_buffer_rep) + (get_log->num_buffers * sizeof(struct ibv_mr));

		actual_reply_size = sizeof(msg_header) + task->reply_msg->pay_len + TU_TAIL_SIZE;
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
		/*set tail to the proper value*/
		// log_info("Setting tail to offset %d", actual_reply_size + (padding -
		// TU_TAIL_SIZE));
		*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;
		task->reply_msg->data = (void *)((uint64_t)task->reply_msg + sizeof(msg_header));
		task->reply_msg->next = task->reply_msg->data;

		task->reply_msg->type = GET_LOG_BUFFER_REP;

		task->reply_msg->ack_arrived = KR_REP_PENDING;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;

		struct msg_get_log_buffer_rep *rep =
			(struct msg_get_log_buffer_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		rep->status = KREON_SUCCESS;
		rep->num_buffers = get_log->num_buffers;
		for (int i = 0; i < rep->num_buffers; i++) {
			rep->mr[i] = *r_desc->r_state->seg[i].mr;
		}

		/*piggyback info for use with the client*/
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		assert(task->reply_msg->request_message_local_addr != NULL);
		log_info("Region master wants a log buffer...DONE");
		task->kreon_operation_status = TASK_COMPLETE;
		break;
	}

	case FLUSH_COMMAND_REQ: {
		//log_info("Master orders a flush, obey your master!");
		struct msg_flush_cmd_req *flush_req =
			(struct msg_flush_cmd_req *)((uint64_t)task->msg + sizeof(struct msg_header));

		struct krm_region_desc *r_desc =
			krm_get_region_based_on_id(mydesc, flush_req->region_key, flush_req->region_key_size);
		if (r_desc->r_state == NULL) {
			log_fatal("No state for backup region %s", r_desc->region->id);
			exit(EXIT_FAILURE);
		}

		struct segment_header *seg =
			(struct segment_header *)r_desc->r_state->seg[flush_req->log_buffer_id].mr->addr;
		seg->segment_id = flush_req->segment_id;
		if (flush_req->log_padding)
			memset((void *)((uint64_t)seg + (SEGMENT_SIZE - flush_req->log_padding)), 0x00,
			       flush_req->log_padding);
#if RCO_BUILD_INDEX_AT_REPLICA
		struct rco_build_index_task t;
		t.r_desc = r_desc;
		t.segment = (struct segment_header *)seg;
		t.log_start = 0; //r_desc->db->db_desc->KV_log_size - (SEGMENT_SIZE - sizeof(struct segment_header));
		t.log_end = SEGMENT_SIZE; //r_desc->db->db_desc->KV_log_size;
		rco_build_index(&t);
#else
		pthread_mutex_lock(&r_desc->db->db_desc->lock_log);
		/*Now take a segment from the allocator and copy the buffer*/
		volatile segment_header *last_log_segment = r_desc->db->db_desc->KV_log_last_segment;
		//log_info("Last log segment id %llu id to flush %llu", last_log_segment->segment_id,
		//	 flush_req->segment_id);

		uint64_t diff = flush_req->end_of_log - r_desc->db->db_desc->KV_log_size;
		int exists = 0;
		if (last_log_segment->segment_id == flush_req->segment_id) {
			//log_info("Forced value log flush due to compaction id %llu", last_log_segment->segment_id);
			if (r_desc->r_state->next_segment_id_to_flush == 0)
				++r_desc->r_state->next_segment_id_to_flush;

			// This op should follow after a partial write
			if (flush_req->end_of_log - r_desc->db->db_desc->KV_log_size >= SEGMENT_SIZE) {
				log_fatal("Corruption");
				exit(EXIT_FAILURE);
			}
			exists = 1;
		} else if (r_desc->r_state->next_segment_id_to_flush != flush_req->segment_id) {
			log_fatal("Corruption non-contiguous segment ids: expected %llu  got "
				  "flush_req id is %llu",
				  r_desc->r_state->next_segment_id_to_flush, flush_req->segment_id);
			log_fatal("last segment in database is %llu", last_log_segment->segment_id);
			assert(0);
			exit(EXIT_FAILURE);
		}

		// log_info("Flushing segment %llu padding is %llu primary offset %llu local
		// diff is %llu",
		//	 flush_req->segment_id, flush_req->log_padding, flush_req->end_of_log,
		// diff);
		if (!exists) {
			++r_desc->r_state->next_segment_id_to_flush;
#if RCO_EXPLICIT_IO
			segment_header *disk_segment = seg_get_raw_log_segment(r_desc->db->volume_desc);
			seg->next_segment = NULL;
			seg->prev_segment = (segment_header *)((uint64_t)last_log_segment - MAPPED);
			if (!write_segment_with_explicit_IO((char *)(uint64_t)seg, SEGMENT_SIZE,
							    (uint64_t)disk_segment - MAPPED, FD_explicit_IO)) {
				log_fatal("Failed to write segment with explicit I/O");
				exit(EXIT_FAILURE);
			}
#else
			memcpy(disk_segment, seg, SEGMENT_SIZE);
			disk_segment->next_segment = NULL;
			disk_segment->prev_segment = (segment_header *)((uint64_t)last_log_segment - MAPPED);
#endif
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
#if RCO_EXPLICIT_IO
			if (!write_segment_with_explicit_IO((char *)(uint64_t)seg + sizeof(struct segment_header),
							    SEGMENT_SIZE - sizeof(struct segment_header),
							    ((uint64_t)last_log_segment - MAPPED) +
								    sizeof(struct segment_header),
							    FD_explicit_IO)) {
				log_fatal("Failed to write segment with explicit I/O");
				exit(EXIT_FAILURE);
			}
#else
			memcpy((struct segment_header *)last_log_segment, seg, SEGMENT_SIZE);
#endif
		}

		r_desc->db->db_desc->KV_log_size += diff;
		pthread_mutex_unlock(&r_desc->db->db_desc->lock_log);
#endif
		/*time for reply :-)*/
		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);

		/*piggyback info for use with the client*/
		task->reply_msg->pay_len = sizeof(struct msg_flush_cmd_rep);

		actual_reply_size = sizeof(msg_header) + sizeof(msg_delete_rep) + TU_TAIL_SIZE;
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
		/*set tail to the proper value*/
		*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;
		task->reply_msg->data = (void *)((uint64_t)task->reply_msg + sizeof(msg_header));
		task->reply_msg->next = task->reply_msg->data;

		task->reply_msg->type = FLUSH_COMMAND_REP;

		task->reply_msg->ack_arrived = KR_REP_PENDING;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;
		struct msg_flush_cmd_rep *flush_rep =
			(struct msg_flush_cmd_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		flush_rep->status = KREON_SUCCESS;
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		task->kreon_operation_status = TASK_COMPLETE;
		// log_info("Responded to server!");
		break;
	}
	case PUT_REQUEST:

		/* retrieve region handle for the corresponding key, find_region
* initiates internally rdma connections if needed
*/
		if (task->key == NULL) {
			task->key = (msg_put_key *)((uint64_t)task->msg + sizeof(struct msg_header));
			task->value =
				(msg_put_value *)((uint64_t)task->key + sizeof(msg_put_key) + task->key->key_size);
			key_length = task->key->key_size;
			assert(key_length != 0);
			r_desc = krm_get_region(mydesc, task->key->key, task->key->key_size);
			if (r_desc == NULL) {
				log_fatal("Region not found for key size %u:%s", task->key->key_size, task->key->key);
				exit(EXIT_FAILURE);
			}

			task->r_desc = (void *)r_desc;
		}
		insert_kv_pair(mydesc, task);
		if (task->kreon_operation_status == TASK_COMPLETE) {
			/*prepare the reply*/
			task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
						   (uint64_t)task->msg->reply);

			if (task->msg->reply_length >= actual_reply_size) {
				task->reply_msg->pay_len = sizeof(msg_put_rep);

				actual_reply_size = sizeof(msg_header) + sizeof(msg_put_rep) + TU_TAIL_SIZE;
				padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
				/*set tail to the proper value*/
				*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size +
					      (padding - TU_TAIL_SIZE)) = TU_RDMA_REGULAR_MSG;
				task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;
				task->reply_msg->data = (void *)((uint64_t)task->reply_msg + sizeof(msg_header));
				task->reply_msg->next = task->reply_msg->data;

				task->reply_msg->type = PUT_REPLY;

				task->reply_msg->ack_arrived = KR_REP_PENDING;
				task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
				task->reply_msg->local_offset = (uint64_t)task->msg->reply;
				task->reply_msg->remote_offset = (uint64_t)task->msg->reply;
				msg_put_rep *put_rep = (msg_put_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
				put_rep->status = KREON_SUCCESS;
			} else {
				log_fatal("SERVER: mr CLIENT reply space not enough  size %" PRIu32
					  " FIX XXX TODO XXX\n",
					  task->msg->reply_length);
				exit(EXIT_FAILURE);
			}
			/*piggyback info for use with the client*/
			task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
			assert(task->reply_msg->request_message_local_addr != NULL);
		}
		break;

	case DELETE_REQUEST: {
		msg_delete_req *del_req = (msg_delete_req *)task->msg->data;
		r_desc = krm_get_region(mydesc, del_req->key, del_req->key_size);
		if (r_desc == NULL) {
			log_fatal("ERROR: Region not found for key %s\n", del_req->key);
			return;
		}
		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);

		/*piggyback info for use with the client*/
		task->reply_msg->pay_len = sizeof(msg_delete_rep);

		actual_reply_size = sizeof(msg_header) + sizeof(msg_delete_rep) + TU_TAIL_SIZE;
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
		/*set tail to the proper value*/
		*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;
		task->reply_msg->data = (void *)((uint64_t)task->reply_msg + sizeof(msg_header));
		task->reply_msg->next = task->reply_msg->data;

		task->reply_msg->type = DELETE_REPLY;

		task->reply_msg->ack_arrived = KR_REP_PENDING;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;
		msg_delete_rep *del_rep = (msg_delete_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		task->kreon_operation_status = TASK_COMPLETE;

		if (delete_key(r_desc->db, del_req->key, del_req->key_size) == SUCCESS) {
			del_rep->status = KREON_SUCCESS;
			// log_info("Deleted key %s successfully", del_req->key);
		} else {
			del_rep->status = KREON_FAILURE;
			// log_info("Deleted key %s not found!", del_req->key);
		}
		break;
	}

	case GET_REQUEST:
		value = NULL;
		/*kreon phase*/
		get_req = (msg_get_req *)task->msg->data;
		r_desc = krm_get_region(mydesc, get_req->key, get_req->key_size);

		if (r_desc == NULL) {
			log_fatal("Region not found for key %s", get_req->key);
			exit(EXIT_FAILURE);
		}
		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);
		get_rep = (msg_get_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		// for (int k = 0; k < 10; k++) {
		value = __find_key(r_desc->db, &get_req->key_size);
		// if (value != NULL)
		//	break;
		//}

		if (value == NULL) {
			// log_warn("key not found key %s : length %u", get_req->key,
			// get_req->key_size);

			get_rep->key_found = 0;
			get_rep->bytes_remaining = 0;
			get_rep->value_size = 0;
			get_rep->offset_too_large = 0;
			goto exit;

		} else {
			get_rep->key_found = 1;
			if (get_req->offset > *(uint32_t *)value) {
				get_rep->offset_too_large = 1;
				get_rep->value_size = 0;
				get_rep->bytes_remaining = *(uint32_t *)value;
				goto exit;
			} else
				get_rep->offset_too_large = 0;
			if (!get_req->fetch_value) {
				get_rep->bytes_remaining = *(uint32_t *)value - get_req->offset;
				get_rep->value_size = 0;
				goto exit;
			}
			uint32_t value_bytes_remaining = *(uint32_t *)value - get_req->offset;
			uint32_t bytes_to_read;
			if (get_req->bytes_to_read <= value_bytes_remaining) {
				bytes_to_read = get_req->bytes_to_read;
				get_rep->bytes_remaining = *(uint32_t *)value - (get_req->offset + bytes_to_read);
			} else {
				bytes_to_read = value_bytes_remaining;
				get_rep->bytes_remaining = 0;
			}
			get_rep->value_size = bytes_to_read;
			// log_info("Client wants to read %u will read
			// %u",get_req->bytes_to_read,bytes_to_read);
			memcpy(get_rep->value, value + sizeof(uint32_t) + get_req->offset, bytes_to_read);
		}

	exit:
		/*piggyback info for use with the client*/
		/*finally fix the header*/
		task->reply_msg->type = GET_REPLY;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->pay_len = sizeof(msg_get_rep) + get_rep->value_size;

		actual_reply_size = sizeof(msg_header) + task->reply_msg->pay_len + TU_TAIL_SIZE;
		padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);
		/*set tail to the proper value*/
		*(uint32_t *)((uint64_t)task->reply_msg + actual_reply_size + (padding - TU_TAIL_SIZE)) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;

		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		task->kreon_operation_status = TASK_COMPLETE;
		break;

	case MULTI_GET_REQUEST: {
		msg_value zero_value;
		zero_value.size = 0;

		multi_get = (msg_multi_get_req *)task->msg->data;
		r_desc = krm_get_region(mydesc, multi_get->seek_key, multi_get->seek_key_size);

		if (r_desc == NULL) {
			log_fatal("Region not found for key size %u:%s", multi_get->seek_key_size, multi_get->seek_key);
			exit(EXIT_FAILURE);
		}
		/*create an internal scanner object*/
		sc = (scannerHandle *)malloc(sizeof(scannerHandle));

		if (multi_get->seek_mode != FETCH_FIRST) {
			// log_info("seeking at key %s", multi_get->seek_key);
			init_dirty_scanner(sc, r_desc->db, &multi_get->seek_key_size, multi_get->seek_mode);
		} else {
			// log_info("seeking at key first key of region");
			init_dirty_scanner(sc, r_desc->db, NULL, GREATER_OR_EQUAL);
		}

		/*put the data in the buffer*/
		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);
		msg_multi_get_rep *buf = (msg_multi_get_rep *)((uint64_t)task->reply_msg + sizeof(msg_header));
		buf->curr_entry = 0;
		buf->end_of_region = 0;
		buf->buffer_overflow = 0;

		buf->capacity =
			task->msg->reply_length - (sizeof(msg_header) + sizeof(msg_multi_get_rep) + TU_TAIL_SIZE);
		buf->remaining = buf->capacity;
		buf->pos = 0;
		buf->num_entries = 0;
		if (isValid(sc)) {
			msg_key *key = (struct msg_key *)(getKeyPtr(sc) - sizeof(uint32_t));
			struct msg_value *value = NULL;

			if (multi_get->fetch_keys_only)
				value = (msg_value *)&zero_value;
			else
				value = (struct msg_value *)(getValuePtr(sc) - sizeof(uint32_t));

			if (msg_push_to_multiget_buf(key, value, buf) == KREON_SUCCESS) {
				while (buf->num_entries <= multi_get->max_num_entries) {
					if (getNext(sc) == END_OF_DATABASE) {
						buf->end_of_region = 1;
						break;
					}
					key = (struct msg_key *)(getKeyPtr(sc) - sizeof(uint32_t));
					if (multi_get->fetch_keys_only)
						value = (msg_value *)&zero_value;
					else
						value = (struct msg_value *)(getValuePtr(sc) - sizeof(uint32_t));

					if (msg_push_to_multiget_buf(key, value, buf) == KREON_FAILURE) {
						break;
					}
				}
			}
		} else
			buf->end_of_region = 1;

		closeScanner(sc);
		free(sc);

		/*finally fix the header*/
		task->reply_msg->type = MULTI_GET_REPLY;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->pay_len = sizeof(msg_multi_get_rep) + (buf->capacity - buf->remaining);
		/*set now the actual capacity*/
		buf->capacity = buf->capacity - buf->remaining;
		buf->remaining = buf->capacity;

		actual_reply_size = sizeof(msg_header) + task->reply_msg->pay_len + TU_TAIL_SIZE;
		if (actual_reply_size % MESSAGE_SEGMENT_SIZE == 0)
			padding = 0;
		else
			padding = MESSAGE_SEGMENT_SIZE - (actual_reply_size % MESSAGE_SEGMENT_SIZE);

		/*set tail to the proper value*/
		*(uint32_t *)((uint64_t)task->reply_msg + (actual_reply_size - TU_TAIL_SIZE) + padding) =
			TU_RDMA_REGULAR_MSG;
		task->reply_msg->padding_and_tail = padding + TU_TAIL_SIZE;

		// assert((actual_reply_size + padding) % MESSAGE_SEGMENT_SIZE == 0);
		// assert((actual_reply_size + padding) <= task->msg->reply_length);

		// log_info("actual size %u padding and tail %u pay_len %u buf capacity
		// %u
		// buf remaining %u",
		//	 actual_reply_size, task->reply_msg->padding_and_tail,
		// task->reply_msg->pay_len, buf->capacity,
		//	 buf->remaining);
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;
		task->reply_msg->request_message_local_addr = task->msg->request_message_local_addr;
		assert(task->reply_msg->request_message_local_addr != NULL);
		task->kreon_operation_status = TASK_COMPLETE;
		break;
	}

	case TEST_REQUEST:
		task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
					   (uint64_t)task->msg->reply);
		/*initialize message*/
		if (task->msg->reply_length < TU_HEADER_SIZE) {
			log_fatal("CLIENT reply space not enough  size %" PRIu32 " FIX XXX TODO XXX\n",
				  task->msg->reply_length);
			exit(EXIT_FAILURE);
		}

		/*assert(task->msg->reply_length == 1024);*/
		task->reply_msg->pay_len = task->msg->pay_len;
		task->reply_msg->padding_and_tail = task->msg->padding_and_tail;
		assert(TU_HEADER_SIZE + task->reply_msg->pay_len + task->reply_msg->padding_and_tail ==
		       task->msg->reply_length);
		task->reply_msg->data = NULL;
		task->reply_msg->next = NULL;

		task->reply_msg->type = TEST_REPLY;
		task->reply_msg->receive = TU_RDMA_REGULAR_MSG;
		task->reply_msg->local_offset = (uint64_t)task->msg->reply;
		task->reply_msg->remote_offset = (uint64_t)task->msg->reply;

		task->reply_msg->ack_arrived = KR_REP_PENDING;
		task->reply_msg->request_message_local_addr = NULL;
		task->kreon_operation_status = TASK_COMPLETE;

		uint32_t *tail = (uint32_t *)((uint64_t)task->reply_msg + task->reply_msg->pay_len +
					      task->reply_msg->padding_and_tail - TU_TAIL_SIZE + sizeof(msg_header));
		/*log_info("tail - reply = %"PRId64"\n", (uint64_t)tail -
* (uint64_t)task->reply_msg);*/
		/*log_info("reply_msg = {.pay_len = %llu, padding_and_tail = %llu}",
* task->reply_msg->pay_len, task->reply_msg->padding_and_tail);*/
		*tail = TU_RDMA_REGULAR_MSG;

		/*piggyback info for use with the client*/
		task->reply_msg->request_message_local_addr = task->notification_addr;
		break;

	case TEST_REQUEST_FETCH_PAYLOAD:
		log_fatal("Message not supported yet");
		exit(EXIT_FAILURE);
	default:
		log_fatal("unknown operation %d", task->msg->type);
		exit(EXIT_FAILURE);
	}
	// free_rdma_received_message(rdma_conn, data_message);
	// assert(reply_data_message->request_message_local_addr);
	if (task->kreon_operation_status == TASK_COMPLETE)
		stats_update(task->server_id, task->thread_id);

	return;
}

/*helper functions*/
void _str_split(char *a_str, const char a_delim, uint64_t **core_vector, uint32_t *num_of_cores)
{
	// DPRINT("%s\n",a_str);
	char *tmp = alloca(128);
	char **result = 0;
	size_t count = 0;

	char *last_comma = 0;

	char delim[2];
	int i;

	strcpy(tmp, a_str);
	delim[0] = a_delim;
	delim[1] = 0;

	/* Count how many elements will be extracted. */
	while (*tmp) {
		if (a_delim == *tmp) {
			count++;
			last_comma = tmp;
		}
		tmp++;
	}

	/* Add space for trailing token. */
	count += last_comma < (a_str + strlen(a_str) - 1);
	count++;
	/* Add space for terminating null string so caller
knows where the list of returned strings ends. */

	result = malloc(sizeof(char *) * count);

	*num_of_cores = count - 1;
	*core_vector = (uint64_t *)malloc(sizeof(uint64_t) * count);
	i = 0;

	if (result) {
		size_t idx = 0;
		char *token = strtok(a_str, delim);

		while (token) {
			assert(idx < count);
			*(result + idx++) = strdup(token);
			if (*token != 0x00) {
				(*core_vector)[i] = strtol(token, (char **)NULL, 10);
				// DPRINT("Core id %d = %llu\n",i,(LLU)(*core_vector)[i]);
				++i;
			}
			token = strtok(0, delim);
		}
		assert(idx == count - 1);
		*(result + idx) = 0;
		free(result);
	}
	return;
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
	char *mount_point;
	int num_of_numa_servers = 0;
	int next_argv;
	if (argc >= 6) {
		// argv[0] program name don't care
		// dev name
		next_argv = 1;
		device_name = argv[next_argv];
		globals_set_dev(device_name);
		++next_argv;
		// mount point
		mount_point = argv[next_argv];
		globals_set_mount_point(mount_point);
		++next_argv;
		// zookeeper
		globals_set_zk_host(argv[next_argv]);
		++next_argv;
		// RDMA subnet
		globals_set_RDMA_IP_filter(argv[next_argv]);
		++next_argv;
		num_of_numa_servers = argc - next_argv;

		/*time to allocate the root server*/
		root_server = (struct ds_root_server *)malloc(sizeof(struct ds_root_server) +
							      (num_of_numa_servers * sizeof(struct ds_root_server *)));
		root_server->num_of_numa_servers = num_of_numa_servers;
		int server_idx = 0;
		// now servers <RDMA port, spinning thread, workers>
		for (int i = next_argv; i < argc; i++) {
			cpu_set_t numa_node_affinity;
			CPU_ZERO(&numa_node_affinity);

			int rdma_port;
			int spinning_thread_id;
			int workers_id[MAX_CORES_PER_NUMA];
			int num_workers;
			char *token;
			char *saveptr = NULL;
			char *rest = argv[i];
			// RDMA port of server
			token = strtok_r(rest, ",", &saveptr);
			if (token == NULL) {
				log_fatal("RDMA port missing in server configuration");
				exit(EXIT_FAILURE);
			}
			char *ptr;
			rdma_port = strtol(token, &ptr, 10);
			log_info("Server %d rdma port: %d", server_idx, rdma_port);
			// Spinning thread of server
			token = strtok_r(NULL, ",", &saveptr);
			if (token == NULL) {
				log_fatal("Spinning thread id missing in server configuration");
				exit(EXIT_FAILURE);
			}
			spinning_thread_id = strtol(token, &ptr, 10);
			log_info("Server %d spinning_thread id: %d", server_idx, spinning_thread_id);

			CPU_SET(spinning_thread_id, &numa_node_affinity);
			// now the worker ids
			int idx = 0;
			num_workers = 0;
			token = strtok_r(NULL, ",", &saveptr);
			while (token != NULL) {
				++num_workers;
				workers_id[idx] = strtol(token, &ptr, 10);
				CPU_SET(workers_id[idx], &numa_node_affinity);
				++idx;
				token = strtok_r(NULL, ",", &saveptr);
			}
			if (num_workers > 0) {
				log_info("Server %d workers follow ", server_idx);
				for (int k = 0; k < num_workers; k++)
					printf("%d ", workers_id[k]);
				printf("\n");
			} else {
				log_fatal("No workers specified for Server %d", server_idx);
				exit(EXIT_FAILURE);
			}

			// now we have all info to allocate ds_numa_server, pin,
			// and inform the root server

			struct ds_numa_server *server = (struct ds_numa_server *)malloc(
				sizeof(struct ds_numa_server) + (num_workers * sizeof(struct ds_worker_thread)));

			// But first let's build each numa server's RDMA channel*/
			/*RDMA channel staff*/
			server->channel = (struct channel_rdma *)malloc(sizeof(struct channel_rdma));
			if (server->channel == NULL) {
				log_fatal("malloc failed could do not get memory for channel");
				exit(EXIT_FAILURE);
			}
			server->channel->sockfd = 0;
			server->channel->context = open_ibv_device(DEFAULT_DEV_IBV);

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
			// int status =
			//	pthread_setaffinity_np(server->poll_cq_cnxt, sizeof(cpu_set_t),
			//&numa_node_affinity);
			// if (status != 0) {
			//	log_fatal("failed to pin poll_cq thread");
			//	exit(EXIT_FAILURE);
			//}
			log_info("Started and set affinity for poll_cq thread of server %d "
				 "at port %d",
				 server->server_id, server->rdma_port);

			server->channel->dynamic_pool =
				mrpool_create(server->channel->pd, -1, DYNAMIC, MEM_REGION_BASE_SIZE);
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
			for (int j = 0; j < num_workers; j++)
				server->spinner.worker[j].worker_id = workers_id[j];
			root_server->numa_servers[server_idx] = server;
			++server_idx;
		}
	} else {
		log_fatal("Error! usage: ./kreon_server <device name> <mount "
			  "point> <zk_host:zk_port> <RDMA_IP_subnet> <server(s) vector>\n "
			  " where server(s) vector is \"<RDMA_PORT>,<Spinning thread core "
			  "id>,<worker id 1>,<worker id 2>,...,<worker id N>\"");
		exit(EXIT_FAILURE);
	}
	/**
* list of chain reaction: main fires socket threads (one per server) and
* then
* spinners.
* Spinners finally fire up their workers
**/
	for (int i = 0; i < root_server->num_of_numa_servers; i++) {
		struct ds_numa_server *server = root_server->numa_servers[i];
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
		int status = pthread_setaffinity_np(root_server->numa_servers[i]->socket_thread_cnxt, sizeof(cpu_set_t),
						    &numa_node_affinity);
		if (status != 0) {
			log_fatal("failed to pin socket thread for server %d at port %d", server->server_id,
				  server->rdma_port);
			exit(EXIT_FAILURE);
		}

		log_info("Started socket thread successfully for Server %d", server->server_id);

		log_info("Starting spinning thread for Server %d", root_server->numa_servers[i]->server_id);
		if (pthread_create(&server->spinner_cnxt, NULL, server_spinning_thread_kernel, &server->spinner) != 0) {
			log_fatal("Failed to create spinner thread for Server: %d", server->server_id);
			exit(EXIT_FAILURE);
		}

		// spinning thread pin staff
		log_info("Pinning spinning thread of Server: %d at port %d at core %d", server->server_id,
			 server->rdma_port, server->spinner.spinner_id);
		cpu_set_t spinning_thread_affinity_mask;
		CPU_ZERO(&spinning_thread_affinity_mask);
		CPU_SET(server->spinner.spinner_id, &spinning_thread_affinity_mask);
		status =
			pthread_setaffinity_np(server->spinner_cnxt, sizeof(cpu_set_t), &spinning_thread_affinity_mask);
		if (status != 0) {
			log_fatal("failed to pin spinning thread");
			exit(EXIT_FAILURE);
		}
		log_info("Pinned successfully spinning thread of Server: %d at port %d at "
			 "core %d",
			 server->server_id, server->rdma_port, server->spinner.spinner_id);

		root_server->numa_servers[i]->meta_server.root_server_id = i;
		log_info("Initializing kreonR metadata server");
		if (pthread_create(&root_server->numa_servers[i]->meta_server_cnxt, NULL, krm_metadata_server,
				   &root_server->numa_servers[i]->meta_server)) {
			log_fatal("Failed to start metadata_server");
			exit(EXIT_FAILURE);
		}
	}

	stats_init(root_server->num_of_numa_servers, root_server->numa_servers[0]->spinner.num_workers);
	// A long long
	sem_init(&exit_main, 0, 0);

	log_info("Kreon server(S) ready");
	if (signal(SIGINT, sigint_handler) == SIG_ERR) {
		log_fatal("can't catch SIGINT");
		exit(EXIT_FAILURE);
	}
	sem_wait(&exit_main);
	log_info("kreonR server exiting");
	return 0;
}
