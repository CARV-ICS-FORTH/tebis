#include "rdma.h"
#include "memory_region_pool.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdint.h>
#define _GNU_SOURCE /* See feature_test_macros(7) */
#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <immintrin.h>
#include <limits.h>
#include <malloc.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "../kreon_server/djb2.h"
#include "../kreon_server/globals.h"
#include "../kreon_server/messages.h"
#include "../kreon_server/metadata.h"
#include "get_clock.h"

#include "../utilities/circular_buffer.h"
#include "../utilities/simple_concurrent_list.h"
#include "../utilities/spin_loop.h"
#include <log.h>

#define CTX_HANDSHAKE_SUCCESS 0
#define CTX_HANDSHAKE_FAILURE 1
#define MAX_COMPLETION_ENTRIES 32

int LIBRARY_MODE = SERVER_MODE; /*two modes for the communication rdma library SERVER and CLIENT*/

int assign_job_to_worker(struct channel_rdma *channel, struct connection_rdma *conn, msg_header *msg,
			 int spinning_thread_id, int sockfd);
uint64_t wake_up_workers_operations = 0;

uint64_t *spinning_threads_core_ids;
uint32_t num_of_spinning_threads;
uint32_t num_of_worker_threads;

/*one port for waiitng client connections, another for waiting connections from
 * other servers*/

void crdma_add_connection_channel(struct channel_rdma *channel, struct connection_rdma *conn);

void check_pending_ack_message(struct connection_rdma *conn);
void force_send_ack(struct connection_rdma *conn);

void on_completion_server(struct rdma_message_context *msg_ctx);

msg_header *triggering_msg_offt_to_real_address(connection_rdma *conn, uint32_t offt)
{
	return (msg_header *)&conn->send_circular_buf->memory_region[offt];
}

uint32_t real_address_to_triggering_msg_offt(connection_rdma *conn, struct msg_header *msg)
{
	return (uint64_t)msg - (uint64_t)conn->send_circular_buf->memory_region;
}

void set_receive_field(struct msg_header *msg, uint8_t value)
{
	msg->receive = value;

	if (!msg->payload_length)
		return;

	struct msg_header *last_msg_header =
		(struct msg_header *)((char *)msg + msg->payload_length + msg->padding_and_tail_size);
	last_msg_header->receive = value;
}

uint8_t get_receive_field(volatile struct msg_header *msg)
{
	struct msg_header *last_msg_header =
		(struct msg_header *)((char *)msg + msg->payload_length + msg->padding_and_tail_size);
	return last_msg_header->receive;
}

static int is_a_send_request(int message_type)
{
	switch (message_type) {
	case PUT_REQUEST:
	case PUT_IF_EXISTS_REQUEST:
	case GET_REQUEST:
	case MULTI_GET_REQUEST:
	case DELETE_REQUEST:
	case TEST_REQUEST:
	case NO_OP:
	case DISCONNECT:
		return 1;
	case TEST_REPLY:
	case PUT_REPLY:
	case GET_REPLY:
	case MULTI_GET_REPLY:
	case DELETE_REPLY:
	case NO_OP_ACK:
		return 0;
	default:
		log_fatal("unknown message type %d", message_type);
		_exit(EXIT_FAILURE);
	}
}

msg_header *client_allocate_rdma_message(connection_rdma *conn, int message_payload_size, int message_type)
{
	uint32_t message_size = MESSAGE_SEGMENT_SIZE;
	uint32_t padding = 0;
	if (message_payload_size > 0) {
		message_size = TU_HEADER_SIZE + message_payload_size + TU_TAIL_SIZE;
		if (message_size % MESSAGE_SEGMENT_SIZE != 0) {
			/*need to pad */
			padding = (MESSAGE_SEGMENT_SIZE - (message_size % MESSAGE_SEGMENT_SIZE));
			message_size += padding;
		}
	}

	assert(message_size % MESSAGE_SEGMENT_SIZE == 0);

	uint8_t is_send_request = is_a_send_request(message_type);

	circular_buffer *c_buf = is_send_request ? conn->send_circular_buf : conn->recv_circular_buf;
	char *addr = NULL;
	uint8_t space_not_ready = 0;
	while (1) {
		if (space_not_ready) {
			pthread_mutex_lock(&conn->allocation_lock);
			space_not_ready = 0;
		}

		circular_buffer_op_status stat = allocate_space_from_circular_buffer(c_buf, message_size, &addr);

		switch (stat) {
		case ALLOCATION_IS_SUCCESSFULL:
			goto init_message;

		case NOT_ENOUGH_SPACE_AT_THE_END:
			if (is_send_request)
				return NULL;

			reset_circular_buffer(c_buf);
			break;
		case SPACE_NOT_READY_YET:
			space_not_ready = 1;
			pthread_mutex_unlock(&conn->allocation_lock);
			break;

		default:
			log_fatal("Unhandled state");
			_exit(EXIT_FAILURE);
		}
	}

	struct msg_header *msg = NULL;

init_message:
	msg = (msg_header *)addr;
	msg->payload_length = 0;
	msg->padding_and_tail_size = 0;
	if (message_payload_size) {
		msg->payload_length = message_payload_size;
		msg->padding_and_tail_size = padding + TU_TAIL_SIZE;
	}

	if (is_send_request)
		msg->msg_type = message_type;
	else
		msg->msg_type = UINT16_MAX;
	msg->offset_in_send_and_target_recv_buffers = (uint64_t)msg - (uint64_t)c_buf->memory_region;

	uint8_t expected_receive_field = is_send_request ? TU_RDMA_REGULAR_MSG : 0;
	set_receive_field(msg, expected_receive_field);
	return msg;
}

int send_rdma_message_busy_wait(connection_rdma *conn, msg_header *msg)
{
	return __send_rdma_message(conn, msg, NULL);
}

void on_completion_client(struct rdma_message_context *msg_ctx);

int client_send_rdma_message(struct connection_rdma *conn, struct msg_header *msg)
{
	struct rdma_message_context msg_ctx;
	client_rdma_init_message_context(&msg_ctx, msg);
	msg_ctx.on_completion_callback = on_completion_client;
	int ret = __send_rdma_message(conn, msg, &msg_ctx);
	if (ret == KREON_SUCCESS) {
		if (!client_rdma_send_message_success(&msg_ctx))
			ret = KREON_FAILURE;
	}
	return ret;
}

int __send_rdma_message(connection_rdma *conn, msg_header *msg, struct rdma_message_context *msg_ctx)
{
	size_t msg_len = TU_HEADER_SIZE + msg->payload_length + msg->padding_and_tail_size;
	if (!msg->payload_length)
		msg_len = TU_HEADER_SIZE;

	/* *
   * do we want to associate any context with the message aka let the completion
   * thread perform any operation
   * for us? For client PUT,GET,MULTI_GET,UPDATE,DELETE we don't
   * */
	void *context = NULL;
	if (!msg_ctx && LIBRARY_MODE == SERVER_MODE) {
		switch (msg->msg_type) {
		case PUT_REQUEST:
		case GET_REQUEST:
		case MULTI_GET_REQUEST:
		case DELETE_REQUEST:
		case TEST_REQUEST:
		case TEST_REQUEST_FETCH_PAYLOAD:
		case GET_LOG_BUFFER_REQ:
		case GET_LOG_BUFFER_REP:
		case FLUSH_COMMAND_REQ:
		case FLUSH_COMMAND_REP:
		case REPLICA_INDEX_GET_BUFFER_REQ:
		case REPLICA_INDEX_GET_BUFFER_REP:
		case REPLICA_INDEX_FLUSH_REQ:
		case REPLICA_INDEX_FLUSH_REP:
		case PUT_REPLY:
		case GET_REPLY:
		case NO_OP:
		case MULTI_GET_REPLY:
		case DELETE_REPLY:
		case TEST_REPLY:
		case TEST_REPLY_FETCH_PAYLOAD:
		case NO_OP_ACK:
			context = NULL;
			break;
		default:
			assert(0);
			log_fatal("Unhandled message type %d", msg->msg_type);
			_exit(EXIT_FAILURE);
		}
	} else {
		context = (void *)msg_ctx;
	}

#if VALIDATE_CHECKSUMS
	if (LIBRARY_MODE == CLIENT_MODE) {
		msg->session_id =
			djb2_hash((unsigned char *)&msg->offset_reply_in_recv_buffer, msg_len - sizeof(uint64_t));
	}
#endif
	while (1) {
		int ret = rdma_post_write(conn->rdma_cm_id, context, msg, msg_len,
					  conn->rdma_memory_regions->local_memory_region, IBV_SEND_SIGNALED,
					  ((uint64_t)conn->peer_mr->addr + msg->offset_in_send_and_target_recv_buffers),
					  conn->peer_mr->rkey);
		if (!ret) {
			break;
		}

		if (conn->status == CONNECTION_ERROR) {
			log_fatal("connection failed !: %s\n", strerror(errno));
			_exit(EXIT_FAILURE);
		}
	}

	return KREON_SUCCESS;
}

void client_free_rpc_pair(connection_rdma *conn, volatile msg_header *reply)
{
	msg_header *request = triggering_msg_offt_to_real_address(conn, reply->triggering_msg_offset_in_send_buffer);

	free_space_from_circular_buffer(conn->recv_circular_buf, (char *)reply, request->reply_length_in_recv_buffer);

	uint32_t size = MESSAGE_SEGMENT_SIZE;
	if (request->payload_length)
		size = TU_HEADER_SIZE + request->payload_length + request->padding_and_tail_size;

	assert(size % MESSAGE_SEGMENT_SIZE == 0);

	free_space_from_circular_buffer(conn->send_circular_buf, (char *)request, size);
}

/*Disconnect*/
void disconnect_and_close_connection(connection_rdma *conn)
{
	//msg_header *disconnect_request = allocate_rdma_message(conn, 0, DISCONNECT);
	/* XXX FIXME XXX fill the disconect function*/
	(void)conn;
	log_debug("REMINDER fix me");
	_exit(EXIT_FAILURE);
	/*send_rdma_message(conn, disconnect_request);
	log_warn("Successfully sent disconnect message, bye bye Caution! Missing "
	       "deallocation of resources follows...\n");
	close_and_free_RDMA_connection(conn->channel, conn);
	*/
}

struct ibv_context *get_rdma_device_context(char *devname)
{
	int num_devices;
	struct ibv_context **dev_list = rdma_get_devices(&num_devices);
	struct ibv_context *rdma_dev = NULL;

	if (num_devices < 1) {
		log_fatal("No RDMA device found. Exiting..");
		exit(EXIT_FAILURE);
	}

	if (!devname) {
		log_info("Using default RDMA device %s", dev_list[0]->device->name);
		return dev_list[0];
	}

	for (int i = 0; i < num_devices; ++i)
		if (!strncmp(dev_list[i]->device->name, devname, strlen(devname))) {
			rdma_dev = dev_list[i];
			break;
		}

	if (!rdma_dev) {
		log_fatal("Cannot find RDMA device %s", devname);
		_exit(EXIT_FAILURE);
	}

	rdma_free_devices(dev_list);

	return rdma_dev;
}

void crdma_init_client_connection_list_hosts(connection_rdma *conn, char **hosts, const int num_hosts,
					     struct channel_rdma *channel, connection_type type)
{
	(void)num_hosts;
	struct ibv_qp_init_attr qp_init_attr;
	if (!strcmp(hosts[0], "127.0.0.1")) {
		log_warn("Connection with local host?");
		return;
	}

	conn->channel = channel;
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.cap.max_send_wr = qp_init_attr.cap.max_recv_wr = MAX_WR;
	qp_init_attr.cap.max_send_sge = qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.cap.max_inline_data = 16;
	qp_init_attr.sq_sig_all = 1;
	qp_init_attr.send_cq = qp_init_attr.recv_cq =
		ibv_create_cq(channel->context, MAX_WR, (void *)conn, channel->comp_channel, 0);
	ibv_req_notify_cq(qp_init_attr.send_cq, 0);
	assert(qp_init_attr.send_cq);
	qp_init_attr.qp_type = IBV_QPT_RC;

	struct rdma_addrinfo hints, *res;
	memset(&hints, 0, sizeof hints);
	hints.ai_port_space = RDMA_PS_TCP;

	char host[1024];
	strcpy(host, hosts[0]);
	char *ip = host;
	char *port;
	char *colon;
	for (colon = host; *colon != ':'; ++colon)
		;
	*colon = '\0';
	port = colon + 1;

	log_info("Connecting to %s at port %s", ip, port);
	int ret = rdma_getaddrinfo(ip, port, &hints, &res);
	if (ret) {
		log_fatal("rdma_getaddrinfo: %s, %s\n", hosts[0], strerror(errno));
		exit(EXIT_FAILURE);
	}

	struct rdma_cm_id *rdma_cm_id;
	// FIXME Need to use channel->pd here instead of NULL
	ret = rdma_create_ep(&rdma_cm_id, res, NULL, &qp_init_attr);
	if (ret) {
		log_fatal("rdma_create_ep: %s", strerror(errno));
		/*raise(SIGINT);*/
		exit(EXIT_FAILURE);
	}
	conn->rdma_cm_id = rdma_cm_id;

	// TODO Check the private data functionality of the connection parameters!!!
	struct rdma_conn_param conn_param;
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.flow_control = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;
	int tries = 0;

	while (tries < 100) {
		ret = rdma_connect(rdma_cm_id, &conn_param);
		if (ret) {
			log_warn("rdma_connect failed reconnecting: %s", strerror(errno));
			usleep(50000);
			++tries;
		} else
			break;
	}
	if (ret) {
		log_fatal("rdma_connect failed: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}
	conn->peer_mr = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
	memset(conn->peer_mr, 0, sizeof(struct ibv_mr));
	struct ibv_mr *recv_mr = rdma_reg_msgs(rdma_cm_id, conn->peer_mr, sizeof(struct ibv_mr));
	struct rdma_message_context msg_ctx;
	client_rdma_init_message_context(&msg_ctx, NULL);
	msg_ctx.on_completion_callback = on_completion_client;
	ret = rdma_post_recv(rdma_cm_id, &msg_ctx, conn->peer_mr, sizeof(struct ibv_mr), recv_mr);
	if (ret) {
		log_fatal("rdma_post_recv: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	struct ibv_mr *send_mr = rdma_reg_msgs(rdma_cm_id, &type, sizeof(type));
	ret = rdma_post_send(rdma_cm_id, NULL, &type, sizeof(type), send_mr, 0);
	if (ret) {
		log_fatal("rdma_post_send: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}
	/*free(host_copy);*/
	switch (type) {
	case MASTER_TO_REPLICA_CONNECTION:
		log_debug("Remote side accepted created a new MASTER_TO_REPLICA_CONNECTION");
		conn->type = MASTER_TO_REPLICA_CONNECTION;
		conn->rdma_memory_regions = mrpool_allocate_memory_region(channel->dynamic_pool, rdma_cm_id);
		break;
	case CLIENT_TO_SERVER_CONNECTION:
		// log_info("Remote side accepted created a new
		// CLIENT_TO_SERVER_CONNECTION");
		conn->type = CLIENT_TO_SERVER_CONNECTION;
		conn->rdma_memory_regions = mrpool_allocate_memory_region(channel->dynamic_pool, rdma_cm_id);
		break;
	case REPLICA_TO_MASTER_CONNECTION:
	case SERVER_TO_CLIENT_CONNECTION:
		log_warn("Should not handle this kind of connection here");
		break;
	default:
		log_fatal("BAD connection type");
		exit(EXIT_FAILURE);
	}
	conn->remaining_bytes_in_remote_rdma_region = conn->rdma_memory_regions->memory_region_length;
	conn->rendezvous = conn->rdma_memory_regions->remote_memory_buffer;

	// Block until server sends memory region information
	sem_wait(&msg_ctx.wait_for_completion);
	rdma_dereg_mr(send_mr);
	rdma_dereg_mr(recv_mr);

	send_mr = rdma_reg_msgs(rdma_cm_id, conn->rdma_memory_regions->remote_memory_region, sizeof(struct ibv_mr));

	// Send memory region information
	ret = rdma_post_send(rdma_cm_id, NULL, conn->rdma_memory_regions->remote_memory_region, sizeof(struct ibv_mr),
			     send_mr, 0);
	if (ret) {
		log_fatal("rdma_post_send: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	conn->status = CONNECTION_OK;
	/*zero all memory*/
	memset(conn->rdma_memory_regions->local_memory_buffer, 0x00, conn->rdma_memory_regions->memory_region_length);
	if (sem_init(&conn->congestion_control, 0, 0) != 0) {
		log_fatal("failed to initialize semaphore reason follows");
		perror("Reason: ");
	}
	conn->sleeping_workers = 0;
	// conn->pending_sent_messages = 0;
	// conn->pending_received_messages = 0;
	conn->offset = 0;
	conn->qp = conn->rdma_cm_id->qp;

#ifdef CONNECTION_BUFFER_WITH_MUTEX_LOCK
	pthread_mutex_init(&conn->buffer_lock, NULL);
	pthread_mutex_init(&conn->allocation_lock, NULL);
#else
	pthread_spin_init(&conn->buffer_lock, PTHREAD_PROCESS_PRIVATE);
#endif
	switch (conn->type) {
	case CLIENT_TO_SERVER_CONNECTION:
		log_debug("Initializing client communication circular buffer");
		conn->send_circular_buf =
			create_and_init_circular_buffer(conn->rdma_memory_regions->local_memory_buffer,
							conn->peer_mr->length, MESSAGE_SEGMENT_SIZE, SEND_BUFFER);
		conn->recv_circular_buf =
			create_and_init_circular_buffer(conn->rdma_memory_regions->remote_memory_buffer,
							conn->peer_mr->length, MESSAGE_SEGMENT_SIZE, RECEIVE_BUFFER);
		conn->reset_point = 0;
		/*Inform the server that you are a client, patch but now I am in a hurry*/
		// log_info("CLIENT: Informing server that I am a client and about my
		// control location\n");
		/*control info*/
		break;

	case MASTER_TO_REPLICA_CONNECTION:
		log_debug("Initializing master to replica communication circular buffer");
		conn->send_circular_buf =
			create_and_init_circular_buffer(conn->rdma_memory_regions->local_memory_buffer,
							conn->peer_mr->length, MESSAGE_SEGMENT_SIZE, SC_SEND_BUFFER);
		conn->recv_circular_buf =
			create_and_init_circular_buffer(conn->rdma_memory_regions->remote_memory_buffer,
							conn->peer_mr->length, MESSAGE_SEGMENT_SIZE, SC_RECEIVE_BUFFER);
		conn->reset_point = 0;
		break;

	default:
		conn->send_circular_buf = NULL;
		conn->recv_circular_buf = NULL;
		conn->reset_point = 0;
		crdma_add_connection_channel(channel, conn);
		conn->send_circular_buf = NULL;
		conn->recv_circular_buf = NULL;
		conn->reset_point = 0;
		crdma_add_connection_channel(channel, conn);
	}
	__sync_fetch_and_add(&channel->nused, 1);
}

void crdma_init_generic_create_channel(struct channel_rdma *channel, char *ib_devname)
{
	channel->sockfd = 0;
	if (ib_devname)
		channel->context = get_rdma_device_context(ib_devname);
	else
		channel->context = get_rdma_device_context(NULL);

	channel->comp_channel = ibv_create_comp_channel(channel->context);
	if (channel->comp_channel == 0) {
		log_fatal("building context reason follows:");
		perror("Reason: \n");
		_exit(EXIT_FAILURE);
	}

	channel->pd = ibv_alloc_pd(channel->context);
	channel->nconn = 0;
	channel->nused = 0;
	channel->connection_created = NULL;

	if (LIBRARY_MODE == CLIENT_MODE) {
		channel->static_pool = NULL;
		channel->dynamic_pool = mrpool_create(channel->pd, -1, DYNAMIC, MEM_REGION_BASE_SIZE);

		pthread_mutex_init(&channel->spin_conn_lock,
				   NULL); // Lock for the conn_list
		channel->spinning_th = 0;
		channel->spinning_conn = 0;
		// log_info("Client: setting spinning threads number to 1");
		channel->spinning_num_th = 1;

		assert(channel->spinning_num_th <= SPINNING_NUM_TH);

		for (int i = 0; i < channel->spinning_num_th; i++) {
			pthread_mutex_init(&channel->spin_list_conn_lock[i], NULL);
			channel->spin_list[i] = init_simple_concurrent_list();
		}

		for (int i = 0; i < channel->spinning_num_th; i++) {
			channel->spin_list[i] = init_simple_concurrent_list();
			channel->spin_num[i] = 0;
			sem_init(&channel->sem_spinning[i], 0, 0);
			spinning_thread_parameters *params =
				(spinning_thread_parameters *)malloc(sizeof(spinning_thread_parameters));
			params->channel = channel;
			params->spinning_thread_id = i;
		}
	}
	/*Creating the thread in charge of the completion channel*/
	if (pthread_create(&channel->cq_poller_thread, NULL, poll_cq, channel) != 0) {
		log_fatal("Failed to create poll_cq thread reason follows:");
		perror("Reason: \n");
		_exit(EXIT_FAILURE);
	}
}

struct channel_rdma *crdma_client_create_channel(char *ib_devname)
{
	struct channel_rdma *channel;

	channel = malloc(sizeof(*channel));

	if (SPINNING_NUM_TH_CLI > SPINNING_NUM_TH)
		channel->spinning_num_th = SPINNING_NUM_TH;
	else
		channel->spinning_num_th = SPINNING_NUM_TH_CLI;

	crdma_init_generic_create_channel(channel, ib_devname);

	return channel;
}

connection_rdma *crdma_client_create_connection_list_hosts(struct channel_rdma *channel, char **hosts, int num_hosts,
							   connection_type type)
{
	connection_rdma *conn;
	/*allocate memory for connection*/
	conn = malloc(sizeof(connection_rdma));
	if (conn == NULL) {
		log_fatal("FATAL ERROR malloc failed\n");
		exit(EXIT_FAILURE);
	}
	memset(conn, 0, sizeof(struct connection_rdma));
	crdma_init_client_connection_list_hosts(conn, hosts, num_hosts, channel, type);
	return conn;
}

void crdma_add_connection_channel(struct channel_rdma *channel, struct connection_rdma *conn)
{
	conn->idconn = channel->nconn;
	// conn->local_mrq->idconn =  conn->idconn;
	++channel->nconn;
	int idx;
	channel->spinning_conn++;
	idx = channel->spinning_conn % channel->spinning_num_th;
	pthread_mutex_lock(&channel->spin_list_conn_lock[idx]);
	/*gesalous new policy*/
	add_last_in_simple_concurrent_list(channel->spin_list[idx], conn);
	conn->responsible_spin_list = channel->spin_list[idx];
	conn->responsible_spinning_thread_id = idx;
	channel->spin_num[idx]++; /*WTF is this? gesalous*/

	pthread_mutex_unlock(&channel->spin_list_conn_lock[idx]);
	sem_post(&channel->sem_spinning[idx]);
}

void ec_sig_handler(int signo)
{
	(void)signo;
	struct sigaction sa = { 0 };

	sigemptyset(&sa.sa_mask);
	sa.sa_handler = ec_sig_handler;
	sigaction(SIGINT, &sa, 0);
}

void zero_rendezvous_locations_l(volatile msg_header *msg, uint32_t length)
{
	assert(length % MESSAGE_SEGMENT_SIZE == 0);
	uint32_t num_of_msg_headers = length / MESSAGE_SEGMENT_SIZE;
	for (uint32_t i = 0; i < num_of_msg_headers; i++) {
		msg[i].receive = 0;
		msg[i].msg_type = UINT16_MAX;
	}
}

void zero_rendezvous_locations(volatile msg_header *msg)
{
	/*for acks that fit entirely in a header payload_length and padding_and_tail_size could be
   * 0.
   * This is ok because we want to ommit sending extra useless bytes. However we
   * need to
   * zero their virtual tail which the initiator has allocated for safety
   */

	uint32_t msg_length = 0;
	if (msg->payload_length > 0 || msg->padding_and_tail_size > 0)
		msg_length = TU_HEADER_SIZE + msg->payload_length + msg->padding_and_tail_size;
	else
		msg_length = MESSAGE_SEGMENT_SIZE;

	zero_rendezvous_locations_l(msg, msg_length);
}

uint32_t wait_for_payload_arrival(msg_header *hdr)
{
	int message_size = 0;
	if (hdr->payload_length > 0) {
		if (get_receive_field(hdr) != TU_RDMA_REGULAR_MSG)
			return 0;
		message_size = MESSAGE_SEGMENT_SIZE + hdr->payload_length + hdr->padding_and_tail_size;
	} else
		/*it's only a header*/
		message_size = MESSAGE_SEGMENT_SIZE;

	return message_size;
}

void update_rendezvous_location(connection_rdma *conn, uint32_t message_size)
{
	assert(message_size % MESSAGE_SEGMENT_SIZE == 0);

	if (conn->type == SERVER_TO_CLIENT_CONNECTION || conn->type == REPLICA_TO_MASTER_CONNECTION) {
		if (message_size < MESSAGE_SEGMENT_SIZE) {
			message_size = MESSAGE_SEGMENT_SIZE;
		}
		if (((uint64_t)conn->rendezvous + message_size) >=
		    ((uint64_t)conn->rdma_memory_regions->remote_memory_buffer +
		     conn->rdma_memory_regions->memory_region_length)) {
			conn->rendezvous = (void *)conn->rdma_memory_regions->remote_memory_buffer;
			// log_info("silent reset");
		} else
			conn->rendezvous = (void *)((uint64_t)conn->rendezvous + (uint32_t)message_size);
	} else {
		log_fatal("Faulty connection type");
		_exit(EXIT_FAILURE);
	}
}

void close_and_free_RDMA_connection(struct channel_rdma *channel, struct connection_rdma *conn)
{
	conn->status = CONNECTION_CLOSING;

	mrpool_free_memory_region(&conn->rdma_memory_regions);

	/*remove connection from its corresponding list*/
	pthread_mutex_lock(&channel->spin_list_conn_lock[conn->responsible_spinning_thread_id]);
	mark_element_for_deletion_from_simple_concurrent_list(conn->responsible_spin_list, conn);
	DPRINT("\t * Removed connection form list successfully :-) \n");
	channel->nconn--;
	channel->nused--;
	pthread_mutex_unlock(&channel->spin_list_conn_lock[conn->responsible_spinning_thread_id]);
	conn->channel = NULL;
	ibv_destroy_cq(conn->rdma_cm_id->qp->send_cq);
	rdma_destroy_qp(conn->rdma_cm_id);
	rdma_destroy_id(conn->rdma_cm_id);

	free(conn);
	DPRINT("\t*Destroyed RDMA connection successfully\n");
}

void client_rdma_init_message_context(struct rdma_message_context *msg_ctx, struct msg_header *msg)
{
	memset(msg_ctx, 0, sizeof(*msg_ctx));
	msg_ctx->msg = msg;
	sem_init(&msg_ctx->wait_for_completion, 0, 0);
	msg_ctx->__is_initialized = 1;
}

bool client_rdma_send_message_success(struct rdma_message_context *msg_ctx)
{
	assert(msg_ctx->__is_initialized == 1);
	// on_completion_client will post this semaphore once it gets to the CQE for
	// this message
	sem_wait(&msg_ctx->wait_for_completion);
	assert((uint64_t)msg_ctx == msg_ctx->wc.wr_id);
	if (msg_ctx->wc.status == IBV_WC_SUCCESS)
		return true;
	else
		return false;
}

static void error_to_string(int error)
{
	switch (error) {
	case IBV_WC_REM_ACCESS_ERR:
		log_fatal("Remote memory error");
		break;
	case IBV_WC_LOC_ACCESS_ERR:
		log_fatal("Local memory error");
		break;
	default:
		log_fatal("Unknown error code");
	}
}

void *poll_cq(void *arg)
{
	struct sigaction sa;
	struct channel_rdma *channel;
	struct connection_rdma *conn;
	struct ibv_cq *cq;
	struct ibv_wc wc[MAX_COMPLETION_ENTRIES];
	void *ev_ctx;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = ec_sig_handler;
	sigaction(SIGINT, &sa, 0);

	pthread_setname_np(pthread_self(), "poll_cq thread");
	channel = (struct channel_rdma *)arg;

	while (1) {
		if (ibv_get_cq_event(channel->comp_channel, &cq, &ev_ctx) != 0) {
			log_fatal("polling cq failure reason follows");
			perror("Reason: \n");
			_exit(EXIT_FAILURE);
		}
		ibv_ack_cq_events(cq, 1);
		if (ibv_req_notify_cq(cq, 0) != 0) {
			perror("ERROR poll_cq: ibv_req_notify_cq\n");
			_exit(EXIT_FAILURE);
		}

		while (1) {
			int rc = ibv_poll_cq(cq, MAX_COMPLETION_ENTRIES, wc);
			if (rc < 0) {
				log_fatal("poll of completion queue failed!");
				exit(EXIT_FAILURE);
			} else if (rc > 0) {
				conn = (connection_rdma *)cq->cq_context;
				assert(conn);
				for (int i = 0; i < rc; i++) {
					struct rdma_message_context *msg_ctx =
						(struct rdma_message_context *)wc[i].wr_id;

					if (wc[i].status != IBV_WC_SUCCESS) {
						error_to_string(wc[i].status);
						assert(0);
					}

					if (msg_ctx && msg_ctx->on_completion_callback) {
						memcpy(&msg_ctx->wc, &wc[i], sizeof(struct ibv_wc));
						msg_ctx->on_completion_callback(msg_ctx);
					}
					memset(&wc[i], 0x00, sizeof(struct ibv_wc));
				}
			} else
				break;
		}
	}
	return NULL;
}

void on_completion_server(struct rdma_message_context *msg_ctx)
{
	struct ibv_wc *wc = &msg_ctx->wc;
	struct connection_rdma *conn = (struct connection_rdma *)msg_ctx->args;
	assert(LIBRARY_MODE == SERVER_MODE);
	if (wc->status == IBV_WC_SUCCESS) {
		switch (wc->opcode) {
		case IBV_WC_SEND:
			// log_info("IBV_WC_SEND code id of connection %d", conn->idconn);
			break;
		case IBV_WC_RECV:
			// log_info("IBV_WC_RECV code id of connection %d", conn->idconn);
			break;
		case IBV_WC_RDMA_WRITE:;
			struct msg_header *msg = msg_ctx->msg;
			if (msg) {
				switch (msg->msg_type) {
				/*server to server new school*/
				case GET_LOG_BUFFER_REQ:
				case GET_LOG_BUFFER_REP:
				case FLUSH_COMMAND_REQ:
				case FLUSH_COMMAND_REP:
					break;
				/*client to server RPCs*/
				case DISCONNECT:
					break;
				default:
					log_fatal("Entered unplanned state FATAL for message type %d", msg->msg_type);
					_exit(EXIT_FAILURE);
				}
			}
			break;
		}

		case IBV_WC_RDMA_READ:
			log_debug("IBV_WC_RDMA_READ code");
			break;
		case IBV_WC_COMP_SWAP:
			log_debug("IBV_WC_COMP_SWAP code");
			break;
		case IBV_WC_FETCH_ADD:
			log_debug("IBV_WC_FETCH_ADD code");
			break;
		case IBV_WC_BIND_MW:
			log_debug("IBV_WC_BIND_MW code");
			break;
		case IBV_WC_RECV_RDMA_WITH_IMM:
			log_debug("IBV_WC_RECV_RDMA_WITH_IMM code");
			break;
		default:
			log_fatal("FATAL unknown code");
			/*exit(EXIT_FAILURE);*/
		}
	} else {
		/*error handling*/
		log_fatal("conn type is %d %s\n", conn->type, ibv_wc_status_str(wc->status));
		conn->status = CONNECTION_ERROR;
		/*raise(SIGINT);*/
		_exit(KREON_FAILURE);
	}

	free(msg_ctx);
}

/* Set as the on_completion_callback for channels in client library mode.  * It
passes the work completion struct to the client through the message's * context
and posts the * semaphore used to block until the message's completion has
arrived */
void on_completion_client(struct rdma_message_context *msg_ctx)
{
	sem_post(&msg_ctx->wait_for_completion);
}

uint32_t calc_offset_in_send_and_target_recv_buffer(struct msg_header *msg, circular_buffer *c_buf)
{
	return (uint64_t)msg - (uint64_t)c_buf->memory_region;
}
