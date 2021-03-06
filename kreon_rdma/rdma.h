#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <semaphore.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include "../kreon_server/conf.h"
#include "../kreon_server/messages.h"
#include "../utilities/circular_buffer.h"
#include "../utilities/macros.h"
#include "../utilities/queue.h"
#include "../utilities/simple_concurrent_list.h"
#include "memory_region_pool.h"

#define MAX_USEC_BEFORE_SLEEPING 5000000

#define TU_CONNECTION_RC 1 // 1 -> RC, 0 -> UC
#define VALIDATE_CHECKSUMS 0

// Allow to perform our own Reliable Connection. It can be used with TU_CONNECTION_RC 1 or 0
//#define TU_CONNECTION_RC_CONTROL 1 // 1 will control time per msg, 0 will not control nothing

#define KEY_MSG_SIZE 76 //(59)   /* Message size without gid. */
#define KEY_MSG_SIZE_GID 116 // (68) //(108)   /* Message size with gid (MGID as well). */

/* The Format of the message we pass through sockets , without passing Gid. */
#define KEY_PRINT_FMT "%04x:%06x:%06x:%08x:%016Lx:%016Lx:"

/* The Format of the message we pass through sockets (With Gid). */
#define KEY_PRINT_FMT_GID \
	"%04x:%06x:%06x:%08x:%016Lx:%016Lx:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:"

#define CONNECTION_BUFFER_WITH_MUTEX_LOCK

#define SPINNING_NO_LIST 1

#define SPINNING_NUM_TH 8 // was 1
#define SPINNING_NUM_TH_CLI 8 // 4

#define MAX_WR 4096
#define MAX_WR_LESS_ONE (MAX_WR - 1)

#define TU_RDMA_MEMORY_REGIONS 1 //We use memory regions, 0 we allocate space for  void *rdma_local_region

typedef enum kr_reply_status { KR_REP_ARRIVED = 430, KR_REP_PENDING = 345, KR_REP_DONT_CARE } kr_reply_status;

#define TU_CONTROL_MSG_BY_RDMA 0 //1 the control messages such as TU_RDMA_MRED_MSG will be sent by RDMA messages,
// 0  These control messages will be sent by SEND/RECEIVE messages

#define TU_RDMA_MSG_DONE 0
#define TU_RDMA_REGULAR_MSG_READY 3
#define TU_RDMA_DISCONNECT_MSG_READY 5
#define TU_RDMA_REGULAR_MSG 10
#define CONNECTION_PROPERTIES \
	9 /*not a message type used for recv flags in messages to indicate that either a
																 DISCONNECT, CHANGE_CONNECTION_PROPERTIES_REQUEST,CHANGE_CONNECTION_PROPERTIES_REPLY follows*/
#define TU_RDMA_RECEIVED_MREND_MSG 9 //To inform the spinning thread to go to the beginning
#define TU_RDMA_ACK_RECEIVED_MREND_REPLY_MSG \
	12 //To inform the client we received the MREND_REPLY_MSG and can be released

#define TU_RDMA_RECEIVED_ACK_MSG \
	14 //To inform the last message we have received. It should be usually sent from the client to the server.
#define TU_RDMA_DISCONNECT_MSG 99

#define SERVER_MODE 149
#define CLIENT_MODE 189
extern int LIBRARY_MODE; /*two modes for the communication rdma library SERVER and CLIENT*/

#define MAX_IDLE_ITERATIONS 1000000

extern uint32_t num_of_spinning_threads;
extern uint32_t num_of_worker_threads;

typedef struct spinning_thread_parameters {
	struct channel_rdma *channel;
	int spinning_thread_id;
} spinning_thread_parameters;

typedef enum connection_type {
	CLIENT_TO_SERVER_CONNECTION = 1,
	SERVER_TO_CLIENT_CONNECTION,
	MASTER_TO_REPLICA_CONNECTION,
	REPLICA_TO_MASTER_CONNECTION,
} connection_type;

//typedef enum region_status { REGION_OK = 1000, REGION_IN_TRANSITION } region_status;

typedef enum connection_status {
	CONNECTION_OK = 5,
	CONNECTION_RESETTING,
	CONNECTION_CLOSING,
	STOPPED_BY_THE_SERVER,
	WAIT_FOR_SERVER_ACK,
	CONNECTION_PENDING_ESTABLISHMENT,
	CONNECTION_ERROR
} connection_status;

typedef enum rdma_allocation_type {
	BLOCKING = 233,
	ASYNCHRONOUS,
} rdma_allocation_type;

typedef enum worker_status { IDLE_SPINNING, IDLE_SLEEPING, BUSY, WORKER_NOT_RUNNING } worker_status;

typedef void (*on_connection_created)(void *vconn);

void *socket_thread(void *args);
#if TU_CONNECTION_RC_CONTROL
// To control the pending messages, a be able to: 1) re-sent in case something is missing, 2) compute the RTT
struct rdma_sent_queue {
	struct msg_header *message; // Message sent
	struct timespec sent_time; // Time in which the message was sent
	struct timespec recv_time; // Time the reply was received.
	int recv_flag; // 1 : The reply message has been received, 0: message sent, but reply not received
	uint64_t ns_rtt;
	int resent;
	int64_t id_msg;
};

#define TU_SIZE_MSG_QUEUE MRQ_MAX_ELEMENTS //512 //4096 //1024
#define TU_BITS_SIZE_MSG_QUEUE 15 //9 //12 //10
#define TU_MASK_SIZE_MSG_QUEUE (MASK(TU_BITS_SIZE_MSG_QUEUE))

#define TU_MSG_FREE 0
#define TU_MSG_SENT 1
#define TU_MSG_RECV 2
#define TU_MSG_RECV_RECV 3

#endif

struct polling_msg {
#if SPINNING_NO_LIST
	void *mem_tail;
	uint64_t tail;
#endif
	uint64_t pos;
	void *mem;
	uint32_t real_pos;
	void *real_mem;
};

struct sr_message {
	enum {
		MSG_MREND,
		MSG_RECEIVED_MREND,
		MSG_ACK,
		MSG_RECEIVED_ACK,
		//MSG_ID_CONN,
		//MSG_REQ
	} type;
	int32_t nsec;
	int64_t id_msg;
	uint32_t pos;
	uint32_t nele;
	uint32_t message_written;
};

struct connection_rdma;
typedef void (*on_completion_callback)(struct ibv_wc *wc, struct connection_rdma *conn);

struct channel_rdma {
	struct ibv_context *context;
	struct ibv_pd *pd;
	struct ibv_comp_channel *comp_channel; //it will be shared by all the connections created.
	int sockfd;
	memory_region_pool *dynamic_pool;
	memory_region_pool *static_pool;

	/*List of connections open*/
	uint32_t nconn; // Num connections openned
	uint32_t nused; // Num connections used

	pthread_t cmthread; // Thread in charge of the socket for receiving the RDMA remote features (LID, GID, rkey, addr, etc.)
	pthread_t cq_poller_thread; //Thread in charge of the comp_channel //completion channel

	int spinning_num_th; //Number of spinning threads. Client and server can have a different number

	sem_t sem_spinning[SPINNING_NUM_TH]; //Thread spinning will be waiting here until first connection is added
	pthread_t spinning_thread[SPINNING_NUM_TH]; /* gesalous new staff, spining threads */
	struct worker_group *spinning_thread_group
		[SPINNING_NUM_TH]; /*gesalous new staff, references to worker threads per spinning thread*/
	SIMPLE_CONCURRENT_LIST *spin_list[SPINNING_NUM_TH];
	pthread_mutex_t spin_list_conn_lock[SPINNING_NUM_TH]; /*protectes the per spinnign thread connection list*/

	int spin_num[SPINNING_NUM_TH]; // Number of connections open
	int spinning_th; // For the id of the threads
	int spinning_conn; // For the conections to figure out which spinning thread should be joined
	pthread_mutex_t spin_th_lock; // Lock for the spin_th
	pthread_mutex_t spin_conn_lock; // Lock for the spin_conn

	on_connection_created connection_created; //Callback function used for created a thread at
	on_completion_callback on_completion;
};

struct pingpong_dest {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
	int gid_index;
};

typedef struct connection_rdma {
	/*new feature circular_buffer, only clients use it*/
	circular_buffer *send_circular_buf;
	circular_buffer *recv_circular_buf;
	char *reset_point;
	volatile connection_type type;
	/*To add to the list of connections open that handles the channel*/

	SIMPLE_CONCURRENT_LIST *list;
#ifdef CONNECTION_BUFFER_WITH_MUTEX_LOCK
	pthread_mutex_t buffer_lock;
	pthread_mutex_t allocation_lock;
#else
	pthread_spinlock_t buffer_lock;
#endif

	sem_t congestion_control; /*used for congestion control during send rdma operation*/
	volatile uint64_t sleeping_workers;
	volatile uint64_t offset;
	/*to which worker this connection has been assigned to*/
	int worker_id;
	/*</gesalous>*/
	void *channel;

	struct rdma_cm_id *rdma_cm_id;
	// FIXME qp, ctx, pd, cq, cq_recv, comp_channel deprecated by rdma_cm_id
	struct ibv_qp *qp;
	struct ibv_context *ctx; //= channel->context
	struct ibv_pd *pd; //= channel->pd
	struct ibv_cq *cq;
	struct ibv_cq *cq_recv;
	struct ibv_comp_channel *comp_channel;
	struct ibv_mr *peer_mr;
	// Info of the remote peer: addr y rkey, needed for sending the RRMA messages
	volatile void *rendezvous;
	/*normal or resetting?*/
	volatile connection_status status;
	memory_region *rdma_memory_regions;
	memory_region *next_rdma_memory_regions;
	struct ibv_mr *next_peer_mr;
	SIMPLE_CONCURRENT_LIST *responsible_spin_list;
	int32_t responsible_spinning_thread_id;
	/* *
	 * used by the spinning thread to identify cases where a rendezvous may be out of
	 * rdma memory, so that it will wait in for a RESET_BUFFER message
	 * */
	uint32_t remaining_bytes_in_remote_rdma_region;
	int idconn;
} connection_rdma;

void crdma_init_generic_create_channel(struct channel_rdma *channel, char *ib_devname);
void crdma_init_client_connection(struct connection_rdma *conn, const char *host, const char *port,
				  struct channel_rdma *channel);
void crdma_init_server_channel(struct channel_rdma *channel);
struct channel_rdma *crdma_server_create_channel(void);
struct channel_rdma *crdma_client_create_channel(char *ib_devname);
uint32_t crdma_free_RDMA_conn(struct connection_rdma **ardma_conn);

struct connection_rdma *crdma_client_create_connection_list_hosts(struct channel_rdma *channel, char **hosts,
								  int num_hosts, connection_type type);

void crdma_init_client_connection_list_hosts(struct connection_rdma *conn, char **hosts, const int num_hosts,
					     struct channel_rdma *channel, connection_type type);

msg_header *allocate_rdma_message(connection_rdma *conn, int message_payload_size, int message_type);
msg_header *client_allocate_rdma_message(connection_rdma *conn, int message_payload_size, int message_type);

int send_rdma_message(connection_rdma *conn, msg_header *msg);
int send_rdma_message_busy_wait(connection_rdma *conn, msg_header *msg);
void set_receive_field(struct msg_header *msg, uint8_t value);
uint8_t get_receive_field(volatile struct msg_header *msg);

/** Free the space allocated from send/recv circular buffers for a specific rpc pair.
 *  we found the request msg from the reply's triggering msg
 *  reply must be volatile as it is receive via the network */
void client_free_rpc_pair(connection_rdma *conn, volatile msg_header *reply);

struct rdma_message_context {
	struct msg_header *msg;
	sem_t wait_for_completion;
	struct ibv_wc wc;
	void (*on_completion_callback)(struct rdma_message_context *msg_ctx);
	void *args;
	uint8_t __is_initialized;
};

void client_rdma_init_message_context(struct rdma_message_context *msg_ctx, struct msg_header *msg);
bool client_rdma_send_message_success(struct rdma_message_context *msg_ctx);
int client_send_rdma_message(struct connection_rdma *conn, struct msg_header *msg);

struct connection_rdma *crdma_client_create_connection(struct channel_rdma *channel);
void close_and_free_RDMA_connection(struct channel_rdma *channel, struct connection_rdma *conn);
void crdma_generic_free_connection(struct connection_rdma **ardma_conn);

/*gesalous, signature here implementation in tu_rdma.c*/
void disconnect_and_close_connection(connection_rdma *conn);
void ec_sig_handler(int signo);
uint32_t wait_for_payload_arrival(msg_header *hdr);
int __send_rdma_message(connection_rdma *conn, msg_header *msg, struct rdma_message_context *msg_ctx);

void zero_rendezvous_locations_l(volatile msg_header *msg, uint32_t length);
void zero_rendezvous_locations(volatile msg_header *msg);
void update_rendezvous_location(struct connection_rdma *conn, uint32_t message_size);

msg_header *triggering_msg_offt_to_real_address(connection_rdma *conn, uint32_t offt);

uint32_t real_address_to_triggering_msg_offt(connection_rdma *conn, struct msg_header *msg);

/*for starting a separate channel for each numa server*/
struct ibv_context *get_rdma_device_context(char *devname);
void *poll_cq(void *arg);

uint32_t calc_offset_in_send_and_target_recv_buffer(struct msg_header *msg, circular_buffer *c_buf);
