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
#ifndef RDMA_H
#define RDMA_H

#include <infiniband/verbs.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>

#include "../tebis_server/messages.h"
#include "../utilities/circular_buffer.h"
#include "../utilities/simple_concurrent_list.h"
#include "memory_region_pool.h"

#define VALIDATE_CHECKSUMS 0

#define CONNECTION_BUFFER_WITH_MUTEX_LOCK

#define SPINNING_NO_LIST 1

#define SPINNING_NUM_TH 8 // was 1
#define SPINNING_NUM_TH_CLI 8 // 4

#define MAX_WR 4096

typedef enum kr_reply_status { KR_REP_ARRIVED = 430, KR_REP_PENDING = 345, KR_REP_DONT_CARE } kr_reply_status;

#define TU_RDMA_REGULAR_MSG 10
#define TU_RDMA_REPLICATION_MSG 20
#define CONNECTION_PROPERTIES \
	9 /*not a message type used for recv flags in messages to indicate that either a
																 DISCONNECT, CHANGE_CONNECTION_PROPERTIES_REQUEST,CHANGE_CONNECTION_PROPERTIES_REPLY follows*/
#define SERVER_MODE 149
#define CLIENT_MODE 189
extern int LIBRARY_MODE; /*two modes for the communication rdma library SERVER and CLIENT*/

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
#endif
