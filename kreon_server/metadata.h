#pragma once
#define KRM_HOSTNAME_SIZE 128
#define IP_SIZE 4
#include "../kreon_lib/btree/btree.h"
#include "../kreon_rdma/rdma.h"
#include "../utilities/list.h"
#include "uthash.h"
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <zookeeper/zookeeper.h>
#define KRM_MAX_REGIONS 1024
#define KRM_MAX_DS_REGIONS 512
#define KRM_MAX_KEY_SIZE 64
#define KRM_MAX_REGION_ID_SIZE 16
#define KRM_MAX_BACKUPS 4
#define KRM_MAX_RDMA_IP_SIZE 22
#define KRM_ROOT_PATH "/kreonR"
#define KRM_SERVERS_PATH "/servers"
#define KRM_SLASH "/"
#define KRM_LEADER_PATH "/leader"
#define KRM_MAILBOX_PATH "/mailbox"
#define KRM_MAIL_TITLE "/msg"
#define KRM_ALIVE_SERVERS_PATH "/alive_dataservers"
#define KRM_ALIVE_LEADER_PATH "/alive_leader"
#define KRM_REGIONS_PATH "/regions"

#define RU_REPLICA_NUM_SEGMENTS 1
#define RU_REGION_KEY_SIZE MSG_MAX_REGION_KEY_SIZE
#define RU_MAX_TREE_HEIGHT 12
#define RU_MAX_NUM_REPLICAS 4
//#define RU_MAX_INDEX_SEGMENTS 4

enum krm_zk_conn_state { KRM_INIT, KRM_CONNECTED, KRM_DISCONNECTED, KRM_EXPIRED };

enum krm_server_state {
	KRM_BOOTING = 1,
	KRM_CLEAN_MAILBOX,
	KRM_SET_DS_WATCHERS,
	KRM_BUILD_DATASERVERS_TABLE,
	KRM_BUILD_REGION_TABLE,
	KRM_ASSIGN_REGIONS,
	KRM_OPEN_LD_REGIONS,
	KRM_LD_ANNOUNCE_JOINED,
	KRM_DS_ANNOUNCE_JOINED,
	KRM_PROCESSING_MSG,
	KRM_WAITING_FOR_MSG
};

enum krm_server_role { KRM_LEADER, KRM_DATASERVER };

enum krm_region_role { KRM_PRIMARY, KRM_BACKUP };
enum krm_region_status { KRM_OPEN, KRM_OPENING, KRM_FRESH, KRM_HALTED };

enum krm_msg_type {
	KRM_OPEN_REGION_AS_PRIMARY = 1,
	KRM_ACK_OPEN_PRIMARY,
	KRM_NACK_OPEN_PRIMARY,
	KRM_OPEN_REGION_AS_BACKUP,
	KRM_ACK_OPEN_BACKUP,
	KRM_NACK_OPEN_BACKUP,
	KRM_CLOSE_REGION,
	KRM_BUILD_PRIMARY
};

enum krm_error_code { KRM_SUCCESS = 0, KRM_BAD_EPOCH, KRM_DS_TABLE_FULL, KRM_REGION_EXISTS };

enum krm_work_task_status {
	/*overall_status*/
	TASK_START = 0,
	TASK_COMPLETE,
	/*mutation operations related*/
	GET_RSTATE,
	INIT_LOG_BUFFERS,
	INS_TO_KREON,
	REPLICATE,
	WAIT_FOR_REPLICATION_COMPLETION,
	ALL_REPLICAS_ACKED,
	SEGMENT_BARRIER,
	FLUSH_REPLICA_BUFFERS,
	SEND_FLUSH_COMMANDS,
	WAIT_FOR_FLUSH_REPLIES,
	TASK_GET_KEY,
	TASK_MULTIGET,
	TASK_DELETE_KEY,
	TASK_NO_OP
};

/*server to server communication related staff*/
struct sc_msg_pair {
	/*out variables*/
	struct msg_header *request;
	struct msg_header *reply;
	struct connection_rdma *conn;
	enum circular_buffer_op_status stat;
};

enum krm_work_task_type { KRM_CLIENT_TASK, KRM_SERVER_TASK };

struct krm_work_task {
	/*from client*/
	bt_insert_req ins_req;
	struct rdma_message_context msg_ctx[RU_MAX_NUM_REPLICAS];
	volatile uint64_t *replicated_bytes[RU_MAX_NUM_REPLICAS];
	uint32_t last_replica_to_ack;
	uint64_t kv_size;
	/*possible messages to other server generated from this task*/
	struct sc_msg_pair communication_buf;
	struct channel_rdma *channel;
	struct connection_rdma *conn;
	msg_header *msg;
	struct krm_region_desc *r_desc;
	struct msg_put_key *key;
	struct msg_put_value *value;
	uint32_t triggering_msg_offset;
	msg_header *reply_msg;
	msg_header *flush_segment_request;
	struct krm_replica_index_state *index;
	int server_id;
	int thread_id;
	int error_code;
	//int suspended;
	int seg_id_to_flush;
	uint64_t rescheduling_counter;
	enum krm_work_task_type task_type;
	enum krm_work_task_status kreon_operation_status;
};

/*this staff are rdma registered*/
struct ru_seg_metadata {
	uint64_t master_segment;
	uint64_t end_of_log;
	uint64_t log_padding;
	uint64_t segment_id;
	uint32_t region_key_size;
	char region_key[RU_REGION_KEY_SIZE];
	uint64_t tail;
};

struct ru_rdma_buffer {
	struct msg_header msg;
	struct ru_seg_metadata metadata;
	char padding[4096 - sizeof(struct ru_seg_metadata)];
	uint8_t seg[SEGMENT_SIZE];
};

struct ru_replica_log_segment {
	struct ru_rdma_buffer *rdma_local_buf;
	struct ru_rdma_buffer *rdma_remote_buf;
	int64_t bytes_wr_per_seg;
	int64_t buffer_free;
};

enum ru_remote_buffer_status {
	RU_BUFFER_UNINITIALIZED,
	RU_BUFFER_REQUESTED,
	RU_BUFFER_REPLIED,
	RU_BUFFER_OK,
	RU_REPLICA_BUFFER_OK
};

struct ru_master_log_buffer_seg {
	struct sc_msg_pair flush_cmd;
	enum ru_remote_buffer_status flush_cmd_stat;
	volatile uint64_t start;
	volatile uint64_t end;
	struct ibv_mr mr;
	volatile uint64_t lc1;
	volatile uint64_t lc2;
	volatile uint64_t replicated_bytes;
};

struct ru_master_log_buffer {
	struct sc_msg_pair p;
	enum ru_remote_buffer_status stat;
	uint32_t segment_size;
	int num_buffers;
	struct ru_master_log_buffer_seg segment[RU_REPLICA_NUM_SEGMENTS];
};

struct ru_master_state {
#if 0
	/*parameters used for remote spills at replica with tiering*/
	node_header *last_node_per_level[RU_MAX_TREE_HEIGHT];
	uint64_t cur_nodes_per_level[RU_MAX_TREE_HEIGHT];
	uint64_t num_of_nodes_per_level[RU_MAX_TREE_HEIGHT];
	uint64_t entries_in_semilast_node[RU_MAX_TREE_HEIGHT];
	uint64_t entries_in_last_node[RU_MAX_TREE_HEIGHT];
	uint32_t current_active_tree_in_the_forest;
#endif
	int num_backup;
	struct ru_master_log_buffer r_buf[KRM_MAX_BACKUPS];
};

struct ru_replica_log_buffer_seg {
	uint32_t segment_size;
	struct ibv_mr *mr;
};

struct ru_replica_state {
	/*for the index staff*/
	struct ibv_mr *index_buffers[MAX_LEVELS][MAX_REPLICA_INDEX_BUFFERS];
	/*for thr KV log*/
	volatile uint64_t next_segment_id_to_flush;
	int num_buffers;
	struct ru_replica_log_buffer_seg seg[RU_REPLICA_NUM_SEGMENTS];
};

struct krm_server_name {
	char hostname[KRM_HOSTNAME_SIZE];
	/*kreon hostname - RDMA port*/
	char kreon_ds_hostname[KRM_HOSTNAME_SIZE * 2];
	char kreon_leader[KRM_HOSTNAME_SIZE];
	char RDMA_IP_addr[KRM_MAX_RDMA_IP_SIZE];
	uint32_t kreon_ds_hostname_length;
	uint64_t epoch;
};

struct krm_region {
	struct krm_server_name primary;
	struct krm_server_name backups[KRM_MAX_BACKUPS];
	uint32_t min_key_size;
	uint32_t max_key_size;
	char id[KRM_MAX_REGION_ID_SIZE];
	char min_key[KRM_MAX_KEY_SIZE];
	char max_key[KRM_MAX_KEY_SIZE];
	uint32_t num_of_backup;
	enum krm_region_status stat;
};

struct krm_segment_entry {
	uint64_t master_seg;
	uint64_t my_seg;
	UT_hash_handle hh;
};

enum di_decode_stage {
	DI_INIT,
	DI_CHECK_NEXT_ENTRY,
	DI_PROCEED,
	DI_LEAF_NODE,
	DI_INDEX_NODE_FIRST_IN,
	DI_INDEX_NODE_LAST_IN,
	DI_INDEX_NODE_LEFT_CHILD,
	DI_INDEX_NODE_PIVOT,
	DI_INDEX_NODE_RIGHT_CHILD,
	DI_CHANGE_SEGMENT,
	DI_ADVANCE_CURSOR,
	DI_COMPLETE
};

struct di_buffer {
	struct krm_region_desc *r_desc;
	char *data;
	uint64_t primary_offt;
	uint64_t replica_offt;
	uint32_t size;
	uint32_t offt;
	uint32_t curr_entry;
	int fd;
	uint8_t level_id;
	uint8_t allocated;
	enum di_decode_stage state;
};

enum krm_replica_buf_status { KRM_BUFS_UNINITIALIZED = 0, KRM_BUFS_INITIALIZING, KRM_BUFS_READY };
struct krm_region_desc {
	pthread_mutex_t region_mgmnt_lock;
	pthread_rwlock_t kreon_lock;

	struct krm_region *region;
	/*for replica_role deserializing the index*/
	pthread_rwlock_t replica_log_map_lock;
	struct krm_segment_entry *replica_log_map;
	struct krm_segment_entry *replica_index_map[MAX_LEVELS];
	//RDMA related staff for sending the index
	struct ibv_mr remote_mem_buf[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct sc_msg_pair rpc[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct rdma_message_context rpc_ctx[KRM_MAX_BACKUPS][MAX_LEVELS];
	struct ibv_mr *local_buffer[MAX_LEVELS];
	uint8_t rpc_in_use[KRM_MAX_BACKUPS][MAX_LEVELS];
	//Staff for deserializing the index at the replicas
	struct di_buffer *index_buffer[MAX_LEVELS][MAX_HEIGHT];

	enum krm_region_role role;
	db_handle *db;
	volatile uint64_t next_segment_to_flush;
	union {
		struct ru_master_state *m_state;
		struct ru_replica_state *r_state;
	};
	volatile int64_t pending_region_tasks;
	enum krm_replica_buf_status replica_buf_status;
	enum krm_region_status status;
};

struct krm_ds_regions {
	struct krm_region_desc *r_desc[KRM_MAX_DS_REGIONS];
	uint64_t lamport_counter_1;
	uint64_t lamport_counter_2;
	uint32_t num_ds_regions;
};

struct krm_leader_regions {
	struct krm_region regions[KRM_MAX_REGIONS];
	int num_regions;
};

struct krm_leader_region_state {
	pthread_mutex_t region_list_lock;
	struct krm_server_name server_id;
	struct krm_region *region;
	enum krm_region_role role;
	enum krm_region_status status;
};

struct krm_leader_ds_region_map {
	uint64_t hash_key;
	struct krm_leader_region_state lr_state;
	UT_hash_handle hh;
};

struct krm_leader_ds_map {
	uint64_t hash_key;
	struct krm_server_name server_id;
	struct krm_leader_ds_region_map *region_map;
	uint32_t num_regions;
	UT_hash_handle hh;
};

struct krm_server_desc {
	struct krm_server_name name;
	char mail_path[KRM_HOSTNAME_SIZE];
	sem_t wake_up;
	pthread_mutex_t msg_list_lock;
	struct klist *msg_list;
	zhandle_t *zh;
	struct rco_pool *compaction_pool;
	uint8_t IP[IP_SIZE];
	uint8_t RDMA_IP[IP_SIZE];
	enum krm_server_role role;
	enum krm_server_state state;
	uint8_t zconn_state;
	uint32_t RDMA_port;
	/*entry in the root table of my dad (numa_server)*/
	int root_server_id;
	/*filled only by the leader server*/
	struct krm_leader_regions *ld_regions;
	struct krm_leader_ds_map *dataservers_map;
	/*filled by the ds*/
	struct krm_ds_regions *ds_regions;
};

struct krm_msg {
	struct krm_region region;
	char sender[KRM_HOSTNAME_SIZE];
	enum krm_msg_type type;
	enum krm_error_code error_code;
	uint64_t epoch;
};

void *krm_metadata_server(void *args);
struct krm_region_desc *krm_get_region(struct krm_server_desc const *server_desc, char *key, uint32_t key_size);
//struct krm_region_desc *krm_get_region_based_on_id(struct krm_server_desc *desc, char *region_id,
//						   uint32_t region_id_size);
int krm_get_server_info(struct krm_server_desc *server_desc, char *hostname, struct krm_server_name *server);

struct channel_rdma *ds_get_channel(struct krm_server_desc const *my_desc);

/*remote compaction related staff*/
struct rco_task_queue {
	pthread_t cnxt;
	pthread_mutex_t queue_lock;
	pthread_cond_t queue_monitor;
	int my_id;
	int sleeping;
	struct klist *task_queue;
};

struct rco_pool {
	pthread_mutex_t pool_lock;
	struct krm_server_desc *rco_server;
	int curr_worker_id;
	int num_workers;
	struct rco_task_queue worker_queue[];
};
#define RCO_POOL_SIZE 1
struct rco_pool *rco_init_pool(struct krm_server_desc *server, int pool_size);
void rco_add_db_to_pool(struct rco_pool *pool, struct krm_region_desc *r_desc);
int rco_send_index_to_group(struct bt_compaction_callback_args *c);
int rco_flush_last_log_segment(void *handle);
void di_set_cursor_buf(char *buf);

int rco_init_index_transfer(uint64_t db_id, uint8_t level_id);
int rco_destroy_local_rdma_buffer(uint64_t db_id, uint8_t level_id);
int rco_send_index_segment_to_replicas(uint64_t db_id, uint64_t dev_offt, struct segment_header *seg, uint32_t size,
				       uint8_t level_id, struct node_header *root);

void di_rewrite_index_with_explicit_IO(struct segment_header *memory_segment, struct krm_region_desc *r_desc,
				       uint64_t primary_seg_offt, uint8_t level_id);

struct rco_build_index_task {
	struct krm_region_desc *r_desc;
	struct segment_header *segment;
	uint64_t log_start;
	uint64_t log_end;
};
void rco_build_index(struct rco_build_index_task *task);

/*server to server communication staff*/
struct sc_msg_pair sc_allocate_rpc_pair(struct connection_rdma *conn, uint32_t request_size, uint32_t reply_size,
					enum message_type type);
struct connection_rdma *sc_get_data_conn(struct krm_server_desc const *mydesc, char *hostname);
struct connection_rdma *sc_get_compaction_conn(struct krm_server_desc *mydesc, char *hostname);
void sc_free_rpc_pair(struct sc_msg_pair *p);
