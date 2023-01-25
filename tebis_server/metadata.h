#ifndef METADATA_H
#define METADATA_H

#include "../tebis_rdma/rdma.h"
#include "../utilities/list.h"
#include "configurables.h"
#include "messages.h"
#include "send_index/send_index_callbacks.h"
#include "uthash.h"
#include <btree/conf.h>
#include <include/parallax/parallax.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <zookeeper/zookeeper.h>

typedef struct level_write_appender *level_write_appender_t;
typedef struct send_index_rewriter *send_index_rewriter_t;
#define MAX_REPLICA_GROUP_SIZE (RU_MAX_NUM_REPLICAS + 1)
#define MAX_SERVER_NAME (128)

enum krm_zk_conn_state { KRM_INIT, KRM_CONNECTED, KRM_DISCONNECTED, KRM_EXPIRED };

enum server_role {
	FAULTY_ROLE = 0,
	PRIMARY,
	PRIMARY_NEWBIE,
	PRIMARY_INFANT,
	PRIMARY_DEAD,
	BACKUP,
	BACKUP_NEWBIE,
	BACKUP_INFANT,
	BACKUP_DEAD,
	ROLE_NUM
};

enum krm_region_role { KRM_PRIMARY, KRM_BACKUP };
enum krm_region_status { KRM_OPEN, KRM_OPENING, KRM_FRESH, KRM_HALTED };

// enum krm_msg_type {
// 	KRM_OPEN_REGION_AS_PRIMARY = 1,
// 	KRM_ACK_OPEN_PRIMARY,
// 	KRM_NACK_OPEN_PRIMARY,
// 	KRM_OPEN_REGION_AS_BACKUP,
// 	KRM_ACK_OPEN_BACKUP,
// 	KRM_NACK_OPEN_BACKUP,
// 	KRM_CLOSE_REGION,
// 	KRM_BUILD_PRIMARY
// };

enum krm_error_code { KRM_SUCCESS = 0, KRM_BAD_EPOCH, KRM_DS_TABLE_FULL, KRM_REGION_EXISTS };

enum krm_work_task_status {
	/*overall_status*/
	TASK_START = 1,
	TASK_COMPLETE,
	/*mutation operations related*/
	GET_RSTATE,
	INIT_LOG_BUFFERS,
	INS_TO_KREON,
	REPLICATE,
	WAIT_FOR_REPLICATION_TURN,
	WAIT_FOR_REPLICATION_COMPLETION,
	ALL_REPLICAS_ACKED,
	SEND_FLUSH_COMMANDS,
	WAIT_FOR_FLUSH_REPLIES,
	TASK_GET_KEY,
	TASK_MULTIGET,
	TASK_DELETE_KEY,
	TASK_NO_OP,
	TASK_FLUSH_L0,
	TASK_CLOSE_COMPACTION,
};

enum tb_kv_category { TEBIS_SMALLORMEDIUM = 0, TEBIS_BIG };

enum tb_rdma_buf_category { TEBIS_L0_RECOVERY_RDMA_BUF, TEBIS_BIG_RECOVERY_RDMA_BUF };
/*server to server communication related staff*/
struct sc_msg_pair {
#define IP_SIZE 4
	/*out variables*/
	struct msg_header *request;
	struct msg_header *reply;
	struct connection_rdma *conn;
	enum circular_buffer_op_status stat;
};

enum krm_work_task_type { KRM_CLIENT_TASK, KRM_SERVER_TASK };

struct krm_work_task {
	/*from client*/
	struct rdma_message_context msg_ctx[RU_MAX_NUM_REPLICAS];
	volatile uint64_t *replicated_bytes;
	struct par_put_metadata insert_metadata;
	uint32_t last_replica_to_ack;
	uint64_t msg_payload_size;
	/*possible messages to other server generated from this task*/
	struct sc_msg_pair communication_buf;
	struct channel_rdma *channel;
	struct connection_rdma *conn;
	msg_header *msg;
	struct region_desc *r_desc;
	struct kv_splice *kv;
	enum tb_kv_category kv_category; /*XXX TODO make these a struct XXX*/
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

enum ru_remote_buffer_status {
	RU_BUFFER_UNINITIALIZED,
	RU_BUFFER_REQUESTED,
	RU_BUFFER_REPLIED,
	RU_BUFFER_OK,
	RU_REPLICA_BUFFER_OK
};

struct ru_primary_to_backup_comm {
	/*msg between primary and backup*/
	struct sc_msg_pair msg_pair;
	/*status of the remote buffers*/
	enum ru_remote_buffer_status stat;
};

struct ru_master_log_buffer_seg {
	/* IMPORTANT, primary's segment is related with many backups memory regions.
	 * The address of each backup's  memory region differ, we have allocated the space with posix memalign*/
	struct ibv_mr mr[KRM_MAX_BACKUPS];
	volatile uint64_t start;
	volatile uint64_t end;
	volatile uint64_t curr_end;
	volatile uint64_t replicated_bytes;
};

struct ru_master_log_buffer {
	struct ru_master_log_buffer_seg segment;
	uint32_t segment_size;
};

struct ru_master_state {
	/*rdma buffer for keeping small and medium kv categories*/
	struct ru_master_log_buffer l0_recovery_rdma_buf;
	/*rdma buffer for keeping big kv category*/
	struct ru_master_log_buffer big_recovery_rdma_buf;
	struct ru_primary_to_backup_comm primary_to_backup[KRM_MAX_BACKUPS];
	/*The flush commands send to backups will be equal to the number of backups.
	 *All flush cmds must be allocated and freed accordingly */
	struct ru_primary_to_backup_comm flush_cmd[KRM_MAX_BACKUPS];
	int num_backup;
};

struct ru_replica_rdma_buffer {
	struct ibv_mr *mr;
	uint32_t rdma_buf_size;
};

struct ru_replica_state {
	/*for the index staff*/
	struct ibv_mr *index_buffer[MAX_LEVELS];
	struct ibv_mr *index_segment_flush_replies[MAX_LEVELS];
	level_write_appender_t wappender[MAX_LEVELS];
	send_index_rewriter_t index_rewriter[MAX_LEVELS];
	/*rdma buffer keeping small and medium kv categories*/
	struct ru_replica_rdma_buffer l0_recovery_rdma_buf;
	/*rdma buffer keepint the big kv category*/
	struct ru_replica_rdma_buffer big_recovery_rdma_buf;
};

struct krm_server_name {
	char hostname[KRM_HOSTNAME_SIZE];
	/*kreon hostname - RDMA port*/
	char kreon_ds_hostname[KRM_HOSTNAME_SIZE];
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
	uint64_t primary_segment_offt;
	uint64_t replica_segment_offt;
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
	struct region_desc *r_desc;
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

struct krm_ds_regions {
	struct region_desc *r_desc[KRM_MAX_DS_REGIONS];
	int num_ds_regions;
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

struct krm_msg {
	struct krm_region region;
	char sender[KRM_HOSTNAME_SIZE];
	// enum krm_msg_type type;
	enum krm_error_code error_code;
	uint64_t epoch;
	uint64_t transaction_id;
};

int krm_get_server_info(struct regs_server_desc *server_desc, char *hostname, struct krm_server_name *server);

struct channel_rdma *ds_get_channel(struct regs_server_desc const *my_desc);

/*remote compaction related staff*/
struct rco_task_queue {
	pthread_t cnxt;
	pthread_mutex_t queue_lock;
	pthread_cond_t queue_monitor;
	int my_id;
	int sleeping;
	struct tebis_klist *task_queue;
};

struct rco_pool {
	pthread_mutex_t pool_lock;
	struct regs_server_desc *rco_server;
	int curr_worker_id;
	int num_workers;
	struct rco_task_queue worker_queue[];
};
#define RCO_POOL_SIZE 1
struct rco_pool *rco_init_pool(struct regs_server_desc *server, int pool_size);
void rco_add_db_to_pool(struct rco_pool *pool, struct region_desc *r_desc);
//int rco_send_index_to_group(struct bt_compaction_callback_args *c);
int rco_flush_last_log_segment(void *handle);
void di_set_cursor_buf(char *buf);

int rco_init_index_transfer(uint64_t db_id, uint8_t level_id);
int rco_destroy_local_rdma_buffer(uint64_t db_id, uint8_t level_id);
//int rco_send_index_segment_to_replicas(uint64_t db_id, uint64_t dev_offt, struct segment_header *seg, uint32_t size,
//				       uint8_t level_id, struct node_header *root);
//void di_rewrite_index_with_explicit_IO(struct segment_header *memory_segment, struct krm_region_desc *r_desc,
//				       uint64_t primary_seg_offt, uint8_t level_id);

/*server to server communication staff*/
struct sc_msg_pair sc_allocate_rpc_pair(struct connection_rdma *conn, uint32_t request_size, uint32_t reply_size,
					enum message_type type);
struct connection_rdma *sc_get_data_conn(struct regs_server_desc const *region_server, char *hostname,
					 char *IP_address);
struct connection_rdma *sc_get_compaction_conn(struct regs_server_desc *region_server, char *hostname,
					       char *IP_address);
void sc_free_rpc_pair(struct sc_msg_pair *p);
void *run_master(void *args);
#endif /* METADATA_H */
