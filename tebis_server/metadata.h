#ifndef METADATA_H
#define METADATA_H

#include "../tebis_rdma/rdma.h"
#include "../utilities/list.h"
#include "configurables.h"
#include "messages.h"
#include "send_index/send_index_callbacks.h"
#include "server_communication.h"
#include "uthash.h"
#include <btree/conf.h>
#include <include/parallax/parallax.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <zookeeper.h>
struct compaction_request;

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

enum krm_error_code { KRM_SUCCESS = 0, KRM_BAD_EPOCH, KRM_DS_TABLE_FULL, KRM_REGION_EXISTS };

enum tb_kv_category { TEBIS_SMALLORMEDIUM = 0, TEBIS_BIG };

enum tb_rdma_buf_category { TEBIS_L0_RECOVERY_RDMA_BUF, TEBIS_MEDIUM_RECOVERY_RDMA_BUF, TEBIS_BIG_RECOVERY_RDMA_BUF };

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
	volatile uint64_t flushed_until_offt;
	volatile uint64_t replicated_bytes;
};

struct ru_master_log_buffer {
	struct ru_master_log_buffer_seg remote_buffers;
	struct ibv_mr *primary_buffer;
	uint32_t primary_buffer_curr_end;
	uint32_t segment_size;
};

struct ru_master_state {
	struct ru_master_log_buffer l0_recovery_rdma_buf;
	struct ru_master_log_buffer medium_recovery_rdma_buf;
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
	struct compaction_request *comp_req[MAX_LEVELS];
	struct ru_replica_rdma_buffer l0_recovery_rdma_buf;
	struct ru_replica_rdma_buffer medium_recovery_rdma_buf;
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

enum krm_replica_buf_status { KRM_BUFS_UNINITIALIZED = 0, KRM_BUFS_INITIALIZING, KRM_BUFS_READY };

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

void *run_master(void *args);
#endif /* METADATA_H */
