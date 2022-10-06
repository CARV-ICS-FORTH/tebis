#include "metadata.h"
#include <zookeeper/zookeeper.h>
#define PATH_SIZE (128)

typedef enum { ERROR = 0, RECONFIGURE_REGION, UPDATE_STATUS } operation_type_t;
typedef struct {
	char zk_path[PATH_SIZE];
	zhandle_t *zk_handle;
} region_log_t;

typedef struct {
	char replica_group_info[MAX_REPLICA_GROUP_SIZE][MAX_SERVER_NAME];
	server_role_t role[MAX_REPLICA_GROUP_SIZE];
} reconfiguration_t;

typedef struct {
	operation_type_t operation_code;
	char region_id[PATH_SIZE];
	union {
		reconfiguration_t reconfiguration;
	};
} region_operation_t;

region_log_t *create_region_log(char *path, unsigned int path_len, zhandle_t *zhandle);
void append_to_region_log(region_log_t *region_log, region_operation_t region_operation);
void trim_region_log(region_log_t *region_log, int64_t max_lsn);
void replay_region_log(region_log_t *region_log);
