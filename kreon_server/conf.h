#pragma once
#include <inttypes.h>
#include <semaphore.h>

/*mvard added defines for return when function succeded or failed*/
typedef enum kreon_op_status {
	KREON_SUCCESS = 1,
	KREON_FAILURE,
	KREON_KEY_NOT_FOUND,
	KREON_VALUE_TOO_LARGE
} kreon_op_status;

/*gesalous, priorities and properties regarding the conections*/
#define HIGH_PRIORITY 1203
#define LOW_PRIORITY 9829

//TODO move properties to a configuration file
#define RDMA_IP_FILTER "192.168.4."

#define NUM_OF_CONNECTIONS_PER_SERVER 4

#define TU_HEADER_SIZE (sizeof(struct msg_header))
#define TU_TAIL_SIZE (sizeof(uint32_t))

#define RCO_DISABLE_REMOTE_COMPACTIONS 1
#define RCO_BUILD_INDEX_AT_REPLICA 1
#define RCO_EXPLICIT_IO 1
#define REGIONS_HASH_BASED 0
