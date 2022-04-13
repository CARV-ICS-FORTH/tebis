#pragma once
#include <assert.h>
#include <inttypes.h>
#include <semaphore.h>

/*mvard added defines for return when function succeded or failed*/
typedef enum kreon_op_status {
	KREON_SUCCESS = 1,
	KREON_FAILURE,
	KREON_KEY_NOT_FOUND,
	KREON_VALUE_TOO_LARGE
} kreon_op_status;

//TODO move properties to a configuration file
#define RDMA_IP_FILTER "192.168.4."

#define NUM_OF_CONNECTIONS_PER_SERVER 1

#define TU_HEADER_SIZE (sizeof(struct msg_header))
#define TU_TAIL_SIZE (sizeof(uint8_t))

/* The following two definitions enable three distinct operating modes in Tebis.
 * 1. RCO_DISABLE_REMOTE_COMPATIONS = 0, RCO_BUILD_INDEX_AT_REPLICA = 0
 *    Primaries send their index to their replicas
 * 2. RCO_DISABLE_REMOTE_COMPACTIONS = 1, RCO_BUILD_INDEX_AT_REPLICA = 0
 *    Primaries only replicate their log, replicas have no index of their own
 * 3. RCO_DISABLE_REMOTE_COMPATIONS = 1, RCO_BUILD_INDEX_AT_REPLICA = 1
 *    Primaries only replicate their log, replicas perform compactions to build an index on their own
 * 4. RCO_DISABLE_REMOTE_COMPACTIONS = 0, RCO_BUILD_INDEX_AT_REPLICA = 1
 *    Unssuported configuration
 */
