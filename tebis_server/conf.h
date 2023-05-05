#pragma once
#include <assert.h>
#include <inttypes.h>
#include <semaphore.h>

enum tebis_op_status {
	TEBIS_FAILURE = 0,
	TEBIS_SUCCESS,
};

//TODO move properties to a configuration file
#define RDMA_IP_FILTER "192.168.4."

#define NUM_OF_CONNECTIONS_PER_SERVER 1

#define TU_HEADER_SIZE (sizeof(struct msg_header))
#define TU_TAIL_SIZE (sizeof(uint8_t))
#define ENABLE_MONITORING 0
