#ifndef TEBIS_TCP_TYPES_H
#define TEBIS_TCP_TYPES_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define TEBIS_TCP_VERSION 0x01000000 //0x000.000.00 [major, minor, patch]

#define __x86_PAGESIZE (1UL << 12)
#define DEF_BUF_SIZE (32UL * __x86_PAGESIZE) // 128KB
#define DEF_KV_SLOTS 16

typedef struct {
	size_t size;
	void *data;

} generic_data_t;

typedef struct {
	generic_data_t key;
	generic_data_t value;

} kv_t;

/** requests - replies **/

typedef enum {

/** buffer scheme: [1B type | 8B nokeys | size_t[] | payload[]] **/

#define OPSNO 5U
#define INIT_CONN_TYPE 0xFF

	/** GET-request family **/

	REQ_GET,
	REQ_DEL,
	REQ_EXISTS,

	/** PUT-request family **/

	REQ_PUT,
	REQ_PUT_IFEX

} req_t;

typedef enum {

	/** buffer scheme: [1B retcode | 8B novals | size_t[] | payload[]] **/

	REP_GET,
	REP_DEL,
	REP_EXISTS,

	REP_PUT,
	REP_PUT_IFEX

} rep_t;

#endif /** TEBIS_TCP_TYPES_H **/
