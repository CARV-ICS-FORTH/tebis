#ifndef TEBIS_TCP_TYPES_H
#define TEBIS_TCP_TYPES_H

#include <linux/types.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define TT_VERSION 0x01000000 //0x000.000.00 [major, minor, patch]
#define TT_MAX_LISTEN 512
#define TT_REPHDR_SIZE 17UL
#define REQHDR_SIZE 9UL

#define __x86_PAGESIZE (1UL << 12)
#define DEF_BUF_SIZE (64UL * __x86_PAGESIZE) // 256KB

#define TT_REQ_SUCC 0 /** TODO: remove, use 'retcode_t' insread */

#define req_in_get_family(req) (((req)->type) < REQ_SCAN)

struct buffer {
	uint64_t bytes;
	char *mem;
};

typedef struct {
	size_t size;
	char *data;

} generic_data_t;

typedef struct {
	uint32_t size;
	char *data;
} generic_data32_t;

typedef struct {
	generic_data32_t key;
	generic_data32_t value;

} kv_t;

typedef enum {

	RETC_SUCCESS,
	RETC_FAIL

} retcode_t;

/** requests - replies **/

#define OPSNO 6U

typedef enum {

	/** old: [1B type | 4B key-size | key | 4B value-size | value] **/
	/** [1B type | 4B key-size | 4B value-size | key | value ] **/

	/** GET-request family **/

	REQ_GET,
	REQ_DEL,
	REQ_EXISTS,
	REQ_SCAN,

	/** PUT-request family **/

	REQ_PUT,
	REQ_PUT_IFEX,

	REQ_INIT_CONN = 0xFF

} req_t;

typedef enum {

	/** old: [1B retc | 8B count | 8B tsize | <8B size, payload> | ...] **/
	/** [1B retcode | 4B count | 4B total-size | <4B size, value> | ...] */

	REP_GET,
	REP_DEL,
	REP_EXISTS,
	REP_SCAN,

	REP_PUT,
	REP_PUT_IFEX

} rep_t;

struct tcp_req_hdr_reference {
	__u8 type;
	__u32 key_size;
	__u32 value_size;

	char key_value[];

} __attribute__((packed));

struct tcp_rep_hdr_reference {
	__u8 return_code;
	__u32 count;
	__u32 total_size;

	char values[];

} __attribute__((packed));

#endif /** TEBIS_TCP_TYPES_H **/
