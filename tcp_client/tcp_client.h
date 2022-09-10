#ifndef TEBIS_TCP_CLIENT_H
#define TEBIS_TCP_CLIENT_H

#include "tebis_tcp_types.h"

typedef void *c_tcp_req;

/** TODO: embed 8-bit 'retc' inside payload->data pointer */

struct tcp_rep {
	int8_t retc; // return code
	generic_data_t payload;
};

typedef void *cHandle;

/**
 * @brief
 *
 */
int chandle_init(cHandle restrict *restrict chandle, const char *restrict addr, const char *restrict port);

/**
 * @brief
 *
 * @param chandle
 */
int chandle_destroy(cHandle chandle);

int c_tcp_req_set_type(cHandle chandle, req_t rtype);

/**
 * @brief
 *
 * @param req
 * @param kv
 * @return int
 */
int c_tcp_req_push(cHandle chandle, generic_data_t *restrict key, generic_data_t *restrict value);

/**
 * @brief
 *
 * @param chandle
 * @param req
 * @return int
 */
int c_tcp_send_req(cHandle chandle);

/**
 * @brief
 *
 * @param chandle
 * @param rep
 * @return ssize_t
 */
int c_tcp_recv_rep(cHandle chandle);

int c_tcp_get_rep_array(cHandle restrict chandle, struct tcp_rep *restrict *restrict rep);

/**
 * @brief
 *
 * @param repbuf
 * @return int
 */
int c_tcp_print_replies(cHandle chandle);

#endif /** TEBIS_TCP_CLIENT_H **/
