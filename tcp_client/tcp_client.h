#ifndef TEBIS_TCP_CLIENT_H
#define TEBIS_TCP_CLIENT_H

#include "tebis_tcp_types.h"

typedef void *c_tcp_req;
typedef void *c_tcp_rep;

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

/**
 * @brief
 *
 * @param rt
 * @return c_tcp_req
 */
c_tcp_req c_tcp_req_init(req_t rt);

/**
 * @brief
 *
 * @param req
 * @return int
 */
int c_tcp_req_destroy(c_tcp_req req);

/**
 * @brief
 *
 * @return c_tcp_rep
 */
c_tcp_rep c_tcp_rep_init(void);

/**
 * @brief
 *
 * @param rep
 * @return int
 */
int c_tcp_rep_destroy(c_tcp_rep rep);

/**
 * @brief
 *
 * @param req
 * @param kv
 * @return int
 */
int c_tcp_req_push_kv(c_tcp_req restrict req, kv_t *restrict kv);

/**
 * @brief
 *
 * @param chandle
 * @param req
 * @return int
 */
int c_tcp_send_req(cHandle restrict chandle, const c_tcp_req restrict req);

/**
 * @brief
 *
 * @param chandle
 * @param rep
 * @return ssize_t
 */
int c_tcp_recv_rep(cHandle restrict chandle, c_tcp_rep restrict rep, generic_data_t *restrict *restrict repbuf);

/**
 * @brief
 *
 * @param repbuf
 * @return int
 */
int c_tcp_print_repbuf(generic_data_t *repbuf);

int fill_req(kv_t *restrict kv, c_tcp_req restrict req, generic_data_t *restrict key, generic_data_t *restrict value);

#endif /** TEBIS_TCP_CLIENT_H **/
