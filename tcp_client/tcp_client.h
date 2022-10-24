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

c_tcp_req c_tcp_req_new(req_t rtype, size_t keysz, size_t paysz);
int c_tcp_req_update(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz);
int c_tcp_req_destroy(c_tcp_req req);
void *c_tcp_req_expose_key(c_tcp_req req);
void *c_tcp_req_expose_payload(c_tcp_req req);

c_tcp_rep c_tcp_rep_new(size_t size);

/**
 * @brief
 *
 * @param chandle
 * @param req
 * @return int
 */
int c_tcp_send_req(cHandle chandle, c_tcp_req req);

/**
 * @brief
 *
 * @param chandle
 * @param rep
 * @return ssize_t
 */
int c_tcp_recv_rep(cHandle restrict chandle, c_tcp_rep restrict rep);

/**
 * @brief
 *
 * @param repbuf
 * @return int
 */
int c_tcp_print_reply(c_tcp_rep rep);

#endif /** TEBIS_TCP_CLIENT_H **/
