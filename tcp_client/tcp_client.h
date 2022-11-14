#ifndef TEBIS_TCP_CLIENT_H
#define TEBIS_TCP_CLIENT_H

#include "tebis_tcp_types.h"

typedef void *c_tcp_req;
typedef void *c_tcp_rep;
typedef void *cHandle;

#define TT_DEF_KEYSZ (1UL << 11) // 2KB
#define TT_DEF_PAYSZ (1UL << 12) // 4KB
#define TT_DEF_REPSZ (1UL << 12) // 4KB

#define TT_DEFAULT_PORT 25565 // Minecraft's port

/**
 * @brief
 *
 */
int chandle_init(cHandle __restrict__ *__restrict__ chandle, const char *__restrict__ addr,
		 const char *__restrict__ port);

/**
 * @brief
 *
 * @param chandle
 */
int chandle_destroy(cHandle chandle);

c_tcp_req c_tcp_req_new(req_t rtype, size_t keysz, size_t paysz);
c_tcp_rep c_tcp_rep_new(size_t size);
int c_tcp_req_update(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz);
int c_tcp_req_destroy(c_tcp_req req);
int c_tcp_rep_destroy(c_tcp_rep rep);
void *c_tcp_req_expose_key(c_tcp_req req);
void *c_tcp_req_expose_payload(c_tcp_req req);
void *c_tcp_rep_expose(c_tcp_rep rep);

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
int c_tcp_recv_rep(cHandle __restrict__ chandle, c_tcp_rep __restrict__ rep);

/**
 * @brief
 *
 * @param repbuf
 * @return int
 */
int c_tcp_print_rep(c_tcp_rep rep);

#endif /** TEBIS_TCP_CLIENT_H **/
