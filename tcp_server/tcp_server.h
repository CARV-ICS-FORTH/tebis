#ifndef TEBIS_TCP_SERVER_H
#define TEBIS_TCP_SERVER_H

#include "tebis_tcp_types.h"

#include <sys/socket.h>

typedef void *sHandle;
typedef void *s_tcp_req;
typedef void *s_tcp_rep;

/**
 * @brief
 *
 * @param shandle
 * @param interface
 * @param port
 * @return int
 */
int shandle_init(sHandle restrict *restrict shandle, int afamily, const char *restrict interface, unsigned short port,
		 uint threads);

/**
 * @brief
 *
 * @param shandle
 * @return int
 */
int shandle_destroy(sHandle shandle);

/**
 * @brief
 *
 * @return s_tcp_req
 */
s_tcp_req s_tcp_req_init(void);

/**
 * @brief
 *
 * @param retcode
 * @return s_tcp_rep
 */
s_tcp_rep s_tcp_rep_init(void);

/**
 * @brief
 *
 * @param req
 */
int s_tcp_req_destroy(s_tcp_req req);

/**
 * @brief
 *
 * @param rep
 * @param gdata
 * @return int
 */
int tcp_rep_push_data(s_tcp_rep restrict rep, generic_data_t *restrict gdata);

void s_tcp_print_req(s_tcp_req req);

#endif /* TEBIS_TCP_SERVER_H */
