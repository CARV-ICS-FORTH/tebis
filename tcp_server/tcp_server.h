#ifndef TEBIS_TCP_SERVER_H
#define TEBIS_TCP_SERVER_H

#include "tebis_tcp_errors.h"
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

void s_tcp_print_req(s_tcp_req req);

#endif /* TEBIS_TCP_SERVER_H */
