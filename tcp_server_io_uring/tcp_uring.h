#ifndef TCP_URING_H
#define TCP_URING_H

// https://manpages.debian.org/unstable/liburing-dev/
// https://unixism.net/loti/ref-liburing/index.html

#define TCP_URING_ENTRIES ((2U << 10)) /** TODO: rethink() / [1, 4096] */

#include <liburing.h>

extern int tcp_server_uring_setup(struct io_uring **ring);
extern int tcp_server_uring_destroy(struct io_uring **ring);

#endif /** TCP_URING_H **/
