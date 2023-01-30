#include <errno.h>
#include <stdlib.h>

#include "tcp_uring.h"

int tcp_server_uring_setup(struct io_uring **ring)
{
	if (!ring) {
		errno = EINVAL;
		return EXIT_FAILURE;
	}

	if (!(*ring = malloc(sizeof(**ring))))
		return EXIT_FAILURE;

	struct io_uring_params uparams;

	// uparams.sq_thread_cpu =
	// uparams.sq_thread_idle =
	uparams.flags = 0;

	if (io_uring_queue_init_params(TCP_URING_ENTRIES, *ring, &uparams) < 0)
		return EXIT_FAILURE;

	return EXIT_SUCCESS;
}

int tcp_server_uring_destroy(struct io_uring **ring)
{
	if (!ring)
		return EXIT_FAILURE;

	free(*ring);
	*ring = NULL;

	return EXIT_SUCCESS;
}
