#ifndef TEBIS_TCP_SERVER_H
#define TEBIS_TCP_SERVER_H

#include "tebis_tcp_errors.h"
#include "tebis_tcp_types.h"

#include <sys/socket.h>

typedef void *sHandle;
typedef void *sConfig;
typedef void *s_tcp_req;
typedef void *s_tcp_rep;

/**
 * @brief Initializes an sConfig Object according to the parameters passed to the program (argv). The
 * parameters @a argc and @a argv must be passed exactly as are, from the @b main()'s parameters,
 * otherwise undefined behavior will occur. Run 'tcp-server --help' for all provided options.
 * On success, 0 is returned. On failure, in case of insufficient or wrong program parameters, the
 * program exits, otherwise -1 is returned and errno is set accordingly.
 *
 * @par ERRORS
 * @b EINVAL The sConfig Object @a sConfig is NULL
 *
 * @param sConfig
 * @param argc
 * @param argv
 * @return int
 */
int server_parse_argv_opts(sConfig __restrict__ *__restrict__ sConfig, int argc,
			   char *__restrict__ argv[__restrict_arr]);

/**
 * @brief
 *
 * @param shandle
 * @return int
 */
int server_print_config(sHandle shandle);

/**
 * @brief Initializes an sHandle Object from the given sConfig Object, which must have been initialized with
 * a call to @b server_parse_argv_opts(). On success, 0 is returned. On failure, -1 is returned and @b errno
 * is set to indicate the error.
 *
 * @par ERRORS
 * @b EINVAL Either at least one parameter is NULL, or @a server_config has not been initialized correctly
 *
 * @param server_handle
 * @param server_config
 * @return int
 */
int server_handle_init(sHandle __restrict__ *__restrict__ server_handle, sConfig __restrict__ server_config);

/**
 * @brief Creates all server threads that will be listening to requests from clients. On success, 0 is
 * returned and clients can talk with the server. On failure, -1 is returned and @b errno is set to
 * indicate the error.
 *
 * @par ERRORS
 * @b EINVAL The sHandle Object @a server_handle isn't initialized correctly or it's NULL
 *
 * @param server_handle
 * @return int
 */
int server_spawn_threads(sHandle server_handle);

/**
 * @brief
 *
 * @param server_handle
 * @return int
 */
int server_wait_threads(sHandle server_handle);

/**
 * @brief
 *
 * @param shandle
 * @return int
 */
int server_handle_destroy(sHandle shandle);

void s_tcp_print_req(s_tcp_req req);

#endif /* TEBIS_TCP_SERVER_H */
