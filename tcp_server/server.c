#include "server_handle.h"
#include <arpa/inet.h>
#include <errno.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef SGX
#include <openenclave/enclave.h>
#endif

// #define LOCALHOST "127.0.0.1"
// #define SITH2_IP_56G "192.168.2.122"
// #define SITH3_IP_56G
// #define SITH4_IP_56G
// #define SITH5_IP_56G "192.168.2.125"
// #define SITH6_IP_56G "192.168.2.126"

uint32_t level0_size = 0;
uint32_t GF = 0;
int main(int argc, char **argv)
{
#ifdef SGX
	oe_load_module_host_socket_interface();
#endif

#ifdef DEBUG_BUILD_TYPE
	log_set_level(LOG_DEBUG);
#elif RELEASE_BUILD_TYPE
	log_set_level(LOG_INFO);
#else
	log_fatal(
		"Build must be in release or debug mode, if not please update the corresponding flags in /path/to/tebis/CMakeLists.txt");
	_exit(EXIT_FAILURE);
#endif
	sConfig sconfig;
	sHandle shandle;

	/** parse/set options **/

	if (server_parse_argv_opts(&sconfig, argc, argv) < 0) {
		log_fatal("server_parse_argv_opts(): %s", strerror(errno));
		exit(errno);
	}

	printf("\033server's pid = %d\n", getpid());

	/** start server **/

	if ((server_handle_init(&shandle, sconfig)) < 0) {
		log_fatal("server_handle_init(): %s: ", strerror(errno));
		exit(errno);
	}

	if (server_print_config(shandle) < 0) {
		log_fatal("server_print_config(): %s", strerror(errno));
		exit(errno);
	}

	if (server_spawn_threads(shandle) < 0) {
		log_fatal("server_spawn_threads(): %s", strerror(errno));
		exit(errno);
	} // blocking call!

	// pause();
	// server_handle_destroy(shandle);

	return EXIT_SUCCESS;
}

// perf top --all-cpus --pid=PID --count-filter=650 --sort cpu,socket,symbol,dso
