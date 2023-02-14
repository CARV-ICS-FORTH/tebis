#include "plog.h"
#include "server_handle.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define LOCALHOST "127.0.0.1"
#define SITH2_IP_56G "192.168.2.122"
#define SITH3_IP_56G
#define SITH4_IP_56G
#define SITH5_IP_56G "192.168.2.125"
#define SITH6_IP_56G "192.168.2.126"

int main(int argc, char **argv)
{
	sConfig sconfig;
	sHandle shandle;

	/** parse/set options **/

	if (server_parse_argv_opts(&sconfig, argc, argv) < 0) { // stupid clang
		plog(PL_ERROR "server_parse_argv_opts(): %s", strerror(errno));
		exit(errno);
	}

	printf("\033server's pid = %d\n", getpid());

	/** start server **/

	if ((server_handle_init(&shandle, sconfig)) < 0) {
		plog(PL_ERROR "server_handle_init(): ", strerror(errno));
		exit(errno);
	}

	if (server_print_config(shandle) < 0) {
		plog(PL_ERROR "server_print_config(): ", strerror(errno));
		exit(errno);
	}

	if (server_spawn_threads(shandle) < 0) {
		plog(PL_ERROR "server_spawn_threads(): ", strerror(errno));
		exit(errno);
	} // blocking call!

	// pause();
	// server_handle_destroy(shandle);

	return EXIT_SUCCESS;
}

// perf top --all-cpus --pid=PID --count-filter=650 --sort cpu,socket,symbol,dso
