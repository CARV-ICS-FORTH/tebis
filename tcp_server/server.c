#include "tcp_server.h"
#include "tebis_tcp_errors.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define SITH3_IP_56G
#define SITH4_IP_56G
#define SITH5_IP_56G "192.168.2.125"
#define SITH6_IP_56G

int main(int argc, char **argv)
{
	printf("%s(%d)\n\n", *argv, argc);
	printf("this.process.pid = %d\n", getpid());

	sHandle shandle;

	if ((shandle_init(&shandle, AF_INET, SITH5_IP_56G, 25565, 16)) < 0) {
		print_debug("shandle_init()");
		exit(EXIT_FAILURE);
	}

	pause();
	shandle_destroy(shandle);

	return EXIT_SUCCESS;
}
