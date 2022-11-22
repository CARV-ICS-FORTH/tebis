#include "tcp_server.h"
#include "tebis_tcp_errors.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	printf("%s(%d)\n\n", *argv, argc);
	printf("this.process.pid = %d\n", getpid());

	sHandle shandle;

	if ((shandle_init(&shandle, AF_INET, "192.168.2.125", 25565, 8)) < 0) {
		print_debug("shandle_init()");
		exit(EXIT_FAILURE);
	}

	pause();
	shandle_destroy(shandle);

	return EXIT_SUCCESS;
}
