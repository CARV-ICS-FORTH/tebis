#include "tcp_server.h"
#include "tebis_tcp_errors.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	printf("this.process.pid = %d\n", getpid());

	sHandle shandle;

	if ((shandle_init(&shandle, AF_INET, "127.0.0.1", 25565)) < 0) {
		print_debug("shandle_init()");
		exit(EXIT_FAILURE);
	}

	s_tcp_req *req;

	if (!(req = s_tcp_req_init())) {
		print_debug("s_tcp_req_init()");
		exit(EXIT_FAILURE);
	}

	if (s_tcp_recv_req(shandle, req) < 0) {
		print_debug("s_tcp_recv_req()");
		exit(EXIT_FAILURE);
	}

	s_tcp_print_req(req);

	s_tcp_rep *rep;

	if (!(rep = s_tcp_rep_init())) {
		print_debug("s_tcp_rep_init()");
		exit(EXIT_FAILURE);
	}

	generic_data_t data;

	data.size = 10UL;
	data.data = malloc(10UL);
	strcpy(data.data, "saloustros");

	if (s_tcp_rep_push_data(rep, &data) < 0) {
		print_debug("s_tcp_rep_push_data()");
		exit(EXIT_FAILURE);
	}

	if (s_tcp_send_rep(shandle, 1, rep) < 0) {
		print_debug("s_tcp_send_rep()");
		exit(EXIT_FAILURE);
	}

	getc(stdin);
	shandle_destroy(shandle);

	return EXIT_SUCCESS;
}
