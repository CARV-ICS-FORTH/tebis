#include "tcp_client.h"
#include "tebis_tcp_errors.h"

#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	printf("this.process.pid = %d\n", getpid());

	cHandle chandle;
	c_tcp_req req;
	c_tcp_rep rep;

	if (chandle_init(&chandle, "localhost", "25565") < 0) {
		print_debug("chandle_init()");
		exit(EXIT_FAILURE);
	}

	if (!(req = c_tcp_req_init(REQ_PUT))) {
		print_debug("tcp_req_init()");
		exit(EXIT_FAILURE);
	}

	if (!(rep = c_tcp_rep_init())) {
		print_debug("tcp_rep_init()");
		exit(EXIT_FAILURE);
	}

	kv_t kv;

	kv.key.size = 10UL;
	kv.key.data = malloc(10UL); // (0)*

	strncpy(kv.key.data, "key123456.", 10UL);

	kv.value.size = 9UL;
	kv.value.data = malloc(9UL); // (1)*

	bzero(kv.value.data, 9UL);
	strncpy(kv.value.data, "somedata!", 9UL);

	if (c_tcp_req_push_kv(req, &kv) < 0) {
		print_debug("tcp_req_commit_data()");
		exit(EXIT_FAILURE);
	}

	kv.key.size = 11UL;
	kv.key.data = malloc(11UL); // previous pointer (0)* is lost

	strncpy(kv.key.data, "key.98765..", 11UL);

	kv.value.size = 15UL;
	kv.value.data = malloc(15UL); // previous pointer (1)* is lost

	bzero(kv.value.data, 15UL);
	strncpy(kv.value.data, "somedata|aaabbb", 15UL);

	if (c_tcp_req_push_kv(req, &kv) < 0) {
		print_debug("tcp_req_commit_data()");
		exit(EXIT_FAILURE);
	}

	if (c_tcp_send_req(chandle, req) < 0) {
		print_debug("tcp_recv_rep()");
		exit(EXIT_FAILURE);
	}

	generic_data_t *repbuf;

	if (c_tcp_recv_rep(chandle, rep, &repbuf) < 0) {
		print_debug("tcp_recv_rep()");
		exit(EXIT_FAILURE);
	}

	c_tcp_print_repbuf(repbuf);
	printf("terminating main()...\n");

	return EXIT_SUCCESS;
}
