#include "tcp_client.h"
#include "tebis_tcp_errors.h"

#include <stdlib.h>
#include <sys/random.h>
#include <unistd.h>

typedef struct gbl_opts {
	req_t rtype;

	uint64_t noreqs;
	uint64_t paysz;
	uint64_t keysz;

	int mode;
} gbl_opts;

extern int debug_print_req(c_tcp_req req);

void process_main_args(int argc, char *restrict *restrict argv, gbl_opts *restrict gopts)
{
	if (argc < 4) {
		printf("usage: client \e[3mreq-type noreqs payload-size key-size [mode]\e[0m\n");
		exit(EXIT_FAILURE);
	}

	long tmp;

	if (!(tmp = strtol(argv[1], NULL, 10)) && errno == EINVAL) {
		printf("'\e[3mreq-type\e[0m' is not a valid number\n");
		exit(EXIT_FAILURE);
	}

	gopts->rtype = tmp;

	if (!(tmp = strtol(argv[2], NULL, 10)) && errno == EINVAL) {
		printf("'\e[3mnoreqs\e[0m' is not a valid number\n");
		exit(EXIT_FAILURE);
	}

	gopts->noreqs = tmp;

	if (!(tmp = strtol(argv[3], NULL, 10)) && errno == EINVAL) {
		printf("'\e[3mpayload-size\e[0m' is not a valid number\n");
		exit(EXIT_FAILURE);
	}

	gopts->paysz = tmp;

	if (!(tmp = strtol(argv[4], NULL, 10)) && errno == EINVAL) {
		printf("'\e[3mkey-size\e[0m' is not a valid number\n");
		exit(EXIT_FAILURE);
	}

	gopts->keysz = tmp;

	if (argc == 6 && !(tmp = strtol(argv[5], NULL, 10)) && errno == EINVAL) {
		// under construction...
		printf("'\e[3mmode\e[0m' is not a valid number\n");
		exit(EXIT_FAILURE);
	}

	gopts->mode = tmp;
}

int generate_random_gdata(void *ptr, uint64_t size)
{
	if (getrandom(ptr, size, 0) < 0) {
		perror("getrandom()");
		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

int main(int argc, char **argv)
{
	gbl_opts gopts;

	printf("this.process.pid = %d\n", getpid());
	process_main_args(argc, argv, &gopts);

	cHandle chandle;

	if (chandle_init(&chandle, "localhost", "25565") < 0) {
		print_debug("chandle_init()");
		exit(EXIT_FAILURE);
	}

	c_tcp_req req;
	c_tcp_rep rep;

	if (!(req = c_tcp_req_new(REQ_GET, gopts.keysz, gopts.paysz))) {
		perror("c_tcp_req_new() failed");
		exit(EXIT_FAILURE);
	}

	char *__key = c_tcp_req_expose_key(req);
	char *__pay = c_tcp_req_expose_payload(req);

	if (!__pay)
		perror("__pay -->");

	generate_random_gdata(__key, gopts.keysz);

	if (debug_print_req(req) < 0)
		print_debug("debug_print_req()");

	if (c_tcp_send_req(chandle, req) < 0) {
		print_debug("c_tcp_send_req");
		exit(EXIT_FAILURE);
	}

	if (!(rep = c_tcp_rep_new(8192UL))) {
		print_debug("c_tcp_rep_new");
		exit(EXIT_FAILURE);
	}

	if (c_tcp_recv_rep(chandle, rep) < 0) {
		print_debug("c_tcp_recv_rep()");
		exit(EXIT_FAILURE);
	}

	if (c_tcp_print_rep(rep) < 0)
		print_debug("c_tcp_print_replies()");

	chandle_destroy(chandle);
	c_tcp_req_destroy(req);
	c_tcp_rep_destroy(rep);
	printf("terminating...\n");

	return EXIT_SUCCESS;
}
