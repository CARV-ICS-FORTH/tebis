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

#define req_in_get_family(rtype) ((rtype) <= REQ_EXISTS)

void process_main_args(int argc, char *restrict *restrict argv, gbl_opts *restrict gopts)
{
	if (argc < 2) {
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

int generate_random_gdata(generic_data_t *gdata, uint64_t size)
{
	if (!(gdata->data = malloc(size)))
		return -(EXIT_FAILURE);

	if (getrandom(gdata->data, size, 0) < 0) {
		perror("getrandom()");
		return -(EXIT_FAILURE);
	}

	gdata->size = size;

	return EXIT_SUCCESS;
}

int main(int argc, char **argv)
{
	gbl_opts gopts;

	printf("this.process.pid = %d\n", getpid());
	process_main_args(argc, argv, &gopts);

	cHandle chandle;

	c_tcp_req req;
	c_tcp_rep rep;

	if (chandle_init(&chandle, "localhost", "25565") < 0) {
		print_debug("chandle_init()");
		exit(EXIT_FAILURE);
	}

	if (!(req = c_tcp_req_init(gopts.rtype))) {
		print_debug("tcp_req_init()");
		exit(EXIT_FAILURE);
	}

	if (!(rep = c_tcp_rep_init())) {
		print_debug("tcp_rep_init()");
		exit(EXIT_FAILURE);
	}

	kv_t kv;

	generic_data_t key;
	generic_data_t val;

	for (uint64_t i = 0UL; i < gopts.noreqs; ++i) {
		generate_random_gdata(&key, gopts.keysz);

		if (!req_in_get_family(gopts.rtype))
			generate_random_gdata(&val, gopts.paysz);

		if (fill_req(&kv, req, &key, &val) < 0) {
			print_debug("fill_req()");
			exit(EXIT_FAILURE);
		}

		if (c_tcp_req_push_kv(req, &kv) < 0) {
			print_debug("tcp_req_commit_data()");
			exit(EXIT_FAILURE);
		}
	}

	if (c_tcp_send_req(chandle, req) < 0) {
		print_debug("c_tcp_recv_rep()");
		exit(EXIT_FAILURE);
	}

	generic_data_t *repbuf;

	if (c_tcp_recv_rep(chandle, rep, &repbuf) < 0) {
		print_debug("c_tcp_recv_rep()");
		exit(EXIT_FAILURE);
	}

	c_tcp_print_repbuf(repbuf);
	(void)getchar();
	printf("terminating main()...\n");

	return EXIT_SUCCESS;
}
