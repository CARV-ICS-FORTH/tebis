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

	kv_t kv;

	generic_data_t key = { .size = gopts.keysz };
	generic_data_t val = { .size = gopts.paysz };

	c_tcp_req_set_type(chandle, gopts.rtype);

	for (uint64_t i = 0UL; i < gopts.noreqs; ++i) {
		key.data = malloc(gopts.keysz);
		generate_random_gdata(key.data, gopts.keysz);

		if (!req_in_get_family(gopts.rtype)) {
			val.data = malloc(gopts.paysz);
			generate_random_gdata(val.data, gopts.paysz);
		}

		if (c_tcp_req_push(chandle, &key, &val) < 0) {
			print_debug("c_tcp_req_pust()");
			exit(EXIT_FAILURE);
		}
	}

	if (c_tcp_send_req(chandle) < 0) {
		print_debug("c_tcp_recv_rep()");
		exit(EXIT_FAILURE);
	}

	struct tcp_rep *reparr;

	if (c_tcp_recv_rep(chandle) < 0)
		print_debug("c_tcp_recv_rep()");

	if (c_tcp_get_rep_array(chandle, &reparr) < 0)
		print_debug("c_tcp_get_rep_array()");

	if (c_tcp_print_replies(chandle) < 0)
		print_debug("c_tcp_print_replies()");

	free(key.data);
	free(val.data);

	c_tcp_print_replies(chandle);
	printf("terminating main()...\n");

	return EXIT_SUCCESS;
}
