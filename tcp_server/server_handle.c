/**
 * @file server_handle.c
 * @author Orestis Chiotakis (orchiot@ics.forth.gr)
 * @brief ...
 * @version 0.1
 * @date 2023-02-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#define _GNU_SOURCE
#include "server_handle.h"
#include "btree/btree.h"
#include "btree/kv_pairs.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include "plog.h"
#include "tebis_tcp_errors.h"
#include <assert.h>
#include <stdint.h>

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
// #include <linux/io_uring.h>
#include "../tebis_server/djb2.h"
// #include <linux/types.h>
#include <log.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <threads.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>

#ifdef SGX
#include "../common/common_ssl/mbedtls_utility.h"
#include <mbedtls/certs.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/debug.h>
#include <mbedtls/entropy.h>
#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>
#include <mbedtls/pk.h>
#include <mbedtls/platform.h>
#include <mbedtls/rsa.h>
#include <mbedtls/ssl.h>
#include <mbedtls/x509.h>
#include <openenclave/enclave.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <uthash.h>
#endif
/*** defaults ***/

#define MAGIC_INIT_NUM (0xCAFEu)
#define EPOLL_MAX_EVENTS 64

#define DEFAULT_PORT 6969U
#define DEFAULT_ADDR "127.0.0.1"
#define PORT_MAX ((1L << 16) - 1L)

/*** server options ***/
#define DECIMAL_BASE 10

#define USAGE_STRING                         \
	"tcp-server: no options specified\n" \
	"try 'tcp-server --help' for more information\n"

#define HELP_STRING                                                                \
	"Usage:\n  tcp-server <-bptf>\nOptions:\n"                                 \
	" -t, --threads <thread-num>  specify number of server threads.\n"         \
	" -b, --bind <if-address>     specify the interface that the server will " \
	"bind to.\n"                                                               \
	" -p, --port <port>           specify the port that the server will be "   \
	"listening\n"                                                              \
	" -f, --file <path>           specify the target (file of db) where "      \
	"parallax will run\n\n"                                                    \
	" -h, --help     display this help and exit\n"                             \
	" -v, --version  display version information and exit\n"
#define NECESSARY_OPTIONS 6

#define VERSION_STRING "tcp-server 0.1\n"

#define ERROR_STRING "\033[1m[\033[31m*\033[0;1m]\033[0m"

#define CONFIG_STRING           \
	"[ Server Config ]\n"   \
	"  - threads = %u\n"    \
	"  - address = %s:%u\n" \
	"  - file = %s\n"       \
	"  - flags = not yet supported\n"

/** server argv[] options **/

struct server_options {
	uint16_t magic_init_num;
	uint32_t threadno;

	const char *paddr; // printable ip address
	long port;
	const char *dbpath;

	struct sockaddr_storage inaddr; // ip address + port
};

/** server worker **/

struct worker {
	// par_handle par_handle;
	struct server_handle *shandle;
	pthread_t tid;

	int32_t epfd;
	int32_t sock;
	uint64_t core;

	struct buffer buf;
	struct par_value pval;
};

#ifdef SGX
struct conn_info {
	int32_t fd;
	mbedtls_ssl_context *ssl_session;
	mbedtls_net_context *client_fd;
	UT_hash_handle hh;
};
#endif

/** server handle **/
#define MAX_PARALLAX_DBS 1
struct server_handle {
	uint16_t magic_init_num;
	uint32_t flags;
	int32_t sock;
	int32_t epfd;

	par_handle par_handle[MAX_PARALLAX_DBS];

	struct server_options *opts;
	struct worker *workers;
#ifdef SGX
	mbedtls_net_context listen_fd;
	struct conn_info *conn_ht;
	pthread_rwlock_t lock;
#endif
};

/** server request/reply **/

struct tcp_req {
	req_t type;
	kv_t kv;
	struct kv_splice_base kv_splice_base;
};

struct tcp_rep {
	retcode_t retc;
	struct par_value val;
};

struct server_handle *g_sh; // CTRL-C
_Thread_local const char *par_error_message_tl;

#define reset_errno() errno = 0
#define __offsetof_struct1(s, f) (uint64_t)(&((s *)(0UL))->f)

#define req_is_invalid(req) ((uint32_t)(req->type) >= OPSNO)
#define req_is_new_connection(req) (req->type == REQ_INIT_CONN)

#define infinite_loop_start() for (;;) {
#define infinite_loop_end() }
#define event_loop_start(index, limit) for (int index = 0; index < limit; ++index) {
#define event_loop_end() }

#define reqbuf_hdr_read_type(req, buf) req->type = *((uint8_t *)(buf))

#define MAX_REGIONS 128

/***** private functions (decl) *****/
#ifdef SGX
static void my_debug(void *ctx, int level, const char *file, int line, const char *str)
{
	((void)level);

	mbedtls_fprintf((FILE *)ctx, "%s:%04d: %s", file, line, str);
	fflush((FILE *)ctx);
}
#endif
/**
 * @brief
 *
 * @param client_sock
 * @param worker
 * @return int
 */
static int __client_version_check(int client_sock, struct worker *worker) __attribute__((nonnull(2)));

/**
 * @brief
 *
 * @param this
 * @return int
 */
static int __handle_new_connection(struct worker *this) __attribute__((nonnull));

/**
 * @brief
 *
 * @param worker
 * @param client_sock
 * @param req
 * @return int
 */
static tterr_e __req_recv(struct worker *restrict this, int client_sock, struct tcp_req *restrict req)
	__attribute__((nonnull(1, 3)));

/**
 * @brief
 *
 * @param arg
 * @return void*
 */
static void *__handle_events(void *arg) __attribute__((nonnull));

static int __par_handle_req(struct worker *restrict this, int client_sock, struct tcp_req *restrict req)
	__attribute__((nonnull));

/**
 * @brief
 *
 */
static void __parse_rest_of_header(char *restrict buf, struct tcp_req *restrict req) __attribute__((nonnull));

/**
 * @brief
 *
 */
#ifndef SGX
static int __pin_thread_to_core(int core);
#endif

/**
 * @brief
 *
 * @param server_handle
 * @param key_size
 * @param key
 * @return par_handle
 */
static par_handle __server_handle_get_db(sHandle restrict server_handle, uint32_t key_size, char *restrict key)
	__attribute__((nonnull));

/**
 * @brief
 *
 * @param signum
 */
static void server_sig_handler_SIGINT(int signum);

/***** public functions *****/

int server_parse_argv_opts(sConfig restrict *restrict sconfig, int argc, char *restrict *restrict argv)
{
	if (!sconfig) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (argc <= 1) {
		fprintf(stderr, USAGE_STRING);
		exit(EXIT_FAILURE);
	}

	struct server_options *opts;
	int opt_sum = 0;

	if (!(*sconfig = calloc(1UL, sizeof(*opts))))
		return -(EXIT_FAILURE);

	opts = *sconfig;

	for (int i = 1; i < argc; ++i) {
		if (argv[i][0] != '-') {
			fprintf(stderr, ERROR_STRING " tcp-server: uknown option '%s'\n", argv[i]);
			free(opts);
			exit(EXIT_FAILURE);
		}

		/*/
     * Both 'struct sockaddr_in' (IPv4) and 'struct sockaddr_in6' (IPv6) have
     * the first two of their struct fields identical. First comes the 'socket
     * family' (2-Bytes) and then the 'port' (2-Bytes). As a result of this,
     * when setting either the port or the family, there is no problem to
     * typecast 'struct sockaddr_storage', which can store every address of
     * every socket family in linux, to any of 'struct sockaddr_in' or 'struct
     * sockaddr_in6'. [/usr/include/netinet/in.h]
     */

		if (!strcmp(argv[i], "-t") || !strcmp(argv[i], "--threads")) {
			reset_errno();
			long thrnum = strtol(argv[++i], NULL, DECIMAL_BASE);

			if (errno) {
				if (errno == EINVAL)
					fprintf(stderr, ERROR_STRING " tcp-server: invalid number in option '%s'\n",
						argv[i - 1U]);
				else
					fprintf(stderr,
						ERROR_STRING " tcp-server: number out-of-range in option '%s'\n",
						argv[i - 1U]);

				free(opts);
				exit(EXIT_FAILURE);
			}

			if (thrnum < 0) {
				fprintf(stderr, ERROR_STRING " tcp-server: invalid number in option '%s'\n",
					argv[i - 1U]);
				free(opts);
				exit(EXIT_FAILURE);
			}

			++opt_sum;
			opts->threadno = (unsigned int)thrnum;
		} else if (!strcmp(argv[i], "-p") || !strcmp(argv[i], "--port")) {
			reset_errno();
			long port = strtol(argv[++i], NULL, DECIMAL_BASE);

			if (errno) {
				if (errno == EINVAL)
					fprintf(stderr, ERROR_STRING " tcp-server: invalid number in option '%s'\n",
						argv[i - 1U]);
				else
					fprintf(stderr,
						ERROR_STRING " tcp-server: number out-of-range in option '%s'\n",
						argv[i - 1U]);

				free(opts);
				exit(EXIT_FAILURE);
			}

			if (port < 0) {
				fprintf(stderr, ERROR_STRING " tcp-server: invalid number in option '%s'\n",
					argv[i - 1U]);
				free(opts);
				exit(EXIT_FAILURE);
			} else if (port > PORT_MAX) {
				fprintf(stderr, ERROR_STRING " tcp-server: port is too big\n");
				free(opts);
				exit(EXIT_FAILURE);
			}

			++opt_sum;
			((struct sockaddr_in *)(&opts->inaddr))->sin_port = htons((unsigned short)(port));
			opts->port = port;
		} else if (!strcmp(argv[i], "-b") || !strcmp(argv[i], "--bind")) {
			if (!argv[++i]) {
				fprintf(stderr, ERROR_STRING " tcp-server: no address provided!\n");
				free(opts);
				exit(EXIT_FAILURE);
			}

			int is_v6 = 0;

			for (int tmp = 0; argv[i][tmp]; ++tmp) // is this address IPv6?
			{
				if (argv[i][tmp] == ':') {
					is_v6 = 1;
					break;
				}
			}

			off_t off;

			if (is_v6) {
				opts->inaddr.ss_family = AF_INET6;
				off = __offsetof_struct1(struct sockaddr_in6, sin6_addr);
			} else {
				opts->inaddr.ss_family = AF_INET;
				off = __offsetof_struct1(struct sockaddr_in, sin_addr);
			}

			if (!inet_pton(opts->inaddr.ss_family, argv[i], (char *)(&opts->inaddr) + off)) {
				fprintf(stderr, ERROR_STRING " tcp-server: invalid address\n");
				free(opts);
				exit(EXIT_FAILURE);
			}

			++opt_sum;
			opts->paddr = argv[i];
		} else if (!strcmp(argv[i], "-L0") || !strcmp(argv[i], "--L0_size")) {
			if (!argv[++i]) {
				fprintf(stderr, ERROR_STRING " tcp-server: no address provided!\n");
				free(opts);
				exit(EXIT_FAILURE);
			}
			level0_size = strtoul(argv[i], NULL, 10);
			level0_size = MB(level0_size);
			++opt_sum;
		} else if (!strcmp(argv[i], "-GF") || !strcmp(argv[i], "--GF")) {
			if (!argv[++i]) {
				fprintf(stderr, ERROR_STRING " tcp-server: no address provided!\n");
				free(opts);
				exit(EXIT_FAILURE);
			}
			GF = strtoul(argv[i], NULL, 10);
			++opt_sum;
		} else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
		help:
			fprintf(stdout, HELP_STRING);
			free(opts);
			exit(EXIT_SUCCESS);
		} else if (!strcmp(argv[i], "-v") || !strcmp(argv[i], "--version")) {
			fprintf(stdout, VERSION_STRING);
			free(opts);
			exit(EXIT_SUCCESS);
		} else if (!strcmp(argv[i], "-f") || !strcmp(argv[i], "--file")) {
			if (!argv[++i]) {
				fprintf(stderr, ERROR_STRING " tcp-server: no file provided!\n");
				free(opts);
				exit(EXIT_FAILURE);
			}

			++opt_sum;
			opts->dbpath = argv[i];
		} else {
			fprintf(stderr, ERROR_STRING " tcp-server: uknown option '%s'\n", argv[i]);
			free(opts);
			exit(EXIT_FAILURE);
		}
	}

	if (opt_sum != NECESSARY_OPTIONS)
		goto help;

	opts->magic_init_num = MAGIC_INIT_NUM;

	return EXIT_SUCCESS;
}

int server_print_config(sHandle server_handle)
{
	if (!server_handle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct server_handle *shandle = server_handle;

	if (shandle->magic_init_num != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	printf(CONFIG_STRING, shandle->opts->threadno, shandle->opts->paddr,
	       ntohs(((struct sockaddr_in *)(&shandle->opts->inaddr))->sin_port), shandle->opts->dbpath);

	return EXIT_SUCCESS;
}

#ifdef SGX
int configure_server_ssl(mbedtls_ssl_config *conf, mbedtls_ctr_drbg_context *ctr_drbg, mbedtls_x509_crt *server_cert,
			 mbedtls_pk_context *pkey)
{
	int ret = 1;
	oe_result_t result = OE_FAILURE;

	result = generate_certificate_and_pkey(server_cert, pkey);
	if (result != OE_OK) {
		plog(PL_ERROR "generate_certificate_and_pkey failed with %s\n", oe_result_str(result));
		goto exit;
	}

	if ((ret = mbedtls_ssl_config_defaults(conf, MBEDTLS_SSL_IS_SERVER, MBEDTLS_SSL_TRANSPORT_STREAM,
					       MBEDTLS_SSL_PRESET_DEFAULT)) != 0) {
		plog(PL_ERROR "mbedtls_ssl_config_defaults returned %d\n", ret);
		goto exit;
	}

	mbedtls_ssl_conf_rng(conf, mbedtls_ctr_drbg_random, ctr_drbg);
	mbedtls_ssl_conf_dbg(conf, my_debug, stdout);

	mbedtls_ssl_conf_authmode(conf, MBEDTLS_SSL_VERIFY_NONE);
	mbedtls_ssl_conf_ca_chain(conf, server_cert->next, NULL);

	if ((ret = mbedtls_ssl_conf_own_cert(conf, server_cert, pkey)) != 0) {
		plog(PL_ERROR "mbedtls_ssl_conf_own_cert returned %d\n", ret);
		goto exit;
	}

	ret = 0;
exit:
	fflush(stdout);
	return ret;
}
#endif

#ifdef SGX
mbedtls_entropy_context entropy;
mbedtls_ctr_drbg_context ctr_drbg;
mbedtls_ssl_config conf;
mbedtls_x509_crt server_cert;
mbedtls_pk_context pkey;
const char *pers = "tls_server";
#endif

int server_handle_init(sHandle restrict *restrict server_handle, sConfig restrict server_config)
{
	if (!server_handle || !server_config) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct server_options *sconf = server_config;

	if (sconf->magic_init_num != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	if (!(*server_handle = malloc(sizeof(struct server_handle) + (sconf->threadno * sizeof(struct worker)))))
		return -(EXIT_FAILURE);

	struct server_handle *shandle = *server_handle;

	shandle->opts = sconf;
	shandle->workers = (struct worker *)((char *)(shandle) + sizeof(struct server_handle));
	shandle->sock = -1;
	shandle->epfd = -1;

#ifdef SGX
	/* Load host resolver and socket interface modules explicitly */
	if (load_oe_modules() != OE_OK) {
		plog(PL_ERROR "loading required Open Enclave modules failed\n");
		goto cleanup;
	}

	shandle->conn_ht = NULL;
	// init mbedtls objects
	int ret = 0;
	char port_str[10];
	sprintf(port_str, "%ld", sconf->port);
	mbedtls_net_init(&shandle->listen_fd);
	mbedtls_ssl_config_init(&conf);
	mbedtls_x509_crt_init(&server_cert);
	mbedtls_pk_init(&pkey);
	mbedtls_entropy_init(&entropy);
	mbedtls_ctr_drbg_init(&ctr_drbg);
	oe_verifier_initialize();

	if ((ret = mbedtls_net_bind(&shandle->listen_fd, sconf->paddr, port_str, MBEDTLS_NET_PROTO_TCP)) != 0) {
		plog(PL_ERROR "mbedtls_net_bind returned %d\n", ret);
		goto cleanup;
	}
	shandle->sock = shandle->listen_fd.fd;
	if ((ret = mbedtls_ctr_drbg_seed(&ctr_drbg, mbedtls_entropy_func, &entropy, (const unsigned char *)pers,
					 strlen(pers))) != 0) {
		plog(PL_ERROR "mbedtls_ctr_drbg_seed returned %d\n", ret);
		goto cleanup;
	}
	// Configure server SSL settings
	ret = configure_server_ssl(&conf, &ctr_drbg, &server_cert, &pkey);
	if (ret != 0) {
		plog(PL_ERROR "configure_server_ssl returned %d\n", ret);
		goto cleanup;
	}
#else
	sa_family_t fam = sconf->inaddr.ss_family;

	if ((shandle->sock = socket(fam, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0)
		goto cleanup;

	int opt = 1;
	setsockopt(shandle->sock, SOL_SOCKET, SO_REUSEADDR, &opt,
		   sizeof(opt)); /** TODO: remove, only for debug purposes! */

	if (bind(shandle->sock, (struct sockaddr *)(&sconf->inaddr),
		 (fam == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6)) < 0)
		goto cleanup;

	if (listen(shandle->sock, TT_MAX_LISTEN) < 0)
		goto cleanup;
#endif
	if ((shandle->epfd = epoll_create1(EPOLL_CLOEXEC)) < 0)
		goto cleanup;

	struct epoll_event epev = { .events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT, .data.fd = shandle->sock };

	if (epoll_ctl(shandle->epfd, EPOLL_CTL_ADD, shandle->sock, &epev) < 0) {
		plog(PL_ERROR "epoll_ctl failed\n");
		goto cleanup;
	}

	/** initialize parallax **/
	const char *error_message = par_format((char *)(sconf->dbpath), MAX_REGIONS);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}
	par_db_options db_options = { .volume_name = (char *)(sconf->dbpath), // fuck clang_format!
				      .create_flag = PAR_CREATE_DB,
				      .db_name = "tcp_server_par.db",
				      .options = par_get_default_options() };
	db_options.options[LEVEL0_SIZE].value = level0_size;
	db_options.options[GROWTH_FACTOR].value = GF;
	log_info("Initializing tebis DBs with L0 %u and GF %u", level0_size, GF);
	char actual_db_name[128] = { 0 };
	for (int i = 0; i < MAX_PARALLAX_DBS; i++) {
		if (snprintf(actual_db_name, sizeof(actual_db_name), "tcp_server_par_%d", i) < 0) {
			plog(PL_ERROR, "Failed to construct db name");
			return -(EXIT_FAILURE);
		}
		db_options.db_name = actual_db_name;
		shandle->par_handle[i] = par_open(&db_options, &error_message);
	}

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	signal(SIGINT, server_sig_handler_SIGINT);
	g_sh = shandle;

	shandle->magic_init_num = MAGIC_INIT_NUM;

	return EXIT_SUCCESS;

cleanup:
#ifdef SGX
	mbedtls_net_free(&shandle->listen_fd);
	mbedtls_x509_crt_free(&server_cert);
	mbedtls_pk_free(&pkey);
	mbedtls_ssl_config_free(&conf);
	mbedtls_ctr_drbg_free(&ctr_drbg);
	mbedtls_entropy_free(&entropy);
	oe_verifier_shutdown();
#endif
	close(shandle->sock);
	close(shandle->epfd);
	free(*server_handle);
	return -(EXIT_FAILURE);
}

int server_handle_destroy(sHandle server_handle)
{
	struct server_handle *shandle = server_handle;

	if (!server_handle || shandle->magic_init_num != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	// signal all threads to cancel
#ifdef SGX
	mbedtls_net_free(&shandle->listen_fd);
	mbedtls_x509_crt_free(&server_cert);
	mbedtls_pk_free(&pkey);
	mbedtls_ssl_config_free(&conf);
	mbedtls_ctr_drbg_free(&ctr_drbg);
	mbedtls_entropy_free(&entropy);
	oe_verifier_shutdown();
#endif

	close(shandle->sock);
	close(shandle->epfd);
	munmap(shandle->workers[0].buf.mem, shandle->opts->threadno * DEF_BUF_SIZE);
	free(shandle->opts);

	const char *error_message = par_close(shandle->par_handle);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	free(server_handle);

	return EXIT_SUCCESS;
}

int server_spawn_threads(sHandle server_handle)
{
	if (!server_handle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct server_handle *shandle = server_handle;

	if (shandle->magic_init_num != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	uint32_t threads = shandle->opts->threadno;

	if ((shandle->workers[0].buf.mem = mmap(NULL, threads * DEF_BUF_SIZE, PROT_READ | PROT_WRITE,
						MAP_PRIVATE | MAP_ANONYMOUS, -1, 0UL)) == MAP_FAILED)
		return -(EXIT_FAILURE);

	uint32_t index;

	for (index = 0U, --threads; index < threads; ++index) {
		shandle->workers[index].core = index;
		shandle->workers[index].sock = shandle->sock;
		shandle->workers[index].epfd = shandle->epfd;
		// shandle->workers[index].par_handle = shandle->par_handle;
		shandle->workers[index].shandle = shandle;
		shandle->workers[index].buf.bytes = DEF_BUF_SIZE;
		shandle->workers[index].buf.mem = shandle->workers[0].buf.mem + (index * DEF_BUF_SIZE);
		shandle->workers[index].pval.val_size = 0U;
		shandle->workers[index].pval.val_buffer_size = KV_MAX_SIZE;
		shandle->workers[index].pval.val_buffer =
			shandle->workers[index].buf.mem + TT_REPHDR_SIZE; // [!] one shared buffer per thread!

		if (pthread_create(&shandle->workers[index].tid, NULL, __handle_events,
				   shandle->workers + index)) { // one of the server threads failed!
			uint32_t tmp;

			/* kill all threads that have been created by now */
			for (tmp = 0; tmp < index; ++tmp)
				pthread_cancel(shandle->workers[tmp].tid);

			for (tmp = 0; tmp < index; ++tmp)
				pthread_join(shandle->workers[tmp].tid, NULL);

			munmap(shandle->workers[0].buf.mem, threads * DEF_BUF_SIZE);

			return -(EXIT_FAILURE);
		}
	}

	// convert 'main()-thread' to 'server-thread'

	shandle->workers[index].core = index;
	shandle->workers[index].tid = pthread_self();
	shandle->workers[index].sock = shandle->sock;
	shandle->workers[index].epfd = shandle->epfd;
	// shandle->workers[index].par_handle = shandle->par_handle;
	shandle->workers[index].shandle = shandle;
	shandle->workers[index].buf.bytes = DEF_BUF_SIZE;
	shandle->workers[index].buf.mem = shandle->workers[0].buf.mem + (index * DEF_BUF_SIZE);
	shandle->workers[index].pval.val_size = 0U;
	shandle->workers[index].pval.val_buffer_size = KV_MAX_SIZE;
	shandle->workers[index].pval.val_buffer = shandle->workers[index].buf.mem + TT_REPHDR_SIZE;

	__handle_events(shandle->workers + index);

	return EXIT_SUCCESS;
}

/***** private functions *****/

static int __handle_new_connection(struct worker *this)
{
	// struct sockaddr_storage caddr = { 0 };
	struct epoll_event epev = { 0 };

	// socklen_t socklen = { 0 };
	int tmpfd = 0;

	if (fcntl(this->sock, F_SETFL, O_NONBLOCK) == -1) {
		perror("fcntl nonblock failure");
		return -(EXIT_FAILURE);
	}

	if (fcntl(this->sock, F_SETFD, FD_CLOEXEC) == -1) {
		perror("fcntl cloexec failure");
		return -(EXIT_FAILURE);
	}
#ifdef SGX
	int ret;
	mbedtls_ssl_context *ssl_session = malloc(sizeof(mbedtls_ssl_context));
	if (ssl_session == NULL) {
		return -(EXIT_FAILURE);
	}
	mbedtls_ssl_init(ssl_session);
	if ((ret = mbedtls_ssl_setup(ssl_session, &conf)) != 0) {
		plog(PL_ERROR "mbedtls_ssl_setup returned %d\n", ret);
		free(ssl_session);
		return -(EXIT_FAILURE);
	}
	mbedtls_net_context *client_fd = malloc(sizeof(mbedtls_net_context));
	if (client_fd == NULL) {
		plog(PL_ERROR "malloc of mbedtls_net_context failed");
		return -(EXIT_FAILURE);
	}
	if ((tmpfd = mbedtls_net_accept(&this->shandle->listen_fd, client_fd, NULL, 0, NULL)) < 0) {
		plog(PL_ERROR "mbedtls_net_accept failed with %s", strerror(errno));
		free(ssl_session);
		free(client_fd);
		return -(EXIT_FAILURE);
	}
	tmpfd = client_fd->fd;
	mbedtls_net_set_nonblock(client_fd);
	mbedtls_ssl_set_bio(ssl_session, client_fd, mbedtls_net_send, mbedtls_net_recv, NULL);
	while ((ret = mbedtls_ssl_handshake(ssl_session)) != 0) {
		if (ret == MBEDTLS_ERR_SSL_CONN_EOF) {
			free(ssl_session);
			free(client_fd);
			return -(EXIT_FAILURE);
		}
		if (ret != MBEDTLS_ERR_SSL_WANT_READ && ret != MBEDTLS_ERR_SSL_WANT_WRITE) {
			plog(PL_ERROR "mbedtls_ssl_handshake returned -0x%x\n", -ret);
			free(ssl_session);
			free(client_fd);
			return -(EXIT_FAILURE);
		}
	}

	/*Add (fd, ssl_session) pair to ht*/
	struct conn_info *conn_info = malloc(sizeof(struct conn_info));
	if (conn_info == NULL)
		exit(EXIT_FAILURE);
	conn_info->fd = client_fd->fd;
	conn_info->ssl_session = ssl_session;
	conn_info->client_fd = client_fd;
	if (pthread_rwlock_wrlock(&this->shandle->lock) != 0)
		exit(EXIT_FAILURE);
	HASH_ADD_INT(this->shandle->conn_ht, fd, conn_info);
	pthread_rwlock_unlock(&this->shandle->lock);

#else
	if ((tmpfd = accept(this->sock, NULL, NULL)) < 0) {
		plog(PL_ERROR "%s", strerror(errno));
		perror("Error is ");
		return -(EXIT_FAILURE);
	}
#endif

	epev.data.fd = tmpfd;
	epev.events = EPOLLIN | EPOLLONESHOT;

	if (epoll_ctl(this->epfd, EPOLL_CTL_ADD, tmpfd, &epev) < 0) {
		perror("__handle_new_connection::epoll_ctl(ADD)");
		close(tmpfd);
#ifdef SGX
		free(ssl_session);
		free(client_fd);
#endif
		return -(EXIT_FAILURE);
	}

	epev.events = EPOLLIN | EPOLLONESHOT;
	epev.data.fd = this->sock;
	/** rearm server socket **/

	if (epoll_ctl(this->epfd, EPOLL_CTL_MOD, this->sock, &epev) < 0) {
		plog(PL_ERROR "epoll_ctl(): %s ---> terminating server!!!", strerror(errno));

		epoll_ctl(this->epfd, EPOLL_CTL_DEL, tmpfd, NULL);
		close(this->sock);
		close(tmpfd);
#ifdef SGX
		free(ssl_session);
		free(client_fd);
#endif
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}

static int __client_version_check(int client_sock, struct worker *worker)
{
	uint32_t version = be32toh(*((uint32_t *)(worker->buf.mem + 1UL)));

	/** TODO: send a respond to client that uses an outdated version */

	if (version != TT_VERSION) {
		errno = ECONNREFUSED;
		return -(EXIT_FAILURE);
	}
#ifdef SGX
	struct conn_info *conn_info;
	if (pthread_rwlock_rdlock(&worker->shandle->lock) != 0)
		exit(EXIT_FAILURE);
	HASH_FIND_INT(worker->shandle->conn_ht, &client_sock, conn_info);
	pthread_rwlock_unlock(&worker->shandle->lock);
	if (mbedtls_ssl_write(conn_info->ssl_session, (const unsigned char *)&version, sizeof(version)) < 0) {
#else
	if (send(client_sock, &version, sizeof(version), 0) < 0) {
#endif
		perror("__client_version_check::send()\n");

		errno = ECONNABORTED;
		return -(EXIT_FAILURE);
	}

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = client_sock };

	if (epoll_ctl(worker->epfd, EPOLL_CTL_MOD, client_sock, &epev) < 0) {
		perror("__client_version_check::epoll_ctl(MOD)");
		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

static void __parse_rest_of_header(char *restrict buf, struct tcp_req *restrict req)
{
	// steps:
	// 1. convert the header (in-buffer) from network-order to host-order*
	// 2. save header's contents inside
	// * parallax will use the same buffer using zero-copy!

	/** GET: [ 1B type | 4B key-size | key ] **/
	/** PUT: [ 1B type | 4B key-size | 4B value-size | key | value ] **/

	*((uint32_t *)(buf + 1UL)) = be32toh(*((uint32_t *)(buf + 1UL)));
	req->kv.key.size = *((uint32_t *)(buf + 1UL));

	if (req->type == REQ_PUT) {
		*((uint32_t *)(buf + 5UL)) = be32toh(*((uint32_t *)(buf + 5UL)));
		req->kv.value.size = *((uint32_t *)(buf + 5UL));
		req->kv.key.data = buf + 9UL;
		req->kv.value.data = (buf + 9UL) + req->kv.key.size;
	} else /* REQ_GET */ {
		req->kv.value.size = 0U;
		req->kv.value.data = NULL;
		req->kv.key.data = buf + 5UL;
	}
}

static tterr_e __req_recv(struct worker *restrict this, int client_sock, struct tcp_req *restrict req)
{
#ifdef SGX
	struct conn_info *conn_info;
	if (pthread_rwlock_rdlock(&this->shandle->lock) != 0)
		exit(EXIT_FAILURE);
	HASH_FIND_INT(this->shandle->conn_ht, &client_sock, conn_info);
	pthread_rwlock_unlock(&this->shandle->lock);
	int ret;
	while ((ret = mbedtls_ssl_read(conn_info->ssl_session, (unsigned char *)this->buf.mem, DEF_BUF_SIZE)) <= 0) {
		if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) {
			continue;
		}
		return TT_ERR_GENERIC;
	}
#else
	int64_t ret = recv(client_sock, this->buf.mem, DEF_BUF_SIZE, 0);
#endif

	if (unlikely(ret < 0))
		return TT_ERR_GENERIC;

	reqbuf_hdr_read_type(req, this->buf.mem);

	if (unlikely(req_is_new_connection(req)))
		return __client_version_check(client_sock, this);

	if (unlikely(req_is_invalid(req)))
		return TT_ERR_NOT_SUP;

	__parse_rest_of_header(this->buf.mem, req);

	if (unlikely(!req->kv.key.size))
		return TT_ERR_ZERO_KEY;

	register uint64_t off = ret;
	register int64_t bytes_left =
		(req->kv.key.size + req->kv.value.size + ((req->type == REQ_GET) ? 5UL : 9UL)) - ret;

	while (bytes_left) {
#ifdef SGX
		while ((ret = mbedtls_ssl_read(conn_info->ssl_session, (unsigned char *)(this->buf.mem + off),
					       bytes_left)) <= 0) {
			if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) {
				continue;
			}
			return TT_ERR_GENERIC;
		}
#else
		ret = recv(client_sock, this->buf.mem + off, bytes_left, 0);
#endif

		if (unlikely(ret < 0))
			return TT_ERR_GENERIC;

		off += ret;
		bytes_left -= ret;
	}

	return TT_ERR_NONE;
}

static void *__handle_events(void *arg)
{
	struct worker *this = arg;
#ifndef SGX
	if (__pin_thread_to_core(this->core) < 0) {
		plog(PL_ERROR "__pin_thread_to_core(): %s", strerror(errno));
		exit(EXIT_FAILURE);
	}
#endif

	/*
          uint32_t key_size = *(uint32_t *)(&this->buf.mem[1]);
          uint32_t value_size = *(uint32_t *)(&this->buf.mem[5]);
          struct tcp_req req = { .kv_splice_base.kv_cat =
     calculate_KV_category(key_size, value_size, insertOp),
                                 .kv_splice_base.kv_splice = (void
     *)(this->buf.mem + 1UL) };
  */
	struct tcp_req req = { 0 };

	int events;
	int client_sock;
	int event_bits;
	int ret;

	struct epoll_event rearm_event = { .events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT };
	struct epoll_event epoll_events[EPOLL_MAX_EVENTS];

	// pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	// push cleanup function (like at_exit())

	infinite_loop_start();

	events = epoll_wait(this->epfd, epoll_events, EPOLL_MAX_EVENTS, -1);

	if (unlikely(events < 0)) {
		plog(PL_ERROR "epoll(): %s", strerror(errno));
		continue;
	}

	event_loop_start(evindex, events);

	client_sock = epoll_events[evindex].data.fd;
	event_bits = epoll_events[evindex].events;

	if (event_bits & EPOLLRDHUP) {
		/** received FIN from client **/

		plog(PL_INFO "client (%d) wants to terminate the connection", client_sock);

		// the server can send some more packets here. If so, client code needs some
		// changes

		shutdown(client_sock, SHUT_WR);
		plog(PL_INFO "terminating connection with client(%d)", client_sock);

		epoll_ctl(this->epfd, EPOLL_CTL_DEL, client_sock, NULL); // kernel 2.6+
		close(client_sock);
#ifdef SGX
		struct conn_info *conn_info;
		if (pthread_rwlock_wrlock(&this->shandle->lock) != 0) {
			continue;
		}
		HASH_FIND_INT(this->shandle->conn_ht, &client_sock, conn_info);
		free(conn_info->ssl_session);
		free(conn_info->client_fd);
		HASH_DEL(this->shandle->conn_ht, conn_info);
		pthread_rwlock_unlock(&this->shandle->lock);
#endif
	} else if (likely(event_bits & EPOLLIN)) /** read event **/
	{
		/** new connection **/
		if (client_sock == this->sock) {
			plog(PL_INFO "new connection", NULL);

			if (__handle_new_connection(this) < 0)
				plog(PL_ERROR "__handle_new_connection() failed: %s\n", strerror(errno));

			continue;
		}

		/** end **/

		/** request **/
		ret = __req_recv(this, client_sock, &req);

		if (unlikely(ret == TT_ERR_CONN_DROP)) {
			plog(PL_INFO "client terminated connection!\n", NULL);
			goto client_error;
		}

		if (unlikely(ret == TT_ERR_GENERIC)) {
			plog(PL_ERROR "__req_recv(): %s", strerror(errno));
			goto client_error;
		}

		if (req.type == REQ_INIT_CONN)
			continue;

		if (unlikely(__par_handle_req(this, client_sock, &req) < 0L)) {
			plog(PL_ERROR "__par_handle_req(): %s", strerror(errno));
			goto client_error;
		}

		/* re-enable getting INPUT-events from the coresponding client */

		rearm_event.data.fd = client_sock;
		epoll_ctl(this->epfd, EPOLL_CTL_MOD, client_sock, &rearm_event);

		continue;

	client_error:
		epoll_ctl(this->epfd, EPOLL_CTL_DEL, client_sock, NULL); // kernel 2.6+
		close(client_sock);
#ifdef SGX
		struct conn_info *conn_info;
		if (pthread_rwlock_wrlock(&this->shandle->lock) != 0) {
			continue;
		}
		HASH_FIND_INT(this->shandle->conn_ht, &client_sock, conn_info);
		free(conn_info->ssl_session);
		free(conn_info->client_fd);
		HASH_DEL(this->shandle->conn_ht, conn_info);
		pthread_rwlock_unlock(&this->shandle->lock);
#endif
	} else if (unlikely(event_bits & EPOLLERR)) /** error **/
	{
		plog(PL_ERROR "events[%d] = EPOLLER, fd = %d\n", evindex, client_sock);
		/** TODO: error handling */
		continue;
	}

	event_loop_end();
	infinite_loop_end();

	__builtin_unreachable();
}

static par_handle __server_handle_get_db(sHandle restrict server_handle, uint32_t key_size, char *restrict key)
{
	struct server_handle *shandle = server_handle;
	uint64_t hash_id = djb2_hash((unsigned char *)key, key_size);

	par_handle par_db = shandle->par_handle[hash_id % MAX_PARALLAX_DBS];
	// printf("Chose db %lu\n", hash_id % MAX_PARALLAX_DBS);
	return par_db;
}

static int __par_handle_req(struct worker *restrict this, int client_sock, struct tcp_req *restrict req)
{
#ifdef SGX
	int ret;
#endif
	par_handle par_db = __server_handle_get_db(this->shandle, req->kv.key.size, req->kv.key.data);
	uint32_t tmp = 0;
	// struct conn_info *conn_info;

	switch (req->type) {
	case REQ_GET:

		/** [ 1B type | 4B key-size | key ] **/

		par_get_serialized(par_db, req->kv.key.data - 4UL, &this->pval,
				   &par_error_message_tl); // '-5UL' temp solution

		if (par_error_message_tl) {
			plog(PL_WARN "par_get_serialized(): %s", par_error_message_tl);

			tmp = strlen(par_error_message_tl) + 1U;
			*((uint8_t *)(this->buf.mem)) = TT_REQ_FAIL;
			*((uint32_t *)(this->buf.mem + 1UL)) = htobe32(tmp);

			memcpy(this->buf.mem + TT_REPHDR_SIZE, par_error_message_tl, tmp);
		} else {
			*((uint8_t *)(this->buf.mem)) = TT_REQ_SUCC;
			*((uint32_t *)(this->buf.mem + 1UL)) = htobe32(this->pval.val_size);

			tmp = this->pval.val_size;
			// memcpy(this->buf.mem + TT_REPHDR_SIZE, this->pval.val_buffer, tmp);
		}

#ifdef SGX
		if (pthread_rwlock_rdlock(&this->shandle->lock) != 0) {
			return -1;
		}
		HASH_FIND_INT(this->shandle->conn_ht, &client_sock, conn_info);
		pthread_rwlock_unlock(&this->shandle->lock);
		while ((ret = mbedtls_ssl_write(conn_info->ssl_session, (const unsigned char *)this->buf.mem,
						TT_REPHDR_SIZE + tmp)) <= 0) {
			if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE)
				continue;
			return -1;
		}
		return 0;
#else
		return send(client_sock, this->buf.mem, TT_REPHDR_SIZE + tmp, 0);
#endif

	case REQ_PUT:

		/** [ 1B type | 4B key-size | 4B value-size | key | value ] **/
		;
		char *serialized_buf = req->kv.key.data - 8UL;
		uint32_t key_size = *(uint32_t *)&serialized_buf[0];
		uint32_t value_size = *(uint32_t *)&serialized_buf[4];
		struct kv_splice_base splice_base = { .kv_cat = calculate_KV_category(key_size, value_size, insertOp),
						      .kv_type = KV_FORMAT,
						      .kv_splice = (struct kv_splice *)serialized_buf };
		par_put_serialized(par_db, (char *)&splice_base, &par_error_message_tl, true,
				   false); // '-8UL' temp solution

		if (par_error_message_tl) {
			plog(PL_ERROR "par_put_serialized(): %s", par_error_message_tl);

			tmp = strlen(par_error_message_tl);
			*((uint8_t *)(this->buf.mem)) = TT_REQ_FAIL;
			*((uint32_t *)(this->buf.mem + 1UL)) = htobe32(tmp);

			memcpy(this->buf.mem + TT_REPHDR_SIZE, par_error_message_tl, tmp);
		} else
			*((uint8_t *)(this->buf.mem)) = TT_REQ_SUCC;

		/* PUT-req does not return a value */

		*((uint32_t *)(this->buf.mem + 1UL)) = 0U;
#ifdef SGX
		if (pthread_rwlock_rdlock(&this->shandle->lock) != 0) {
			return -1;
		}
		HASH_FIND_INT(this->shandle->conn_ht, &client_sock, conn_info);
		pthread_rwlock_unlock(&this->shandle->lock);
		while ((ret = mbedtls_ssl_write(conn_info->ssl_session, (const unsigned char *)this->buf.mem,
						TT_REPHDR_SIZE) <= 0)) {
			if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE)
				continue;
			return -1;
		}
		return 0;
#else
		return send(client_sock, this->buf.mem, TT_REPHDR_SIZE, 0);
#endif

	default:

		printf("req->type = %d\n", req->type);
		errno = ENOTSUP;
		return -(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

#ifndef SGX
static int __pin_thread_to_core(int core)
{
	cpu_set_t cpuset;

	CPU_ZERO(&cpuset);
	CPU_SET(core, &cpuset);

	return sched_setaffinity(0, sizeof(cpuset), &cpuset);
}
#endif

/***** server signal handlers *****/

void server_sig_handler_SIGINT(int signum)
{
	printf("received \033[1;31mSIGINT (%d)\033[0m\n", signum);

	server_handle_destroy(g_sh);
	printf("\n");
	_Exit(EXIT_SUCCESS);
}
