/** TODO: support reply for SCAN-request */

#define _GNU_SOURCE

#include "server_handle.h"
#include "parallax/parallax.h"
#include "plog.h"

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
// #include <linux/io_uring.h>
#include <linux/types.h>
#include <log.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>

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

#define HELP_STRING                                                                                \
	"Usage:\n  tcp-server <-bptf>\nOptions:\n"                                                 \
	" -t, --threads <thread-num>  specify number of server threads.\n"                         \
	" -b, --bind <if-address>     specify the interface that the server will bind to.\n"       \
	" -p, --port <port>           specify the port that the server will be listening\n"        \
	" -f, --file <path>           specify the target (file of db) where parallax will run\n\n" \
	" -h, --help     display this help and exit\n"                                             \
	" -v, --version  display version information and exit\n"
#define NECESSARY_OPTIONS 4

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
	__u16 magic_init_num;
	__u32 threadno;

	const char *paddr; // printable ip address
	const char *dbpath;

	struct sockaddr_storage inaddr; // ip address + port
};

/** server worker **/

struct worker {
	pthread_t tid;
	par_handle par_handle;

	__u64 task_counter;
	__s32 epfd;
	__s32 sock;

	struct buffer buf;
};

/** server handle **/

struct server_handle {
	__u16 magic_init_num;
	__u32 flags;
	__s32 sock;
	__s32 epfd;

	par_handle par_handle;

	struct server_options *opts;
	struct worker *workers;
};

/** server request/reply **/

struct tcp_req {
	req_t type;
	kv_t kv;
};

struct tcp_rep {
	retcode_t retc;
	struct par_value val;

	/** TODO: linked list ---> do we know how big each 'SCAN' request is? */
};

struct server_handle *g_sh; // CTRL-C

#define reset_errno() errno = 0
#define offset_of_struct_field(x, f) (__u64)(&((typeof(x) *)(0UL))->f)
#define offset_of_struct_field$(s, f) (__u64)(&((s *)(0UL))->f)

#define req_is_invalid(req) ((__u32)(req->type) >= OPSNO)
#define req_is_new_connection(req) (req->type == REQ_INIT_CONN)
#define req_has_value(req) (req->type >= REQ_SCAN)

#define infinite_loop_start() for (;;) {
#define infinite_loop_end() }
#define event_loop_start(index, limit) for (int index = 0; index < limit; ++index) {
#define event_loop_end() }
#define likely(expr) __builtin_expect(expr, 1L)
#define unlikely(expr) __builtin_expect(expr, 0L)

#define repbuf_hdr_write_retcode(rep, retc) *((__u8 *)(rep->val.val_buffer)) = (__u8)(retc) // return code

#define repbuf_hdr_write_count(rep, count) *((__u64 *)(rep->val.val_buffer + 1UL)) = htobe64((__u64)(count)) // count

#define repbuf_hdr_write_total_size(rep, tsize) \
	*((__u64 *)(rep->val.val_buffer + 9UL)) = htobe64((__u64)(tsize)) // total size

#define reqbuf_hdr_read_type(req, buf) req->type = *((__u8 *)(buf.mem))

#define reqbuf_hdr_read_key_size(req, buf) req->kv.key.size = be64toh(*((__u32 *)(buf.mem + 1UL)))

#define reqbuf_hdr_read_payload_size(req, buf) \
	req->kv.value.size = be64toh(*((__u32 *)(buf.mem + 5UL + req->kv.key.size)))

#define MAX_REGIONS 128

/***** private functions (decl) *****/

/**
 * @brief
 *
 * @param client_sock
 * @param worker
 * @return int
 */
static int $client_version_check(int client_sock, struct worker *worker) __attribute__((nonnull(2)));

/**
 * @brief
 *
 * @param this
 * @return int
 */
static int $handle_new_connection(struct worker *this) __attribute__((nonnull));

/**
 * @brief
 *
 * @param worker
 * @param client_sock
 * @param req
 * @return int
 */
static int $req_recv(struct worker *restrict this, int client_sock, struct tcp_req *restrict req)
	__attribute__((nonnull(1, 3)));

/**
 * @brief
 *
 * @param client_sock
 * @param rep
 * @return int
 */
static inline long int $rep_send(int client_sock, struct tcp_rep *rep) __attribute__((nonnull(2), always_inline));

/**
 * @brief
 *
 * @param arg
 * @return void*
 */
static void *$handle_events(void *arg) __attribute__((nonnull));

static int $par_handle_req(struct worker *restrict this, struct tcp_req *restrict req, struct tcp_rep *restrict rep)
	__attribute__((nonnull));

/***** public functions *****/

void server_sig_handler_SIGINT(int signum);

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

	if (!(*sconfig = malloc(sizeof(*opts))))
		return -(EXIT_FAILURE);

	opts = *sconfig;

	for (int i = 1; i < argc; ++i) {
		if (argv[i][0] != '-') {
			fprintf(stderr, ERROR_STRING " tcp-server: uknown option '%s'\n", argv[i]);
			free(opts);
			exit(EXIT_FAILURE);
		}

		/*/
		* Both 'struct sockaddr_in' (IPv4) and 'struct sockaddr_in6' (IPv6) have the first two of their struct
		* fields identical. First comes the 'socket family' (2-Bytes) and then the 'port' (2-Bytes). As a
		* result of this, when setting either the port or the family, there is no problem to typecast
		* 'struct sockaddr_storage', which can store every address of every socket family in linux, to
		* any of 'struct sockaddr_in' or 'struct sockaddr_in6'. [/usr/include/netinet/in.h]
		/*/

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

			off64_t off;

			if (is_v6) {
				opts->inaddr.ss_family = AF_INET6;
				off = offset_of_struct_field$(struct sockaddr_in6, sin6_addr);
			} else {
				opts->inaddr.ss_family = AF_INET;
				off = offset_of_struct_field$(struct sockaddr_in, sin_addr);
			}

			if (!inet_pton(opts->inaddr.ss_family, argv[i], &(opts->inaddr) + off)) {
				fprintf(stderr, ERROR_STRING " tcp-server: invalid address\n");
				free(opts);
				exit(EXIT_FAILURE);
			}

			++opt_sum;
			opts->paddr = argv[i];
		} else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
			fprintf(stdout, HELP_STRING);
			free(opts);
			exit(EXIT_SUCCESS);
		} else if (!strcmp(argv[i], "-v") || !strcmp(argv[i], "--version")) {
		version:
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
		goto version;

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
	shandle->workers = (struct worker *)((__u32 *)(shandle) + sizeof(struct server_handle));
	shandle->sock = -1;
	shandle->epfd = -1;

	typeof(sconf->inaddr.ss_family) fam = sconf->inaddr.ss_family;

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

	if ((shandle->epfd = epoll_create1(EPOLL_CLOEXEC)) < 0)
		goto cleanup;

	struct epoll_event epev = { .events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT, .data.fd = shandle->sock };

	if (epoll_ctl(shandle->epfd, EPOLL_CTL_ADD, shandle->sock, &epev) < 0)
		goto cleanup;

	/** initialize paralalx **/

	const char *error_message = par_format((char *)(sconf->dbpath), MAX_REGIONS);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	par_db_options db_options = { .volume_name = (char *)(sconf->dbpath), // fuck clang_format!
				      .create_flag = PAR_CREATE_DB,
				      .db_name = "tcp_server_par.db",
				      .options = par_get_default_options() };

	shandle->par_handle = par_open(&db_options, &error_message);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	signal(SIGINT, server_sig_handler_SIGINT);
	g_sh = shandle;

	shandle->magic_init_num = MAGIC_INIT_NUM;

	return EXIT_SUCCESS;

cleanup:
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

	close(shandle->sock);
	close(shandle->epfd);
	munmap(shandle->workers[0].buf.mem, shandle->opts->threadno * DEF_BUF_SIZE);

	const char *error_message = par_close(shandle->par_handle);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	// free(shandle->opts); bug! out-of-bounds access!
	// free(server_handle); bug! same!

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

	__u32 threads = shandle->opts->threadno;

	if ((shandle->workers[0].buf.mem = mmap(NULL, threads * DEF_BUF_SIZE, PROT_READ | PROT_WRITE,
						MAP_PRIVATE | MAP_ANONYMOUS, -1, 0UL)) == MAP_FAILED)
		return -(EXIT_FAILURE);

	__u32 index;

	for (index = 0U, --threads; index < threads; ++index) {
		shandle->workers[index].sock = shandle->sock;
		shandle->workers[index].epfd = shandle->epfd;
		shandle->workers[index].par_handle = shandle->par_handle;
		shandle->workers[index].buf.bytes = DEF_BUF_SIZE;
		shandle->workers[index].buf.mem = shandle->workers[0].buf.mem + (index * DEF_BUF_SIZE);

		if (pthread_create(&shandle->workers[index].tid, NULL, $handle_events,
				   shandle->workers + index)) { // one of the server threads failed!
			__u32 tmp;

			/* kill all threads that have been created by now */
			for (tmp = 0; tmp < index; ++tmp)
				pthread_cancel(shandle->workers[tmp].tid);

			for (tmp = 0; tmp < index; ++tmp)
				pthread_join(shandle->workers[tmp].tid, NULL);

			munmap(shandle->workers[0].buf.mem, threads * DEF_BUF_SIZE);

			return -(EXIT_FAILURE);
		}
	}

	shandle->workers[index].tid = pthread_self();
	shandle->workers[index].sock = shandle->sock;
	shandle->workers[index].epfd = shandle->epfd;
	shandle->workers[index].buf.bytes = DEF_BUF_SIZE;
	shandle->workers[index].buf.mem = shandle->workers[0].buf.mem + (index * DEF_BUF_SIZE);

	$handle_events(shandle->workers + index); // convert 'main()-thread' to 'server-thread'

	return EXIT_SUCCESS;
}

/***** private functions (def) *****/

static int $handle_new_connection(struct worker *this)
{
	struct sockaddr_storage caddr;
	struct epoll_event epev;

	socklen_t socklen;
	int tmpfd;

	if ((tmpfd = accept4(this->sock, (struct sockaddr *)(&caddr), &socklen, SOCK_CLOEXEC | SOCK_NONBLOCK)) < 0) {
		plog(PL_ERROR "%s", strerror(errno));
		return -(EXIT_FAILURE);
	}

	//

	epev.data.fd = tmpfd;
	epev.events = EPOLLIN | EPOLLONESHOT;

	if (epoll_ctl(this->epfd, EPOLL_CTL_ADD, tmpfd, &epev) < 0) {
		perror("$handle_new_connection::epoll_ctl(ADD)");
		close(tmpfd);

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

		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

static int $client_version_check(int client_sock, struct worker *worker)
{
	__u32 version = be32toh(*((__u8 *)(worker->buf.mem) + 1UL));

	/** TODO: send a respond to client that uses an outdated version */

	if (version != TT_VERSION) {
		errno = ECONNREFUSED;
		return -(EXIT_FAILURE);
	}

	if (send(client_sock, &version, sizeof(version), 0) < 0) {
		perror("$client_version_check::send()\n");

		errno = ECONNABORTED;
		return -(EXIT_FAILURE);
	}

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = client_sock };

	if (epoll_ctl(worker->epfd, EPOLL_CTL_MOD, client_sock, &epev) < 0) {
		perror("$client_version_check::epoll_ctl(MOD)");
		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

static int $req_recv(struct worker *restrict this, int client_sock, struct tcp_req *restrict req)
{
	if (unlikely(recv(client_sock, this->buf.mem, DEF_BUF_SIZE, 0) < 0))
		return -(EXIT_FAILURE);

	reqbuf_hdr_read_type(req, this->buf);

	/** TODO: think(): will the request (req-type) ever be invalid/wrong? req_is_invalid() macro */

	if (unlikely(req_is_new_connection(req)))
		return $client_version_check(client_sock, this);

	/* [1B type | 4B key-size | key | 4B value-size | value] */

	reqbuf_hdr_read_key_size(req, this->buf);
	reqbuf_hdr_read_payload_size(req, this->buf);

	req->kv.key.data = this->buf.mem + 5UL;

	if (req_has_value(req))
		req->kv.value.data = this->buf.mem + req->kv.key.size + 9UL;

	return EXIT_SUCCESS;
}

static inline long int $rep_send(int client_sock, struct tcp_rep *rep)
{
	/** buffer scheme: [1B retc | 8B count | 8B tsize | <8B size, payload> | ...] **/

	repbuf_hdr_write_retcode(rep, rep->retc);
	repbuf_hdr_write_count(rep, 1UL);
	repbuf_hdr_write_total_size(rep, rep->val.val_size);

	return send(client_sock, rep->val.val_buffer, rep->val.val_size + TT_REPHDR_SIZE, 0);
}

static void *$handle_events(void *arg)
{
	struct worker *this = arg;
	struct tcp_req req = { 0 };
	struct tcp_rep rep = { .val.val_buffer = this->buf.mem,
			       .val.val_buffer_size = this->buf.bytes - 9U,
			       .val.val_size = 0U };

	int events;
	int client_sock;
	int event_bits;

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

		// the server can send some more packets here. If so, client code needs some changes

		shutdown(client_sock, SHUT_WR);
		plog(PL_INFO "terminating connection with client(%d)", client_sock);

		epoll_ctl(this->epfd, EPOLL_CTL_DEL, client_sock, NULL); // kernel 2.6+
		close(client_sock);
	} else if (likely(event_bits & EPOLLIN)) /** read event **/
	{
		/** new connection **/

		if (client_sock == this->sock) {
			plog(PL_INFO "new connection");

			if ($handle_new_connection(this) < 0)
				plog(PL_ERROR "$handle_new_connection() failed: %s\n", strerror(errno));

			continue;
		}

		/** end **/

		/** request **/

		++this->task_counter; // get info about load balancing (temp)

		if (unlikely($req_recv(this, client_sock, &req) < 0))
			goto client_error;

		$par_handle_req(this, &req, &rep);

		if (unlikely($rep_send(client_sock, &rep) < 0L)) {
			//
			goto client_error;
		}

		/* re-enable getting INPUT-events from the coresponding client */

		rearm_event.data.fd = client_sock;
		epoll_ctl(this->epfd, EPOLL_CTL_MOD, client_sock, &rearm_event);

		continue;

	client_error:
		epoll_ctl(this->epfd, EPOLL_CTL_DEL, client_sock, NULL); // kernel 2.6+
		close(client_sock);

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

/** TODO: change buffer schemes according to parallax */
static int $par_handle_req(struct worker *restrict this, struct tcp_req *restrict req, struct tcp_rep *restrict rep)
{
	const char *error_message;

	switch (req->type) {
	case REQ_GET:

		par_get_serialized(this->par_handle, req->kv.key.data, &rep->val, &error_message);

		if (error_message) {
			plog(PL_ERROR "par_get(): %s");
			return -(EXIT_FAILURE);
		}

		break;

	case REQ_PUT:

		par_put_serialized(this->par_handle, req->kv.key.data, &error_message, 1, 0);

		if (error_message) {
			plog(PL_ERROR "par_put(): %s");
			return -(EXIT_FAILURE);
		}

		break;

	default:

		errno = ENOTSUP;
		return -(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

/***** server signal handlers *****/

void server_sig_handler_SIGINT(int signum)
{
	printf("received \033[1;31mSIGINT (%d)\033[0m --- printing thread-stats\n", signum);

	for (uint i = 0, lim = g_sh->opts->threadno; i < lim; ++i)
		printf("worker[%u] completed %llu tasks\n", i, g_sh->workers[i].task_counter);

	server_handle_destroy(g_sh);
	printf("\n");
	_Exit(EXIT_SUCCESS);
}

/** TODO: replies on the same NUMA node */
