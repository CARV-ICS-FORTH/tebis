#define _GNU_SOURCE

#include "server_handle.h"
#include "plog.h"

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
// #include <linux/io_uring.h>
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

#define HELP_STRING                                                                           \
	"Usage:\n  tcp-server <-bpt>\nOptions:\n"                                             \
	" -t, --threads <thread-num>  specify number of server threads.\n"                    \
	" -b, --bind <if-address>     specify the interface that the server will bind to.\n"  \
	" -p, --port <port>           specify the port that the server will be listening\n\n" \
	" -h, --help     display this help and exit\n"                                        \
	" -v, --version  display version information and exit\n"

#define VERSION_STRING "tcp-server 0.1\n"

#define ERROR_STRING "\e[1m[\e[31m*\e[0;1m]\e[0m"

#define CONFIG_STRING         \
	"[ Server Config ]\n" \
	"  - threads = %u\n"  \
	"  - address = %s\n"  \
	"  - port = %u\n"     \
	"  - flags = not yet supported\n"

/** server argv[] options **/

struct server_options {
	uint16_t magic_init_num;

	unsigned int threadno;
	const char *paddr; // printable ip address
	struct sockaddr_storage inaddr; // ip address + port
};

/** server worker **/

struct worker {
	pthread_t tid;
	uint64_t task_counter;

	int32_t epfd;
	int32_t sock;

	struct buffer buf;
};

/** server handle **/

struct server_handle {
	uint16_t magic_init_num;

	uint32_t flags;
	int32_t sock;
	int32_t epfd;

	struct server_options *opts;
	struct worker *workers;
};

/** server request/reply **/

struct tcp_req {
	req_t type;
	kv_t kv;
};

struct tcp_rep {
	char retc;
	uint64_t paysz; /** TODO: if Tebis returns blobs, replace that with 'count' */

	struct buffer buf; /** TODO: work only with worker's buffer */

	/** TODO: linked list ---> do we know how big each 'SCAN' request is? */
};

struct server_handle *g_sh; // CTRL-C
char g_dummy_responce[1000] = "123456789012345678901234567890123456789\0";

#define reset_errno() errno = 0
#define offset_of_struct_field(x, f) (uint64_t)(&((typeof(x) *)(0UL))->f)
#define offset_of_struct_field$(s, f) (uint64_t)(&((s *)(0UL))->f)

#define is_req_invalid(req) ((uint32_t)(req->type) >= OPSNO)
#define is_req_new_conn(req) (req->type == REQ_INIT_CONN)

#define infinite_loop_start() for (;;) {
#define infinite_loop_end() }
#define event_loop_start(index, limit) for (int index = 0; index < limit; ++index) {
#define event_loop_end() }

/***** private functions (decl) *****/

/**
 * @brief
 *
 * @param cliefd
 * @param worker
 * @return int
 */
static int $client_version_check(int cliefd, struct worker *worker) __attribute__((nonnull));

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
 * @param cliefd
 * @param req
 * @return int
 */
static int $req_recv(struct worker *restrict worker, int cliefd, struct tcp_req *restrict req)
	__attribute__((nonnull(1, 3)));

static int $rep_new(struct tcp_rep *restrict rep, struct worker *this, int retcode, size_t paysz);

/**
 * @brief
 *
 */
static void *$rep_expose_payload(struct tcp_rep *rep) __attribute__((nonnull));

/**
 * @brief
 *
 * @param cliefd
 * @param rep
 * @return int
 */
static int $rep_send(int cliefd, struct tcp_rep *restrict rep) __attribute__((nonnull));

/**
 * @brief
 *
 * @param arg
 * @return void*
 */
static void *$handle_events(void *arg) __attribute__((nonnull));

/**
 * @brief
 *
 * @param type
 * @return const char*
 */
static const char *$req_printable(req_t type);

/**
 * @brief
 *
 * @param req
 */
static void $req_print(struct tcp_req *req) __attribute__((nonnull));

/***** public functions *****/

void server_sig_handler_SIGINT(int signum);

int server_parse_argv_opts(sConfig restrict *restrict sconfig, int argc, char *restrict argv[__restrict_arr])
{
	if (!sconfig) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (argc == 1) {
		fprintf(stderr, USAGE_STRING);
		exit(EXIT_FAILURE);
	}

	struct server_options *opts = *sconfig;

	if (!(opts = malloc(sizeof(*opts))))
		return -(EXIT_FAILURE);

	for (unsigned int i = 1U; i < argc; ++i) {
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

			if (!inet_pton(opts->inaddr.ss_family, argv[i], &opts->inaddr + off)) {
				fprintf(stderr, ERROR_STRING " tcp-server: invalid address\n");
				free(opts);
				exit(EXIT_FAILURE);
			}

			opts->paddr = argv[i];
		} else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
			fprintf(stdout, HELP_STRING);
			free(opts);
			exit(EXIT_SUCCESS);
		} else if (!strcmp(argv[i], "-v") || !strcmp(argv[i], "--version")) {
			fprintf(stdout, VERSION_STRING);
			free(opts);
			exit(EXIT_SUCCESS);
		} else {
			fprintf(stderr, ERROR_STRING " tcp-server: uknown option '%s'\n", argv[i]);
			free(opts);
			exit(EXIT_FAILURE);
		}
	}

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
	       ntohs(((struct sockaddr_in *)(&shandle->opts))->sin_port));

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

	struct server_handle *shandle = *server_handle;

	if (!(shandle = malloc(sizeof(struct server_handle) + (shandle->opts->threadno * sizeof(struct worker)))))
		return -(EXIT_FAILURE);

	shandle->opts = sconf;
	shandle->workers = (struct worker *)((uint8_t *)(shandle) + sizeof(struct server_handle));
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

	/** temp --- dummy response / TODO: remove()**/

	for (uint64_t i = 0UL; i < 100UL; ++i)
		memcpy(g_dummy_responce + i * 10, "saloustros", 10UL);

	/**********/

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

	// munmap() per-thread buffers
	// signal all threads to cancel

	close(shandle->sock);
	close(shandle->epfd);
	free(server_handle);
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

	for (uint32_t index = 0U; index < threads; ++index) {
		shandle->workers[index].sock = shandle->sock;
		shandle->workers[index].epfd = shandle->epfd;
		shandle->workers[index].buf.bytes = DEF_BUF_SIZE;
		shandle->workers[index].buf.mem = shandle->workers[0].buf.mem + (index * DEF_BUF_SIZE);

		if (pthread_create(&shandle->workers[index].tid, NULL, $handle_events,
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

	return EXIT_SUCCESS;
}

int server_wait_threads(sHandle server_handle)
{
	/** TODO: wait for all threads or an error? */
	/** TODO: make main()-thread handle signals etc? */

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

	epoll_ctl(this->epfd, EPOLL_CTL_MOD, this->sock, &epev); /** TODO: always successful? */

	return EXIT_SUCCESS;
}

static int $client_version_check(int cliefd, struct worker *worker)
{
	uint32_t version = be32toh(*((uint8_t *)(worker->buf.mem) + 1UL));

	/** TODO: send a respond to client that uses an outdated version */

	if (version != TT_VERSION) {
		errno = ECONNREFUSED;
		return -(EXIT_FAILURE);
	}

	if (send(cliefd, &version, sizeof(version), 0) < 0) {
		perror("$client_version_check::send()\n");

		errno = ECONNABORTED;
		return -(EXIT_FAILURE);
	}

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = cliefd };

	if (epoll_ctl(worker->epfd, EPOLL_CTL_MOD, cliefd, &epev) < 0) {
		perror("$client_version_check::epoll_ctl(MOD)");
		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

static int $req_recv(struct worker *restrict this, int cliefd, struct tcp_req *restrict req)
{
	/** TODO: find a fixed number */

	if (recv(cliefd, this->buf.mem, 64UL * __x86_PAGESIZE, 0) < 0)
		return -(EXIT_FAILURE);

	req->type = *((uint8_t *)(this->buf.mem));

	if (is_req_invalid(req)) {
		plog(PL_WARN "invalid request, terminating connection...");

		/** TODO: send() retcode + discard receive buffer instead of CTL_DEL (1) */
		/** TODO: just send the appropriate return code without closing the connection (2) */

		epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd, NULL);
		close(cliefd);

		errno = ENOTSUP;
		return -(EXIT_FAILURE);
	}

	if (is_req_new_conn(req) && $client_version_check(cliefd, this) < 0) {
		plog(PL_ERROR "$client_version_check(): %s", strerror(errno));
		epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd,
			  NULL); // kernel 2.6+
		close(cliefd);
	}

	// recv buffer: [1B type | 8B keysz | 8B paysz | payload<key|data>]

	req->kv.key.size = be64toh(*((uint64_t *)(this->buf.mem + 1UL)));
	req->kv.value.size = be64toh(*((uint64_t *)(this->buf.mem + 9UL)));
	req->kv.key.data = this->buf.mem + TT_REQHDR_SIZE;
	req->kv.value.data = req->kv.key.data + req->kv.key.size;

	return EXIT_SUCCESS;
}

static const char *$req_printable(req_t type)
{
	switch (type) {
	case REQ_GET:
		return "REQ_GET";
	case REQ_DEL:
		return "REQ_DEL";
	case REQ_EXISTS:
		return "REQ_EXISTS";
	case REQ_SCAN:
		return "REQ_SCAN";
	case REQ_PUT:
		return "REQ_PUT";
	case REQ_PUT_IFEX:
		return "REQ_PUT_IF_EX";
	case REQ_INIT_CONN:
		return "REQ_INIT_CONN";

	default:
		return "wrong req-type";
	}
}

static void $req_print(struct tcp_req *req)
{
	kv_t *kv = &req->kv;

	printf("\e[1;91m%s\e[0m\n", $req_printable(req->type));

	printf("  - key.size = %lu (0x%lx)\n", kv->key.size, *((uint64_t *)(kv->key.data)));

	if (!req_in_get_family(req))
		printf("  - val.size = %lu (0x%lx)\n\n", kv->value.size, *((uint64_t *)(kv->value.data)));
}

/** TODO: finish reply mechanism (check all rep-functions - SCAN) */
// do I provide a blob for Tebis to write to or
// Tebis send me a ready-to-send blob ?
static int $rep_new(struct tcp_rep *restrict rep, struct worker *restrict this, int retcode, size_t paysz)
{
	/** TODO: make that work for SCAN request too */

	struct tcp_rep *trep = (void *)(this->buf.mem);
	uint64_t tsize = TT_REPHDR_SIZE + sizeof(*trep);

	if (retcode == TT_REQ_SUCC)
		tsize += paysz + 8UL; //temp
	else
		paysz = 0UL; // failure-reply has 0-length payload

	trep->buf.bytes = this->buf.bytes - sizeof(*trep);

	if (trep->buf.bytes < tsize) {
		errno = ENOMEM;
		return -(EXIT_FAILURE);
	}

	trep->buf.mem = (char *)(this->buf.mem) + sizeof(*trep);
	trep->retc = retcode;
	trep->paysz = paysz;

	*((char *)(trep->buf.mem)) = retcode;
	*((uint64_t *)(trep->buf.mem + 1UL)) = htobe64(1UL); // count
	*((uint64_t *)(trep->buf.mem + 9UL)) = htobe64(paysz + 8UL); // total-size
	trep->buf.mem += TT_REPHDR_SIZE;

	return EXIT_SUCCESS;
}

static void *$rep_expose_payload(struct tcp_rep *rep)
{
	return rep->buf.mem;
}

static int $rep_send(int cliefd, struct tcp_rep *rep)
{
	uint64_t tsize = rep->paysz + sizeof(uint64_t) + TT_REPHDR_SIZE;

	/** TODO: add count too */

	if (send(cliefd, rep->buf.mem - TT_REPHDR_SIZE, tsize, 0) < 0)
		return -(EXIT_FAILURE);

	return EXIT_SUCCESS;
}

static void *$handle_events(void *arg)
{
	struct worker *this = arg;
	struct tcp_req req;
	struct tcp_rep rep;

	int events;
	int cliefd;
	int evbits;

	struct epoll_event rearm_event = { .events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT };
	struct epoll_event epoll_events[EPOLL_MAX_EVENTS];

	// pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	// push cleanup function (like at_exit())

	infinite_loop_start();

	events = epoll_wait(this->epfd, epoll_events, EPOLL_MAX_EVENTS, -1);

	if (events < 0) {
		plog(PL_ERROR "epoll() (continue)");
		continue;
	}

	event_loop_start(evindex, events);

	cliefd = epoll_events[evindex].data.fd;
	evbits = epoll_events[evindex].events;

	if (evbits & EPOLLRDHUP) {
		/** received FIN from client **/

		plog(PL_INFO "client (%d) wants to terminate the connection", cliefd);

		// the server can send some more packets here. If so, client code needs some changes

		shutdown(cliefd, SHUT_WR);
		plog(PL_INFO "terminating connection with client(%d)", cliefd);

		epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd, NULL); // kernel 2.6+
		close(cliefd);
	} else if (evbits & EPOLLIN) /** read event **/
	{
		/** new connection **/

		if (cliefd == this->sock) {
			plog(PL_INFO "new connection");

			if ($handle_new_connection(this) < 0)
				plog(PL_ERROR "$handle_new_connection() failed: %s\n", strerror(errno));

			continue;
		}

		/** end **/

		/** request **/

		++this->task_counter; // get info about load balancing (temp)

		if ($req_recv(this, cliefd, &req) < 0) {
			plog(PL_ERROR "$req_recv(): %s", strerror(errno));
			epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd, NULL); // kernel 2.6+
			close(cliefd);

			continue;
		}

		/* tebis_handle_request(); */

		/** TODO: !temp response (SCAN not supported)! change that **/

		/* s_tcp_rep *rep = $rep_new(this, TT_REQ_SUCC, 40UL);
		char *tptr = $rep_expose_payload(rep);
		*((uint64_t *)(tptr)) = 40UL;
		memcpy(tptr + sizeof(uint64_t), g_dummy_responce, 40UL); */

		if ($rep_send(cliefd, &rep) < 0) {
			plog(PL_ERROR "$rep_send(): %s", strerror(errno));

			epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd, NULL); // kernel 2.6+
			close(cliefd);

			continue;
		}

		/* re-enable getting INPUT-events from coresponding client */

		rearm_event.data.fd = cliefd;
		epoll_ctl(this->epfd, EPOLL_CTL_MOD, cliefd, &rearm_event);
	} else if (evbits & EPOLLERR) /** error **/
	{
		plog(PL_ERROR "events[%d] = EPOLLER, fd = %d\n", evindex, cliefd);
		/** TODO: error handling */
		continue;
	}

	event_loop_end();
	infinite_loop_end();
}

/***** server signal handlers *****/

void server_sig_handler_SIGINT(int signum)
{
	printf("received \e[1;31mSIGINT (%d)\e[0m --- printing thread-stats\n", signum);

	for (uint i = 0, lim = g_sh->opts->threadno; i < lim; ++i)
		printf("worker[%u] completed %lu tasks\n", i, g_sh->workers[i].task_counter);

	server_handle_destroy(g_sh);
	printf("\n");
	_Exit(EXIT_SUCCESS);
}

/** TODO: replies on the same NUMA node */
