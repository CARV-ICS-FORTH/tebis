#define _GNU_SOURCE

#include "server_handle.h"
#include "parallax/parallax.h"
#include "plog.h"

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
// #include <linux/io_uring.h>
#include <linux/types.h>
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

	par_handle parhandle;

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
	__u64 paylsz; /** TODO: if Tebis returns blobs, replace that with 'count' */

	struct buffer buf; /** TODO: work only with worker's buffer */

	/** TODO: linked list ---> do we know how big each 'SCAN' request is? */
};

struct server_handle *g_sh; // CTRL-C
char g_dummy_responce[1000] = "123456789012345678901234567890123456789\0";

#define reset_errno() errno = 0
#define offset_of_struct_field(x, f) (__u64)(&((typeof(x) *)(0UL))->f)
#define offset_of_struct_field$(s, f) (__u64)(&((s *)(0UL))->f)

#define is_req_invalid(req) ((__u32)(req->type) >= OPSNO)
#define is_req_new_conn(req) (req->type == REQ_INIT_CONN)

#define infinite_loop_start() for (;;) {
#define infinite_loop_end() }
#define event_loop_start(index, limit) for (int index = 0; index < limit; ++index) {
#define event_loop_end() }
#define likely(expr) __builtin_expect(expr, 1L)
#define unlikely(expr) __builtin_expect(expr, 0L)

#define repbuf_hdr_write_retcode(rep, retc) *((__u8 *)(rep->buf.mem)) = (__u8)(retc) // return code
#define repbuf_hdr_write_count(rep, count) *((__u64 *)(rep->buf.mem + 1UL)) = htobe64((__u64)(count)) // count
#define repbuf_hdr_write_total_size(rep, tsize) *((__u64 *)(rep->buf.mem + 9UL)) = htobe64((__u64)(tsize)) // total size
#define reqbuf_hdr_read_type(req, buf) req->type = *((__u8 *)(buf.mem))
#define reqbuf_hdr_read_key_size(req, buf) req->kv.key.size = be64toh(*((__u64 *)(buf.mem + 1UL)))
#define reqbuf_hdr_read_payload_size(req, buf) req->kv.value.size = be64toh(*((__u64 *)(buf.mem + 9UL)))

#define MAX_REGIONS 128U

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

static void *$rep_init(struct tcp_rep *restrict rep, struct worker *this, retcode_t retc, size_t paylsz)
	__attribute__((nonnull(1, 2)));

/**
 * @brief
 *
 * @param cliefd
 * @param rep
 * @return int
 */
static int $rep_send(int cliefd, struct tcp_rep *rep) __attribute__((nonnull(2)));

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
static void $req_print(struct tcp_req *req) __attribute__((nonnull, unused));

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

	const char *error_message = par_format(sconf->dbpath, MAX_REGIONS);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	par_db_options db_options = { .volume_name = sconf->dbpath,
				      .create_flag = PAR_CREATE_DB,
				      .db_name = "tcp_server_par.db",
				      .options = par_get_default_options() };

	shandle->parhandle = par_open(&db_options, &error_message);

	if (error_message) {
		plog(PL_ERROR "%s", error_message);
		return -(EXIT_FAILURE);
	}

	/** temp --- dummy response / TODO: remove()**/

	for (__u64 i = 0UL; i < 100UL; ++i)
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

	// signal all threads to cancel

	close(shandle->sock);
	close(shandle->epfd);
	munmap(shandle->workers[0].buf.mem, shandle->opts->threadno * DEF_BUF_SIZE);

	const char *error_message = par_close(shandle->parhandle);

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
	__u32 version = be32toh(*((__u8 *)(worker->buf.mem) + 1UL));

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
	if (unlikely(recv(cliefd, this->buf.mem, DEF_BUF_SIZE, 0) < 0))
		return -(EXIT_FAILURE);

	reqbuf_hdr_read_type(req, this->buf);

	// will the request (req-type) ever be invalid/wrong? is_req_invalid() macro

	if (unlikely(is_req_new_conn(req)))
		return $client_version_check(cliefd, this);

	// recv buffer: [1B type | 8B keysz | 8B paysz | payload<key|data>]

	reqbuf_hdr_read_key_size(req, this->buf);
	reqbuf_hdr_read_payload_size(req, this->buf);

	req->kv.key.data = this->buf.mem + TT_REQHDR_SIZE;
	req->kv.value.data = (char *)(req->kv.key.data) + req->kv.key.size;

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

	printf("\033[1;91m%s\033[0m\n", $req_printable(req->type));

	printf("  - key.size = %lu (0x%llx)\n", kv->key.size, *((__u64 *)(kv->key.data)));

	if (!req_in_get_family(req))
		printf("  - val.size = %lu (0x%llx)\n\n", kv->value.size, *((__u64 *)(kv->value.data)));
}

/** TODO: support reply for SCAN-request */
/** TODO: rename to rep_update() */
static void *$rep_init(struct tcp_rep *restrict rep, struct worker *restrict this, retcode_t retc, size_t paylsz)
{
	/** buffer scheme: [1B retc | 8B count | 8B tsize | <8B size, payload> | ...] **/

	rep->buf.mem = this->buf.mem;
	rep->retc = retc;

	repbuf_hdr_write_retcode(rep, retc);

	if (retc != RETC_SUCCESS) {
		rep->paylsz = 0UL;
		rep->buf.bytes = TT_REPHDR_SIZE;

		repbuf_hdr_write_count(rep, 0UL);
		repbuf_hdr_write_total_size(rep, 0UL);

		return NULL;
	}

	rep->paylsz = paylsz;
	rep->buf.bytes = TT_REPHDR_SIZE + paylsz;

	repbuf_hdr_write_count(rep, 1UL);
	repbuf_hdr_write_total_size(rep, paylsz);

	/** TODO: remove_code_below(), Parallax knows the buffer size and will provide bounded sizes */

	/* if (rep->buf.bytes > DEF_BUF_SIZE) {
		errno = ENOMEM;
		return NULL;
	} */

	return rep->buf.mem + TT_REPHDR_SIZE;
}

static int $rep_send(int cliefd, struct tcp_rep *rep)
{
	if (unlikely(send(cliefd, rep->buf.mem, rep->buf.bytes, 0) < 0))
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

	void *repbuf;

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
	} else if (likely(evbits & EPOLLIN)) /** read event **/
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

		if (unlikely($req_recv(this, cliefd, &req) < 0)) {
			/** TODO: better error handling? */

			plog(PL_ERROR "$req_recv(): %s", strerror(errno));
			epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd, NULL); // kernel 2.6+
			close(cliefd);

			continue;
		}

		/** TODO: embed error handling inside rep_send to avoid 2-if-statements */

		/* tebis_handle_request(); */
		repbuf = $rep_init(&rep, this, RETC_SUCCESS, 40UL);

		memcpy(repbuf, g_dummy_responce, 40UL);

		if (unlikely($rep_send(cliefd, &rep) < 0)) {
			/** TODO: better error handling? */

			plog(PL_ERROR "$req_send(): %s", strerror(errno));
			epoll_ctl(this->epfd, EPOLL_CTL_DEL, cliefd, NULL); // kernel 2.6+
			close(cliefd);

			continue;
		}

		/* re-enable getting INPUT-events from the coresponding client */

		rearm_event.data.fd = cliefd;
		epoll_ctl(this->epfd, EPOLL_CTL_MOD, cliefd, &rearm_event);
	} else if (unlikely(evbits & EPOLLERR)) /** error **/
	{
		plog(PL_ERROR "events[%d] = EPOLLER, fd = %d\n", evindex, cliefd);
		/** TODO: error handling */
		continue;
	}

	event_loop_end();
	infinite_loop_end();

	__builtin_unreachable();
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
