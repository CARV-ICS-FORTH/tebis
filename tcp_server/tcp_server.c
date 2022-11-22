#define _GNU_SOURCE

#include "tcp_server.h"
#include "tebis_tcp_errors.h"

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
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

#define MAGIC_INIT_NUM (0xCAFE)
#define EPOLL_MAX_EVENTS 64

typedef struct {
	pthread_t tid;
	uint64_t task_counter;

	int32_t sock;
	int32_t epfd;

	struct buffer buf;

} worker_t;

typedef struct {
	uint16_t magic_init_num;
	uint16_t flags;

	int32_t sock;
	int32_t epfd;

	uint32_t num_of_workers;
	worker_t *workers;

} server_handle;

typedef struct {
	req_t type;
	kv_t kv;

} tcp_req;

typedef struct {
	char retc;
	uint64_t paysz;

	struct buffer buf;

	// linked list ---> do we know how big each small SCAN request is?

} tcp_rep;

server_handle *g_sh; // debug
char g_dummy_responce[1000];

#define is_req_invalid(rtype) ((uint32_t)(rtype) >= OPSNO)
#define is_req_init_conn_type(req) (((req).type) == REQ_INIT_CONN)

/*******************************************************************/

static const char *printable_req(req_t type)
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

/**
 * @brief
 *
 * @param clifd
 * @param worker
 * @return int
 */
static int client_version_check(int clifd, worker_t *worker)
{
	uint32_t version = be32toh(*((uint8_t *)(worker->buf.mem) + 1UL));

	/** TODO: send a respond to client that uses an outdated version */

	if (version != TT_VERSION) {
		errno = ECONNREFUSED;
		return -(EXIT_FAILURE);
	}

	if (send(clifd, &version, sizeof(version), 0) < 0) {
		log_error("client_version_check::send()\n");

		errno = ECONNABORTED;
		return -(EXIT_FAILURE);
	}

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = clifd };

	if (epoll_ctl(worker->epfd, EPOLL_CTL_MOD, clifd, &epev) < 0) {
		log_error("client_version_check::epoll_ctl(MOD)");
		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

/**
 * @brief
 *
 * @param this
 * @return int
 */
static int handle_new_connection(worker_t *this)
{
	struct sockaddr_storage caddr;
	struct epoll_event epev;

	socklen_t socklen;
	int tmpfd;

	for (;;) {
		if ((tmpfd = accept4(this->sock, (struct sockaddr *)(&caddr), &socklen, SOCK_CLOEXEC | SOCK_NONBLOCK)) <
		    0) {
			if (errno == EAGAIN ||
			    errno ==
				    EWOULDBLOCK) /** TODO: is this ever going to happen? I don't think so, remove that? */
				break;

			log_error("handle_new_connection::accept4()");
			continue; // replace with "return -(EXIT_FAILURE)" ? EPOLLET
		}

		epev.data.fd = tmpfd;
		epev.events = EPOLLIN | EPOLLONESHOT;

		if (epoll_ctl(this->epfd, EPOLL_CTL_ADD, tmpfd, &epev) < 0) {
			log_error("handle_new_connection::epoll_ctl(ADD)");
			close(tmpfd);

			return -(EXIT_FAILURE);
		}

		epev.events = EPOLLIN | EPOLLONESHOT;
		epev.data.fd = this->sock;

		epoll_ctl(this->epfd, EPOLL_CTL_MOD, this->sock, &epev); // always successfull
	}

	return EXIT_SUCCESS;
}

s_tcp_rep s_tcp_rep_new(worker_t *this, char retcode, size_t paysz);
static void *s_tcp_rep_expose_payload(s_tcp_rep rep);
static int tcp_recv_req(worker_t *restrict worker, int clifd, tcp_req *restrict req);
static int tcp_send_rep(int clifd, s_tcp_rep rep);
static int get_req_hdr(worker_t *restrict worker, int clifd, tcp_req *restrict req);

static void *thread_routine(void *arg)
{
	worker_t *this = arg;
	tcp_req req;

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT };

	// pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	// push cleanup function (like at_exit())

	for (;;) {
		struct epoll_event events[EPOLL_MAX_EVENTS];
		int norfds;

		norfds = epoll_wait(this->epfd, events, EPOLL_MAX_EVENTS, -1);

		if (norfds < 0) {
			log_error("epoll()");
			continue;
		}

		int clifd;
		int eventbits;
		int fdindex;
		int tmp;

		for (fdindex = 0; fdindex < norfds; ++fdindex) {
			clifd = events[fdindex].data.fd;
			eventbits = events[fdindex].events;

			// printf("t[%d]: events = 0x%x\n", getpid(), eventbits);

			if (eventbits & EPOLLHUP) /** connection closed by peer **/
			{
				/** terminate connection with client **/

				printf("terminating connection...\n");

				epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
				close(clifd);
			} else if (eventbits & EPOLLIN) /** read event **/
			{
				if (clifd == this->sock) /** new connection **/
				{
					printf("new connection!\n");
					handle_new_connection(this);
				} else { /** request **/

					++this->task_counter; // get info about load balancing (temp)
					tmp = get_req_hdr(this, clifd, &req);

					if (tmp < 0) /** errors **/
					{
						if (tmp == TT_ERR_CONN_DROP)
							printf("conenction was closed by peer\n");
						else
							log_error("thread_routine::get_req_hdr()");

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
						close(clifd);

						continue;
					}

					if (is_req_init_conn_type(req)) {
						if (client_version_check(clifd, this) < 0) {
							log_warn("thread_routine::client_version_check()");
							epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd,
								  NULL); // kernel 2.6+
							close(clifd);
						}

						continue;
					}

					if (is_req_invalid(req.type)) {
						log_warn("invalid request");

						/** TODO: send() retcode + discard receive buffer instead of CTL_DEL */

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL);
						close(clifd);
						continue;
					}

					if (tcp_recv_req(this, clifd, &req) < 0) {
						log_error("tcp_recv_req()");

						/** TODO: respond with an error message, insted of just close(clifd) */

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
						close(clifd);

						continue;
					}

					/* tebis_handle_request(); */

					/** temp response **/

					s_tcp_rep *rep = s_tcp_rep_new(this, TT_REQ_SUCC, 10UL);
					char *tptr = s_tcp_rep_expose_payload(rep);
					*((uint64_t *)(tptr)) = 1000UL;
					memcpy(tptr + sizeof(uint64_t), g_dummy_responce, 10UL);

					if (tcp_send_rep(clifd, rep) < 0) {
						log_error("tcp_send_rep()");

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
						close(clifd);

						continue;
					}

					/* re-enable getting INPUT-events from coresponding client */

					epev.data.fd = clifd;
					epoll_ctl(this->epfd, EPOLL_CTL_MOD, clifd, &epev);
				}
			} else if (eventbits & EPOLLERR) /** error **/
			{
				log_warn("t%d events[%d] = EPOLLER\n", gettid(), fdindex);
				/** TODO: error handling */
				continue;
			}
		}
	}

	return NULL;
}

static int spawn_workers(server_handle *sh, uint32_t workers)
{
	uint32_t index;

	if ((sh->workers[0].buf.mem = mmap(NULL, workers * DEF_BUF_SIZE, PROT_READ | PROT_WRITE,
					   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0UL)) == MAP_FAILED)
		return -(EXIT_FAILURE);

	for (index = 0; index < workers; ++index) {
		sh->workers[index].sock = sh->sock;
		sh->workers[index].epfd = sh->epfd;
		sh->workers[index].buf.bytes = DEF_BUF_SIZE;
		sh->workers[index].buf.mem = sh->workers[0].buf.mem + (index * DEF_BUF_SIZE);

		if (pthread_create(&sh->workers[index].tid, NULL, thread_routine, sh->workers + index)) {
			uint32_t tmp;

			for (tmp = 0; tmp < index; ++tmp)
				pthread_cancel(sh->workers[tmp].tid);

			for (tmp = 0; tmp < index; ++tmp)
				pthread_join(sh->workers[tmp].tid, NULL);

			munmap(sh->workers[0].buf.mem, workers * DEF_BUF_SIZE);

			return -(EXIT_FAILURE);
		}
	}

	return EXIT_SUCCESS;
}

/*******************************************************************/

void sig_handler_SIGINT(int signum);

int shandle_init(sHandle restrict *restrict shandle, int afamily, const char *restrict addr, unsigned short port,
		 unsigned nothreads)
{
	if (!shandle || !addr) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct sockaddr_storage addrstore;
	socklen_t addrlen;

	if (afamily == AF_INET) /** IPv4 init() **/
	{
		if (inet_pton(afamily, addr, &(((struct sockaddr_in *)(&addrstore))->sin_addr)) != 1) {
			errno = EINVAL;
			return -(EXIT_FAILURE);
		}

		((struct sockaddr_in *)(&addrstore))->sin_family = AF_INET;
		((struct sockaddr_in *)(&addrstore))->sin_port = htons(port);

		addrlen = sizeof(struct sockaddr_in);
	} else if (afamily == AF_INET6) /** IPv6 init() **/
	{
		if (inet_pton(afamily, addr, &(((struct sockaddr_in6 *)(&addrstore))->sin6_addr)) != 1) {
			errno = EINVAL;
			return -(EXIT_FAILURE);
		}

		((struct sockaddr_in6 *)(&addrstore))->sin6_family = AF_INET6;
		((struct sockaddr_in6 *)(&addrstore))->sin6_port = htons(port);

		addrlen = sizeof(struct sockaddr_in6);
	} else // address family not supported
	{
		errno = EAFNOSUPPORT;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	signal(SIGINT, sig_handler_SIGINT);

	if (!(*shandle = malloc(sizeof(server_handle) + (nothreads * sizeof(worker_t)))))
		return -(EXIT_FAILURE);

	server_handle *sh = *shandle;

	sh->workers = (worker_t *)((uint8_t *)(sh) + sizeof(server_handle));
	sh->num_of_workers = nothreads;
	sh->sock = -1;
	sh->epfd = -1;

	if ((sh->sock = socket(afamily, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0)
		goto cleanup;

	int opt = 1;
	setsockopt(sh->sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); /** TODO: remove, only for debug purposes! */

	if (bind(sh->sock, (struct sockaddr *)(&addrstore), addrlen) < 0)
		goto cleanup;

	if (listen(sh->sock, TT_MAX_LISTEN) < 0)
		goto cleanup;

	if ((sh->epfd = epoll_create1(EPOLL_CLOEXEC)) < 0)
		goto cleanup;

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = sh->sock };

	if (epoll_ctl(sh->epfd, EPOLL_CTL_ADD, sh->sock, &epev) < 0)
		goto cleanup;

	/** temp **/

	g_sh = sh;
	for (uint64_t i = 0UL; i < 100UL; ++i)
		memcpy(g_dummy_responce + i * 10, "saloustros", 10UL);

	/**********/

	if (spawn_workers(sh, nothreads) < 0)
		goto cleanup;

	sh->magic_init_num = MAGIC_INIT_NUM;

	return EXIT_SUCCESS;

cleanup:
	close(sh->sock);
	close(sh->epfd);
	free(*shandle);

	return -(EXIT_FAILURE);
}

int shandle_destroy(sHandle shandle)
{
	server_handle *sh = shandle;

	if (!shandle || sh->magic_init_num != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	//

	close(sh->sock);
	free(shandle);

	return EXIT_SUCCESS;
}

s_tcp_rep s_tcp_rep_new(worker_t *this, char retcode, size_t paysz)
{
	tcp_rep *trep = (void *)(this->buf.mem);
	uint64_t tsize = TT_REPHDR_SIZE + sizeof(*trep);

	if (retcode == TT_REQ_SUCC)
		tsize += paysz; // extra page for future use!
	else
		paysz = 0UL; // failure-reply has 0-length payload

	trep->buf.bytes = this->buf.bytes - sizeof(*trep);

	if (trep->buf.bytes < tsize) {
		errno = ENOMEM;
		return NULL;
	}

	trep->buf.mem = (char *)(this->buf.mem) + sizeof(*trep);
	trep->retc = retcode;
	trep->paysz = paysz;

	*((char *)(trep->buf.mem)) = retcode;
	*((uint64_t *)(trep->buf.mem + 1UL)) = htobe64(1UL); // count
	*((uint64_t *)(trep->buf.mem + 9UL)) = htobe64(8UL + 10UL); // total-size
	trep->buf.mem += TT_REPHDR_SIZE;

	return trep;
}

static void *s_tcp_rep_expose_payload(s_tcp_rep rep)
{
	return ((tcp_rep *)(rep))->buf.mem;
}

static int tcp_recv_req(worker_t *restrict this, int clifd, tcp_req *restrict req)
{
	if (!this || clifd < 0 || !req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	uint64_t keysz = req->kv.key.size;
	uint64_t paysz = req->kv.value.size;
	uint64_t req_total_size = keysz + paysz;
	int64_t bytes_read;

	if ((bytes_read = recv(clifd, this->buf.mem, req_total_size, 0)) < 0)
		return -(EXIT_FAILURE);

	req->kv.key.data = this->buf.mem;
	req->kv.value.data = this->buf.mem + keysz;

	return EXIT_SUCCESS;
}

static int get_req_hdr(worker_t *restrict worker, int clifd, tcp_req *restrict req)
{
	int64_t ret;

	if ((ret = recv(clifd, worker->buf.mem, TT_REQHDR_SIZE, 0)) < 0)
		return -(EXIT_FAILURE);

	if (!ret) {
		errno = ECONNABORTED;
		return TT_ERR_CONN_DROP;
	}

	req->type = *((uint8_t *)(worker->buf.mem));

	if (req->type != REQ_INIT_CONN) {
		req->kv.key.size = be64toh(*((uint64_t *)(worker->buf.mem + 1UL)));
		req->kv.value.size = be64toh(*((uint64_t *)(worker->buf.mem + 9UL)));
	}

	return EXIT_SUCCESS;
}

static int tcp_send_rep(int clifd, s_tcp_rep rep)
{
	tcp_rep *trep = rep;
	uint64_t tsize = trep->paysz + sizeof(uint64_t) + TT_REPHDR_SIZE;

	if (send(clifd, trep->buf.mem - TT_REPHDR_SIZE, tsize, 0) < 0)
		return -(EXIT_FAILURE);

	return EXIT_SUCCESS;
}

void s_tcp_print_req(s_tcp_req req)
{
	if (!req)
		return;

	tcp_req *treq = req;
	kv_t *kv = &treq->kv;

	printf("\033[1;91m%s\033[0m\n", printable_req(treq->type));

	printf("  - key.size = %lu (0x%lx)\n", kv->key.size, *((uint64_t *)(kv->key.data)));

	if (!req_in_get_family(treq))
		printf("  - val.size = %lu (0x%lx)\n\n", kv->value.size, *((uint64_t *)(kv->value.data)));
}

/*******************************************************************/

void sig_handler_SIGINT(int signum)
{
	printf("received \033[1;31mSIGINT (%d)\033[0m --- printing thread-stats\n", signum);

	for (uint i = 0, lim = g_sh->num_of_workers; i < lim; ++i)
		printf("worker[%u] completed %lu tasks\n", i, g_sh->workers[i].task_counter);

	printf("\n");
	_Exit(EXIT_SUCCESS);
}
