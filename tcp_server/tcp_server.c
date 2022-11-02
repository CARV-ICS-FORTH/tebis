#define _GNU_SOURCE

#include "tcp_server.h"
// #include "tebis_tcp_errors.h"

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>

#define MAGIC_INIT_NUM (0xCAFE)
#define EPOLL_MAX_EVENTS 50

typedef struct {
	pthread_t tid;

	int32_t sock;
	int32_t epfd;

	struct buffer buf;

} worker_t;

typedef struct {
	uint16_t magic_init_num;
	uint16_t flags;

	int32_t sock;
	int32_t epfd;

	worker_t *workers;

} server_handle;

typedef struct {
	req_t type;
	kv_t kv;

} tcp_req;

typedef struct {

	int32_t retc;
	uint64_t paysz;

	struct buffer buf;

} tcp_rep;

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

	if (version != TT_VERSION) {
		errno = ECONNREFUSED;
		return -(EXIT_FAILURE);
	}

	if (send(clifd, &version, sizeof(version), 0) < 0) {
		perror("client_version_check::send()\n");

		errno = ECONNABORTED;
		return -(EXIT_FAILURE);
	}

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = clifd };

	if (epoll_ctl(worker->epfd, EPOLL_CTL_MOD, clifd, &epev) < 0) {
		perror("client_version_check::epoll_ctl(MOD)");
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
	for (;;) {
		struct sockaddr_storage caddr;
		struct epoll_event epev;

		socklen_t socklen;
		int tmpfd;

		if ((tmpfd = accept4(this->sock, (struct sockaddr *)(&caddr), &socklen, SOCK_CLOEXEC | SOCK_NONBLOCK)) <
		    0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;

			perror("handle_new_connection::accept4()");
			continue; // replace with "return -(EXIT_FAILURE)" ? EPOLLET
		}

		epev.data.fd = tmpfd;
		epev.events = EPOLLIN | EPOLLONESHOT;

		if (epoll_ctl(this->epfd, EPOLL_CTL_ADD, tmpfd, &epev) < 0) {
			perror("handle_new_connection::epoll_ctl(ADD)");
			close(tmpfd);
		}

		epev.events = EPOLLIN | EPOLLONESHOT;
		epev.data.fd = this->sock;

		if (epoll_ctl(this->epfd, EPOLL_CTL_MOD, this->sock, &epev) < 0) {
			perror("handle_new_connection::epoll_ctl(MOD)");
			exit(EXIT_FAILURE); // Will this ever happen ?
		}
	}

	return EXIT_SUCCESS;
}

s_tcp_rep s_tcp_rep_new(worker_t *this, int retcode, size_t paysz);
static void *s_tcp_rep_expose_payload(s_tcp_rep rep);
static int tcp_recv_req(worker_t *restrict worker, int clifd, tcp_req *restrict req);
static int tcp_send_rep(worker_t *restrict worker, int clifd, s_tcp_rep restrict rep);
static int get_req_hdr(worker_t *restrict worker, int clifd, tcp_req *restrict req);

static void *thread_routine(void *arg)
{
	worker_t *this = arg;
	tcp_req req;

	// pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	// push cleanup function (like at_exit())

	for (;;) {
		struct epoll_event events[EPOLL_MAX_EVENTS];
		struct epoll_event epev;

		int norfds;

		norfds = epoll_wait(this->epfd, events, EPOLL_MAX_EVENTS, -1);

		if (norfds < 0) {
			perror("epoll()");
			continue;
		}

		int clifd;
		int eventbits;
		int fdindex;
		int tmp;

		for (fdindex = 0; fdindex < norfds; ++fdindex) {
			clifd = events[fdindex].data.fd;
			eventbits = events[fdindex].events;

			printf("t[%lu]: events = 0x%x\n", this->tid, eventbits);

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

					tmp = get_req_hdr(this, clifd, &req);

					if (tmp < 0) /** errors **/
					{
						if (tmp == TT_ERR_CONN_DROP)
							printf("conenction was closed by peer\n");
						else
							perror("thread_routine::get_req_hdr()");

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
						close(clifd);

						continue;
					}

					if (is_req_init_conn_type(&req)) {
						if (client_version_check(clifd, this) < 0) {
							perror("thread_routine::client_version_check()");
							epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd,
								  NULL); // kernel 2.6+
							close(clifd);
						}

						continue;
					}

					if (is_req_invalid(&req)) {
						printf("\e[91minvalid request\e[0m\n");

						/** TODO: send() retcode + discard receive buffer */

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL);
						close(clifd);
						continue;
					}

					if ( tcp_recv_req(this, clifd, &req) < 0 )
					{
						dprint("tcp_recv_req() failed!");

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
						close(clifd);

						continue;
					}

					s_tcp_print_req(&req);

					s_tcp_rep *rep = s_tcp_rep_new(this, TT_REQ_SUCC, 10UL);
					memcpy(s_tcp_rep_expose_payload(rep), "saloustros", 10UL);

					/* tebis_handle_request(); */

					if (tcp_send_rep(this, clifd, rep) < 0) {
						perror("tcp_send_rep()");
						/* abort_send_rep(); (?) */
						continue;
					}
				}
			} else if (eventbits & EPOLLERR) /** error **/
			{
				printf("t%d events[%d] = EPOLLER\n", gettid(), fdindex);
				/** TODO: error handling */
				continue;
			}
		}
	}

	return NULL;
}

static int spawn_workers(server_handle *sh, uint32_t workers)
{
	int32_t index;

	if ((sh->workers[0].buf.mem = mmap(NULL, workers * DEF_BUF_SIZE, PROT_READ | PROT_WRITE,
					   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0UL)) == MAP_FAILED)
		return -(EXIT_FAILURE);

	for (index = 0; index < workers; ++index) {
		sh->workers[index].sock = sh->sock;
		sh->workers[index].epfd = sh->epfd;
		sh->workers[index].buf.bytes = DEF_BUF_SIZE;
		sh->workers[index].buf.mem = sh->workers[0].buf.mem + (index * DEF_BUF_SIZE);

		if (pthread_create(&sh->workers[index].tid, NULL, thread_routine, sh->workers + index)) {
			int32_t tmp;

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

int shandle_init(sHandle restrict *restrict shandle, int afamily, const char *restrict addr, unsigned short port,
		 uint nothreads)
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

	if (!(*shandle = malloc(sizeof(server_handle) + (nothreads * sizeof(worker_t)))))
		return -(EXIT_FAILURE);

	server_handle *sh = *shandle;

	sh->workers = (worker_t *)((uint8_t *)(sh) + sizeof(server_handle));
	sh->sock = -1;
	sh->epfd = -1;

	if ((sh->sock = socket(afamily, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0)
		goto cleanup;

	int opt = 1;
	setsockopt(sh->sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	if (bind(sh->sock, (struct sockaddr *)(&addrstore), addrlen) < 0)
		goto cleanup;

	if (listen(sh->sock, TT_MAX_LISTEN) < 0)
		goto cleanup;

	if ((sh->epfd = epoll_create1(EPOLL_CLOEXEC)) < 0)
		goto cleanup;

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = sh->sock };

	if (epoll_ctl(sh->epfd, EPOLL_CTL_ADD, sh->sock, &epev) < 0)
		goto cleanup;

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

s_tcp_rep s_tcp_rep_new(worker_t *this, int retcode, size_t paysz)
{
	if ( !this )
	{
		errno = EINVAL;
		return NULL;
	}

	tcp_rep *trep;
	uint64_t tsize = TT_REPHDR_SIZE + sizeof(*trep);

	if ( this->buf.bytes < tsize )
	{
		errno = ENOMEM;
		return NULL;
	}

	if ( retcode == TT_REQ_SUCC )
		tsize += paysz + __x86_PAGESIZE; // extra page for future use!
	else
		paysz = 0UL; // failure-reply has 0-length payload

	trep->buf.mem = (char *)(this->buf.mem) + sizeof(*trep);
	trep->buf.bytes = this->buf.bytes - sizeof(*trep);
	trep->retc = retcode;
	trep->paysz = paysz;

	*((char *)(trep->buf.mem)) = retcode;
	*((uint64_t *)(trep->buf.mem + 1UL)) = htobe64(paysz);
	trep->buf.mem += TT_REPHDR_SIZE;

	return trep;
}

static void *s_tcp_rep_expose_payload(s_tcp_rep rep)
{
	if ( !rep )
	{
		errno = EINVAL;
		return NULL;
	}

	return ((tcp_rep *)(rep))->buf.mem;
}

static int tcp_recv_req(worker_t *restrict this, int clifd, tcp_req *restrict req)
{
	if (!this || clifd < 0 || !req)
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	uint64_t keysz = req->kv.key.size;
	uint64_t paysz = req->kv.value.size;
	uint64_t req_total_size = keysz + paysz;
	int64_t bytes_read;

	/** TODO: respond with an error message, insted of just close(clifd) */

	if ( (bytes_read = recv(clifd, this->buf.mem, req_total_size, 0)) < 0) {
		perror("tcp_recv_req::read()");
		epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
		close(clifd);

		return -(EXIT_FAILURE);
	}

	char *tmp = malloc(req_total_size);

	if ( !tmp )
	{
		perror("tcp_recv_req::epoll_ctl(MOD)");
		close(clifd);

		return -(EXIT_FAILURE);
	}

	req->kv.key.data = tmp;
	req->kv.value.data = tmp + keysz;

	memcpy(req->kv.key.data, this->buf.mem, keysz);
	memcpy(req->kv.value.data, this->buf.mem + keysz, paysz);

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = clifd };

	if (epoll_ctl(this->epfd, EPOLL_CTL_MOD, clifd, &epev) < 0) {
		perror("tcp_recv_req::epoll_ctl(MOD)");
		close(clifd);

		return -(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}

static int get_req_hdr(worker_t *restrict worker, int clifd, tcp_req *restrict req)
{
	int64_t ret;

	if ((ret = recv(clifd, worker->buf.mem, TT_REQHDR_SIZE, 0)) < 0)
		return -(EXIT_FAILURE);

	if (!ret)
	{
		errno = ECONNABORTED;
		return TT_ERR_CONN_DROP;
	}

	req->type = *((uint8_t *)(worker->buf.mem));

	if (req->type != REQ_INIT_CONN) {
		req->kv.key.size = be64toh(*((uint64_t *)(worker->buf.mem + 1UL)));
		req->kv.value.size = be64toh(*((uint64_t *)(worker->buf.mem + 9UL)));

		printf("hdr: req->type = %d\n", req->type);
		printf("hdr: req->keysz = %lu\n", req->kv.key.size);
	}

	return EXIT_SUCCESS;
}

static int tcp_send_rep(worker_t *restrict worker, int clifd, s_tcp_rep restrict rep)
{
	tcp_rep *trep = rep;
	uint64_t tsize = trep->paysz + TT_REPHDR_SIZE;

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

	printf("\e[1;91m%s\e[0m\n", printable_req(treq->type));

	printf("  - key.size = %lu (0x%lx)\n", kv->key.size, *((uint64_t *)(kv->key.data)));

	if (!req_in_get_family(treq))
		printf("  - val.size = %lu (0x%lx)\n\n", kv->value.size, *((uint64_t *)(kv->value.data)));
}
