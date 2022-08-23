/** TODO: check if req/rep struct is initialized, using MAGIC num */
/** TODO: implement s_tcp_rep_destroy() */

#define _GNU_SOURCE

#include "tcp_server.h"

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

	struct {
		uint64_t size;
		char *mem;
	} buf;

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

	uint64_t nokvs;
	uint64_t bytes;

	struct {
		uint64_t size;
		kv_t *kv;

	} kvarray;

} tcp_req;

struct datalist_node {
	int retc;
	generic_data_t data;
	struct datalist_node *next;
};

typedef struct {
	int retc;

	struct {
		struct datalist_node *head;
		struct datalist_node *tail;

		uint64_t bytes;
		uint64_t size;

	} datalist;

} tcp_rep;

#define req_in_get_family(req) ((req->type) <= REQ_EXISTS)
#define is_req_init_conn_type(req) ((req->type) == REQ_INIT_CONN)
#define is_req_invalid(req) ((uint32_t)((req->type)) > REQ_PUT_IFEX)
#define MAX_LISTEN_CLIENTS 512
#define BUFHDR_SIZE 17UL

#define CONN_CLOSED -2

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

	if (version != TEBIS_TCP_VERSION) {
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

static int tcp_recv_req(worker_t *restrict worker, int clifd, tcp_req *restrict req);
static int tcp_send_rep(worker_t *restrict worker, int clifd, int8_t retcode, s_tcp_rep restrict rep);
static int get_req_hdr(worker_t *restrict worker, int clifd, tcp_req *restrict req);

static void *thread_routine(void *arg)
{
	worker_t *this = arg;
	tcp_req *req = s_tcp_req_init();

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

			printf("t[%lu]: events = %d\n", this->tid, eventbits);

			if (eventbits & EPOLLHUP) /** connection closed by peer **/
			{
				/** terminate connection with client **/

				epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
				close(clifd);
			} else if (eventbits & EPOLLIN) /** read event **/
			{
				if (clifd == this->sock) /** new connection **/
				{
					printf("new connection!\n");
					handle_new_connection(this);
				} else { /** request **/

					tmp = get_req_hdr(this, clifd, req);

					if (tmp < 0) /** errors **/
					{
						if (tmp == CONN_CLOSED)
							printf("conenction was closed by peer\n");
						else
							perror("thread_routine::get_req_hdr()");

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
						close(clifd);

						continue;
					}

					if (is_req_init_conn_type(req)) {
						if (client_version_check(clifd, this) < 0) {
							perror("thread_routine::client_version_check()");
							epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd,
								  NULL); // kernel 2.6+
							close(clifd);
						}

						continue;
					}

					if (is_req_invalid(req)) {
						printf("\e[91minvalid request\e[0m\n");

						/** TODO: send() error code + discard receive buffer */

						epoll_ctl(this->epfd, EPOLL_CTL_DEL, clifd, NULL);
						close(clifd);
						continue;
						;
					}

					tcp_recv_req(this, clifd, req);
					s_tcp_print_req(req);

					s_tcp_rep *rep = s_tcp_rep_init();
					generic_data_t gdata;

					/* tebis_handle_request(); */

					for (int i = 0; i < 2; ++i) {
						if (i) {
							gdata.data = malloc(7UL);
							gdata.size = 7UL;

							strcpy(gdata.data, "giorgos");
						} else {
							gdata.data = malloc(10UL);
							gdata.size = 10UL;

							strcpy(gdata.data, "saloustros");
						}

						if (tcp_rep_push_data(rep, &gdata) < 0) {
							perror("tcp_rep_push_data()");
							/* abort_send_rep(); */
							continue;
						}
					}

					if (tcp_send_rep(this, clifd, 1, rep) < 0) {
						perror("tcp_send_rep()");
						/* abort_send_rep(); */
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
		sh->workers[index].buf.size = DEF_BUF_SIZE;
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

	if (bind(sh->sock, (struct sockaddr *)(&addrstore), addrlen) < 0)
		goto cleanup;

	if (listen(sh->sock, MAX_LISTEN_CLIENTS) < 0)
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

s_tcp_req s_tcp_req_init(void)
{
	tcp_req *req;

	if (!(req = malloc(sizeof(*req))))
		return NULL;

	req->kvarray.size = DEF_KV_SLOTS;
	req->nokvs = 0UL;

	if (!(req->kvarray.kv = malloc(DEF_KV_SLOTS * sizeof(*req->kvarray.kv)))) {
		free(req);
		return NULL;
	}

	return req;
}

s_tcp_rep s_tcp_rep_init(void)
{
	tcp_rep *trep;

	if (!(trep = malloc(sizeof(*trep))))
		return NULL;

	if (!(trep->datalist.head = calloc(1UL, sizeof(*(trep->datalist.head))))) // dummy node
	{
		free(trep);
		return NULL;
	}

	trep->datalist.size = 0UL;
	trep->datalist.bytes = 0UL;
	trep->datalist.tail = trep->datalist.head;

	return trep;
}

int s_tcp_req_destroy(s_tcp_req req)
{
	if (!req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	free(((tcp_req *)(req))->kvarray.kv);
	free(req);

	return EXIT_SUCCESS;
}

int tcp_rep_push_data(s_tcp_rep restrict rep, generic_data_t *restrict gdata)
{
	if (!rep || !gdata) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	tcp_rep *trep = rep;

	if (!(trep->datalist.tail->next = calloc(1UL, sizeof(*(trep->datalist.tail)))))
		return -(EXIT_FAILURE);

	trep->datalist.tail->next->data = *gdata;
	trep->datalist.tail = trep->datalist.tail->next;

	++trep->datalist.size;
	trep->datalist.bytes += gdata->size;

	return EXIT_SUCCESS;
}

static int tcp_recv_req(worker_t *restrict worker, int clifd, tcp_req *restrict req)
{
	if (req->nokvs > req->kvarray.size) {
		void *tptr;

		free(req->kvarray.kv);

		if (!(tptr = malloc(req->nokvs * sizeof(*req->kvarray.kv))))
			return -(EXIT_FAILURE);

		req->kvarray.size = req->nokvs;
		req->kvarray.kv = tptr;
	}

	if (recv(clifd, worker->buf.mem, req->bytes, 0) < 0) {
		perror("tcp_recv_req::read()");
		epoll_ctl(worker->epfd, EPOLL_CTL_DEL, clifd, NULL); // kernel 2.6+
		close(clifd);

		return -(EXIT_FAILURE);
	}

	struct epoll_event epev = { .events = EPOLLIN | EPOLLONESHOT, .data.fd = clifd };

	if (epoll_ctl(worker->epfd, EPOLL_CTL_MOD, clifd, &epev) < 0) {
		perror("tcp_recv_req::epoll_ctl(MOD)");
		return -(EXIT_FAILURE);
	}

	uint64_t sindex; // sizes index
	uint64_t dindex; // data index
	uint64_t iter;

	/** read sizes **/

	if (req_in_get_family(req))
		dindex = req->nokvs * sizeof(uint64_t);
	else /** PUT family **/
		dindex = req->nokvs * (sizeof(uint64_t) + sizeof(uint64_t));

	for (sindex = iter = 0UL; iter < req->nokvs; ++iter) {
		kv_t *tkv = req->kvarray.kv + iter;

		tkv->key.size = be64toh(*((uint64_t *)(worker->buf.mem + sindex)));
		sindex += sizeof(uint64_t);

		/** TODO: replace all of these malloc()s with a single one, outside the loop */

		if (!(tkv->key.data = malloc(tkv->key.size)))
			return -(EXIT_FAILURE); // any error handling?

		memcpy(tkv->key.data, worker->buf.mem + dindex, tkv->key.size);
		dindex += tkv->key.size;

		if (!req_in_get_family(req)) {
			/** PUT family (put, put-if-ex) **/

			tkv->value.size = be64toh(*((uint64_t *)(worker->buf.mem + sindex)));
			sindex += sizeof(uint64_t);

			if (!(tkv->value.data = malloc(tkv->value.size)))
				return -(EXIT_FAILURE); // any error handling?

			memcpy(tkv->value.data, worker->buf.mem + dindex, tkv->value.size);
			dindex += tkv->value.size;
		}
	}

	return EXIT_SUCCESS;
}

static int get_req_hdr(worker_t *restrict worker, int clifd, tcp_req *restrict req)
{
	int64_t ret;

	if ((ret = recv(clifd, worker->buf.mem, BUFHDR_SIZE, 0)) < 0)
		return -(EXIT_FAILURE);

	if (!ret)
		return CONN_CLOSED;

	req->type = *((uint8_t *)(worker->buf.mem));

	if (req->type != REQ_INIT_CONN) {
		req->nokvs = be64toh(*((uint64_t *)(worker->buf.mem + 1UL)));
		req->bytes = be64toh(*((uint64_t *)(worker->buf.mem + 9UL)));
	} else {
		req->nokvs = 0UL;
		req->bytes = 0UL;
	}

	return EXIT_SUCCESS;
}

static int tcp_send_rep(worker_t *restrict worker, int clifd, int8_t retcode, s_tcp_rep restrict rep)
{
	tcp_rep *trep = rep;

	*((int8_t *)(worker->buf.mem)) = retcode;
	*((uint64_t *)(worker->buf.mem + 1UL)) = htobe64(trep->datalist.size);

	struct datalist_node *dtn; // data-node

	uint64_t sindex = 1UL + sizeof(trep->datalist.size); // sizes-index
	uint64_t dindex = sindex + (trep->datalist.size * sizeof(dtn->data.size)); // data-index

	for (dtn = trep->datalist.head->next; dtn; dtn = dtn->next) {
		*((uint64_t *)(worker->buf.mem + sindex)) = htobe64(dtn->data.size);
		sindex += sizeof(dtn->data.size);

		memcpy(worker->buf.mem + dindex, dtn->data.data, dtn->data.size);
		dindex += dtn->data.size;
	}

	uint64_t tsz =
		trep->datalist.bytes + (trep->datalist.size * sizeof(uint64_t)) + 1UL + sizeof(trep->datalist.size);

	if (send(clifd, worker->buf.mem, tsz, 0) < 0)
		return -(EXIT_FAILURE);

	return EXIT_SUCCESS;
}

void s_tcp_print_req(s_tcp_req req)
{
	if (!req)
		return;

	tcp_req *treq = req;
	kv_t *arr = treq->kvarray.kv;

	printf("\e[1;91m%s\e[0m (%lu)\n", printable_req(treq->type), treq->nokvs);

	for (uint64_t i = 0UL, lim = treq->nokvs; i < lim; ++i) {
		printf("  - key.size = %lu (0x%lx)\n", arr[i].key.size, *((uint64_t *)(arr[i].key.data)));

		if (!req_in_get_family(treq))
			printf("  - val.size = %lu (0x%lx)\n\n", arr[i].value.size, *((uint64_t *)(arr[i].value.data)));
	}
}
