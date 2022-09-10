/** TODO: replace every 'epoll_wait()' with 'epoll_pwait()' >>> signal handling */

#include "tcp_client.h"
#include "tebis_tcp_errors.h"

#include <arpa/inet.h>

#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/uio.h>

#define TEBIS_TCP_PORT 25565 // Minecraft's port
#define REPBUF_HDR_SIZE 16UL
#define DEF_REP_SLOTS 4UL

struct kvlist_node {
	kv_t kv;
	struct kvlist_node *next;
};

struct buffer {
	uint64_t bytes;
	void *mem;
};

struct internal_tcp_rep {
	uint64_t nokvs; // number of kv's

	uint64_t slots;
	struct tcp_rep *replies;

	struct buffer rep_data;
	struct buffer net_buf;
};

struct internal_tcp_req {
	req_t type;

	struct {
		struct kvlist_node *head;
		struct kvlist_node *tail;

		uint64_t bytes;
		uint64_t nokvs;

	} kvlist;

	struct {
		uint64_t bytes;
		void *mem;
	} buf;

	uint32_t flags;
};

struct client_handle {
	uint16_t flags1;
	uint16_t flags2;

#define MAGIC_INIT_NUM (0xCAFE)
#define CLHF_SND_REQ (1 << 0)

	int sock;

	struct internal_tcp_rep reply;
	struct internal_tcp_req request;
};

/*******************************************************************/

#include <stdio.h> // debug

#define req_in_get_family(rtype) (rtype <= REQ_EXISTS)
#define is_req_invalid(req) ((uint32_t)((req->type)) >= OPSNO)

/*****************************************************************************/

static int server_version_check(int ssock)
{
	uint8_t tbuf[5];

	*tbuf = REQ_INIT_CONN;
	*(tbuf + 1UL) = htobe32(TEBIS_TCP_VERSION);

	if (send(ssock, tbuf, 5UL, 0) < 0) {
		dprint("send()");
		return -(EXIT_FAILURE);
	}

	int64_t ret = recv(ssock, tbuf, 4UL, 0);

	if (ret < 0) {
		dprint("recv()");
		return -(EXIT_FAILURE);
	} else if (!ret) {
		fprintf(stderr, "server has been shut down!\n");
		return -(EXIT_FAILURE);
	}

	printf("TEBIS_SERVER_VERSION: 0x%x\n", *((uint32_t *)(tbuf)));

	return EXIT_SUCCESS;
}

static int chandle_init_reply(cHandle chandle)
{
	struct client_handle *ch = chandle;
	void *mem;

	if ((mem = mmap(NULL, DEF_BUF_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0UL)) ==
	    MAP_FAILED) {
		dprint("mmap()");
		return -(EXIT_FAILURE);
	}

	ch->reply.net_buf.mem = mem;
	ch->reply.net_buf.bytes = DEF_BUF_SIZE;

	if (!(mem = calloc(DEF_REP_SLOTS, sizeof(*ch->reply.replies)))) {
		munmap(ch->reply.net_buf.mem, DEF_BUF_SIZE);
		dprint("calloc()");

		return -(EXIT_FAILURE);
	}

	ch->reply.replies = mem;
	ch->reply.slots = DEF_REP_SLOTS;
	ch->reply.nokvs = 0UL;
	ch->reply.rep_data.bytes = 0UL;
	ch->reply.rep_data.mem = NULL;

	return EXIT_SUCCESS;
}

static int chandle_init_request(cHandle chandle)
{
	struct client_handle *ch = chandle;

	if (!(ch->request.kvlist.head = calloc(1UL, sizeof(struct kvlist_node)))) // dummy node
	{
		dprint("calloc()");
		return -(EXIT_FAILURE);
	}

	ch->request.kvlist.nokvs = 0UL;
	ch->request.kvlist.bytes = 0UL;
	ch->request.kvlist.tail = ch->request.kvlist.head; // dummy node

	if (!(ch->request.buf.mem = malloc(DEF_BUF_SIZE))) {
		dprint("malloc()");
		return -(EXIT_FAILURE);
	}

	ch->request.buf.bytes = DEF_BUF_SIZE;

	return EXIT_SUCCESS;
}

int chandle_init(cHandle restrict *restrict chandle, const char *restrict addr, const char *restrict port)
{
	if (!chandle || !addr || !port) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (!(*chandle = malloc(sizeof(struct client_handle)))) {
		dprint("malloc()");
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	struct client_handle *ch = *chandle;

	ch->flags1 = 0U;
	ch->flags2 = CLHF_SND_REQ;

	if (chandle_init_reply(*chandle) < 0) {
		dprint("chandle_init_reply()");
		return -(EXIT_FAILURE);
	}

	if (chandle_init_request(*chandle) < 0) {
		dprint("chandle_init_request()");
		return -(EXIT_FAILURE);
	}

	int retc;

	struct addrinfo hints;
	struct addrinfo *res;
	struct addrinfo *rp;

	bzero(&hints, sizeof(hints));

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_ADDRCONFIG | AI_NUMERICSERV;

	if ((retc = getaddrinfo(addr, port, &hints, &res))) {
		gai_strerror(retc); // debug only, set errno ?
		return -(EXIT_FAILURE);
	}

	if (!res)
		return -(EXIT_FAILURE); // set errno

	for (rp = res; rp; rp = rp->ai_next) {
		if ((ch->sock = socket(rp->ai_family, SOCK_STREAM | SOCK_CLOEXEC, 0)) < 0) {
			print_debug("socket()");
			continue;
		}

		if (!connect(ch->sock, rp->ai_addr, rp->ai_addrlen)) {
			freeaddrinfo(res);

			if (server_version_check(ch->sock) < 0) {
				close(ch->sock);
				continue;
			}

			ch->flags1 = MAGIC_INIT_NUM;
			return EXIT_SUCCESS;
		} else
			close(ch->sock);
	}

	freeaddrinfo(res);

	errno = ECONNREFUSED;
	return -(EXIT_FAILURE); // set errno ?
}

int c_tcp_req_set_type(cHandle chandle, req_t rtype)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;

	if ((ch->flags1 != MAGIC_INIT_NUM) || is_req_invalid((&ch->request))) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	ch->request.type = rtype;

	return EXIT_SUCCESS;
}

int c_tcp_req_push(cHandle chandle, generic_data_t *restrict key, generic_data_t *restrict value)
{
	if (!chandle || !key || !value) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	struct client_handle *ch = chandle;
	struct internal_tcp_req *ireq = &(ch->request);

	ch->request.flags = 1;

	/** TODO: replace list with list-of-ararys */

	struct kvlist_node *kvn;

	if (!(kvn = malloc(sizeof(*kvn))))
		return -(EXIT_FAILURE);

	kvn->kv.key = *key;
	kvn->next = NULL;

	ireq->kvlist.tail->next = kvn;
	ireq->kvlist.tail = kvn;

	++ireq->kvlist.nokvs;
	ireq->kvlist.bytes += key->size;

	if (!req_in_get_family(ireq->type)) { // PUT family
		kvn->kv.value = *value;
		ireq->kvlist.bytes += value->size;
	}

	return EXIT_SUCCESS;
}

int c_tcp_send_req(cHandle chandle)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;

	if (ch->flags1 != MAGIC_INIT_NUM) // chandle is not initialized!
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (!(ch->flags2 & CLHF_SND_REQ)) // client waits for a 'reply' (not a 'request')
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct internal_tcp_req *ireq = &(ch->request);

	if (ireq->kvlist.head == ireq->kvlist.tail) // not a single kv is pushed
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	uint64_t sindex; // sizes-index
	uint64_t dindex; // data-index
	uint64_t tsz; // total-size

	sindex = 1UL + sizeof(uint64_t) + sizeof(uint64_t);
	tsz = ireq->kvlist.bytes + sindex;
	dindex = sindex;

	printf("ireq->type = %d\nbytes = %lu (list: %lu)\n", ireq->type, tsz, ireq->kvlist.bytes);

	if (req_in_get_family(ireq->type))
		tsz += (ireq->kvlist.nokvs * sizeof(uint64_t));
	else /** PUT family **/
		tsz += (ireq->kvlist.nokvs * 2 * sizeof(uint64_t));

	if (tsz > ch->request.buf.bytes) {
		free(ch->request.buf.mem);

		if (!(ch->request.buf.mem = malloc(tsz)))
			return -(EXIT_FAILURE);

		ch->request.buf.bytes = tsz;
	}

	*((uint8_t *)(ch->request.buf.mem)) = ireq->type;
	*((uint64_t *)(ch->request.buf.mem + 1UL)) = htobe64(ireq->kvlist.nokvs);
	*((uint64_t *)(ch->request.buf.mem + 1UL + sizeof(uint64_t))) = htobe64(tsz);

	if (req_in_get_family(ireq->type))
		dindex += (ireq->kvlist.nokvs * sizeof(uint64_t));
	else /* PUT family */
		dindex += (ireq->kvlist.nokvs * (sizeof(uint64_t) + sizeof(uint64_t)));

	for (struct kvlist_node *prev, *kvn = ireq->kvlist.head->next; kvn;) {
		prev = kvn;

		/** GET family (get, del, exists) **/

		*((uint64_t *)(ch->request.buf.mem + sindex)) = htobe64(kvn->kv.key.size);
		sindex += sizeof(kvn->kv.key.size);

		memcpy(ch->request.buf.mem + dindex, kvn->kv.key.data, kvn->kv.key.size);
		dindex += kvn->kv.key.size;

		if (!req_in_get_family(ireq->type)) // branch predictor saves the day? (test!!!)
		{
			/** PUT family (put, put-if-ex) **/

			*((uint64_t *)(ch->request.buf.mem + sindex)) = htobe64(kvn->kv.value.size);
			sindex += sizeof(kvn->kv.value.size);
			memcpy(ch->request.buf.mem + dindex, kvn->kv.value.data, kvn->kv.value.size);
			dindex += kvn->kv.value.size;
		}

		kvn = kvn->next;
		free(prev);
	}

	ireq->kvlist.head->next = NULL;
	ireq->kvlist.tail = ireq->kvlist.head;

	printf("send(%lu)\n", tsz);

	if (send(ch->sock, ch->request.buf.mem, tsz, 0) < 0)
		return -(EXIT_FAILURE);

	ch->flags2 &= ~(CLHF_SND_REQ);

	return EXIT_SUCCESS;
}

int c_tcp_recv_rep(cHandle chandle)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;

	/* clients waits to send() a 'request' (not to reacv() a 'reply') */

	if ((ch->flags1 != MAGIC_INIT_NUM) || (ch->flags2 & CLHF_SND_REQ)) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	ssize_t ret;

	if ((ret = recv(ch->sock, ch->reply.net_buf.mem, REPBUF_HDR_SIZE, 0)) < 0) {
		dprint("recv(0)");
		return -(EXIT_FAILURE);
	}

	if (!ret || ret != REPBUF_HDR_SIZE) {
		/* connection terminated (!ret) */

		dprint("recv(1)");
		return -(EXIT_FAILURE);
	}

	struct internal_tcp_rep *irep = &ch->reply;
	uint64_t bytes_to_read;

	irep->nokvs = be64toh(*((uint64_t *)(ch->reply.net_buf.mem)));
	bytes_to_read = be64toh(*((uint64_t *)(ch->reply.net_buf.mem + sizeof(uint64_t)))); // total-payload-size

	if (irep->nokvs > irep->slots) {
		free(irep->replies);

		if (!(irep->replies = calloc(irep->nokvs, sizeof(*ch->reply.replies)))) {
			dprint("calloc()");
			return -(EXIT_FAILURE);
		}

		irep->slots = irep->nokvs;
		irep->replies[0].payload.data = irep->rep_data.mem;
	}

	if (bytes_to_read > irep->rep_data.bytes) {
		// bytes_to_read = payload-size

		free(irep->rep_data.mem);

		if (!(irep->rep_data.mem = malloc(bytes_to_read))) {
			dprint("malloc()");
			return -(EXIT_FAILURE);
		}

		irep->rep_data.bytes = bytes_to_read;
	}

	// !this recv() isn't the optimal solution... think another one (reads a few bytes)!

	if ((ret = recv(ch->sock, ch->reply.net_buf.mem, irep->nokvs, 0)) < 0) {
		dprint("recv(2)");
		return -(EXIT_FAILURE);
	}

	if (!ret || ret != irep->nokvs) {
		/* connection terminated (!ret) */

		dprint("recv(3)");
		return -(EXIT_FAILURE);
	}

	uint64_t lim;
	uint64_t c;

	for (lim = irep->nokvs, c = 0UL; c < lim; ++c) {
		uint64_t index = 0UL;

		irep->replies[c].retc = *((int8_t *)(ch->reply.net_buf.mem + index));
		++index;

		if (irep->replies[c].retc == REQ_COMPLETED)
			bytes_to_read += sizeof(uint64_t);
	}

	if (bytes_to_read > irep->net_buf.bytes) {
		munmap(irep->net_buf.mem, irep->net_buf.bytes);

		uint64_t mmsize = (bytes_to_read | 0xfffUL) + 1UL; // PAGESIZE granularity

		printf("tsz = 0x%lx \\ %lu\n", bytes_to_read, bytes_to_read);
		printf("mmsize-page-granulate = 0x%lx \\ %lu\n", mmsize, mmsize);

		if ((irep->net_buf.mem = mmap(NULL, mmsize + (16UL * __x86_PAGESIZE), PROT_READ | PROT_WRITE,
					      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0UL)) == MAP_FAILED) {
			dprint("mmap()");
			return -(EXIT_FAILURE);
		}

		irep->net_buf.bytes = mmsize;
	}

	if ((ret = recv(ch->sock, ch->reply.net_buf.mem, bytes_to_read, 0)) < 0) {
		dprint("recv(0)");
		return -(EXIT_FAILURE);
	}

	if (!ret || (ret != bytes_to_read)) {
		/* connection terminated (!ret) */

		dprint("recv(1)");
		return -(EXIT_FAILURE);
	}

	uint64_t dindex = irep->nokvs * sizeof(uint64_t); // data-index
	uint64_t tindex = 0UL; // temporary-index
	uint64_t sindex = 0UL; // sizes-index
	uint64_t tsize = 0UL; // temporary-size

	for (c = 0UL; c < lim; ++c) // replace with do {} while();
	{
		/** TODO: avoid multiple cache misses by reading sequentially (?) */

		tsize = irep->replies[c].payload.size = be64toh(*((uint64_t *)(ch->reply.net_buf.mem + sindex)));
		sindex += sizeof(uint64_t);

		irep->replies[c].payload.data = irep->rep_data.mem + tindex;
		memcpy(irep->replies[c].payload.data, ch->reply.net_buf.mem + dindex, tsize);
		dindex += tsize;
		tindex += tsize;
	}

	/* last excess element (null) is the 'terminating element' */

	irep->replies[c].payload.data = NULL;
	irep->replies[c].payload.size = 0UL;

	ch->flags2 |= CLHF_SND_REQ;

	return EXIT_SUCCESS;
}

int c_tcp_get_rep_array(cHandle restrict chandle, struct tcp_rep *restrict *restrict rep)
{
	if (!chandle || !rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;

	if (ch->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	*rep = ch->reply.replies;

	return EXIT_SUCCESS;
}

int c_tcp_print_replies(cHandle chandle)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;

	if (ch->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	struct tcp_rep *repbuf = ch->reply.replies;

	while (repbuf->payload.data) {
		printf("- size = %lu\n", repbuf->payload.size);
		printf("- data = %s\n\n", (char *)(repbuf->payload.data));

		++repbuf;
	}

	return EXIT_SUCCESS;
}
