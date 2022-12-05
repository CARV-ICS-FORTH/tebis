/** TODO: replace every 'epoll_wait()' with 'epoll_pwait()' >>> signal handling (log) */

#define _GNU_SOURCE 1

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
#include <sys/types.h>

#include <ucontext.h>

#define TT_MAP_PROT (PROT_READ | PROT_WRITE)
#define TT_MAP_FLAGS (MAP_ANON | MAP_PRIVATE)

#define req_uses_payload(rtype) (rtype >= REQ_SCAN)
#define is_req_invalid(rtype) ((uint32_t)(rtype) >= OPSNO)

struct internal_tcp_rep {
	uint32_t retc;
	uint32_t flags;
	uint64_t count;
	uint64_t size;

	uint64_t bindex; // buf-index used for pop()ing tcp replies
	struct buffer buf;
};

struct internal_tcp_req {
	req_t type;
	uint32_t flags;

	uint64_t keysz;
	uint64_t paysz;

	struct buffer buf;

	const char __pad[24U];
};

struct client_handle {
	uint16_t flags1;
	uint16_t flags2;

#define MAGIC_INIT_NUM (0xCAFE)
#define CLHF_SND_REQ (1 << 0)

	//{ int (*destroy)(void)}
	// uint64_t x = &((struct client_handle *)(0)->destroy)

	int sock;
};

/*****************************************************************************/

static int server_version_check(int ssock)
{
	uint8_t tbuf[5];

	*tbuf = REQ_INIT_CONN;
	*(tbuf + 1UL) = htobe32(TT_VERSION);

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

	int retc;

	struct addrinfo hints;
	struct addrinfo *res;
	struct addrinfo *rp;

	memset(&hints, 0, sizeof(hints));

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
		}

		close(ch->sock);
	}

	freeaddrinfo(res);

	errno = ECONNREFUSED;
	return -(EXIT_FAILURE); // set errno ?
}

int chandle_destroy(cHandle chandle)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;

	if ((ch->flags1 != MAGIC_INIT_NUM)) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	close(ch->sock);
	free(chandle);

	return EXIT_SUCCESS;
}

/*****************************************************************************/

c_tcp_req c_tcp_req_factory(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz)
{
	struct internal_tcp_req *ireq;
	uint64_t tsize;

	if (is_req_invalid(rtype) || !keysz) {
		errno = ENOTSUP;
		return NULL;
	}

	if (!req_uses_payload(rtype)) // [PUT,  PUT_IF_EXIST]
		paysz = 0UL;

	if (req) /* update() existed request */
	{
		ireq = *req;

		if (ireq->flags != MAGIC_INIT_NUM) {
			errno = EINVAL;
			return NULL;
		}

		tsize = keysz + paysz + TT_REQHDR_SIZE;

		if (tsize <= ireq->buf.bytes)
			ireq->buf.mem = (char *)(ireq) + sizeof(*ireq);
		else {
			tsize = ((tsize + sizeof(*ireq)) | 0xfffUL) + 1UL; // efficient page-alignment

			void *tmp = mremap(ireq, ireq->buf.bytes + sizeof(*ireq), tsize, MREMAP_MAYMOVE);

			if (tmp == MAP_FAILED)
				return NULL;

			*req = tmp;
			ireq = tmp;
			ireq->buf.bytes = tsize - sizeof(*ireq);
		}
	} else /* create() new request */
	{
		tsize = ((TT_REQHDR_SIZE + sizeof(*ireq) + keysz + paysz) | 0xfffUL) + 1UL; // efficient page-align

		if ((ireq = mmap(NULL, tsize, TT_MAP_PROT, TT_MAP_FLAGS, -1, 0UL)) == MAP_FAILED)
			return NULL;

		ireq->buf.bytes = tsize - sizeof(*ireq);
		ireq->flags = MAGIC_INIT_NUM;
	}

	ireq->buf.mem = (char *)(ireq) + sizeof(*ireq);
	ireq->keysz = keysz;
	ireq->paysz = paysz;
	ireq->type = rtype;

	*((char *)(ireq->buf.mem)) = rtype;
	*((uint64_t *)(ireq->buf.mem + 1UL)) = htobe64(keysz);
	*((uint64_t *)(ireq->buf.mem + 9UL)) = htobe64(paysz);
	ireq->buf.mem += TT_REQHDR_SIZE;

	return ireq;
}

int c_tcp_req_destroy(c_tcp_req req)
{
	if (!req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct internal_tcp_req *ireq = req;

	if (ireq->flags != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	if (munmap(ireq, ireq->buf.bytes + sizeof(*ireq)) < 0)
		return -(EXIT_FAILURE);

	return EXIT_SUCCESS;
}

void *c_tcp_req_expose_key(c_tcp_req req)
{
	if (!req) {
		errno = EINVAL;
		return NULL;
	}

	struct internal_tcp_req *ireq = req;

	if (ireq->flags != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	return ireq->buf.mem;
}

void *c_tcp_req_expose_payload(c_tcp_req req)
{
	if (!req) {
		errno = EINVAL;
		return NULL;
	}

	struct internal_tcp_req *ireq = req;

	if (ireq->flags != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return NULL;
	}

	// do not allow the exposure of payload's buffer (write) for
	// requests [REQ_GET, REQ_DEL, REQ_EXISTS, REQ_SCAN]

	if (ireq->type <= REQ_SCAN) {
		errno = ENODATA;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	return ireq->buf.mem + ireq->keysz;
}

c_tcp_rep c_tcp_rep_new(size_t size)
{
	if (!size) {
		errno = EINVAL;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	struct internal_tcp_rep *irep;
	uint64_t tsize = ((TT_REPHDR_SIZE + sizeof(*irep) + size) | 0xfffUL) + 1UL; // efficient page-alignment

	if ((irep = mmap(NULL, tsize, TT_MAP_PROT, TT_MAP_FLAGS, -1, 0UL)) == MAP_FAILED)
		return NULL;

	irep->buf.mem = (char *)(irep) + sizeof(*irep);
	irep->buf.bytes = tsize - sizeof(*irep);
	irep->flags = MAGIC_INIT_NUM;
	irep->bindex = 0UL;
	irep->count = 0UL;
	irep->size = 0UL;
	irep->retc = 0U;

	return irep;
}

static int c_tcp_rep_update(c_tcp_rep *rep, int retc, size_t size, size_t count) // internal-use-only
{
	struct internal_tcp_rep *irep = *rep;
	uint64_t tsize = TT_REPHDR_SIZE + size;

	if (tsize <= irep->buf.bytes)
		irep->buf.mem = (char *)(irep) + sizeof(*irep);
	else {
		if (munmap(irep->buf.mem, irep->buf.bytes + sizeof(*irep)) < 0)
			return -(EXIT_FAILURE);

		tsize = ((tsize + sizeof(*irep)) | 0xfffUL) + 1UL;

		if ((*rep = mmap(NULL, tsize, TT_MAP_PROT, TT_MAP_FLAGS, -1, 0UL)) == MAP_FAILED)
			return -(EXIT_FAILURE);

		irep = *rep;
		irep->buf.mem = (char *)(irep) + sizeof(*irep);
		irep->buf.bytes = tsize - sizeof(*irep);
	}

	irep->count = count;
	irep->size = tsize;
	irep->retc = retc;
	irep->bindex = 0UL;

	return EXIT_SUCCESS;
}

int c_tcp_rep_destroy(c_tcp_rep rep)
{
	if (!rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct internal_tcp_rep *irep = rep;

	if (irep->flags != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	if (munmap(irep, irep->buf.bytes + sizeof(*irep)) < 0)
		return -(EXIT_FAILURE);

	return EXIT_SUCCESS;
}

int c_tcp_rep_pop_value(c_tcp_rep rep, generic_data_t *val)
{
	if (!rep || !val) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct internal_tcp_rep *irep = rep;

	if (irep->flags != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (irep->bindex >= irep->size) {
		errno = ENODATA;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	char *tmp = irep->buf.mem + irep->bindex;

	val->size = *((uint64_t *)(tmp));
	val->data = tmp + sizeof(uint64_t);

	irep->bindex += sizeof(uint64_t) + val->size;

	return EXIT_SUCCESS;
}

/*****************************************************************************/

int c_tcp_send_req(cHandle chandle, c_tcp_req req)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;
	struct internal_tcp_req *ireq = req;

	if ((ch->flags1 != MAGIC_INIT_NUM) || (ireq->flags != MAGIC_INIT_NUM)) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (!(ch->flags2 & CLHF_SND_REQ)) // client waits for a 'reply' (not a 'request')
	{
		errno = EPERM;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	uint64_t tsize = ireq->keysz + ireq->paysz + TT_REQHDR_SIZE;

	if (send(ch->sock, ireq->buf.mem - TT_REQHDR_SIZE, tsize, 0) < 0)
		return -(EXIT_FAILURE);

	ch->flags2 &= ~(CLHF_SND_REQ);

	return EXIT_SUCCESS;
}

int c_tcp_recv_rep(cHandle restrict chandle, c_tcp_rep restrict rep)
{
	if (!chandle || !rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;
	struct internal_tcp_rep *irep = rep;

	if (ch->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (ch->flags2 & CLHF_SND_REQ) {
		errno = EPERM;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	int64_t bytes_read;

	if ((bytes_read = read(ch->sock, irep->buf.mem, TT_REPHDR_SIZE)) < 0) {
		dprint("read() failed!");
		printf("read() returned: %ld\n", bytes_read);
		return -(EXIT_FAILURE);
	}

	irep->retc = *((uint8_t *)(irep->buf.mem));
	irep->count = be64toh(*((uint64_t *)(irep->buf.mem + 1UL)));
	irep->size = be64toh(*((uint64_t *)(irep->buf.mem + 9UL))); // total size (payload-sizes + payloads)

	if (irep->retc != TT_REQ_SUCC) {
		/// TODO: return a custom error-code or set an appropriate errno (?)
		errno = ECANCELED;
		ch->flags2 |= CLHF_SND_REQ;
		return -(EXIT_FAILURE);
	}

	if (c_tcp_rep_update((void **)&rep, irep->retc, irep->size, irep->count) < 0) {
		dprint("c_tcp_rep_update() failed!");
		return -(EXIT_FAILURE);
	}

	if ((bytes_read = read(ch->sock, irep->buf.mem, irep->size)) < 0) {
		/// TODO: ask again (irep->size > kernel-recv-buf ????)
		dprint("read() failed!");
		printf("read() returned: %ld\n", bytes_read);
		return -(EXIT_FAILURE);
	}

	ch->flags2 |= CLHF_SND_REQ;

	return EXIT_SUCCESS;
}
