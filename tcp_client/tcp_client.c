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
#include <sys/types.h>

#define TT_DEFAULT_PORT 25565 // Minecraft's port
#define TT_MAP_PROT (PROT_READ | PROT_WRITE)
#define TT_MAP_FLAGS (MAP_ANON | MAP_PRIVATE)

struct internal_tcp_rep {
	uint32_t retc;
	uint64_t size;

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

	int sock;

	struct internal_tcp_rep reply;
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

	/** TODO: what else? */
	close(ch->sock);

	return EXIT_SUCCESS;
}

////////////////////////////////////////////////////////////////////

c_tcp_req c_tcp_req_new(req_t rtype, size_t keysz, size_t paysz)
{
	/** TODO: check that 'rtype' is a valid request */

	if (rtype < REQ_SCAN)
		paysz = 0UL;

	struct internal_tcp_req *ireq;
	uint64_t tsize = ((TT_REQHDR_SIZE + sizeof(*ireq) + keysz + paysz) | 0xfff) + 1UL; // efficient page-align

	if ((ireq = mmap(NULL, tsize, TT_MAP_PROT, TT_MAP_FLAGS, -1, 0UL)) == MAP_FAILED)
		return NULL;

	ireq->buf.mem = (char *)(ireq) + sizeof(*ireq);
	ireq->buf.bytes = tsize - sizeof(*ireq);
	ireq->keysz = keysz;
	ireq->paysz = paysz;
	ireq->flags = MAGIC_INIT_NUM;
	ireq->type = rtype;

	*((char *)(ireq->buf.mem)) = rtype;
	*((uint64_t *)(ireq->buf.mem + 1UL)) = htobe64(keysz);
	*((uint64_t *)(ireq->buf.mem + 9UL)) = htobe64(paysz);
	ireq->buf.mem += TT_REQHDR_SIZE;

	return ireq;
}

// tmp:
int c_tcp_req_update(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz)
{
	if (!req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** TODO: error-check if rtype is a valid request */

	struct internal_tcp_req *ireq = *req;

	if (ireq->flags != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	if (rtype < REQ_SCAN)
		paysz = 0UL;

	uint64_t tsize = keysz + paysz + TT_REQHDR_SIZE;

	if (tsize <= ireq->buf.bytes - sizeof(*ireq))
		ireq->buf.mem = (char *)(ireq) + sizeof(*ireq);
	else {
		if (munmap(ireq, ireq->buf.bytes + sizeof(*ireq)) < 0)
			return -(EXIT_FAILURE);

		tsize = ((tsize + sizeof(*ireq)) | 0xfff) + 1UL; // page-alignment

		if ((*req = mmap(NULL, tsize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0UL)) == MAP_FAILED)
			return -(EXIT_FAILURE);

		ireq = *req;
		ireq->buf.mem = (char *)(ireq) + sizeof(*ireq);
		ireq->buf.bytes = tsize - sizeof(*ireq);
	}

	ireq->type = rtype;
	ireq->keysz = keysz;
	ireq->paysz = paysz;

	*((char *)(ireq->buf.mem)) = rtype;
	*((uint64_t *)(ireq->buf.mem + 1UL)) = keysz;
	*((uint64_t *)(ireq->buf.mem + 9UL)) = paysz;
	ireq->buf.mem += TT_REQHDR_SIZE;

	return EXIT_SUCCESS;
}

/** TODO: merge req_update() and req_new() */

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

/** TODO: find a way to reuse c_tcp_req (avoid mallocs) */

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

	if (ireq->type < REQ_SCAN) {
		errno = ENODATA;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	return ireq->buf.mem + ireq->keysz;
}

c_tcp_rep c_tcp_rep_new(size_t size)
{
	struct internal_tcp_rep *irep;
	uint64_t tsize = ((TT_REPHDR_SIZE + sizeof(*irep) + size) | 0xfff) + 1UL;

	if ((irep = mmap(NULL, tsize, TT_MAP_PROT, TT_MAP_FLAGS, -1, 0UL)) == MAP_FAILED)
		return NULL;

	irep->buf.mem = (char *)(irep) + sizeof(*irep);
	irep->buf.bytes = tsize - sizeof(*irep);
	irep->size = 0UL;
	irep->retc = 0U;

	return irep;
}

////////////////////////////////////////////////////////////////////

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
		errno = EINVAL;
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

	/* clients waits to send() a 'request' (not to reacv() a 'reply') */

	if ((ch->flags1 != MAGIC_INIT_NUM) || (ch->flags2 & CLHF_SND_REQ)) {
		errno = EINVAL;
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
	irep->size = be64toh(*((uint64_t *)(irep->buf.mem + 1UL)));

	if (irep->retc != TT_REQ_SUCC) {
		ch->flags2 |= CLHF_SND_REQ;
		return EXIT_SUCCESS;
	}

	if ((bytes_read = read(ch->sock, irep->buf.mem, irep->size)) < 0) {
		dprint("read() failed!");
		printf("read() returned: %ld\n", bytes_read);
		return -(EXIT_FAILURE);
	}

	ch->flags2 |= CLHF_SND_REQ;

	return EXIT_SUCCESS;
}

int c_tcp_print_reply(c_tcp_rep rep)
{
	if (!rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	struct internal_tcp_rep *irep = rep;

	printf("- size = %lu\n", irep->size);
	printf("- data = %s\n\n", (char *)(irep->buf.mem));

	return EXIT_SUCCESS;
}

int debug_print_req(c_tcp_req req)
{
	if (!req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	struct internal_tcp_req *ireq = req;

	printf("ireq->type = %d\n", ireq->type);
	printf("ireq->keysz = %lu\n", ireq->keysz);
	printf("ireq->paysz = %lu\n", ireq->paysz);
	printf("ireq->buf.bytes = %lu\n", ireq->buf.bytes);
	printf("ireq->buf.mem = 0x%lX\n", *((uint64_t *)(ireq->buf.mem)));

	return EXIT_SUCCESS;
}
