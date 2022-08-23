/** TODO: free list ---> tcp_req_destroy() */
/** TODO: replace list with array-list ---> tcp_req_push_kv() */
/** TODO: validate version with server ---> chandle_init() */
/** TODO: check if req/rep struct is initialized, using MAGIC num */
/** TODO: make a retcode parser function */

#include "tcp_client.h"
#include "tebis_tcp_errors.h"

#include <arpa/inet.h>

#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// #include <sys/mman.h>
#include <sys/socket.h>

#define TEBIS_TCP_PORT 25565 // Minecraft's port


typedef struct {
	uint16_t flags1;
	uint16_t flags2;

	#define MAGIC_INIT_NUM (0xCAFE)
	#define CLHF_SND_REQ (1 << 0)

	int sock;

	struct {
		uint64_t size;
		char *mem;

	} buf;

} client_handle;

struct kvlist_node {
	kv_t kv;
	struct kvlist_node *next;
};

typedef struct {
	req_t type;

	struct {
		struct kvlist_node *head;
		struct kvlist_node *tail;

		uint64_t bytes;
		uint64_t nokvs;

	} kvlist;

	uint32_t flags;
	uint64_t pad[2]; // future use: options

} tcp_req;

typedef struct {
	int8_t retc;

	uint64_t nokvs;

	struct { // only data visible to user

		uint64_t size;
		generic_data_t *payload;

	} ret_to_usr;

	struct {
		uint64_t bytes;
		void *mem;

	} buf;

} tcp_rep;

/*******************************************************************/

#include <stdio.h>

#define check_result(ret) check_result_func(ret, __LINE__) // debug-only, not release
#define req_in_get_family(rtype) (rtype <= REQ_EXISTS)

void check_result_func(int64_t ret, int line)
{
	if (ret < 0LL) {
		fprintf(stderr, "[%d] %s: %s (%d)\n", line, "failure occured", strerror(errno), errno);
		exit(EXIT_FAILURE);
	}
}

/*****************************************************************************/

static int server_version_check(int ssock)
{
	uint8_t tbuf[5];

	*tbuf = REQ_INIT_CONN;
	*(tbuf + 1UL) = htobe32(TEBIS_TCP_VERSION);

	send(ssock, tbuf, 5UL, 0);
	
	int64_t ret = recv(ssock, tbuf, 4UL, 0);

	if ( ret < 0 )
	{
		perror("server_version_check::recv()");
		return -(EXIT_FAILURE);
	}
	else if ( !ret )
	{
		fprintf(stderr, "server has shutdown!\n");
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

	if (!(*chandle = malloc(sizeof(client_handle))))
		return -(EXIT_FAILURE);

	/** END OF ERROR HANDLING **/

	client_handle *ch = *chandle;

	ch->flags1 = MAGIC_INIT_NUM;
	ch->flags2 = CLHF_SND_REQ;
	// ch->buf.mem = (char *)(ch) + sizeof(client_handle);

	// connect tothe server

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
			return -(EXIT_FAILURE); // return or try next one?
		}

		if (!connect(ch->sock, rp->ai_addr, rp->ai_addrlen)) {
			
			freeaddrinfo(res);

			if ( server_version_check(ch->sock) < 0 )
			{
				close(ch->sock);
				return -(EXIT_FAILURE);
			}

			return EXIT_SUCCESS;
		} else
			close(ch->sock);
	}

	freeaddrinfo(res);

	return -(EXIT_FAILURE); // set errno ?
}

int chandle_destroy(cHandle chandle)
{
	if (!chandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	client_handle *ch = chandle;

	close(ch->sock);
	free(ch->buf.mem);
	free(ch);

	return EXIT_SUCCESS;
}

c_tcp_req c_tcp_req_init(req_t rt)
{
	if ((uint32_t)(rt) >= 6U) {
		errno = EINVAL;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	tcp_req *treq;

	if (!(treq = calloc(1UL, sizeof(*treq))))
		return NULL;

	if (!(treq->kvlist.head = calloc(1UL, sizeof(*(treq->kvlist.head))))) // dummy node
	{
		free(treq);
		return NULL;
	}

	treq->type = rt;
	treq->kvlist.nokvs = 0UL;
	treq->kvlist.tail = treq->kvlist.head; // dummy node

	return treq;
}

int c_tcp_req_destroy(c_tcp_req req)
{
	if (!req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	tcp_req *treq = req;

	/** TODO: free list */

	free(treq->kvlist.head);
	free(treq);

	return EXIT_SUCCESS;
}

int c_tcp_rep_destroy(c_tcp_rep rep)
{
	if (!rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	tcp_rep *trep = rep;

	free(trep->buf.mem);
	free(trep->ret_to_usr.payload);

	return EXIT_SUCCESS;
}

c_tcp_rep c_tcp_rep_init(void)
{
	tcp_rep *trep;

	if (!(trep = malloc(sizeof(*trep))))
		return NULL;

	if (!(trep->buf.mem = malloc(DEF_BUF_SIZE))) {
		free(trep);
		return NULL;
	}

	trep->buf.bytes = DEF_BUF_SIZE;

	return trep;
}

int c_tcp_req_push_kv(c_tcp_req restrict req, kv_t *restrict kv)
{
	tcp_req *treq = req;

	if (!req || !kv) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	/** TODO: replace list with array-list */

	if (!(treq->kvlist.tail->next = calloc(1UL, sizeof(*(treq->kvlist.tail->next)))))
		return -(EXIT_FAILURE);

	treq->kvlist.tail->next->kv = *kv;
	treq->kvlist.tail = treq->kvlist.tail->next;

	++treq->kvlist.nokvs;
	treq->kvlist.bytes += kv->key.size;

	if (!req_in_get_family(treq->type)) // belongs in PUT request family
		treq->kvlist.bytes += kv->value.size;

	return EXIT_SUCCESS;
}

int c_tcp_send_req(cHandle restrict chandle, const c_tcp_req restrict req)
{
	if (!chandle || !req) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	client_handle *ch = chandle;

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

	tcp_req *treq = req;

	if (treq->kvlist.head == treq->kvlist.tail) // not a single kv is pushed
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	uint64_t sindex; // sizes-index
	uint64_t dindex; // data-index

	sindex = 1UL + sizeof(treq->kvlist.nokvs) + sizeof(treq->kvlist.bytes);
	ch->buf.size = treq->kvlist.bytes + sindex;

	printf("treq->type = %d\nbytes = %lu (list: %lu)\n", treq->type, ch->buf.size, treq->kvlist.bytes);

	if ( req_in_get_family(treq->type))
		ch->buf.size += (treq->kvlist.nokvs * sizeof(uint64_t));
	else  /** PUT family **/
		ch->buf.size += (treq->kvlist.nokvs * 2 * sizeof(uint64_t));

	if (!(ch->buf.mem = calloc(1UL, ch->buf.size)))
		return -(EXIT_FAILURE);

	*((uint8_t *)(ch->buf.mem)) = treq->type;
	*((uint64_t *)(ch->buf.mem + 1UL)) = htobe64(treq->kvlist.nokvs);
	*((uint64_t *)(ch->buf.mem + 1UL + sizeof(treq->kvlist.nokvs))) = htobe64(ch->buf.size);

	dindex = sindex;

	if (req_in_get_family(treq->type))
		dindex += (treq->kvlist.nokvs * sizeof(treq->kvlist.head->kv.key.size));
	else /* PUT family */
		dindex += (treq->kvlist.nokvs *
			   (sizeof(treq->kvlist.head->kv.key.size) + sizeof(treq->kvlist.head->kv.value.size)));

	for (struct kvlist_node *prev, *kvn = treq->kvlist.head->next; kvn;) {
		prev = kvn;

		/** GET family (get, del, exists) **/

		*((uint64_t *)(ch->buf.mem + sindex)) = htobe64(kvn->kv.key.size);
		sindex += sizeof(kvn->kv.key.size);
		memcpy(ch->buf.mem + dindex, kvn->kv.key.data, kvn->kv.key.size);
		dindex += kvn->kv.key.size;

		if (!req_in_get_family(treq->type)) // branch predictor <3
		{
			/** PUT family (put, put-if-ex) **/

			*((uint64_t *)(ch->buf.mem + sindex)) = htobe64(kvn->kv.value.size);
			sindex += sizeof(kvn->kv.value.size);
			memcpy(ch->buf.mem + dindex, kvn->kv.value.data, kvn->kv.value.size);
			dindex += kvn->kv.value.size;
		}

		kvn = kvn->next;
		free(prev);
	}

	treq->kvlist.head->next = NULL;
	treq->kvlist.tail = treq->kvlist.head;

	printf("send(%lu)\n", ch->buf.size);

	if (send(ch->sock, ch->buf.mem, ch->buf.size, 0) < 0)
		return -(EXIT_FAILURE);

	ch->flags2 &= ~(CLHF_SND_REQ);

	return EXIT_SUCCESS;
}

int c_tcp_recv_rep(cHandle restrict chandle, c_tcp_rep restrict rep, generic_data_t *restrict *restrict repbuf)
{
	if (!chandle || !rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	client_handle *ch = chandle;

	if (ch->flags2 & CLHF_SND_REQ) // clients waits to send a 'request' (not a 'reply')
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	ssize_t ret;

	if ((ret = recv(ch->sock, ch->buf.mem, DEF_BUF_SIZE, 0)) < 0)
		return -(EXIT_FAILURE);

	tcp_rep *trep = rep;
	uint64_t index = 0UL;

	trep->retc = *((int8_t *)(ch->buf.mem));
	++index;

	trep->nokvs = be64toh(*((uint64_t *)(ch->buf.mem + index)));
	index += sizeof(trep->nokvs);

	if (!(trep->ret_to_usr.payload = calloc(trep->nokvs + 1UL, sizeof(*(trep->ret_to_usr.payload)))))
		return -(EXIT_FAILURE);

	trep->ret_to_usr.size = trep->nokvs;
	trep->ret_to_usr.payload[trep->nokvs].data = NULL; // last elem = NULL
	trep->ret_to_usr.payload[trep->nokvs].size = 0UL;

	uint64_t lim = trep->nokvs;
	uint64_t tsz = 0UL;
	uint64_t c = 0UL;

	/** read size_t[] (of data) array **/

	for (uint64_t t; c < lim; ++c) // replace with do {} while();
	{
		t = be64toh(*((uint64_t *)(ch->buf.mem + index)));

		tsz += t;
		trep->ret_to_usr.payload[c].size = t;
		index += sizeof(trep->ret_to_usr.payload->size);
	}

	if (tsz > trep->buf.bytes) // bytes not slots/cells
	{
		free(trep->buf.mem);

		if (!(trep->buf.mem = malloc(tsz))) {
			free(trep->ret_to_usr.payload);
			return -(EXIT_FAILURE);
		}

		trep->buf.bytes = tsz;
	}

	// copy network buffer in local buffer 'mem'

	memcpy(trep->buf.mem, ch->buf.mem + index, tsz);

	trep->ret_to_usr.payload[0].data = trep->buf.mem;
	index = trep->ret_to_usr.payload[0].size;

	for (c = 1UL; c < lim; ++c) {
		trep->ret_to_usr.payload[c].data = trep->buf.mem + index;
		index += trep->ret_to_usr.payload[c].size;
	}

	trep->ret_to_usr.payload[c].data = NULL;
	trep->ret_to_usr.payload[c].size = 0UL;
	*repbuf = trep->ret_to_usr.payload;

	ch->flags2 |= CLHF_SND_REQ;

	return trep->retc;
}

int c_tcp_print_repbuf(generic_data_t *repbuf)
{
	if (!repbuf) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	printf("repbuf:\n");

	while (repbuf->data) {
		printf("  - size = %lu\n", repbuf->size);
		printf("  - data = %s\n\n", (char *)(repbuf->data));

		++repbuf;
	}

	return EXIT_SUCCESS;
}

int fill_req(kv_t *restrict kv, c_tcp_req restrict req, generic_data_t *restrict key, generic_data_t *restrict val)
{
	if ( !kv || !req || !key )
	{
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	tcp_req * treq = req;
	size_t tsz;

	kv->key.size = key->size;

	if ( req_in_get_family(treq->type) )
	{
		kv->value.size = 0UL;
		tsz = key->size;
	}
	else
	{
		kv->value.size = val->size;
		tsz = key->size + val->size;
	}

	void * tptr = malloc(tsz);

	if ( !tptr )
		return -(EXIT_FAILURE);

	kv->key.data = tptr;
	memcpy(kv->key.data, key->data, key->size);

	if ( req_in_get_family(treq->type))
		kv->value.data = NULL;
	else
	{
		kv->value.data = tptr + key->size;
		memcpy(kv->value.data, val->data, val->size);
	}

	return EXIT_SUCCESS;
}
