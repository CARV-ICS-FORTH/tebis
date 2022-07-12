/** TODO: replace all of these mallocs with a single one, outside the loop ---> s_tcp_recv_req() */
/** TODO: make sh->buf resizable ---> s_tcp_send_rep() */
/** TODO: validate version with server (errno = ECONNREFUSED) ---> shandle_init() */
/** TODO: check if req/rep struct is initialized, using MAGIC num */
/** TODO: implement s_tcp_rep_destroy() */

#define _GNU_SOURCE

#include "tcp_server.h"

#include <arpa/inet.h>

#include <endian.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>

typedef struct {
	uint16_t flags1;
	uint16_t flags2;

#define MAGIC_INIT_NUM (0xCAFE)

	int sockfd;
	int clientfd;

	struct {
		uint64_t size;
		void *mem;

	} buf;

} server_handle;

typedef struct {
	req_t type;

	uint64_t nokvs;

	struct {
		uint64_t size;
		kv_t *kv;

	} kvarray;

} tcp_req;

struct datalist_node {
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

#define req_int_get_family(rtype) ((rtype) <= REQ_EXISTS)

/*******************************************************************/

static const char *printable_req(req_t type);

int shandle_init(sHandle restrict *restrict shandle, int afamily, const char *restrict addr, unsigned short port)
{
	if (!shandle || !addr) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct sockaddr_storage addrstore;
	socklen_t addrlen;

	if (afamily == AF_INET) // ipv4
	{
		if (inet_pton(afamily, addr, &(((struct sockaddr_in *)(&addrstore))->sin_addr)) != 1) {
			errno = EINVAL;
			return -(EXIT_FAILURE);
		}

		((struct sockaddr_in *)(&addrstore))->sin_family = AF_INET;
		((struct sockaddr_in *)(&addrstore))->sin_port = htons(port);

		addrlen = sizeof(struct sockaddr_in);
	} else if (afamily == AF_INET6) // ipv6
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

	if (!(*shandle = malloc(sizeof(server_handle))))
		return -(EXIT_FAILURE);

	server_handle *sh = *shandle;

	sh->flags1 = MAGIC_INIT_NUM;

	if (!(sh->buf.mem = malloc(DEF_BUF_SIZE))) {
		free(*shandle);
		return -(EXIT_FAILURE);
	}

	sh->buf.size = DEF_BUF_SIZE;

	if ((sh->sockfd = socket(afamily, SOCK_STREAM | SOCK_CLOEXEC, 0)) < 0) {
		free(sh->buf.mem);
		free(*shandle);

		return -(EXIT_FAILURE);
	}

	if (bind(sh->sockfd, (struct sockaddr *)(&addrstore), addrlen) < 0) {
		close(sh->sockfd);
		free(sh->buf.mem);
		free(*shandle);

		return -(EXIT_FAILURE);
	}

	if (listen(sh->sockfd, 10) < 0) {
		close(sh->sockfd);
		free(sh->buf.mem);
		free(*shandle);

		return -(EXIT_FAILURE);
	}

	struct sockaddr_storage client;

	if ((sh->clientfd = accept4(sh->sockfd, (struct sockaddr *)(&client), &addrlen, SOCK_CLOEXEC)) <
	    0) // user accept4()
		goto cleanup;

	/** TODO: validate version with server (errno = ECONNREFUSED) */

	sh->flags1 = MAGIC_INIT_NUM;

	return EXIT_SUCCESS;

cleanup:
	close(sh->sockfd);
	free(sh->buf.mem);
	free(*shandle);

	return -(EXIT_FAILURE);
}

int shandle_destroy(sHandle shandle)
{
	server_handle *sh = shandle;

	if (!shandle || sh->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	close(sh->sockfd);
	free(sh->buf.mem);
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

	return EXIT_SUCCESS;
}

int s_tcp_recv_req(sHandle restrict shandle, s_tcp_req restrict req)
{
	if (!shandle) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	server_handle *sh = shandle;

	if (sh->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	if (recv(sh->clientfd, sh->buf.mem, sh->buf.size, 0) < 0)
		return -(EXIT_FAILURE);

	tcp_req *treq = req; // tcp-request

	treq->type = *((uint8_t *)(sh->buf.mem));
	treq->nokvs = be64toh(*((uint64_t *)(sh->buf.mem + 1UL)));

	if (treq->nokvs > treq->kvarray.size) {
		void *tptr;

		free(treq->kvarray.kv);

		if (!(tptr = malloc(treq->nokvs * sizeof(*treq->kvarray.kv))))
			return -(EXIT_FAILURE);

		treq->kvarray.size = treq->nokvs;
		treq->kvarray.kv = tptr;
	}

	uint64_t sindex; // sizes index
	uint64_t dindex; // data index

	sindex = 1UL + sizeof(treq->nokvs);
	dindex = sindex;

	if (req_int_get_family(treq->type))
		dindex += (treq->nokvs * sizeof(treq->kvarray.kv->key.size));
	else /*in PUT family */
		dindex += (treq->nokvs * (sizeof(treq->kvarray.kv->key.size) + sizeof(treq->kvarray.kv->value.size)));

	for (uint64_t i = 0UL, lim = treq->nokvs; i < lim; ++i) {
		kv_t *tkv = treq->kvarray.kv + i;

		tkv->key.size = be64toh(*((uint64_t *)(sh->buf.mem + sindex)));
		sindex += sizeof(tkv->key.size);

		/** TODO: replace all of these malloc()s with a single one, outside the loop */

		if (!(tkv->key.data = malloc(tkv->key.size)))
			return -(EXIT_FAILURE); // any error handling?

		memcpy(tkv->key.data, sh->buf.mem + dindex, tkv->key.size);
		dindex += tkv->key.size;

		if (!req_int_get_family(treq->type)) {
			/** PUT family (put, put-if-ex) **/

			tkv->value.size = be64toh(*((typeof(tkv->value.size) *)(sh->buf.mem + sindex)));
			sindex += sizeof(tkv->value.size);

			if (!(tkv->value.data = malloc(tkv->value.size)))
				return -(EXIT_FAILURE); // any error handling?

			memcpy(tkv->value.data, sh->buf.mem + dindex, tkv->value.size);
			dindex += tkv->value.size;
		}
	}

	return EXIT_SUCCESS;
}

int s_tcp_rep_push_data(s_tcp_rep restrict rep, generic_data_t *restrict gdata)
{
	if (!rep || !gdata) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	tcp_rep *trep = rep;

	if (!(trep->datalist.tail->next = calloc(1UL, sizeof(*(trep->datalist.tail->next)))))
		return -(EXIT_FAILURE);

	trep->datalist.tail->next->data = *gdata;
	trep->datalist.tail = trep->datalist.tail->next;

	++trep->datalist.size;
	trep->datalist.bytes += gdata->size;

	return EXIT_SUCCESS;
}

int s_tcp_send_rep(sHandle shandle, int8_t retcode, s_tcp_rep restrict rep)
{
	server_handle *sh = shandle;

	if (!shandle || !rep || sh->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	tcp_rep *trep = rep;

	*((int8_t *)(sh->buf.mem)) = retcode;
	*((uint64_t *)(sh->buf.mem + 1UL)) = htobe64(trep->datalist.size);

	struct datalist_node *dtn; // data-node

	uint64_t sindex = 1UL + sizeof(trep->datalist.size); // sizes-index
	uint64_t dindex = sindex + (trep->datalist.size * sizeof(dtn->data.size)); // data-index

	for (dtn = trep->datalist.head->next; dtn; dtn = dtn->next) {
		*((uint64_t *)(sh->buf.mem + sindex)) = htobe64(dtn->data.size);
		sindex += sizeof(dtn->data.size);

		memcpy(sh->buf.mem + dindex, dtn->data.data, dtn->data.size);
		dindex += dtn->data.size;
	}

	uint64_t tsz =
		trep->datalist.bytes + (trep->datalist.size * sizeof(uint64_t)) + 1UL + sizeof(trep->datalist.size);

	if (send(sh->clientfd, sh->buf.mem, tsz, 0) < 0)
		return -(EXIT_FAILURE);

	return EXIT_SUCCESS;
}

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

	default:
		return "wrong req-type";
	}
}

void s_tcp_print_req(s_tcp_req req)
{
	if (!req)
		return;

	tcp_req *treq = req;
	kv_t *arr = treq->kvarray.kv;

	printf("\e[1;91m%s\e[0m (%lu)\n", printable_req(treq->type), treq->nokvs);

	for (uint64_t i = 0UL, lim = treq->nokvs; i < lim; ++i) {
		printf("  - key.size = %lu (%s)\n", arr[i].key.size, (char *)(arr[i].key.data));
		printf("  - val.size = %lu (%s)\n\n", arr[i].value.size, (char *)(arr[i].value.data));
	}
}
