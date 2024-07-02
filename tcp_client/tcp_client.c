/** TODO: replace every 'epoll_wait()' with 'epoll_pwait()' >>> signal handling
 * (log) */

#define _GNU_SOURCE 1

#include "tcp_client.h"
#include "tebis_tcp_errors.h"

#include <arpa/inet.h>
#include <linux/types.h>

#include <errno.h>
#include <log.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>

#ifdef SGX
#include "../common/common_ssl/common.h"
#include <openenclave/host.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#endif

#define TT_MAP_PROT (PROT_READ | PROT_WRITE)
#define TT_MAP_FLAGS (MAP_ANON | MAP_PRIVATE)

#define req_has_value(rtype) (rtype > REQ_SCAN)
#define is_req_invalid(rtype) ((__u32)(rtype) >= OPSNO)

#define reqhdr_type_offset 0UL
#define reqhdr_keysz_offset 1UL
#define reqhdr_valsz_offset 5UL
#define reqhdr_key_offset 9UL

struct internal_tcp_rep {
	__u32 retc;
	__u32 flags;
	__u32 size;

	struct buffer buf;
};

struct internal_tcp_req {
	req_t type;

	__u32 flags;
	__u32 keysz;
	__u32 paysz;

	struct buffer buf;

	const char __pad[24U];
};

struct client_handle {
	uint16_t flags1;
	uint16_t flags2;

#define MAGIC_INIT_NUM (0xCAFE)
#define CLHF_SND_REQ (1 << 0)

	//{ int (*destroy)(void)}
	// __u64 x = &((struct client_handle *)(0)->destroy)

	int sock;
#ifdef SGX
	SSL *ssl;
	X509 *cert;
	SSL_CTX *ctx;
#endif
};

/*****************************************************************************/

static int server_version_check(struct client_handle *ch)
{
	__u8 tbuf[5];

	*tbuf = REQ_INIT_CONN;
	*((__u32 *)(tbuf + 1UL)) = htobe32(TT_VERSION);

#ifndef SGX
	if (send(ch->sock, tbuf, 5UL, 0) < 0) {
		log_error("send()");
		return -(EXIT_FAILURE);
	}
#else
	if (SSL_write(ch->ssl, tbuf, 5UL) < 0) {
		log_error("SSL_write()");
		return -(EXIT_FAILURE);
	}
#endif

#ifndef SGX
	int64_t ret = recv(ch->sock, tbuf, 4UL, 0);
#else
	int64_t ret = SSL_read(ch->ssl, tbuf, 4UL);
#endif

	if (ret < 0) {
		log_error("recv()");
		return -(EXIT_FAILURE);
	} else if (!ret) {
		fprintf(stderr, "server has been shut down!\n");
		return -(EXIT_FAILURE);
	}

	printf("TEBIS_SERVER_VERSION: 0x%x\n", *((__u32 *)(tbuf)));
	return EXIT_SUCCESS;
}

#ifdef SGX
X509 *cert = NULL;
SSL_CTX *ctx = NULL;

void SSL_done(SSL *ssl, X509 *cert, SSL_CTX *ctx)
{
	if (ssl)
		SSL_free(ssl);
	if (cert)
		X509_free(cert);
	if (ctx)
		SSL_CTX_free(ctx);
}

int verify_callback(int preverify_ok, X509_STORE_CTX *ctx);

void ssl_init(void)
{
	OpenSSL_add_all_algorithms();
	ERR_load_BIO_strings();
	ERR_load_crypto_strings();
	SSL_load_error_strings();

	if (SSL_library_init() < 0) {
		log_error("SSL library init error");
		SSL_done(NULL, cert, ctx);
	}

	if ((ctx = SSL_CTX_new(SSLv23_client_method())) == NULL) {
		printf("TLS client: unable to create a new SSL context\n");
		SSL_done(NULL, cert, ctx);
	}

	// choose TLSv1.2 by excluding SSLv2, SSLv3 ,TLS 1.0 and TLS 1.1
	SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2);
	SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);
	SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1);
	SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_1);
	// specify the verify_callback for custom verification
	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, &verify_callback);
=======
void ssl_init(struct client_handle *chandle)
{
	static int flag = 0;
	bool init = __sync_bool_compare_and_swap(&flag, 0, 1);
	if (init) {
		OpenSSL_add_all_algorithms();
		ERR_load_BIO_strings();
		ERR_load_crypto_strings();
		SSL_load_error_strings();
	}

	if ((chandle->ctx = SSL_CTX_new(SSLv23_client_method())) == NULL) {
		log_error("TLS client: unable to create a new SSL context");
		SSL_done(NULL, chandle->cert, chandle->ctx);
	}

	// choose TLSv1.2 by excluding SSLv2, SSLv3 ,TLS 1.0 and TLS 1.1
	SSL_CTX_set_options(chandle->ctx, SSL_OP_NO_SSLv2);
	SSL_CTX_set_options(chandle->ctx, SSL_OP_NO_SSLv3);
	SSL_CTX_set_options(chandle->ctx, SSL_OP_NO_TLSv1);
	SSL_CTX_set_options(chandle->ctx, SSL_OP_NO_TLSv1_1);
	SSL_CTX_set_verify(chandle->ctx, SSL_VERIFY_NONE, NULL);
>>>>>>> 23f1689 ([feat] SSL sockets)
}
#endif

int chandle_init(cHandle restrict *restrict chandle, const char *restrict addr, const char *restrict port)
{
	if (!chandle || !addr || !port) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (!(*chandle = malloc(sizeof(struct client_handle)))) {
		log_error("malloc()");
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/
	struct client_handle *ch = *chandle;

	ch->flags1 = 0U;
	ch->flags2 = CLHF_SND_REQ;

#ifdef SGX
  ch->ssl = NULL;
  ch->ctx = NULL;
  ch->cert = NULL;
	ssl_init(ch);
	if ((ch->ssl = SSL_new(ch->ctx)) == NULL) {
		log_error("SSL_new()");
		SSL_done(ch->ssl, ch->cert, ch->ctx);
		return -(EXIT_FAILURE);
	}
#endif

	int retc;

	struct addrinfo hints;
	struct addrinfo *res;
	struct addrinfo *rp;

	memset(&hints, 0, sizeof(hints));

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_ADDRCONFIG | AI_NUMERICSERV;

	if ((retc = getaddrinfo(addr, port, &hints, &res))) {
		gai_strerror(retc);
#ifdef SGX
		SSL_done(ch->ssl, ch->cert, ch->ctx);
#endif
		return -(EXIT_FAILURE);
	}

	if (!res) {
#ifdef SGX
		SSL_done(ch->ssl, ch->cert, ch->ctx);
#endif
		return -(EXIT_FAILURE);
	}

	for (rp = res; rp; rp = rp->ai_next) {
		if ((ch->sock = socket(rp->ai_family, SOCK_STREAM | SOCK_CLOEXEC, 0)) < 0) {
			print_debug("socket()");
			continue;
		}

		if (!connect(ch->sock, rp->ai_addr, rp->ai_addrlen)) {
			ch->flags1 = MAGIC_INIT_NUM;
			freeaddrinfo(res);
#ifdef SGX
			SSL_set_fd(ch->ssl, ch->sock);
			int error;
			if ((error = SSL_connect(ch->ssl)) != 1) {
				log_error("SSL_connect");
				SSL_get_error(ch->ssl, error);
				SSL_done(ch->ssl, ch->cert, ch->ctx);
				return -(EXIT_FAILURE);
			}
#endif
			if (server_version_check(ch) < 0) {
#ifdef SGX
				SSL_done(ch->ssl, ch->cert, ch->ctx);
#endif
				close(ch->sock);
				continue;
			}
			return EXIT_SUCCESS;
		}
#ifdef SGX
		SSL_done(ch->ssl, ch->cert, ch->ctx);
#endif
		close(ch->sock);
	}

	freeaddrinfo(res);

	errno = ECONNREFUSED;
	return -(EXIT_FAILURE);
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

	__u64 discardbuf;

	shutdown(ch->sock, SHUT_WR); // sends FIN
	recv(ch->sock, &discardbuf, sizeof(discardbuf), 0); // zero will be returned
	close(ch->sock);
#ifdef SGX
	SSL_done(ch->ssl, ch->cert, ch->ctx);
#endif
	free(chandle);
	return EXIT_SUCCESS;
}

/*****************************************************************************/

c_tcp_req c_tcp_req_factory(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz)
{
	struct internal_tcp_req *ireq;
	__u64 tsize;

	if (is_req_invalid(rtype) || !keysz) {
		errno = ENOTSUP;
		return NULL;
	}

	if (!req_has_value(rtype)) // [PUT,  PUT_IF_EXIST]
		paysz = 0UL;

	if (req) /* update() existed request */
	{
		ireq = *req;

		if (ireq->flags != MAGIC_INIT_NUM) {
			errno = EINVAL;
			return NULL;
		}

		tsize = keysz + paysz + (req_has_value(rtype) ? __reqhdr_size : 5UL);

		if (tsize > ireq->buf.bytes) {
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
		tsize = ((__reqhdr_size + sizeof(*ireq) + keysz + paysz) | 0xfffUL) + 1UL; // efficient page-align

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
	*((__u32 *)(ireq->buf.mem + 1UL)) = htobe32(keysz);

	if (req_has_value(rtype))
		*((__u32 *)(ireq->buf.mem + 5UL)) = htobe32(paysz);

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

	__u64 off = req_has_value(ireq->type) ? reqhdr_key_offset : 5UL;

	return ireq->buf.mem + off;
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

	// do not allow the exposure of payload's buffer for
	// requests [REQ_GET, REQ_DEL, REQ_EXISTS, REQ_SCAN]

	if (!req_has_value(ireq->type)) {
		errno = ENODATA;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	return ireq->buf.mem + reqhdr_key_offset + ireq->keysz;
}

c_tcp_rep c_tcp_rep_new(size_t size)
{
	if (!size) {
		errno = EINVAL;
		return NULL;
	}

	/** END OF ERROR HANDLING **/

	struct internal_tcp_rep *irep;
	__u64 tsize = ((TT_REPHDR_SIZE + sizeof(*irep) + size) | 0xfffUL) + 1UL; // efficient page-alignment

	if ((irep = mmap(NULL, tsize, TT_MAP_PROT, TT_MAP_FLAGS, -1, 0UL)) == MAP_FAILED)
		return NULL;

	irep->buf.mem = (char *)(irep) + sizeof(*irep);
	irep->buf.bytes = tsize - sizeof(*irep);
	irep->flags = MAGIC_INIT_NUM;
	irep->size = 0UL;
	irep->retc = 0U;

	return irep;
}

static int c_tcp_rep_update(struct internal_tcp_rep **irep, int retc,
			    size_t size) // internal-use-only
{
	/** TODO: irep->buf.mem is unecessary. irep->buf.mem is always equal to irep +
   * sizeof(*irep) */

	struct internal_tcp_rep *_irep = *irep;

	if (size > _irep->buf.bytes) {
		size = ((size + TT_REPHDR_SIZE + sizeof(*irep)) | 0xfffUL) + 1UL;

		if ((*irep = mremap(irep, _irep->buf.bytes, size, MREMAP_MAYMOVE)) == MAP_FAILED) {
			log_error("mmap() failed");
			perror("mmap()");
			return -(EXIT_FAILURE);
		}

		_irep = *irep;
		_irep->buf.mem = (char *)(irep) + sizeof(*irep);
		_irep->buf.bytes = size - sizeof(*irep);
	}

	_irep->size = size;
	_irep->retc = retc;

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

	/* if (!(ch->flags2 & CLHF_SND_REQ)) // client waits for a 'reply' (not a
  'request')
  {
          errno = EPERM;
          return -(EXIT_FAILURE);
  } */

	/** END OF ERROR HANDLING **/

	__u64 total_size = ireq->keysz;

	if (req_has_value(ireq->type))
		total_size += __reqhdr_size + ireq->paysz; // PUThdr
	else
		total_size += 5UL; // temporary, GEThdr
#ifndef SGX
	if (send(ch->sock, ireq->buf.mem, total_size, 0) <= 0)
		return -(EXIT_FAILURE);
#else
	int bytes_written = 0;
	while ((bytes_written = SSL_write(ch->ssl, ireq->buf.mem, (size_t)total_size)) <= 0) {
		int error = SSL_get_error(ch->ssl, bytes_written);
		if (error == SSL_ERROR_WANT_WRITE || error == SSL_ERROR_WANT_READ)
			continue;
		log_error("TLS client failed! SSL_write returned %d\n", error);
		SSL_done(ch->ssl, ch->cert, ch->ctx);
		return -(EXIT_FAILURE);
	}
#endif

	ch->flags2 &= ~(CLHF_SND_REQ);

	return EXIT_SUCCESS;
}

int c_tcp_recv_rep(cHandle restrict chandle, c_tcp_rep *rep)
{
	if (!chandle || !rep) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	struct client_handle *ch = chandle;
	struct internal_tcp_rep *irep = *rep;

	if (ch->flags1 != MAGIC_INIT_NUM) {
		errno = EINVAL;
		return -(EXIT_FAILURE);
	}

	if (ch->flags2 & CLHF_SND_REQ) { // per-thread flag
		errno = EPERM;
		return -(EXIT_FAILURE);
	}

	/** END OF ERROR HANDLING **/

	int64_t bytes_read;
#ifndef SGX
	if ((bytes_read = read(ch->sock, irep->buf.mem, TT_REPHDR_SIZE)) < 0) {
		log_error("read() returned: %ld", bytes_read);
		return -(EXIT_FAILURE);
	}
#else
	while ((bytes_read = SSL_read(ch->ssl, irep->buf.mem, TT_REPHDR_SIZE)) <= 0) {
		int error = SSL_get_error(ch->ssl, bytes_read);
		if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE)
			continue;
		log_error("TLS client failed! SSL_read returned error=%d\n", error);
		SSL_done(ch->ssl, ch->cert, ch->ctx);
		return -(EXIT_FAILURE);
	}
#endif

	irep->retc = *((uint8_t *)(irep->buf.mem));
	irep->size = be32toh(*((__u32 *)(irep->buf.mem + 1UL)));

	if (c_tcp_rep_update(&irep, irep->retc, irep->size) < 0) {
		log_error("c_tcp_rep_update() failed! (size: %u)\n", irep->size);
		return -(EXIT_FAILURE);
	}

#ifndef SGX
	if ((bytes_read = read(ch->sock, irep->buf.mem, irep->size)) < 0) {
		log_error("read() returned: %ld", bytes_read);
		return -(EXIT_FAILURE);
	}
#else
	while (irep->size != 0 && (bytes_read = SSL_read(ch->ssl, irep->buf.mem, irep->size)) <= 0) {
		int error = SSL_get_error(ch->ssl, bytes_read);
		if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE)
			continue;
		log_error("TLS client failed! SSL_read returned error=%d\n", error);
		SSL_done(ch->ssl, ch->cert, ch->ctx);
		return -(EXIT_FAILURE);
	}
#endif

	ch->flags2 |= CLHF_SND_REQ;

	return EXIT_SUCCESS;
}

char *printable_req[OPSNO] = { "REQ_GET", "REQ_DEL", "REQ_EXISTS", "REQ_SCAN", "REQ_PUT", "REQ_UPDATE" };

void c_tcp_print_req(c_tcp_req req)
{
	struct internal_tcp_req *ireq = req;

	printf("[ \033[1;31m%s\033[0m ]\n", printable_req[ireq->type]);
	printf("  > ireq->keysz = %u\n"
	       "  > ireq->paysz = %u\n",
	       ireq->keysz, ireq->paysz);
	printf("  > key = '%s'\n", ireq->buf.mem + __reqhdr_size);
}
