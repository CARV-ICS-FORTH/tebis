#ifndef TEBIS_TCP_CLIENT_H
#define TEBIS_TCP_CLIENT_H

#include "tebis_tcp_types.h"

typedef void *c_tcp_req;
typedef void *c_tcp_rep;
typedef void *cHandle;

#define TT_DEF_KEYSZ (1UL << 11) // 2KB
#define TT_DEF_PAYSZ (1UL << 12) // 4KB
#define TT_DEF_REPSZ (1UL << 12) // 4KB

#define TT_DEFAULT_PORT 25565 // Minecraft's port

/** TODO: add documentation */

/**
 * @brief
 *
 */
int chandle_init(cHandle __restrict__ *__restrict__ chandle, const char *__restrict__ addr,
		 const char *__restrict__ port);

/**
 * @brief
 *
 * @param chandle
 */
int chandle_destroy(cHandle chandle);

/**
 * @brief Creates a new request object or updates an existing one. If @a req is equal to @b NULL ,then a new c_tcp_req object
 * is created, otherwise @a req is updated with the provided (new) key and payload sizes. If @a rtype belongs in GET-family,
 * then @a paysz is ignored! On success, of these two options, the former will return the newly allocated request object,
 * while the latter will both return the updated request object and will set @a req to point the updated object too. If the
 * sum of @a keysz and @a paysz is less than the @a req sum, then no allocation occures. If it is greater, @a req is
 * destroyed and a new request object is allocated. On failure, @b NULL is returned for both cases, @a req is neither
 * destroyed nor points to another request object and @b errno is also set to indicate the error.
 *
 * @param req request object
 * @param rtype request's type
 * @param keysz key's size
 * @param paysz payload's size or length for SCAN request
 * @return c_tcp_req
 */
c_tcp_req c_tcp_req_factory(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz);

/**
 * @brief Destroys the provided request object. On success, @b EXIT_SUCCESS is returned and @a req becomes invalid.
 * On failure, @b EXIT_FAILURE is returned and @b errno is set to indicate the error.
 *
 * @param req request object
 * @return int
 */
int c_tcp_req_destroy(c_tcp_req req);

/**
 * @brief On success, an aboslute pointer to an internal key-buffer is returned. Writting more than @a keysz bytes
 * ( as stated in @b c_tcp_req_factory() ) to that buffer causes undefined behavior. On failure, @b NULL is returned and
 * @b errno is set to indicate the error.
 *
 * @param req
 * @return void*
 */
void *c_tcp_req_expose_key(c_tcp_req req);

/**
 * @brief On success, an abosule pointer to an internal payload-buffer is returned. Writting more than @a paysz bytes
 * ( as stated in @b c_tcp_req_factory() ) to that buffer causes undefined behavior. In case of a SCAN request ( set with
 * @b c_tcp_req_factory) ),(only the) @a length (int) of the SCAN request must be stored to the internal payload-buffer.
 * On failure, @b NULL is returned and @b errno is set to indicate the error.
 *
 * @param req
 * @return void*
 */
void *c_tcp_req_expose_payload(c_tcp_req req);

c_tcp_rep c_tcp_rep_new(size_t size);
int c_tcp_rep_destroy(c_tcp_rep rep);
int c_tcp_rep_pop_value(c_tcp_rep rep, generic_data_t *val);
int c_tcp_rep_get_all(c_tcp_rep rep, generic_data_t *val_array);

/**
 * @brief
 *
 * @param chandle
 * @param req
 * @return int
 */
int c_tcp_send_req(cHandle chandle, c_tcp_req req);

/**
 * @brief
 *
 * @param chandle
 * @param rep
 * @return ssize_t
 */
int c_tcp_recv_rep(cHandle __restrict__ chandle, c_tcp_rep __restrict__ rep);

#endif /** TEBIS_TCP_CLIENT_H **/
