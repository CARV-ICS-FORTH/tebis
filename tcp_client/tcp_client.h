#ifndef TEBIS_TCP_CLIENT_H
#define TEBIS_TCP_CLIENT_H

#include "tebis_tcp_types.h"

typedef void *c_tcp_req; /// TODO: change to 'struct internal_tcp_req *' instead of 'void *' (safe coding)
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
 * @brief Creates a new request object or updates an existing one. If @a req is equal to NULL ,then a new c_tcp_req object
 * is created, otherwise @a req is updated with the provided (new) key and payload sizes. If @a rtype is @b REQ_GET, @b REQ_DEL,
 * or @b REQ_EXISTS then @a paysz is ignored! If it is REQ_SCAN, then @a paysz acts as the length of the scan request.
 * On success, of these two options, the former will return the newly allocated request object, while the latter will
 * both return the updated request object and will set @a req to point to the updated object too. If the sum of @a keysz
 * and @a paysz is less than the @a req current's sum, then no allocation occures. If it is greater, @a req is destroyed and
 * a new request object is allocated. Check @b c_tcp_req_destroy() for more info on a request object's destruction.
 * On failure, NULL is returned for both cases, @a req is neither destroyed nor pointsto another request object and
 * @b errno is also set to indicate the error.
 *
 * @par ERRORS
 * @b EINVAL  The request object @a req is not initialized, in case of an update
 * @b ENOTSUP The request object's type @a rtype is not supported
 *
 * @param req request object
 * @param rtype type of request
 * @param keysz size of key
 * @param paysz size of payload or length for SCAN request
 * @return c_tcp_req
 */
c_tcp_req c_tcp_req_factory(c_tcp_req *req, req_t rtype, size_t keysz, size_t paysz);

/**
 * @brief Destroys the provided request object. On success, 0 is returned and @a req becomes invalid. As a result, any
 * absolute pointers returned by either @b c_tcp_req_expose_key() or @b c_tcp_req_expose_payload() will become invalid too.
 * On failure, -1 is returned and @b errno is set to indicate the error.
 *
 * @par ERRORS
 * @b EINVAL The request object @a req is either equal to NULL, or not initialized
 *
 * @param req request object
 * @return int
 */
int c_tcp_req_destroy(c_tcp_req req);

/**
 * @brief On success, an aboslute pointer to an internal key-buffer is returned. Writting more than @a keysz bytes
 * ( as stated in @b c_tcp_req_factory() ) to that buffer causes undefined behavior. On failure, NULL is returned and
 * @b errno is set to indicate the error.
 *
 * @par ERRORS
 * @b EINVAL  The request object @a req is either NULL, or not initialized
 *
 * @param req
 * @return void*
 */
void *c_tcp_req_expose_key(c_tcp_req req);

/**
 * @brief On success, an abosule pointer to an internal payload-buffer is returned. Writting more than @a paysz bytes
 * ( as stated in @b c_tcp_req_factory() ) to that buffer causes undefined behavior. In case of a SCAN request ( set with
 * @b c_tcp_req_factory) ),(only the) @a length (int) of the SCAN request must be stored to the internal payload-buffer.
 * On failure, NULL is returned and @b errno is set to indicate the error.
 *
 * @par ERRORS
 * @b EINVAL  The request object @a req is either NULL, or not initialized
 * @b ENODATA The request object @a req is [GET | DEL | EXISTS], thus no there is no need to use payload
 *
 * @param req
 * @return void*
 */
void *c_tcp_req_expose_payload(c_tcp_req req);

c_tcp_rep c_tcp_rep_new(size_t size);

/**
 * @brief
 *
 * @param rep
 * @return int
 */
int c_tcp_rep_destroy(c_tcp_rep rep);

/**
 * @brief Pops a value from the reply object @a rep. Calling this function more than once on a reply object, makes sense
 * only when the object is a response from a SCAN request. Other requests will respond/reply only with a single value.
 * On success, 0 is returned and @a val will be updated. On failure, -1 is returned and @b errno is set to indicate the error.
 *
 * @par ERRORS
 *
 * @b EINVAL  The reply object @a rep is either NULL or not initialized
 * @b EINVAL  The value @a val is NULL
 * @b ENODATA All available values have been poped
 *
 * @param rep
 * @param val
 * @return int
 */
int c_tcp_rep_pop_value(c_tcp_rep rep, generic_data_t *val);

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
int c_tcp_recv_rep(cHandle __restrict__ chandle, c_tcp_rep *rep); /** TODO: put 'restrict' again */

#endif /** TEBIS_TCP_CLIENT_H **/
