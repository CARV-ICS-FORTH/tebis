#ifndef TEBIS_TCP_ERRORS_H
#define TEBIS_TCP_ERRORS_H

#include <errno.h>
#include <stddef.h>

/* functions returning -1, must read errno for further explanation */

#include <stdio.h>
#include <string.h>

#define print_debug(msg) fprintf(stderr, "[%d] %s: %s (%d)\n", __LINE__, msg, strerror(errno), errno)
#define dprint(msg) fprintf(stderr, "\033[93m%s\033[0m::\033[91m%d\033[0m ---> %s\n", "__FUNCTION__", __LINE__, msg)

#define TTE_WAIT_REP -2
#define TTE_WAIT_REQ -3

/**
 * @brief Tebis-TCP Error (tterr)
 *
 */
typedef enum {

	TT_ERR_NONE = 0,
	TT_ERR_GENERIC = -1,
	TT_ERR_CONN_DROP = -2,
	TT_ERR_NOT_SUP = -3,
	TT_ERR_ZERO_KEY = -4
} tterr_e;

#endif /** TEBIS_TCP_ERRORS_H **/
