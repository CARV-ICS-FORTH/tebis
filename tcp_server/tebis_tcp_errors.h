#ifndef TEBIS_TCP_ERRORS_H
#define TEBIS_TCP_ERRORS_H 1

#include <errno.h>
#include <stddef.h>

/* functions returning -1, must read errno for further explanation */

#include <stdio.h>
#include <string.h>

#define print_debug(msg) fprintf(stderr, "[%d] %s: %s (%d)\n", __LINE__, msg, strerror(errno), errno)
#define dprint(msg) fprintf(stderr, "\e[93m%s\e[0m::\e[91m%d\e[0m ---> %s\n", __FUNCTION__, __LINE__, msg)

#define TTE_WAIT_REP -2
#define TTE_WAIT_REQ -3

typedef enum {

    TT_ERR_CONN_DROP = 2,
    #define TT_ERR_CONN_DROP -(TT_ERR_CONN_DROP)
    TT_ERR_
} tterr_t;

#endif /** TEBIS_TCP_ERRORS_H **/
