#ifndef TEBIS_TCP_ERRORS_H
#define TEBIS_TCP_ERRORS_H 1

#include <errno.h>
#include <stddef.h>

/* functions returning -1, must read errno for further explanation */

#include <stdio.h>
#include <string.h>

#define print_debug(msg) fprintf(stderr, "[%d] %s: %s (%d)\n", __LINE__, msg, strerror(errno), errno)
#define dprint(msg) fprintf(stderr, "\033[93m%s\033[0m::\033[91m%d\033[0m ---> %s\n", "deprecated", __LINE__, msg)

#define TTE_WAIT_REP -2
#define TTE_WAIT_REQ -3

/* char * tcp_error_strings[] =\
{
    NULL,
    NULL,
    "client expects to receive a reply",
    "client expects to send a request"
};

char * tcp_print_error(int errc)
{
    if ( errc > 0 )
    {
        errno = EINVAL;
        return NULL;
    }

    return tcp_error_strings[-errc];
} */

#endif /** TEBIS_TCP_ERRORS_H **/
