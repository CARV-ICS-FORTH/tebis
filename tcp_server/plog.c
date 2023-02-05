#include "plog.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

int plog_internal(const char *restrict format, const char *restrict file, const char *restrict func, int line, ...)
{
	if (!format) {
		errno = EINVAL;
		return EXIT_FAILURE;
	}

	va_list arglist;

	pthread_mutex_lock(&g_lock);

	va_start(arglist, line);
	fprintf(stderr, format, file, func, line);
	vfprintf(stderr, format + strlen(format) + 1UL, arglist);
	fprintf(stderr, "\n");
	va_end(arglist);

	pthread_mutex_unlock(&g_lock);

	return EXIT_SUCCESS;
}
