#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "../utilities/macros.h"
#include "globals.h"
#include "../build/external-deps/log/src/log.h"

static globals global_vars = { NULL, -1 };
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

char *globals_get_zk_host(void)
{
	if (global_vars.zk_host_port == NULL) {
		ERRPRINT("Zookeeper host,port not set!\n");
		exit(EXIT_FAILURE);
	}
	return global_vars.zk_host_port;
}

void globals_set_zk_host(char *host)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
	if (global_vars.zk_host_port == NULL) {
		global_vars.zk_host_port = (char *)malloc(strlen(host) + 1);
		strcpy(global_vars.zk_host_port, host);
	} else {
		log_warn("Zookeeper already set at %s", global_vars.zk_host_port);
	}
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

int globals_get_RDMA_connection_port(void)
{
	return global_vars.RDMA_connection_port;
}
void globals_set_RDMA_connection_port(int port)
{
	global_vars.RDMA_connection_port = port;
}

