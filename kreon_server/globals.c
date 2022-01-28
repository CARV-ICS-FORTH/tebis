#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/hdreg.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include "../kreon_lib/allocator/allocator.h"
#include "../utilities/macros.h"
#include "conf.h"
#include "globals.h"
#include "../kreon_rdma/rdma.h"
#include <log.h>

struct globals {
	char *zk_host_port;
	char *RDMA_IP_filter;
	char *dev;
	char *mount_point;
	off64_t volume_size;
	struct channel_rdma *channel;
	uint32_t L0_size;
	uint32_t growth_factor;
	int connections_per_server;
	int job_scheduling_max_queue_depth;
	int worker_spin_time_usec;
	int is_volume_init;
	int send_index;
};
static struct globals global_vars = { .zk_host_port = NULL,
				      .RDMA_IP_filter = NULL,
				      .dev = NULL,
				      .volume_size = 0,
				      .is_volume_init = 0,
				      .channel = NULL,
				      .connections_per_server = NUM_OF_CONNECTIONS_PER_SERVER,
				      .job_scheduling_max_queue_depth = 8,
				      .worker_spin_time_usec = 100,
				      .L0_size = 64000,
				      .growth_factor = 4,
				      .send_index = 0 };

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

char *globals_get_RDMA_IP_filter(void)
{
	if (global_vars.RDMA_IP_filter == NULL) {
		log_fatal("RDMA_IP_filter host,port not set!\n");
		exit(EXIT_FAILURE);
	}
	return global_vars.RDMA_IP_filter;
}

void globals_set_RDMA_IP_filter(char *RDMA_IP_filter)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
	if (global_vars.RDMA_IP_filter == NULL) {
		global_vars.RDMA_IP_filter = (char *)malloc(strlen(RDMA_IP_filter) + 1);
		strcpy(global_vars.RDMA_IP_filter, RDMA_IP_filter);
	} else {
		log_warn("RDMA_IP_filter already set at %s", global_vars.RDMA_IP_filter);
	}
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

char *globals_get_zk_host(void)
{
	if (global_vars.zk_host_port == NULL) {
		log_fatal("Zookeeper host,port not set!\n");
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

int globals_get_connections_per_server(void)
{
	return global_vars.connections_per_server;
}

void globals_set_connections_per_server(int connections_per_server)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}

	if (global_vars.connections_per_server != -1)
		log_warn("Connections per server is already set to %d! New value is %d.",
			 global_vars.connections_per_server, connections_per_server);
	global_vars.connections_per_server = connections_per_server;

	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

int globals_get_job_scheduling_max_queue_depth(void)
{
	return global_vars.job_scheduling_max_queue_depth;
}

void globals_set_job_scheduling_max_queue_depth(int job_scheduling_max_queue_depth)
{
	log_warn("Parameter job_scheduling_max_queue_depth changed from %d to %d",
		 global_vars.job_scheduling_max_queue_depth, job_scheduling_max_queue_depth);
	global_vars.job_scheduling_max_queue_depth = job_scheduling_max_queue_depth;
}

int globals_get_worker_spin_time_usec(void)
{
	return global_vars.worker_spin_time_usec;
}

void globals_set_worker_spin_time_usec(int worker_spin_time_usec)
{
	global_vars.worker_spin_time_usec = worker_spin_time_usec;
}

void globals_set_dev(char *dev)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
	if (global_vars.dev == NULL)
		global_vars.dev = strdup(dev);
	else {
		log_warn("dev already set to %s", global_vars.dev);
		return;
	}

	int FD = open(dev, O_RDWR);
	if (FD == -1) {
		log_fatal("failed to open %s reason follows", dev);
		perror("Reason");
		exit(EXIT_FAILURE);
	}
	if (strncmp(dev, "/dev/", 5) == 0) {
		if (ioctl(FD, BLKGETSIZE64, &global_vars.volume_size) == -1) {
			log_fatal("failed to determine volume's size", dev);
			exit(EXIT_FAILURE);
		}
		log_info("%s is a block device of size %llu", dev, global_vars.volume_size);

	} else {
		off64_t end_of_file;
		end_of_file = lseek64(FD, 0, SEEK_END);
		if (end_of_file == -1) {
			log_fatal("failed to determine file's %s size exiting...", dev);
			perror("ioctl");
			exit(EXIT_FAILURE);
		}
		global_vars.volume_size = end_of_file;
		log_info("%s is a file of size %llu", dev, global_vars.volume_size);
		global_vars.mount_point = strdup(dev);
	}
	FD = close(FD);
	if (FD == -1) {
		log_fatal("failed to open %s reason follows");
		perror("Reason");
		exit(EXIT_FAILURE);
	}

	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

char *globals_get_dev(void)
{
	return global_vars.dev;
}

uint64_t globals_get_dev_size()
{
	return global_vars.volume_size;
}

void globals_create_rdma_channel(void)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
	if (global_vars.channel == NULL)
		global_vars.channel = crdma_client_create_channel(NULL);
	else
		log_warn("rdma channel already set");
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

void globals_set_rdma_channel(struct channel_rdma *channel)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}

	if (global_vars.channel == NULL)
		global_vars.channel = channel;
	else
		log_warn("rdma channel already set");

	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

struct channel_rdma *globals_get_rdma_channel(void)
{
	return global_vars.channel;
}

void globals_init_volume(void)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
	if (global_vars.dev == NULL) {
		log_fatal("Device is not set yet!");
		exit(EXIT_FAILURE);
	}
	if (global_vars.is_volume_init) {
		log_warn("Volume/File %s already initialized probably by another numa server", global_vars.dev);
		goto exit;
	}

	off64_t size;
	int fd = open(global_vars.dev, O_RDWR);
	if (fd == -1) {
		perror("open");
		exit(EXIT_FAILURE);
	}
	if (strlen(global_vars.dev) >= 5 && strncmp(global_vars.dev, "/dev/", 5) == 0) {
		log_info("Volume is a device %s", global_vars.dev);
		if (ioctl(fd, BLKGETSIZE64, &size) == -1) {
			log_fatal("Failed to determine underlying block device size %s", global_vars.dev);
			perror("ioctl");
			exit(EXIT_FAILURE);
		}
		log_info("underyling volume is a block device %s of size %ld bytes", global_vars.dev, size);
		volume_init(global_vars.dev, 0, size, 0);
	} else {
		log_info("Retrieving file: %s size...", global_vars.dev);
		size = lseek64(fd, 0, SEEK_END);
		if (size == -1) {
			log_fatal("failed to determine file size exiting...");
			perror("ioctl");
			exit(EXIT_FAILURE);
		}
		log_info("Volume is a file %s of size %ld bytes", global_vars.dev, size);
		close(fd);
		volume_init(global_vars.dev, 0, size, 1);
	}

	global_vars.volume_size = size;
	global_vars.is_volume_init = 1;

exit:
	//initializing volume this erases everything!
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		exit(EXIT_FAILURE);
	}
}

void globals_set_l0_size(uint32_t l0_size)
{
	global_vars.L0_size = l0_size;
}

uint32_t globals_get_l0_size(void)
{
	return global_vars.L0_size;
}

void globals_set_growth_factor(uint32_t growth_factor)
{
	global_vars.growth_factor = growth_factor;
}

uint32_t globals_get_growth_factor(void)
{
	return global_vars.growth_factor;
}

void globals_set_send_index(int flag)
{
	global_vars.send_index = flag;
}

int globals_get_send_index(void)
{
	return global_vars.send_index;
}
