#define _LARGEFILE64_SOURCE
#include "globals.h"
#include "../tebis_rdma/rdma.h"
#include "conf.h"
#include <fcntl.h>
#include <include/parallax/parallax.h>
#include <linux/fs.h>
#include <linux/hdreg.h>
#include <log.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct globals {
	char *zk_host_port;
	char *RDMA_IP_filter;
	char *dev;
	char *mount_point;
	off64_t volume_size;
	struct channel_rdma *channel;
	FILE *trace_file;
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
				      .trace_file = NULL,
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
		_exit(EXIT_FAILURE);
	}
	return global_vars.RDMA_IP_filter;
}

void globals_set_RDMA_IP_filter(char *RDMA_IP_filter)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
	}
	if (global_vars.RDMA_IP_filter == NULL) {
		global_vars.RDMA_IP_filter = (char *)malloc(strlen(RDMA_IP_filter) + 1);
		strcpy(global_vars.RDMA_IP_filter, RDMA_IP_filter);
	} else {
		log_warn("RDMA_IP_filter already set at %s", global_vars.RDMA_IP_filter);
	}
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
	}
}

char *globals_get_zk_host(void)
{
	if (global_vars.zk_host_port == NULL) {
		log_fatal("Zookeeper host,port not set!\n");
		_exit(EXIT_FAILURE);
	}
	return global_vars.zk_host_port;
}

void globals_set_zk_host(char *host)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
	}
	if (global_vars.zk_host_port == NULL) {
		global_vars.zk_host_port = (char *)malloc(strlen(host) + 1);
		strcpy(global_vars.zk_host_port, host);
	} else {
		log_warn("Zookeeper already set at %s", global_vars.zk_host_port);
	}
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
	}
}

int globals_get_connections_per_server(void)
{
	return global_vars.connections_per_server;
}

int globals_get_job_scheduling_max_queue_depth(void)
{
	return global_vars.job_scheduling_max_queue_depth;
}

int globals_get_worker_spin_time_usec(void)
{
	return global_vars.worker_spin_time_usec;
}

void globals_set_dev(char *dev)
{
	if (pthread_mutex_lock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
	}
	if (global_vars.dev == NULL)
		global_vars.dev = strdup(dev);
	else {
		log_warn("dev already set to %s", global_vars.dev);
		return;
	}

	int file_descriptor = open(dev, O_RDWR);
	if (file_descriptor == -1) {
		log_fatal("failed to open %s reason follows", dev);
		perror("Reason");
		_exit(EXIT_FAILURE);
	}
	if (strncmp(dev, "/dev/", 5) == 0) {
		if (ioctl(file_descriptor, BLKGETSIZE64, &global_vars.volume_size) == -1) {
			log_fatal("failed to determine volume's size");
			_exit(EXIT_FAILURE);
		}
		log_info("%s is a block device of size %lu", dev, global_vars.volume_size);

	} else {
		off64_t end_of_file;
		end_of_file = lseek64(file_descriptor, 0, SEEK_END);
		if (end_of_file == -1) {
			log_fatal("failed to determine file's %s size exiting...", dev);
			perror("ioctl");
			_exit(EXIT_FAILURE);
		}
		global_vars.volume_size = end_of_file;
		log_info("%s is a file of size %lu", dev, global_vars.volume_size);
		global_vars.mount_point = strdup(dev);
	}
	file_descriptor = close(file_descriptor);
	if (file_descriptor == -1) {
		log_fatal("failed to close the file");
		perror("Reason");
		_exit(EXIT_FAILURE);
	}

	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
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
		_exit(EXIT_FAILURE);
	}
	if (global_vars.channel == NULL)
		global_vars.channel = crdma_client_create_channel(NULL);
	else
		log_warn("rdma channel already set");
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
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
		_exit(EXIT_FAILURE);
	}
	if (global_vars.dev == NULL) {
		log_fatal("Device is not set yet!");
		_exit(EXIT_FAILURE);
	}
	if (global_vars.is_volume_init) {
		log_warn("Volume/File %s already initialized probably by another numa server", global_vars.dev);
		goto exit;
	}

	off64_t size;
	int fd = open(global_vars.dev, O_RDWR);
	if (fd == -1) {
		perror("open");
		_exit(EXIT_FAILURE);
	}
	if (strlen(global_vars.dev) >= 5 && strncmp(global_vars.dev, "/dev/", 5) == 0) {
		log_info("Volume is a device %s", global_vars.dev);
		if (ioctl(fd, BLKGETSIZE64, &size) == -1) {
			log_fatal("Failed to determine underlying block device size %s", global_vars.dev);
			perror("ioctl");
			_exit(EXIT_FAILURE);
		}
		log_info("underyling volume is a block device %s of size %ld bytes", global_vars.dev, size);
		char *error_message = par_format(global_vars.dev, 128);
		if (error_message) {
			log_fatal("Error uppon formating, error %s", error_message);
			_exit(EXIT_FAILURE);
		}
	} else {
		log_info("Retrieving file: %s size...", global_vars.dev);
		size = lseek64(fd, 0, SEEK_END);
		if (size == -1) {
			log_fatal("failed to determine file size exiting...");
			perror("ioctl");
			_exit(EXIT_FAILURE);
		}
		log_info("Volume is a file %s of size %ld bytes", global_vars.dev, size);
		close(fd);
		char *error_message = par_format(global_vars.dev, 128);
		if (error_message) {
			log_fatal("Error uppon formating, error %s", error_message);
			_exit(EXIT_FAILURE);
		}
	}

	global_vars.volume_size = size;
	global_vars.is_volume_init = 1;

exit:
	//initializing volume this erases everything!
	if (pthread_mutex_unlock(&g_lock) != 0) {
		log_fatal("Failed to acquire lock");
		_exit(EXIT_FAILURE);
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

void globals_set_send_index(int enable)
{
	global_vars.send_index = enable;
}

int globals_get_send_index(void)
{
	return global_vars.send_index;
}

void globals_open_trace_file(const char *filename)
{
	global_vars.trace_file = fopen(filename, "a");
}

static FILE *globals_get_trace_file(void)
{
	return global_vars.trace_file;
}

void globals_append_trace_file(uint32_t key_size, void *key, uint32_t value_size, void *value, enum operation_type op)
{
	FILE *fptr = globals_get_trace_file();
	if (op == TEB_GET)
		fprintf(fptr, "GET %u %s\n", key_size, (char *)key);
	else
		fprintf(fptr, "PUT %u %.*s %u %.*s\n", key_size, key_size, (char *)key, value_size, value_size,
			(char *)value);
}

void globals_close_trace_file(void)
{
	FILE *fptr = globals_get_trace_file();
	fclose(fptr);
}
