#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <assert.h>
#include "../kreon_lib/allocator/allocator.h"
#include "../kreon_lib/btree/btree.h"
#include <log.h>
#define DEV "/usr/local/gesalous/mounts/kreon1.dat"

#define AT_SEGMENT_SIZE (2 * 1024 * 1024)
#define AT_NUM_SEGMENTS 20480
#define AT_BITMAP_BUDDIES_SIZE ((4096 - sizeof(uint64_t)) / sizeof(uint64_t))
extern uint64_t collisions;
extern uint64_t hits;
extern uint64_t sleep_times;
extern int32_t max_tries;
int count;
int cpu_id = 0;
struct at_allocator_bitmap_buddies {
	uint64_t l_epoch;
	uint64_t l_bitmap[AT_BITMAP_BUDDIES_SIZE];
	uint64_t r_epoch;
	uint64_t r_bitmap[AT_BITMAP_BUDDIES_SIZE];
};

static struct volume_descriptor *at_init_volume(char *volumeName, char *key, uint64_t start, uint64_t size)
{
	struct volume_descriptor *volume_desc = malloc(sizeof(volume_descriptor));
	volume_desc->state = VOLUME_IS_OPEN;
	volume_desc->snap_preemption = SNAP_INTERRUPT_DISABLE;
	volume_desc->last_snapshot = get_timestamp();
	volume_desc->last_commit = get_timestamp();
	volume_desc->last_sync = get_timestamp();

	volume_desc->volume_name = malloc(strlen(volumeName) + 1);
	strcpy(volume_desc->volume_name, volumeName);
	volume_desc->volume_id = malloc(strlen(key) + 1);
	strcpy(volume_desc->volume_id, key);
	volume_desc->open_databases = init_list(&destoy_db_list_node);
	volume_desc->offset = start;
	volume_desc->size = size;
	/*allocator lock*/
	MUTEX_INIT(&(volume_desc->allocator_lock), NULL);
	/*free operations log*/
	MUTEX_INIT(&(volume_desc->FREE_LOG_LOCK), NULL);
	allocator_init(volume_desc);
	volume_desc->reference_count++;
	/*soft state about the in use pages of level-0 for each BUFFER_SEGMENT_SIZE
* segment inside the volume*/
	volume_desc->segment_utilization_vector_size =
		((volume_desc->volume_superblock->dev_size_in_blocks -
		  (1 + FREE_LOG_SIZE + volume_desc->volume_superblock->bitmap_size_in_blocks)) /
		 (AT_SEGMENT_SIZE / DEVICE_BLOCK_SIZE)) *
		2;
	volume_desc->segment_utilization_vector = (uint16_t *)malloc(volume_desc->segment_utilization_vector_size);
	if (volume_desc->segment_utilization_vector == NULL) {
		log_fatal("failed to allocate memory for segment utilization vector of "
			  "size %lu",
			  volume_desc->segment_utilization_vector_size);
		exit(EXIT_FAILURE);
	}
	memset(volume_desc->segment_utilization_vector, 0x00, volume_desc->segment_utilization_vector_size);
	return volume_desc;
}

int main()
{
	log_info("Initialized volume %s", DEV);

	int64_t size;
	int fd = open(DEV, O_RDWR);
	if (fd == -1) {
		perror("open");
		exit(EXIT_FAILURE);
	}
	if (strlen(DEV) >= 5 && strncmp(DEV, "/dev/", 5) == 0) {
		log_info("underyling volume is a device %s", DEV);
		if (ioctl(fd, BLKGETSIZE64, &size) == -1) {
			log_fatal("Failed to determine underlying block device size %s", DEV);
			perror("ioctl");
			exit(EXIT_FAILURE);
		}
		log_info("underyling volume is a block device %s of size %ld bytes", DEV, size);
		volume_init(DEV, 0, size, 0);
	} else {
		log_info("querying size of file %s", DEV);
		size = lseek(fd, 0, SEEK_END);
		if (size == -1) {
			log_fatal("failed to determine file size exiting...");
			perror("ioctl");
			exit(EXIT_FAILURE);
		}
		log_info("underyling volume is a file %s of size %ld bytes", DEV, size);
		volume_init(DEV, 0, size, 1);
	}
	close(fd);
	struct volume_descriptor *volume_desc = at_init_volume(DEV, "0", 0, size);
	for (int i = 0; i < AT_NUM_SEGMENTS; i++) {
		void *address = allocate((void *)volume_desc, AT_SEGMENT_SIZE, 0, 0);
		if (((uint64_t)address - MAPPED) % AT_SEGMENT_SIZE != 0) {
			log_info("After successfull %d allocation bitamp is ", i);
			void *addr = volume_desc->bitmap_start;

			while (addr < volume_desc->bitmap_end) {
				struct at_allocator_bitmap_buddies *b = (struct at_allocator_bitmap_buddies *)addr;
				if (b->l_epoch >= b->r_epoch) {
					for (int i = 0; i < AT_BITMAP_BUDDIES_SIZE; i++) {
						if (b->l_bitmap[i] != 0xFFFFFFFFFFFFFFFF) {
							log_info("Left(valid) bitmap[%d] = %llu", i, b->l_bitmap[i]);
						}
					}
				} else {
					for (int i = 0; i < AT_BITMAP_BUDDIES_SIZE; i++) {
						if (b->r_bitmap[i] != 0xFFFFFFFFFFFFFFFF) {
							log_info("Right(valid) bitmap[%d] = %llu", i, b->r_bitmap[i]);
						}
					}
				}
				addr += sizeof(struct at_allocator_bitmap_buddies);
				printf("\n*** end of buddy pair ****\n");
			}
			assert(0);
		}
		if (i % 10 == 0)
			snapshot(volume_desc);
		free_block(volume_desc, address, AT_SEGMENT_SIZE);
	}
	log_info("Done :D");
	return 1;
}
