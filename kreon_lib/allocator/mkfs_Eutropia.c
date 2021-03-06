#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <log.h>
#include "allocator.h"
#include "../btree/conf.h"

/*
 * Input: File descriptor, offset, relative position from where it has to be
 * read (SEEK_SET/SEEK_CUR/SEEK_END)
 *    pointer to databuffer, size of data to be read
 * Output: -1 on failure of lseek64/read
 *     number of bytes read on success.
 * Note: This reads absolute offsets in the disk.
 */
static int32_t lread(int32_t fd, off64_t offset, int whence, void *ptr, size_t size)
{
	if (size % 4096 != 0) {
		printf("FATAL read request size %d not a multiple of 4k, harmful\n", (int32_t)size);
		exit(-1);
	}
	if (offset % 4096 != 0) {
		printf("FATAL read-seek request size %lld not a multiple of 4k, harmful\n", (long long)offset);
		exit(-1);
	}
	if (lseek64(fd, offset, whence) == -1) {
		fprintf(stderr, "lseek: fd:%d, offset:%ld, whence:%d, size:%lu\n", fd, offset, whence, size);
		perror("lread");
		return -1;
	}
	if (read(fd, ptr, size) == -1) {
		fprintf(stderr, "lread-!: fd:%d, offset:%ld, whence:%d, size:%lu\n", fd, offset, whence, size);
		perror("lread");
		return -1;
	}
	return 1;
}

int main(int argc, char *argv[])
{
	superblock sp;
	pr_system_catalogue dev_catalogue;
	char *ptr;
	uint64_t start;
	uint64_t size;
	int32_t bytes_read = 0;
	int32_t fd;

	if (argc != 4) {
		printf("mkfs_Eutropia <Volume name> <offset in bytes> <size in bytes>\n");
		exit(-1);
	}

	start = strtoul(argv[2], &ptr, 10);
	size = strtoul(argv[3], &ptr, 10);
	printf("mkfs: Initializing volume %s start %llu size %llu\n", (char *)argv[1], (LLU)start, (LLU)size);
	fd = volume_init(argv[1], start, size, 0);
	printf("\n\n----- Successfully initialized device -------\n\n");

	bytes_read = lread(fd, 0, SEEK_SET, &sp, DEVICE_BLOCK_SIZE);
	if (bytes_read == -1) {
		fprintf(stderr, "(lread) Function = %s, code = %d,  ERROR = %s\n", __func__, errno, strerror(errno));
		return -1;
	}
	/* print state of the device  */
	printf("*************  <Superblock> ***********\n");
	printf("Bitmap size in blocks %llu\n", (LLU)sp.bitmap_size_in_blocks);
	printf("Device size in blocks %llu\n", (LLU)sp.dev_size_in_blocks);
	printf("Data addressed in blocks %llu\n", (LLU)sp.dev_addressed_in_blocks);
	printf("Unmapped blocks %llu\n", (LLU)sp.unmapped_blocks);
	printf("System catalogue address %llu\n", (LLU)sp.system_catalogue);
	printf("************* </Superblock> ***********\n");

	bytes_read =
		lread(fd, (off_t)sp.system_catalogue, SEEK_SET, &dev_catalogue, (size_t)sizeof(pr_system_catalogue));
	if (bytes_read == -1) {
		fprintf(stderr, "(lread) Function = %s, code = %d,  ERROR = %s\n", __func__, errno, strerror(errno));
		return -1;
	}

	printf("*************  <System_Catalogue> ***********\n");
	printf("Epoch %llu \n", (LLU)dev_catalogue.epoch);
	printf("Free log position = %llu\n", (LLU)dev_catalogue.free_log_position);
	printf("Free log last free %llu\n", (LLU)dev_catalogue.free_log_last_free);
	printf("*************  </System_Catalogue> ***********\n");

	close(fd);
	return 1;
}
