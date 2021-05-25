#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include "asyncio.h"
#include "log.h"

#define MAX_REQS 128
// Application-defined structure for tracking I/O requests
struct asyncio_ctx_s {
	struct {
		int state; // Status of request
		char *buffer; // Internal buffer
		struct aiocb aiocbp; // Asynchronous I/O control block
	} requests[MAX_REQS];
	pthread_mutex_t lock;
};

// Initialize the array of I/O requests for the asynchronous I/O
struct asyncio_ctx_s *asyncio_create_context()
{
	struct asyncio_ctx_s *ctx = malloc(sizeof(*ctx));
	memset(ctx, 0, sizeof(*ctx));
	pthread_mutex_init(&ctx->lock, NULL);
	log_info("New AIO context created");
	return ctx;
}

// Check to find available slot in the i/o request array for the new i/o
// request.
// Return the 'index' of the available slot in the array, or return '-1' if all
// the slots are active and allocated.
static int find_slot(struct asyncio_ctx_s *ctx)
{
	for (int i = 0; i < MAX_REQS; i++) {
		if (ctx->requests[i].state == 0) {
			ctx->requests[i].state = -1;
			return i;
		}

		// If the ctx->requests is in active state, then check if it has finished.
		// Update the state based on the return valuew of aio_error().
		if (ctx->requests[i].state == EINPROGRESS) {
			ctx->requests[i].state = aio_error(&ctx->requests[i].aiocbp);

			switch (ctx->requests[i].state) {
			case 0:
				if (ctx->requests[i].buffer != NULL) {
					free(ctx->requests[i].buffer);
					ctx->requests[i].buffer = NULL;
				}
				return i;
				break;
			case EINPROGRESS:
				break;
			case ECANCELED:
				log_fatal("AIO request was cancelled");
				exit(1);
			default:
				log_fatal("AIO error: %s", strerror(ctx->requests[i].state));
				raise(SIGINT);
				exit(1);
			}
		}
	}

	return -1;
}

// Add new I/O request in the array
// Arguments:
//	fd	   - File descriptor
//	data   - Data to be transfered to the device
//	size   - Size of the data
//	offset - Write the data to the specific offset in the file
//
void asyncio_post_write(struct asyncio_ctx_s *ctx, int fd, char *data, size_t size, uint64_t offset)
{
	int check; // Check status
	int slot; // Find available slot for the request

	// Spin here until find an available slot for the request
	pthread_mutex_lock(&ctx->lock);
	do {
		slot = find_slot(ctx);
	} while (slot == -1);
	pthread_mutex_unlock(&ctx->lock);

	// Create and initialize the aiocb structure.
	// If we don't init to 0, we have undefined behavior.
	// E.g. through sigevent op.aio_sigevent there could be a callback function
	// being set, that the program tries to call - which will then fail.
	struct aiocb obj = { 0 };

	ctx->requests[slot].buffer = NULL;
	obj.aio_fildes = fd;
	obj.aio_offset = offset;
	/*posix_memalign((void **)&ctx->requests[slot].buffer, 512, size * sizeof(char));*/
	/*memcpy(ctx->requests[slot].buffer, data, size);*/
	obj.aio_buf = data;
	obj.aio_nbytes = size;

	ctx->requests[slot].aiocbp = obj;
	check = aio_write(&ctx->requests[slot].aiocbp);
	ctx->requests[slot].state = EINPROGRESS;

	if (check) {
		log_fatal("Failed to post AIO write");
		exit(1);
	}
	/*log_info("Posted AIO write operation in slot %d", slot);*/
}

// Traverse tthe array to check if all the i/o requests have been completed. We
// check the state of the i/o request and update the state of each request.
// Return 1 if all the requests are completed succesfully
// Return 0, otherwise
int asyncio_all_done(struct asyncio_ctx_s *ctx)
{
	pthread_mutex_lock(&ctx->lock);
	for (int i = 0; i < MAX_REQS; i++) {
		if (ctx->requests[i].state == EINPROGRESS) {
			ctx->requests[i].state = aio_error(&ctx->requests[i].aiocbp);

			switch (ctx->requests[i].state) {
			case 0:
				if (ctx->requests[i].buffer != NULL) {
					free(ctx->requests[i].buffer);
					ctx->requests[i].buffer = NULL;
				}
				break;
			case EINPROGRESS:
				pthread_mutex_unlock(&ctx->lock);
				return 0;
				break;
			case ECANCELED:
				log_fatal("AIO request was cancelled");
				exit(1);
			default:
				log_fatal("AIO error: %s", strerror(ctx->requests[i].state));
				raise(SIGINT);
				exit(1);
			}
		}
	}
	pthread_mutex_unlock(&ctx->lock);

	return 1;
}
