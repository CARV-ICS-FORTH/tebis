#ifndef __ASYNCIO_H__
#define __ASYNCIO_H__

#include <aio.h>
#include <stdint.h>
typedef struct asyncio_ctx_s *asyncio_ctx;

// Initialize the array of I/O requests for the asynchronous I/O
asyncio_ctx asyncio_create_context(void);

// Add new I/O request in the list
// Arguments:
//	fd	   - File descriptor
//	reqNum - Request Number
//	data   - Data to be transfered to the device
//	size   - Size of the data
//	offset - Write the data to the specific offset in the file
void asyncio_post_write(asyncio_ctx ctx, int fd, char *data, size_t size, uint64_t offset);

// Traverse tthe array to check if all the i/o requests have been completed. We
// check the state of the i/o request and update the state of each request.
// Return 1 if all the requests are completed succesfully
// Return 0, otherwise
int asyncio_all_done(asyncio_ctx ctx);

#endif // __ASYNCIO_H__
