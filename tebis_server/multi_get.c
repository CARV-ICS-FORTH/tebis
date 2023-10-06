#include "conf.h"
#include "messages.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include "region_desc.h"
#include "region_server.h"
#include "request.h"
#include "work_task.h"
#include <assert.h>
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#define MGET_GET_REPLY_MSG(X)                                                        \
	(msg_header *)((uint64_t)X->conn->rdma_memory_regions->local_memory_buffer + \
		       (uint64_t)X->msg->offset_reply_in_recv_buffer)

struct multi_get {
	struct request req;
	const struct msg_multi_get_req *mget_msg;
};

static enum work_task_status mget_process(const struct request *request, struct work_task *task)
{
	// static uint64_t served_requests;
	if (request->type != MULTI_GET_REQUEST) {
		log_fatal("Wrong request type Cannot handle!");
		_exit(EXIT_FAILURE);
	}
	struct multi_get *mget = (struct multi_get *)request;

	//Find the region object
	struct region_desc *region = regs_get_region_desc(mget->req.region_server, (char *)mget->mget_msg->seek_key,
							  mget->mget_msg->seek_key_size);

	if (region == NULL) {
		log_fatal("Region not found for key %s", mget->mget_msg->seek_key);
		_exit(EXIT_FAILURE);
	}

	task->reply_msg = MGET_GET_REPLY_MSG(task);
	struct msg_multi_get_rep *mget_reply =
		(struct msg_multi_get_rep *)&((char *)task->reply_msg)[sizeof(msg_header)];

	uint32_t max_KVs = mget->mget_msg->max_num_entries;
	uint32_t num_KVs = 0;
	uint32_t available_bytes = mget->req.net_request->reply_length_in_recv_buffer -
				   (sizeof(msg_header) + sizeof(*mget_reply) + MESSAGE_SEGMENT_SIZE);
	uint32_t response_size = 0;
	char *KV_buffer = &((char *)mget_reply)[sizeof(*mget_reply)];

	//Parallax start
	if (!region_desc_enter_parallax(region, task)) {
		// later...
		return TASK_START;
	}

	const char *error = NULL;
	struct par_key seek_key = { .size = mget->mget_msg->seek_key_size, .data = mget->mget_msg->seek_key };
	par_scanner scanner = par_init_scanner(region_desc_get_db(region), &seek_key, PAR_GREATER_OR_EQUAL, &error);
	// log_info("Searching for up to max_KVS: %u", max_KVs);
	while (par_is_valid(scanner) && num_KVs < max_KVs) {
		struct par_key key = par_get_key(scanner);
		struct par_value value = par_get_value(scanner);
		uint32_t size = key.size + value.val_size + (2 * sizeof(uint32_t));
		if (size > available_bytes - response_size)
			break;
		memcpy(&KV_buffer[response_size], &key.size, sizeof(key.size));
		response_size += sizeof(key.size);
		memcpy(&KV_buffer[response_size], key.data, key.size);
		response_size += key.size;
		memcpy(&KV_buffer[response_size], &value.val_size, sizeof(value.val_size));
		response_size += sizeof(value.val_size);
		memcpy(&KV_buffer[response_size], value.val_buffer, value.val_size);
		response_size += value.val_size;
		++num_KVs;
		// log_info("Added response_size = %u available_bytes: %u", response_size, available_bytes);

		par_get_next(scanner);
	}
	par_close_scanner(scanner);
	region_desc_leave_parallax(region);

	//Parallax end
	// log_info("Retrieved %u KV pairs of %u requested total response size: %u", num_KVs, max_KVs, response_size);
	msg_fill_reply_header(task->reply_msg, (struct msg_header *)mget->req.net_request,
			      sizeof(msg_multi_get_rep) + response_size, MULTI_GET_REPLY);

	mget_reply->num_entries = num_KVs;
	mget_reply->curr_entry = 0;
	mget_reply->end_of_region = 0;
	msg_set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	// log_info("Served %lu multi gets", ++served_requests);

	return TASK_COMPLETE;
}

static void mget_destructor(struct request *request)
{
	if (request->type != MULTI_GET_REQUEST) {
		log_fatal("Not a multi get request");
		_exit(EXIT_FAILURE);
	}
	struct multi_get *m_get = (struct multi_get *)request;
	free(m_get);
}

struct request *mget_constructor(const struct regs_server_desc *region_server, msg_header *msg)
{
	if (msg->msg_type != MULTI_GET_REQUEST) {
		log_debug("This is not a multi get request");
		return NULL;
	}

	struct multi_get *mget = calloc(1UL, sizeof(*mget));

	mget->req.region_server = region_server;
	mget->req.net_request = msg;
	mget->req.type = msg->msg_type;
	mget->req.execute = mget_process;
	mget->req.destruct = mget_destructor;

	mget->mget_msg = (struct msg_multi_get_req *)&(((char *)msg)[sizeof(*msg)]);
	assert(mget->mget_msg);

	return (struct request *)mget;
}
