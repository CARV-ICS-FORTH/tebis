#include "conf.h"
#include "messages.h"
#include "request.h"
#include "work_task.h"
#include <assert.h>
#include <log.h>
#include <stdlib.h>
#include <unistd.h>
struct multi_get {
	struct request req;
	struct msg_multi_get_req *net_req;
};

static void mget_fill_reply_header(msg_header *reply_msg, struct work_task *task, uint32_t payload_size,
				   uint16_t msg_type)
{
	uint32_t reply_size = sizeof(struct msg_header) + payload_size + TU_TAIL_SIZE;
	uint32_t padding = MESSAGE_SEGMENT_SIZE - (reply_size % MESSAGE_SEGMENT_SIZE);

	reply_msg->padding_and_tail_size = 0;
	reply_msg->payload_length = payload_size;
	if (reply_msg->payload_length != 0)
		reply_msg->padding_and_tail_size = padding + TU_TAIL_SIZE;

	reply_msg->offset_reply_in_recv_buffer = UINT32_MAX;
	reply_msg->reply_length_in_recv_buffer = UINT32_MAX;
	reply_msg->offset_in_send_and_target_recv_buffers = task->msg->offset_reply_in_recv_buffer;
	reply_msg->triggering_msg_offset_in_send_buffer = task->msg->triggering_msg_offset_in_send_buffer;
	reply_msg->session_id = task->msg->session_id;
	reply_msg->msg_type = msg_type;
	reply_msg->op_status = 0;
	reply_msg->receive = TU_RDMA_REGULAR_MSG;
}

static void mget_set_receive_field(struct msg_header *msg, uint8_t value)
{
	msg->receive = value;

	if (!msg->payload_length)
		return;

	struct msg_header *last_msg_header =
		(struct msg_header *)((char *)msg + msg->payload_length + msg->padding_and_tail_size);
	last_msg_header->receive = value;
}

static enum work_task_status mget_process(const struct regs_server_desc *region_server_desc, struct work_task *task)
{
	(void)region_server_desc;
	(void)task;
	log_info("Do nothing hestika!");

	task->reply_msg = (void *)((uint64_t)task->conn->rdma_memory_regions->local_memory_buffer +
				   (uint64_t)task->msg->offset_reply_in_recv_buffer);
	mget_fill_reply_header(task->reply_msg, task, 64, MULTI_GET_REPLY);
	struct msg_multi_get_rep *reply =
		(struct msg_multi_get_rep *)&(((char *)task->reply_msg)[sizeof(struct msg_header)]);

	reply->num_entries = 1;
	reply->curr_entry = 0;
	reply->end_of_region = 0;
	reply->buffer_overflow = 0;
	reply->pos = 0;
	reply->pos = 0;
	reply->capacity = 0;
	mget_set_receive_field(task->reply_msg, TU_RDMA_REGULAR_MSG);
	return TASK_COMPLETE;
}

static void multi_get_destructor(struct request *request)
{
	if (request->type != MULTI_GET_REQUEST) {
		log_fatal("Not a multi get request");
		_exit(EXIT_FAILURE);
	}
	struct multi_get *m_get = (struct multi_get *)request;
	free(m_get);
}

struct request *multi_get_constructor(msg_header *msg)
{
	struct multi_get *m_get = calloc(1UL, sizeof(*m_get));
	if (msg->msg_type != MULTI_GET_REQUEST) {
		log_debug("This is not a multi get request");
		return NULL;
	}
	m_get->req.type = msg->msg_type;
	m_get->req.execute = mget_process;
	m_get->req.destruct = multi_get_destructor;

	m_get->net_req = (struct msg_multi_get_req *)&(((char *)msg)[sizeof(*msg)]);
	return &m_get->req;
}
