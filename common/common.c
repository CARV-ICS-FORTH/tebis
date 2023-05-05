// Copyright [2019] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "common.h"
#include "../tebis_rdma/rdma.h"
#include "../tebis_server/conf.h"
#include "../tebis_server/messages.h"
#include "../utilities/circular_buffer.h"
#include <assert.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <log.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdint.h>
#include <string.h>
// IWYU pragma: no_forward_declare rdma_cm_id

extern void on_completion_client(struct rdma_message_context *);
int teb_send_heartbeat(struct rdma_cm_id *rdma_cm_id)
{
	struct rdma_message_context send_ctx;
	//log_info("Sending heartbeat!");
	client_rdma_init_message_context(&send_ctx, NULL);
	send_ctx.on_completion_callback = on_completion_client;
	if (rdma_post_write(rdma_cm_id, &send_ctx, NULL, 0, NULL, IBV_SEND_SIGNALED, 0, 0)) {
		log_warn("Failed to send heartbeat: %s", strerror(errno));
		return TEBIS_FAILURE;
	}

	if (!client_rdma_send_message_success(&send_ctx)) {
		log_warn("Remote side is down!");
		return TEBIS_FAILURE;
	}
	return TEBIS_SUCCESS;
}

/* Spin until an incoming message has been detected. For our RDMA network protocol implementation, this is
 * detected by reading the value TU_RDMA_REGULAR_MSG in the last byte of the buffer where we expect the
 * message.
 *
 * return TEBIS_SUCCESS if a message is successfully received
 * return TEBIS_FAILURE if the remote side is down
 */
static int rdma_write_spin_wait_for_msg_tail(struct rdma_cm_id *rdma_cm_id, volatile struct msg_header *msg)
{
	const unsigned timeout = 1700000; // 10 sec
	int ret = TEBIS_SUCCESS;

	while (1) {
		for (uint32_t i = 0; get_receive_field(msg) != TU_RDMA_REGULAR_MSG && i < timeout; ++i)
			;

		if (get_receive_field(msg) == TU_RDMA_REGULAR_MSG)
			break;

		if (teb_send_heartbeat(rdma_cm_id) == TEBIS_FAILURE)
			return TEBIS_FAILURE;
	}
	return ret;
}

static int rdma_write_spin_wait_for_header_tail(struct rdma_cm_id *rdma_cm_id, volatile struct msg_header *msg)
{
	const unsigned timeout = 1700000; // 10 sec
	int ret = TEBIS_SUCCESS;

	while (1) {
		for (uint32_t i = 0; msg->receive != TU_RDMA_REGULAR_MSG && i < timeout; ++i)
			;

		if (msg->receive == TU_RDMA_REGULAR_MSG)
			break;

		if (teb_send_heartbeat(rdma_cm_id) == TEBIS_FAILURE)
			return TEBIS_FAILURE;
	}
	return ret;
}

int teb_spin_for_message_reply(struct msg_header *req, struct connection_rdma *conn)
{
	volatile struct msg_header *rep_header =
		(struct msg_header *)&conn->recv_circular_buf->memory_region[req->offset_reply_in_recv_buffer];
	//Spin until header arrives
	if (rdma_write_spin_wait_for_header_tail(conn->rdma_cm_id, rep_header) == TEBIS_FAILURE)
		return TEBIS_FAILURE;

	assert(rep_header->receive == TU_RDMA_REGULAR_MSG);
	if (!rep_header->payload_length)
		return TEBIS_SUCCESS;

	//Spin until payload arrives
	if (rdma_write_spin_wait_for_msg_tail(conn->rdma_cm_id, rep_header) == TEBIS_FAILURE)
		return TEBIS_FAILURE;

	return TEBIS_SUCCESS;
}
