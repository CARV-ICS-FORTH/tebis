// Copyright [2021] [FORTH-ICS]
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
#include "messages.h"
#include "../tebis_rdma/rdma.h"
#include "conf.h"
#include <assert.h>
#include <string.h>

int msg_push_to_multiget_buf(msg_key *key, msg_value *val, msg_multi_get_rep *buf)
{
	uint32_t total_size = key->size + val->size + sizeof(msg_key) + sizeof(msg_value);
	if (buf->remaining < total_size) {
		return TEBIS_FAILURE;
	}
	memcpy(buf->kv_buffer + buf->pos, key, sizeof(msg_key) + key->size);
	//log_info("key %s", buf->kv_buffer + buf->pos+4);
	buf->pos += (sizeof(msg_key) + key->size);
	assert(buf->pos < buf->capacity);
	buf->remaining -= (sizeof(msg_key) + key->size);
	memcpy(buf->kv_buffer + buf->pos, val, sizeof(msg_value) + val->size);
	buf->pos += (sizeof(msg_key) + val->size);
	assert(buf->pos < buf->capacity);
	buf->remaining -= (sizeof(msg_value) + val->size);
	++buf->num_entries;
	//log_info("entries %u",buf->num_entries);
	//log_info("added key %u:%s and val size %u",key->size, key->key,val->size);
	return TEBIS_SUCCESS;
}

void msg_fill_reply_header(msg_header *reply_msg, msg_header *request_msg, uint32_t payload_size, uint16_t msg_type)
{
	uint32_t reply_size = sizeof(struct msg_header) + payload_size + TU_TAIL_SIZE;
	uint32_t padding = MESSAGE_SEGMENT_SIZE - (reply_size % MESSAGE_SEGMENT_SIZE);

	reply_msg->padding_and_tail_size = payload_size > 0 ? padding + TU_TAIL_SIZE : 0;

	reply_msg->payload_length = payload_size;
	reply_msg->offset_reply_in_recv_buffer = UINT32_MAX;
	reply_msg->reply_length_in_recv_buffer = UINT32_MAX;
	reply_msg->offset_in_send_and_target_recv_buffers = request_msg->offset_reply_in_recv_buffer;
	reply_msg->triggering_msg_offset_in_send_buffer = request_msg->triggering_msg_offset_in_send_buffer;
	reply_msg->session_id = request_msg->session_id;
	reply_msg->msg_type = msg_type;
	reply_msg->op_status = 0;
	reply_msg->receive = TU_RDMA_REGULAR_MSG;
}

void msg_set_receive_field(msg_header *msg, uint8_t value)
{
	msg->receive = value;

	if (!msg->payload_length)
		return;

	struct msg_header *last_msg_header =
		(struct msg_header *)((char *)msg + msg->payload_length + msg->padding_and_tail_size);
	last_msg_header->receive = value;
}

inline uint32_t msg_calc_mget_req_payload(int32_t key_size)
{
	return sizeof(struct msg_multi_get_req) + key_size;
}

inline uint32_t msg_calc_mget_reply_payload(uint32_t buf_size)
{
	return sizeof(struct msg_multi_get_rep) + buf_size;
}
