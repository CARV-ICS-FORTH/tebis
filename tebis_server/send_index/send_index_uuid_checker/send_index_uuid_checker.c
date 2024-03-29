// Copyright [2023] [FORTH-ICS]
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
#include "send_index_uuid_checker.h"
#include "../tebis_server/messages.h"
#include "../tebis_server/server_communication.h"
#include <assert.h>
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

struct uuid_tuple {
	uint64_t request_uuid;
	uint64_t reply_uuid;
};

static struct uuid_tuple send_index_uuid_checker_get_uuids(struct sc_msg_pair *msg_pair,
							   enum message_type request_msg_type)
{
	struct uuid_tuple uuids_pair;
	switch (request_msg_type) {
	case REPLICA_INDEX_GET_BUFFER_REQ:;
		struct s2s_msg_replica_index_get_buffer_req *get_buffer_request =
			(struct s2s_msg_replica_index_get_buffer_req *)((uint64_t)msg_pair->request +
									sizeof(struct msg_header));
		struct s2s_msg_replica_index_get_buffer_rep *get_buffer_reply =
			(struct s2s_msg_replica_index_get_buffer_rep *)((uint64_t)msg_pair->reply +
									sizeof(struct msg_header));
		uuids_pair.request_uuid = get_buffer_request->uuid;
		uuids_pair.reply_uuid = get_buffer_reply->uuid;
		break;
	case CLOSE_COMPACTION_REQUEST:;
		struct s2s_msg_close_compaction_request *close_compaction_request =
			(struct s2s_msg_close_compaction_request *)((uint64_t)msg_pair->request +
								    sizeof(struct msg_header));
		struct s2s_msg_close_compaction_reply *close_compaction_reply =
			(struct s2s_msg_close_compaction_reply *)((uint64_t)msg_pair->reply +
								  sizeof(struct msg_header));
		uuids_pair.request_uuid = close_compaction_request->uuid;
		uuids_pair.reply_uuid = close_compaction_reply->uuid;
		break;
	case REPLICA_INDEX_SWAP_LEVELS_REQUEST:;
		struct s2s_msg_swap_levels_request *swap_levels_request =
			(struct s2s_msg_swap_levels_request *)((uint64_t)msg_pair->request + sizeof(struct msg_header));
		struct s2s_msg_swap_levels_reply *swap_levels_reply =
			(struct s2s_msg_swap_levels_reply *)((uint64_t)msg_pair->reply + sizeof(struct msg_header));
		uuids_pair.request_uuid = swap_levels_request->uuid;
		uuids_pair.reply_uuid = swap_levels_reply->uuid;
		break;
	case FLUSH_COMMAND_REQ:;
		struct s2s_msg_flush_cmd_req *flush_command_req =
			(struct s2s_msg_flush_cmd_req *)((char *)msg_pair->request + sizeof(struct msg_header));
		struct s2s_msg_flush_cmd_rep *flush_command_rep =
			(struct s2s_msg_flush_cmd_rep *)((char *)msg_pair->reply + sizeof(struct msg_header));
		uuids_pair.request_uuid = flush_command_req->uuid;
		uuids_pair.reply_uuid = flush_command_rep->uuid;
		break;
	case FLUSH_L0_REQUEST:;
		struct s2s_msg_flush_L0_req *flush_req_L0_payload =
			(struct s2s_msg_flush_L0_req *)((char *)msg_pair->request + sizeof(struct msg_header));
		struct s2s_msg_flush_L0_rep *flush_rep_L0_payload =
			(struct s2s_msg_flush_L0_rep *)((char *)msg_pair->reply + sizeof(struct msg_header));
		uuids_pair.request_uuid = flush_req_L0_payload->uuid;
		uuids_pair.reply_uuid = flush_rep_L0_payload->uuid;
		break;
	case REPLICA_FLUSH_MEDIUM_LOG_REQUEST:;
		struct s2s_msg_replica_flush_medium_log_req *flush_medium_log_req =
			(struct s2s_msg_replica_flush_medium_log_req *)((char *)msg_pair->request +
									sizeof(struct msg_header));
		struct s2s_msg_replica_flush_medium_log_rep *flush_medium_log_rep =
			(struct s2s_msg_replica_flush_medium_log_rep *)((char *)msg_pair->reply +
									sizeof(struct msg_header));
		uuids_pair.request_uuid = flush_medium_log_req->uuid;
		uuids_pair.reply_uuid = flush_medium_log_rep->uuid;
		break;
	case BUILD_INDEX_COMPACT_L0_REQUEST:;
		struct s2s_msg_compact_L0_request *compact_request =
			(struct s2s_msg_compact_L0_request *)((char *)msg_pair->request + sizeof(struct msg_header));
		struct s2s_msg_compact_L0_reply *compact_reply =
			(struct s2s_msg_compact_L0_reply *)((char *)msg_pair->reply + sizeof(struct msg_header));
		uuids_pair.request_uuid = compact_request->uuid;
		uuids_pair.reply_uuid = compact_reply->uuid;
		break;
	default:
		log_fatal("uuid Checker of send index does not support request msg %u", request_msg_type);
		assert(0);
		_exit(EXIT_FAILURE);
	}
	return uuids_pair;
}

void send_index_uuid_checker_validate_uuid(struct sc_msg_pair *msg_pair, enum message_type request_msg_type)
{
	assert(msg_pair);
	struct uuid_tuple uuid_pairs = send_index_uuid_checker_get_uuids(msg_pair, request_msg_type);
	if (uuid_pairs.request_uuid != uuid_pairs.reply_uuid) {
		log_fatal("Mismatch in piggybacked uuid");
		assert(0);
		_exit(EXIT_FAILURE);
	}
}
