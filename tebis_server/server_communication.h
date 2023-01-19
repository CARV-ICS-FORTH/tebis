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
#ifndef SERVER_COMMUNICATION_H
#define SERVER_COMMUNICATION_H
#include "../utilities/circular_buffer.h"
#include "messages.h"

/*server to server communication related staff*/
struct sc_msg_pair {
	/*out variables*/
	struct msg_header *request;
	struct msg_header *reply;
	struct connection_rdma *conn;
	enum circular_buffer_op_status stat;
};

struct sc_msg_pair sc_allocate_rpc_pair(struct connection_rdma *conn, uint32_t request_size, uint32_t reply_size,
					enum message_type type);
void sc_free_rpc_pair(struct sc_msg_pair *p);
#endif
