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
#ifndef COMMON_H
#define COMMON_H
struct connection_rdma;
struct msg_header;
struct rdma_cm_id;

int teb_spin_for_message_reply(struct msg_header *req, struct connection_rdma *conn);

int teb_send_heartbeat(struct rdma_cm_id *rdma_cm_id);

#endif // COMMON_H_
