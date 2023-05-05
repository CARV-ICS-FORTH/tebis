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
