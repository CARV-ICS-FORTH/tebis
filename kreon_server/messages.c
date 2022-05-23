
#include "messages.h"
#include <assert.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int msg_push_to_multiget_buf(msg_key *key, msg_value *val, msg_multi_get_rep *buf)
{
	uint32_t total_size = key->size + val->size + sizeof(msg_key) + sizeof(msg_value);
	if (buf->remaining < total_size) {
		return KREON_FAILURE;
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
	return KREON_SUCCESS;
}
