#include "msg_factory.h"
#include "btree/lsn.h"
#include "log.h"
#include <btree/key_splice.h>
#include <btree/kv_pairs.h>
#include <stdint.h>
#include <string.h>

struct get_request_data {
	int32_t offset;
	int32_t bytes_to_read;
	int32_t fetch_value;
	int32_t key_size;
	char key[];
} __attribute__((packed));

struct get_reply_data {
	int32_t bytes_remaining;
	int32_t value_size;
	int8_t key_found;
	int8_t offset_too_large;
	char value[];
} __attribute__((packed));

inline int32_t calculate_put_request_msg_size(int32_t key_size, int32_t value_size)
{
	int32_t kv_size = get_kv_metadata_size() + key_size + value_size + get_tail_size();
	int32_t lsn_size = get_lsn_size();
	return lsn_size + kv_size;
}

inline int32_t calculate_get_request_msg_size(int32_t key_size)
{
	int32_t get_request_msg_size = sizeof(struct get_request_data) + key_size;
	return get_request_msg_size;
}

inline int32_t calculate_get_reply_msg_size(int32_t buf_size)
{
	int32_t get_reply_msg_size = sizeof(struct get_reply_data) + buf_size;
	return get_reply_msg_size;
}

void create_put_msg(struct msg_data_put_request data, msg_header *msg_header)
{
	char *msg_payload = (char *)msg_header + sizeof(struct msg_header);
	struct lsn *lsn = (struct lsn *)msg_payload;
	set_lsn_id(lsn, UINT64_MAX);

	struct kv_splice *kv = (struct kv_splice *)(msg_payload + get_lsn_size());
	set_key(kv, data.key, data.key_size);
	set_value(kv, data.value, data.value_size);
	//clients should not care for tail sizes. This approach is chosen to avoid different formats between clients/servers
	set_sizes_tail(kv, UINT8_MAX);
	set_payload_tail(kv, UINT8_MAX);
}

struct kv_splice *put_msg_get_kv_offset(msg_header *msg)
{
	char *msg_payload = (char *)msg + sizeof(msg_header);
	struct kv_splice *kv = (struct kv_splice *)(msg_payload + get_lsn_size());
	return kv;
}

struct lsn *put_msg_get_lsn_offset(msg_header *msg)
{
	char *msg_payload = (char *)msg + sizeof(msg_header);
	struct lsn *lsn = (struct lsn *)(msg_payload);
	return lsn;
}

int32_t put_msg_get_payload_size(msg_header *msg)
{
	char *msg_payload = (char *)msg + sizeof(msg_header);
	struct kv_splice *kv = (struct kv_splice *)(msg_payload + get_lsn_size());
	return get_lsn_size() + get_kv_size(kv);
}

void create_get_request_msg(int32_t key_size, char *key, int32_t bytes_to_read, char *msg_payload_offt)
{
	struct get_request_data *get_msg = (struct get_request_data *)msg_payload_offt;
	get_msg->offset = 0;
	get_msg->bytes_to_read = bytes_to_read;
	get_msg->fetch_value = 1;
	get_msg->key_size = key_size;
	memcpy(get_msg->key, key, key_size);
}

struct msg_data_get_request get_request_get_msg_data(msg_header *msg)
{
	struct get_request_data *msg_payload = (struct get_request_data *)((char *)msg + sizeof(msg_header));
	struct msg_data_get_request req_data = { .bytes_to_read = msg_payload->bytes_to_read,
						 .fetch_value = msg_payload->fetch_value,
						 .key = msg_payload->key,
						 .key_size = msg_payload->key_size,
						 .offset = msg_payload->offset };
	return req_data;
}

void create_get_reply_msg(struct msg_data_get_reply data, msg_header *msg)
{
	struct get_reply_data *msg_payload = (struct get_reply_data *)((char *)msg + sizeof(msg_header));
	msg_payload->bytes_remaining = data.bytes_remaining;
	msg_payload->key_found = data.key_found;
	msg_payload->offset_too_large = data.offset_too_large;
	msg_payload->value_size = data.value_size;
	if (data.value != NULL && data.value_size != 0) {
		memcpy(msg_payload->value, data.value, data.value_size);
	}
}

struct msg_data_get_reply get_reply_get_msg_data(msg_header *msg)
{
	struct get_reply_data *msg_payload = (struct get_reply_data *)((char *)msg + sizeof(msg_header));
	struct msg_data_get_reply reply_data = { .bytes_remaining = msg_payload->bytes_remaining,
						 .key_found = msg_payload->key_found,
						 .offset_too_large = msg_payload->offset_too_large,
						 .value_size = msg_payload->offset_too_large,
						 .value = msg_payload->value };
	return reply_data;
}

int32_t get_reply_get_payload_size(msg_header *msg)
{
	struct msg_data_get_reply reply_data = get_reply_get_msg_data(msg);
	return sizeof(struct get_reply_data) + reply_data.value_size;
}

char *get_msg_get_key_slice_t(msg_header *msg)
{
	struct get_request_data *msg_payload = (struct get_request_data *)((char *)msg + sizeof(msg_header));
	return (char *)&msg_payload->key_size;
}

void put_msg_print_msg(msg_header *msg)
{
	struct lsn *lsn = put_msg_get_lsn_offset(msg);
	struct kv_splice *kv = put_msg_get_kv_offset(msg);
	log_debug("< lsn %ld - key %s key_size %u>", lsn->id, get_key_offset_in_kv(kv), get_key_size(kv));
}
