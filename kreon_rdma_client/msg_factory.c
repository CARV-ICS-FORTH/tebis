#include "msg_factory.h"
#include "btree/lsn.h"
#include <btree/key_splice.h>
#include <btree/kv_pairs.h>
#include <stdint.h>

struct get_msg_data {
	int32_t offset;
	int32_t bytes_to_read;
	int32_t fetch_value;
	int32_t key_size;
	char key[];
} __attribute__((packed));

inline int32_t calculate_put_msg_size(int32_t key_size, int32_t value_size)
{
	int32_t kv_size = get_kv_metadata_size() + key_size + value_size + get_tail_size();
	int32_t lsn_size = get_lsn_size();
	return lsn_size + kv_size;
}

inline int32_t calculate_get_msg_size(int32_t key_size)
{
	int32_t get_msg_size = sizeof(struct get_msg_data) + key_size;
	return get_msg_size;
}

void create_put_msg(struct put_msg_data data, char *msg)
{
	struct lsn *lsn = (struct lsn *)msg;
	set_lsn_id(lsn, UINT64_MAX);

	struct kv_splice *kv = (struct kv_splice *)(msg + get_lsn_size());
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

void create_get_msg(int32_t key_size, char *key, int32_t reply_size, char *msg_payload_offt)
{
	struct get_msg_data *get_msg = (struct get_msg_data *)msg_payload_offt;
	get_msg->offset = 0;
	get_msg->bytes_to_read = reply_size;
	get_msg->fetch_value = 1;
	get_msg->key_size = key_size;
	memcpy(get_msg->key, key, key_size);
}

char *get_msg_get_key_offset(msg_header *msg)
{
	struct get_msg_data *msg_payload = (struct get_msg_data *)((char *)msg + sizeof(msg_header));
	return msg_payload->key;
}

int32_t get_msg_get_key_size(msg_header *msg)
{
	struct get_msg_data *msg_payload = (struct get_msg_data *)((char *)msg + sizeof(msg_header));
	return msg_payload->key_size;
}

int32_t get_msg_get_offset(msg_header *msg)
{
	struct get_msg_data *msg_payload = (struct get_msg_data *)((char *)msg + sizeof(msg_header));
	return msg_payload->offset;
}

int32_t get_msg_get_bytes_to_read(msg_header *msg)
{
	struct get_msg_data *msg_payload = (struct get_msg_data *)((char *)msg + sizeof(msg_header));
	return msg_payload->bytes_to_read;
}

int32_t get_msg_get_fetch_value(msg_header *msg)
{
	struct get_msg_data *msg_payload = (struct get_msg_data *)((char *)msg + sizeof(msg_header));
	return msg_payload->fetch_value;
}
