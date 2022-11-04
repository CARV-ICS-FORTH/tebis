#ifndef MSG_FACTORY_H
#define MSG_FACTORY_H
#include "../kreon_server/messages.h"
#include <stdint.h>

struct put_msg_data {
	int32_t key_size;
	char *key;
	int32_t value_size;
	char *value;
};

int32_t calculate_put_msg_size(int32_t key_size, int32_t value_size);

int32_t calculate_get_msg_size(int32_t key_size);

void create_put_msg(struct put_msg_data data, char *msg);

struct kv_splice *put_msg_get_kv_offset(msg_header *msg);

struct lsn *put_msg_get_lsn_offset(msg_header *msg);

int32_t put_msg_get_payload_size(msg_header *msg);

void create_get_msg(int32_t key_size, char *key, int32_t reply_size, char *msg_payload_offt);

int32_t get_msg_get_fetch_value(msg_header *msg);

int32_t get_msg_get_bytes_to_read(msg_header *msg);

int32_t get_msg_get_offset(msg_header *msg);

int32_t get_msg_get_key_size(msg_header *msg);

char *get_msg_get_key_offset(msg_header *msg);
#endif //MSG_FACTORY_H
