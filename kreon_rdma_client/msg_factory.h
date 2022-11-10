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

extern int32_t calculate_put_msg_size(int32_t key_size, int32_t value_size);

extern int32_t calculate_get_msg_size(int32_t key_size);

extern void create_put_msg(struct put_msg_data data, msg_header *msg_header);

extern struct kv_splice *put_msg_get_kv_offset(msg_header *msg);

extern struct lsn *put_msg_get_lsn_offset(msg_header *msg);

extern int32_t put_msg_get_payload_size(msg_header *msg);

extern void create_get_msg(int32_t key_size, char *key, int32_t reply_size, char *msg_payload_offt);

extern int32_t get_msg_get_fetch_value(msg_header *msg);

extern int32_t get_msg_get_bytes_to_read(msg_header *msg);

extern int32_t get_msg_get_offset(msg_header *msg);

extern int32_t get_msg_get_key_size(msg_header *msg);

extern char *get_msg_get_key_offset(msg_header *msg);

extern void put_msg_print_msg(msg_header *msg);
#endif //MSG_FACTORY_H
