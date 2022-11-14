#ifndef MSG_FACTORY_H
#define MSG_FACTORY_H
#include "../tebis_server/messages.h"
#include <stdint.h>

struct put_msg_data {
	int32_t key_size;
	char *key;
	int32_t value_size;
	char *value;
};

/** returns the msg size of a put msg
  * @param key_size: the size of the key to be placed in the put msg
  * @param value_size: the size of the value to be placed in the put msg
*/
extern int32_t calculate_put_msg_size(int32_t key_size, int32_t value_size);

/** returns the msg size of a get msg
  * @param key_size: the size of the key to be placed in the put msg
*/
extern int32_t calculate_get_msg_size(int32_t key_size);

/** fills a put msg based on the data parameter
  * @param data: a put_msg_data struct containing necessary informations for the put msg
  * @param msg_header: a ptr pointing to the msg_header of the message to be filled
*/
extern void create_put_msg(struct put_msg_data data, msg_header *msg_header);

/** Returns a ptr to the kv payload part of the put_msg
  * @param msg_header: a ptr pointing to the msg_header of the message
*/
extern struct kv_splice *put_msg_get_kv_offset(msg_header *msg);

/** Returns a ptr to the lsn part of the put_msg
  * @param msg_header: a ptr pointing to the msg_header of the message
*/
extern struct lsn *put_msg_get_lsn_offset(msg_header *msg);

/** Returns the payload size of a put msg
  * @param msg_header: a ptr pointing to the msg_header of the message
*/
extern int32_t put_msg_get_payload_size(msg_header *msg);

//TODO (@geostyl) create a get_msg_data accordingly to the put_msg_data
/** fills a get msg based on the parameters provided
  * @param key_size: the size of the key given
  * @param key: the key to be retrieved
  * @param reply_size: the size of the reply in the circular buffer
  * @param msg_payload_offt: the payload of the msg to be filled
*/
extern void create_get_msg(int32_t key_size, char *key, int32_t reply_size, char *msg_payload_offt);

/** Retrieve the fetch_value information in the get_msg
  * @param msg_header: the header of the msg
*/
extern int32_t get_msg_get_fetch_value(msg_header *msg);

/** Retrieve the bytes to read information in the get_msg
  * @param msg_header: the header of the msg
*/
extern int32_t get_msg_get_bytes_to_read(msg_header *msg);

/** Retrieve the offset information in the get_msg
  * @param msg_header: the header of the msg
*/
extern int32_t get_msg_get_offset(msg_header *msg);

/** Retrieve the key size in the get_msg
  * @param msg_header: the header of the msg
*/
extern int32_t get_msg_get_key_size(msg_header *msg);

/** Retrieve the key offset in the get_msg
  * @param msg_header: the header of the msg
*/
extern char *get_msg_get_key_offset(msg_header *msg);

// TODO: (@geostyl) remove this function
/** For debugging purposes
  * @param msg_header: the header of the msg
*/
extern void put_msg_print_msg(msg_header *msg);

extern char *get_msg_get_key_slice_t(msg_header *msg);
#endif //MSG_FACTORY_H
