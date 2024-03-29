#ifndef MSG_FACTORY_H
#define MSG_FACTORY_H
#include "../tebis_server/messages.h"
#include <stdint.h>

struct msg_data_put_request {
	int32_t key_size;
	char *key;
	int32_t value_size;
	char *value;
};

struct msg_data_get_request {
	int32_t offset;
	int32_t bytes_to_read;
	int32_t fetch_value;
	int32_t key_size;
	char *key;
};

struct msg_data_get_reply {
	int32_t bytes_remaining;
	int32_t value_size;
	int8_t key_found;
	int8_t offset_too_large;
	char *value;
};

/** returns the msg size of a put msg
  * @param key_size: the size of the key to be placed in the put msg
  * @param value_size: the size of the value to be placed in the put msg
*/
extern int32_t calculate_put_request_msg_size(int32_t key_size, int32_t value_size);

/** returns the msg size of a get request msg
  * @param key_size: the size of the key to be placed in the put msg
*/
extern int32_t calculate_get_request_msg_size(int32_t key_size);

/** returns the msg size of a get reply msg
  * @param buf_size: the size of the buffer where the value will be stored
*/
extern int32_t calculate_get_reply_msg_size(int32_t buf_size);

/** fills a put msg based on the data parameter
  * @param data: a put_msg_data struct containing necessary informations for the put msg
  * @param msg_header: a ptr pointing to the msg_header of the message to be filled
*/
extern void create_put_request_msg(struct msg_data_put_request data, msg_header *msg_header);

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

/** fills a get msg based on the parameters provided
  * @param key_size: the size of the key given
  * @param key: the key to be retrieved
  * @param reply_size: the size of the reply in the circular buffer
  * @param msg_payload_offt: the payload of the msg to be filled
*/
extern void create_get_request_msg(int32_t key_size, char *key, int32_t bytes_to_read, char *msg_payload_offt);

/** fills a get_req_msg_data struct with information from the internals of a get request msg
 * @param msg: pointer to the msg_header of a get request msg
 */
extern struct msg_data_get_request get_request_get_msg_data(msg_header *msg);

extern void create_get_reply_msg(struct msg_data_get_reply data, msg_header *msg);

/** files a msg_data_get_reply struct with informations contained in the msg payload
 * @param msg: pointer to the msg_header of a get reply msg
 */
extern struct msg_data_get_reply get_reply_get_msg_data(msg_header *msg);

int32_t get_reply_get_payload_size(msg_header *msg);
// TODO: (@geostyl) remove this function
/** For debugging purposes
  * @param msg_header: the header of the msg
*/
extern void put_msg_print_msg(msg_header *msg);

extern char *get_msg_get_key_slice_t(msg_header *msg);

extern char *get_reply_get_kv_offset(msg_header *msg);
#endif //MSG_FACTORY_H
