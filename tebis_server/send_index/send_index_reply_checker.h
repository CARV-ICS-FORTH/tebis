#ifndef SEND_INDEX_SEGMENT_MSG_REPLY_CHECKER_H_
#define SEND_INDEX_SEGMENT_MSG_REPLY_CHECKER_H_
#include "../../tebis_rdma/rdma.h"

typedef struct send_index_reply_checker *send_index_reply_checker_t;

send_index_reply_checker_t send_index_reply_checker_init(void);
void send_index_reply_checker_add_msg(send_index_reply_checker_t reply_checker, msg_header *msg, uint32_t replica_id);
msg_header *send_index_reply_checker_get_msg(send_index_reply_checker_t reply_checker, uint32_t replica_id);
void send_index_reply_checker_spin_for_reply(send_index_reply_checker_t reply_checker, struct connection_rdma *conn,
					     uint32_t replica_id);
uint8_t send_index_reply_checker_got_msg(send_index_reply_checker_t reply_checker);

#endif // SEND_INDEX_SEGMENT_MSG_REPLY_CHECKER_H_
