#include "send_index_reply_checker.h"
#include "../../common/common.h"
#include "../metadata.h"
#include <stdlib.h>

struct send_index_reply_checker {
	msg_header *msg[KRM_MAX_BACKUPS];
	uint8_t is_init;
	uint8_t got_msg;
};

send_index_reply_checker_t send_index_reply_checker_init(void)
{
	struct send_index_reply_checker *new_reply_checker =
		(struct send_index_reply_checker *)calloc(1, sizeof(struct send_index_reply_checker));
	new_reply_checker->is_init = 1;
	return new_reply_checker;
}

void send_index_reply_checker_add_msg(send_index_reply_checker_t reply_checker, msg_header *msg, uint32_t replica_id)
{
	struct send_index_reply_checker *checker = (struct send_index_reply_checker *)reply_checker;
	assert(checker->is_init);
	checker->msg[replica_id] = msg;
	checker->got_msg = 1;
}

msg_header *send_index_reply_checker_get_msg(send_index_reply_checker_t reply_checker, uint32_t replica_id)
{
	struct send_index_reply_checker *checker = (struct send_index_reply_checker *)reply_checker;
	assert(checker->is_init && checker->got_msg);
	return checker->msg[replica_id];
}

void send_index_reply_checker_spin_for_reply(send_index_reply_checker_t reply_checker, struct connection_rdma *conn,
					     uint32_t replica_id)
{
	struct send_index_reply_checker *checker = (struct send_index_reply_checker *)reply_checker;
	assert(checker->is_init);
	teb_spin_for_message_reply(checker->msg[replica_id], conn);
}

uint8_t send_index_reply_checker_got_msg(send_index_reply_checker_t reply_checker)
{
	return reply_checker->got_msg;
}
