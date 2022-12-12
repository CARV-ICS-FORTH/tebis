#ifndef COMMON_H_
#define COMMON_H_
#include "../tebis_rdma/rdma.h"
#include "../tebis_server/messages.h"
#include <rdma/rdma_verbs.h>

int teb_spin_for_message_reply(struct msg_header *req, struct connection_rdma *conn);

int teb_send_heartbeat(struct rdma_cm_id *rdma_cm_id);

#endif // COMMON_H_
