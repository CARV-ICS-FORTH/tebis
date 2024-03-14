#ifndef MULTI_GET_H
#define MULTI_GET_H
#include "messages.h"
#include "region_server.h"
struct request *mget_constructor(const struct regs_server_desc *region_server, msg_header *msg);
#endif
