#ifndef REQUEST_FACTORY_H
#define REQUEST_FACTORY_H
#include "messages.h"
#include "region_server.h"
#include "request.h"
typedef struct request_factory *request_factory_t;
typedef struct request *(*req_construct)(const struct regs_server_desc *, msg_header *);

request_factory_t factory_get_instance(void);
bool factory_register(request_factory_t factory, enum message_type, req_construct constructor);
struct request *factory_create_req(request_factory_t factory, const struct regs_server_desc *region_server,
				   msg_header *msg);

#endif
