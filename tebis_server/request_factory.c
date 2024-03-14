#include "request_factory.h"
#include "messages.h"
#include <log.h>
#include <stdlib.h>
#include <unistd.h>
#define FACTORY_SIZE MESSAGES_NUMBER
#define FACTORY_CHECK_MSG_TYPE(X)                                                 \
	do {                                                                      \
		if (X > MESSAGES_NUMBER) {                                        \
			log_fatal("Wrong message type not part of the protocol"); \
			_exit(EXIT_FAILURE);                                      \
		}                                                                 \
	} while (0);

struct request_factory {
	req_construct constructors[FACTORY_SIZE];
	bool initialized;
};

request_factory_t factory_get_instance(void)
{
	static struct request_factory factory = {
		.initialized = false,
	};
	return &factory;
}

bool factory_register(request_factory_t factory, enum message_type msg_type, req_construct constructor)
{
	FACTORY_CHECK_MSG_TYPE(msg_type)

	if (factory->constructors[msg_type]) {
		log_debug("Constructor already talken");
		return false;
	}
	factory->constructors[msg_type] = constructor;
	return true;
}

struct request *factory_create_req(request_factory_t factory, const struct regs_server_desc *region_server,
				   msg_header *msg)
{
	FACTORY_CHECK_MSG_TYPE(msg->msg_type)
	return factory->constructors[msg->msg_type] ? factory->constructors[msg->msg_type](region_server, msg) : NULL;
}
