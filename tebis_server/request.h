#ifndef REQUEST_H
#define REQUEST_H
#include "messages.h"
#include "work_task.h"

typedef enum work_task_status (*process)(const struct request *request, struct work_task *task);
typedef void (*destroy)(struct request *request);
struct request {
	enum message_type type;
	const struct regs_server_desc *region_server;
	const msg_header *net_request;
	process execute;
	destroy destruct;
};

#endif
