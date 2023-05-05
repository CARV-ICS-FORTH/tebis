#ifndef COMMAND_H
#define COMMAND_H
#include "../metadata.h"
#include "mregion.h"
#include <stddef.h>
#include <stdint.h>
typedef enum MC_command_code {
	OPEN_REGION_START = 1,
	OPEN_REGION_ACK,
	OPEN_REGION_COMMIT,
	CLOSE_REGION_START,
	CLOSE_REGION_ACK,
	CLOSE_REGION_COMMIT,
	UPGRADE_TO_PRIMARY_START,
	UPGRADE_TO_PRIMARY_ACK,
	UPGRADE_TO_PRIMARY_COMMIT,
	RECONFIGURE_GROUP
} MC_command_code_t;
typedef struct MC_command *MC_command_t;
extern MC_command_t MC_create_command(enum MC_command_code code, mregion_t mregion, enum server_role role,
				      uint64_t cmd_id);
extern void MC_destroy_command(MC_command_t command);
extern MC_command_t MC_deserialize_command(char *buffer, size_t size);
extern int MC_get_command_size(MC_command_t command);
extern void MC_print_command(MC_command_t command);
extern MC_command_code_t MC_get_command_code(MC_command_t command);
extern char *MC_get_region_id(MC_command_t command);
extern uint64_t MC_get_command_id(MC_command_t command);
extern enum server_role MC_get_role(MC_command_t command);
extern char *MC_get_buffer(MC_command_t command);
extern uint32_t MC_get_buffer_size(MC_command_t command);
#endif
