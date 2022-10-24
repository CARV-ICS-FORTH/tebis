#include "command.h"
#include "../metadata.h"
#include <log.h>

struct MC_command {
	char region_id[KRM_MAX_REGION_ID_SIZE];
	uint64_t cmd_id;
	enum server_role role;
	enum MC_command_code code;
};

MC_command_t MC_create_command(enum MC_command_code code, char *region_id, enum server_role role, uint64_t cmd_id)
{
	if (strlen(region_id) >= KRM_MAX_REGION_ID_SIZE) {
		log_warn("Region id exceeds max buffer size");
		return NULL;
	}
	MC_command_t cmd = calloc(1UL, sizeof(*cmd));
	strncpy(cmd->region_id, region_id, KRM_MAX_REGION_ID_SIZE);
	cmd->code = code;
	cmd->role = role;
	cmd->cmd_id = cmd_id;
	return cmd;
}

void MC_destroy_command(MC_command_t command)
{
	memset(command, 0xFE, sizeof(*command));
	free(command);
}

int MC_get_command_size(void)
{
	return sizeof(struct MC_command);
}

void MC_print_command(MC_command_t command)
{
	log_info("Command: region_id: %s, cmd_id: %lu role: %d, code %d", command->region_id, command->cmd_id,
		 command->role, command->code);
}

MC_command_code_t MC_get_command_code(MC_command_t command)
{
	return command->code;
}
char *MC_get_region_id(MC_command_t command)
{
	return command->region_id;
}

uint64_t MC_get_command_id(MC_command_t command)
{
	return command->cmd_id;
}

enum server_role MC_get_role(MC_command_t command)
{
	return command->role;
}
