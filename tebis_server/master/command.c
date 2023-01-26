// Copyright [2023] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "command.h"
#include "../metadata.h"
#include "mregion.h"
#include <log.h>
#include <stdlib.h>
#include <string.h>

struct MC_command {
	uint64_t cmd_id;
	enum server_role role;
	enum MC_command_code code;
	uint32_t buffer_size;
	char buffer[];
};

MC_command_t MC_create_command(enum MC_command_code code, mregion_t mregion, enum server_role role, uint64_t cmd_id)
{
	MC_command_t cmd = calloc(1UL, sizeof(*cmd) + MREG_get_region_size());
	cmd->buffer_size = MREG_get_region_size();
	cmd->code = code;
	cmd->role = role;
	cmd->cmd_id = cmd_id;
	MREG_serialize_region(mregion, cmd->buffer, MREG_get_region_size());
	return cmd;
}

void MC_destroy_command(MC_command_t command)
{
	memset(command, 0xFE, sizeof(*command));
	free(command);
}

int MC_get_command_size(MC_command_t command)
{
	log_debug("Command size is header: %lu payload(mregion): %u", sizeof(struct MC_command), command->buffer_size);
	return sizeof(struct MC_command) + command->buffer_size;
}

void MC_print_command(MC_command_t command)
{
	mregion_t mregion = MREG_deserialize_region(command->buffer, MREG_get_region_size());
	log_debug("Command: region_id: %s, cmd_id: %lu role: %d, code %d", MREG_get_region_id(mregion), command->cmd_id,
		  command->role, command->code);
}

MC_command_code_t MC_get_command_code(MC_command_t command)
{
	return command->code;
}
char *MC_get_region_id(MC_command_t command)
{
	mregion_t mregion = MREG_deserialize_region(command->buffer, MREG_get_region_size());
	return MREG_get_region_id(mregion);
}

uint64_t MC_get_command_id(MC_command_t command)
{
	return command->cmd_id;
}

enum server_role MC_get_role(MC_command_t command)
{
	return command->role;
}

MC_command_t MC_deserialize_command(char *buffer, size_t size)
{
	MC_command_t command = (MC_command_t)buffer;
	if (size < sizeof(struct MC_command) + command->buffer_size) {
		log_warn("Buffer too small");
		return NULL;
	}
	return (MC_command_t)buffer;
}

char *MC_get_buffer(MC_command_t command)
{
	return command->buffer;
}

uint32_t MC_get_buffer_size(MC_command_t command)
{
	return command->buffer_size;
}
