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
#include "zk_utils.h"
#include <assert.h>
#include <log.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <zookeeper/zookeeper.h>
char *zk_error_code[] = { "ZOK", "ZNONODE", "UNKNOWN_CODE", "ZBADARGUMENTS", "ZNODEEXISTS" };
char *zku_concat_strings(int num, ...)
{
	const char *tmp_string;

	va_list arguments;
	va_start(arguments, num);

	int total_length = 1;
	int x;
	for (x = 0; x < num; x++) {
		tmp_string = va_arg(arguments, const char *);
		if (tmp_string != NULL) {
			//LOG_DEBUG(("Counting path with this path %s (%d)", tmp_string, num));
			total_length += strlen(tmp_string);
		}
	}
	va_end(arguments);
	char *path = (char *)calloc(1, total_length);
	va_start(arguments, num);
	int idx = 0;
	for (x = 0; x < num; x++) {
		tmp_string = va_arg(arguments, const char *);
		if (tmp_string != NULL) {
			memcpy(&path[idx], tmp_string, strlen(tmp_string));
			//strcat(path, tmp_string);
			idx += strlen(tmp_string);
			if (idx >= total_length) {
				log_fatal("idx = %d total_length %d", idx, total_length);
				assert(0);
				exit(EXIT_FAILURE);
			}
		}
	}
	va_end(arguments);
	return path;
}

char *zku_op2String(int rc)
{
	switch (rc) {
	case ZOK:
		return zk_error_code[0];
	case ZNONODE:
		return zk_error_code[1];
	case ZBADARGUMENTS:
		return zk_error_code[3];
	case ZNODEEXISTS:
		return zk_error_code[4];
	default:
		log_warn("code is %d", rc);
		return zk_error_code[2];
	}
}

int zku_key_cmp(int key_size_1, char *key_1, int key_size_2, char *key_2)
{
	bool key_1_is_infinity = false;
	bool key_2_is_infinity = false;

	if (key_size_1 == 3 && memcmp(key_1, "+oo", 3) == 0)
		key_1_is_infinity = true;

	if (key_size_2 == 3 && memcmp(key_2, "+oo", 3) == 0)
		key_2_is_infinity = true;

	if (key_1_is_infinity && !key_2_is_infinity)
		return 1;

	if (!key_1_is_infinity && key_2_is_infinity)
		return -1;

	int ret = memcmp(key_1, key_2, key_size_1 <= key_size_2 ? key_size_1 : key_size_2);

	if (0 == ret)
		return key_size_1 - key_size_2;

	return ret > 0 ? 1 : -1;
}
