#include "zk_utils.h"
#include <assert.h>
#include <log.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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

int64_t zku_key_cmp(int key_size_1, char *key_1, int key_size_2, char *key_2)
{
	int ret;
	char key_1_is_infinity = 0;
	char key_2_is_infinity = 0;

	if (key_size_1 == 3 && memcmp(key_1, "+oo", 3) == 0)
		key_1_is_infinity = 1;

	if (key_size_2 == 3 && memcmp(key_2, "+oo", 3) == 0)
		key_2_is_infinity = 1;

	if (key_1_is_infinity && !key_2_is_infinity)
		return 1;

	if (!key_1_is_infinity && key_2_is_infinity)
		return -1;

	if (key_size_1 <= key_size_2)
		ret = memcmp(key_1, key_2, key_size_1);
	else
		ret = memcmp(key_1, key_2, key_size_2);

	if (ret > 0)
		return 1;
	else if (ret < 0)
		return -1;
	else {
		/*prefix is the same larger wins*/
		return key_size_1 - key_size_2;
	}
}
