#ifndef ZK_UTILS_H_
#define ZK_UTILS_H_

#include <stdint.h>
char *zku_concat_strings(int num, ...);
char *zku_op2String(int rc);
int zku_key_cmp(int key_size_1, char *key_1, int key_size_2, char *key_2);
#endif
