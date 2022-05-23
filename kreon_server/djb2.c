#include "djb2.h"

unsigned long djb2_hash(unsigned char const *buf, uint32_t length)
{
	unsigned long hash = 5381;

	for (uint32_t i = 0; i < length; ++i) {
		hash = ((hash << 5) + hash) + buf[i]; /* hash * 33 + c */
	}

	return hash;
}
