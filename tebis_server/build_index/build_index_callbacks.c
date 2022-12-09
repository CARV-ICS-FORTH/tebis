#include "build_index_callbacks.h"
#include <log.h>

void build_index_segment_is_full_callback(void *context, uint64_t seg_offt, enum log_category log_type)
{
	(void)context;
	(void)seg_offt;
	(void)log_type;
	log_debug("build index segment is full callback called");
}
