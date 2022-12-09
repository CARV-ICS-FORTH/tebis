#ifndef BUILD_INDEX_CALLBACKS_H
#define BUILD_INDEX_CALLBACKS_H
#include <parallax/parallax.h>
#include <stdint.h>

void build_index_segment_is_full_callback(void *context, uint64_t seg_offt, enum log_category log_type);

#endif // BUILD_INDEX_CALLBACKS_H
