// Copyright [2019] [FORTH-ICS]
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
#ifndef TEBIS_RDMA_CLIENT_H
#define TEBIS_RDMA_CLIENT_H

#include <stddef.h>
#include <stdint.h>
#define KRC_GET_OFFT_DEFAULT_SIZE 2048

typedef void *krc_scannerp;
typedef enum krc_ret_code {
	KRC_SUCCESS = 0,
	KRC_FAILURE,
	KRC_ZK_FAILURE_CONNECT,
	KRC_PUT_FAILURE,
	KRC_KEY_NOT_FOUND,
	KRC_KEY_EXISTS,
	KRC_OFFSET_TOO_LARGE
} krc_ret_code;

typedef enum krc_scan_state {
	KRC_UNITIALIZED = 2,
	KRC_FETCH_NEXT_BATCH,
	KRC_ISSUE_MGET_REQ,
	KRC_ADVANCE,
	KRC_STOP_FILTER,
	KRC_PREFIX_FILTER,
	KRC_END_OF_DB,
	KRC_BUFFER_OVERFLOW,
	KRC_INVALID
} krc_scan_state;

typedef enum krc_seek_mode { KRC_GREATER_OR_EQUAL = 1, KRC_GREATER } krc_seek_mode;
typedef struct krc_key {
	uint32_t key_size;
	char key_buf[];
} krc_key;

typedef struct krc_value {
	uint32_t val_size;
	char val_buf[];
} krc_value;

krc_ret_code krc_init(char *zookeeper_host);
krc_ret_code krc_close(void);

krc_ret_code krc_put(uint32_t key_size, void *key, uint32_t val_size, void *value);
krc_ret_code krc_put_if_exists(uint32_t key_size, void *key, uint32_t val_size, void *value);
//krc_ret_code krc_put_with_offset(uint32_t key_size, void *key, uint32_t offset, uint32_t val_size, void *value);
//krc_value *krc_get(uint32_t key_size, void *key, uint32_t reply_length, uint32_t *error_code);
//krc_value *krc_get_with_offset(uint32_t key_size, void *key, uint32_t offset, uint32_t size, uint32_t *error_code);
krc_ret_code krc_get(uint32_t key_size, char *key, char **buffer, uint32_t *size, uint32_t offset);
uint8_t krc_exists(uint32_t key_size, void *key);
krc_ret_code krc_delete(uint32_t key_size, void *key);

/*scanner API*/
krc_scannerp krc_scan_init(uint32_t prefetch_num_entries, uint32_t prefetch_mem_size_hint);
void krc_scan_set_start(krc_scannerp sp, uint32_t start_key_size, void *start_key, krc_seek_mode seek_mode);
void krc_scan_set_stop(krc_scannerp sp, uint32_t stop_key_size, void *stop_key, krc_seek_mode seek_mode);
void krc_scan_set_prefix_filter(krc_scannerp sp, uint32_t prefix_size, void *prefix);
void krc_scan_fetch_keys_only(krc_scannerp sp);
uint8_t krc_scan_get_next(krc_scannerp sp, char **key, size_t *keySize, char **value, size_t *valueSize);
uint8_t krc_scan_is_valid(krc_scannerp sp);
void krc_scan_close(krc_scannerp sp);

/*asynchronous staff*/
uint8_t krc_start_async_thread(void);
typedef void (*callback)(void *);
krc_ret_code krc_aput(uint32_t key_size, void *key, uint32_t val_size, void *value, callback t, void *context);
krc_ret_code krc_aput_if_exists(uint32_t key_size, void *key, uint32_t val_size, void *value, callback t,
				void *context);
krc_ret_code krc_aget(uint32_t key_size, char *key, uint32_t *buf_size, char *buf, callback t, void *context);
krc_ret_code krc_amget(uint32_t key_size, const char *key, uint32_t *buf_size, char *buf, callback t, void *context,
		       uint32_t max_entries);
#endif
