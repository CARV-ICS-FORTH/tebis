#include "../tebis_rdma_client/client_utils.h"
#include "../tebis_rdma_client/tebis_rdma_client.h"
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#define BASE 100000UL
#define KEY_PREFIX "@kakakis"
#define NUM_KEYS 65536UL
#define KEY_SIZE 128UL
#define VALUE_SIZE 128UL

static void put_callback(void *context)
{
	static uint64_t num_keys = 0;
	(void)context;
	++num_keys;
	log_info("Put inside done keys: %lu", ++num_keys);
	free(context);
}

static void mget_callback(void *context)
{
	static uint64_t num_mgets;
	(void)context;
	log_info("Multi get inside done: %lu", ++num_mgets);
	free(context);
}

int main(int argc, char *argv[])
{
	if (argc != 2) {
		log_fatal("Usage: %s zookeeper_host:zookeeper_port", argv[0]);
		_exit(EXIT_FAILURE);
	}

	if (krc_init(argv[1]) != KRC_SUCCESS) {
		log_fatal("Failed to init library");
		_exit(EXIT_FAILURE);
	}

	log_info("Checking region health....");
	char test_region_min_key[32] = { 0 };
	struct cu_region_desc *c_desc = cu_get_region(test_region_min_key, 32);
	krc_exists(c_desc->region.min_key_size, c_desc->region.min_key);

	while (1) {
		if (c_desc->region.max_key_size == 3 && memcmp(c_desc->region.max_key, "+oo", 3) == 0) {
			log_debug("Last region reached");
			break;
		}
		c_desc = cu_get_region(c_desc->region.max_key, c_desc->region.max_key_size);
		log_info("Probing region with min key %s", c_desc->region.max_key);
		krc_exists(c_desc->region.min_key_size, c_desc->region.min_key);
	}
	log_info("Regions healthy!");

	krc_start_async_thread();
	log_info("Starting population for %lu keys...", NUM_KEYS);

	for (uint64_t i = BASE; i < (BASE + NUM_KEYS); i++) {
		char *kv_buf = calloc(1UL, KEY_SIZE + VALUE_SIZE);
		char *key_buf = kv_buf;
		char *value_buf = &kv_buf[KEY_SIZE];
		memcpy(key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		if (i % 100 == 0)
			log_info("inserted up to %lu th key", i);

		if (sprintf(&key_buf[strlen(KEY_PREFIX)], "%lu", i) < 0) {
			log_fatal("sprintf failed");
			_exit(EXIT_FAILURE);
		}
		size_t key_size = strlen(key_buf);
		size_t value_size = VALUE_SIZE;
		memset(value_buf, 0xDD, value_size);
		krc_aput(key_size, key_buf, value_size, value_buf, put_callback, kv_buf);
	}
	log_info("Population ended");
	log_info("Testing multi gets");

	for (uint64_t i = BASE; i < (BASE + NUM_KEYS); i++) {
		char *kv_buf = calloc(1UL, KEY_SIZE + (10 * VALUE_SIZE));
		char *key_buf = kv_buf;
		char *value_buf = &kv_buf[KEY_SIZE];
		memcpy(key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		if (i % 100 == 0)
			log_info("looked up to %lu th key", i);

		if (sprintf(key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i) < 0) {
			log_fatal("Failed sprintf");
			_exit(EXIT_FAILURE);
		}
		size_t key_size = strlen(key_buf);
		uint32_t value_buf_size = 10 * VALUE_SIZE;
		krc_amget(key_size, key_buf, &value_buf_size, value_buf, mget_callback, kv_buf, 10);
	}
	log_info("************ ALL TESTS SUCCESSFULL! ************");
	while (1)
		;
	return 1;
}
