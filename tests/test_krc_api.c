#include "../kreon_rdma_client/client_utils.h"
#include "../kreon_rdma_client/kreon_rdma_client.h"
#include <alloca.h>
#include <assert.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../kreon_server/globals.h"

#define PREFIX_TEST_KEYS 300
#define PREFIX_1 "@0lala" // userakiaslala"
#define PREFIX_2 "@0lalb" // userakiaslalb"
#define PREFIX_3 "@0lalc" // userakiaslalc"
#define infinity "+oo"
#define NUM_KEYS 100000
#define BASE 1000000
#define NUM_REGIONS 16
#define KEY_PREFIX "#b"
#define ZERO_VALUE_PREFIX "b/dummy"
#define TOMBSTONE "b/dummz"
#define KV_SIZE 1024
#define UPDATES 100
#define SCAN_SIZE 50
#define PREFETCH_ENTRIES 16
#define PREFETCH_MEM_SIZE (32 * 1024)
char ZOOKEEPER[256];
char HOST[256]; //"tie3.cluster.ics.forth.gr-8080"

typedef struct key {
	uint32_t key_size;
	char key_buf[];
} key;

typedef struct value {
	uint32_t value_size;
	char value_buf[];
} value;

int main(int argc, char *argv[])
{
	size_t s_key_size = 0;
	char *s_key = NULL;
	char *get_buffer = NULL;
	uint32_t get_size;
	size_t s_value_size = 0;
	char *s_value = NULL;
	uint32_t error_code;

	if (argc != 2) {
		log_fatal("Usage: %s zookeeper_host:zookeeper_port", argv[0]);
		exit(EXIT_FAILURE);
	}

	if (krc_init(argv[1]) != KRC_SUCCESS) {
		log_fatal("Failed to init library");
		exit(EXIT_FAILURE);
	}

	uint64_t i = 0;
	uint64_t j = 0;
	log_debug("Checking region health....");
	char test_region_min_key[32] = { 0 };
	struct cu_region_desc *c_desc = cu_get_region(test_region_min_key, 32);
	krc_exists(c_desc->region.min_key_size, c_desc->region.min_key);

	while (1) {
		if (c_desc->region.max_key_size == 3 && memcmp(c_desc->region.max_key, "+oo", 3) == 0) {
			log_debug("Last region reached");
			break;
		}
		c_desc = cu_get_region(c_desc->region.max_key, c_desc->region.max_key_size);
		log_debug("Probing region with min key %s", c_desc->region.max_key);
		krc_exists(c_desc->region.min_key_size, c_desc->region.min_key);
	}
	log_debug("Regions healthy!");

	log_debug("Testing scan in empty db");
	krc_scannerp sc = krc_scan_init(16, 64 * 1024);
	while (krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
		log_fatal("Test failed scanner returned something on empty db! %s", s_key);
		exit(EXIT_FAILURE);
	}
	krc_scan_close(sc);
	log_debug("Scan in empty db scenario success!");

	log_debug("Testing scan in a single key value db");

	key *k = (key *)malloc(KV_SIZE);
	memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
	sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)1234556);
	k->key_size = strlen(k->key_buf);
	value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
	v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
	memset(v->value_buf, 0xDD, v->value_size);
	log_debug("inserting key %s", k->key_buf);
	krc_put(k->key_size, k->key_buf, v->value_size, v->value_buf);
	log_debug("Testing single value scan");
	sc = krc_scan_init(16, 64 * 1024);
	int entries = 0;
	while (krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size))
		++entries;
	if (entries != 1) {
		log_debug("Scan in single key value db scenario failed expected 1 got %d!", entries);
		exit(EXIT_FAILURE);
	}
	log_debug("Single value scan success!");
	krc_scan_close(sc);
#if 0
  log_debug("Deleting key");
	if (krc_delete(k->key_size, k->key_buf) != KRC_SUCCESS) {
		log_fatal("key %s not found failed to clean state from scan in single key "
			  "value db scenario!",
			  k->key_buf);
		exit(EXIT_FAILURE);
	}
#endif
	// exit(EXIT_SUCCESS);
	log_debug("Starting population for %lu keys...", NUM_KEYS);
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		if (i % 100000 == 0)
			log_debug("inserted up to %llu th key", i);

		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
		v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
		memset(v->value_buf, 0xDD, v->value_size);
		krc_put(k->key_size, k->key_buf, v->value_size, v->value_buf);
	}
	log_debug("Population ended, testing gets");

	get_buffer = NULL;
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		if (i % 100000 == 0)
			log_debug("looked up to %llu th key", i);

		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		error_code = krc_get(k->key_size, k->key_buf, &get_buffer, &get_size, 0);

		if (error_code != KRC_SUCCESS) {
			log_fatal("key %s not found test failed!");
			exit(EXIT_FAILURE);
		}
		if (get_size != KV_SIZE - ((2 * sizeof(key)) + k->key_size)) {
			log_fatal("Corrupted value size got %u expected %u", get_size,
				  KV_SIZE - ((2 * sizeof(key)) + k->key_size));
			exit(EXIT_FAILURE);
		}
		free(get_buffer);
		get_buffer = NULL;
	}
#if 0
	log_debug("Gets successful ended, testing small put/get with offset....");
	uint64_t offset = 0;
	uint32_t sum = 0;
	uint32_t sum_g = 0;
	memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
	sprintf(k->key_buf + strlen(KEY_PREFIX), "%d", 40000000);
	k->key_size = strlen(k->key_buf);
	v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
	v->value_size = sizeof(uint32_t);

	krc_put_with_offset(k->key_size, k->key_buf, 0, sizeof(uint32_t) * UPDATES, v->value_buf);
	for (i = 0; i < UPDATES; i++) {
		*(uint32_t *)(v->value_buf) = i;
		krc_put_with_offset(k->key_size, k->key_buf, offset, v->value_size, v->value_buf);
		sum += i;
		offset += sizeof(uint32_t);
	}
	/*perform get with offset to verify it is correct*/

	offset = 0;
	get_buffer = (char *)malloc(sizeof(uint32_t));

	for (i = 0; i < UPDATES; i++) {
		get_size = 4;
		error_code = krc_get(k->key_size, k->key_buf, &get_buffer, &get_size, offset);
		if (error_code != KRC_SUCCESS) {
			log_fatal("key not found");
			exit(EXIT_FAILURE);
		}
		if (get_size != sizeof(uint32_t)) {
			log_fatal("Corrupted value got %u expected %u", get_size, sizeof(uint32_t));
			exit(EXIT_FAILURE);
		}
		log_debug("Num got is %u", *(uint32_t *)get_buffer);
		sum_g += (*(uint32_t *)get_buffer);
		offset += sizeof(uint32_t);
	}
	free(get_buffer);

	if (sum_g != sum) {
		log_fatal("Sums differ expected %u sum got %u", sum, sum_g);
		exit(EXIT_FAILURE);
	}

	log_debug("Put/get with offset successful expected %u got %u", sum, sum_g);
#endif
	log_debug("testing small scans....");

	for (i = BASE; i < (BASE + (NUM_KEYS - SCAN_SIZE)); i++) {
		if (i % 10000 == 0)
			log_debug("<Scan no %llu>", i);

		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);

		sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);
		krc_scan_set_start(sc, k->key_size, k->key_buf, KRC_GREATER_OR_EQUAL);
		if (!krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
			log_fatal("Test failed key %s invalid scanner (it shoulddn't!)", k->key_buf);
			exit(EXIT_FAILURE);
		}

		if (k->key_size != s_key_size || memcmp(k->key_buf, s_key, k->key_size) != 0) {
			log_fatal("Test failed key %s not found scanner instead returned %d:%s", k->key_buf, s_key_size,
				  s_key);
			assert(0);
			exit(EXIT_FAILURE);
		}

		for (j = 1; j <= SCAN_SIZE; j++) {
			/*construct the key we expect*/
			memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
			sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i + j);
			k->key_size = strlen(k->key_buf);

			if (!krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
				log_fatal("Test failed key %s not found scanner reason scan "
					  "invalid!(it shouldn't)",
					  k->key_buf);
				exit(EXIT_FAILURE);
			}
			if (k->key_size != s_key_size || memcmp(k->key_buf, s_key, k->key_size) != 0) {
				log_fatal("Test failed key %s not found scanner instead returned %d:%s "
					  "j was %d",
					  k->key_buf, s_key_size, s_key, j);
				exit(EXIT_FAILURE);
			}

			if (i % 10000 == 0)
				log_debug("</Scan no %llu>", i);
		}
		krc_scan_close(sc);
	}
	log_info("small scan test Successfull");

	log_debug("Running a full scan");

	sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);
	char minus_inf[7] = { '\0' };
	krc_scan_set_start(sc, 7, minus_inf, KRC_GREATER_OR_EQUAL);
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);

		if (!krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
			log_fatal("Test failed key %s invalid scanner (it shoulddn't!)", s_key);
			exit(EXIT_FAILURE);
		}
		if (k->key_size != s_key_size || memcmp(k->key_buf, s_key, k->key_size) != 0) {
			log_fatal("Test failed key %s not found scanner instead returned %d:%s", k->key_buf, s_key_size,
				  s_key);
			exit(EXIT_FAILURE);
		}
	}
	krc_scan_close(sc);
	log_info("full scan test Successfull");

	log_debug("Running a full scan fetching only keys");

	sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);
	krc_scan_set_start(sc, 7, minus_inf, KRC_GREATER_OR_EQUAL);
	krc_scan_fetch_keys_only(sc);
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);

		if (!krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
			log_fatal("Test failed key %s invalid scanner (it shoulddn't!)", s_key);
			exit(EXIT_FAILURE);
		}
		if (k->key_size != s_key_size || memcmp(k->key_buf, s_key, k->key_size) != 0) {
			log_fatal("Test failed key %s not found scanner instead returned %d:%s", k->key_buf, s_key_size,
				  s_key);
			exit(EXIT_FAILURE);
		}
		// log_debug("zero value key %s", k->key_buf);
	}
	krc_scan_close(sc);
	log_info("full scan - only keys test Successfull");

	int tuples = 0;
	log_debug("Testing stop key");
	sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);
	krc_scan_set_start(sc, 7, minus_inf, KRC_GREATER_OR_EQUAL);
	sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)BASE + (NUM_KEYS / 2));
	krc_scan_set_stop(sc, strlen(k->key_buf), k->key_buf, KRC_GREATER);
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		if (!krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size))
			break;

		if (k->key_size != s_key_size || memcmp(k->key_buf, s_key, k->key_size) != 0) {
			log_fatal("Test failed key %s not found scanner instead returned %d:%s", k->key_buf, s_key_size,
				  s_key);
			exit(EXIT_FAILURE);
		}
		++tuples;
	}
	if (tuples != NUM_KEYS / 2) {
		log_fatal("Error tuples got %d expected %d", tuples, NUM_KEYS / 2);
		exit(EXIT_FAILURE);
	}

	krc_scan_close(sc);
	log_debug("Stop key test successful");
	log_debug("Testing prefix key");
	tuples = 0;
	sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);

	krc_scan_set_start(sc, 7, minus_inf, KRC_GREATER_OR_EQUAL);
	sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)BASE + 111);
	krc_scan_set_prefix_filter(sc, strlen(k->key_buf), k->key_buf);
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		if (!krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size))
			break;
		++tuples;
	}
	if (tuples != 1) {
		log_fatal("Error tuples got %d expected %d", tuples, 1);
		exit(EXIT_FAILURE);
	}
	log_debug("Prefix key test successful");
	krc_scan_close(sc);
#if 0
	log_debug("Deleting half keys");
	for (i = BASE; i < BASE + (NUM_KEYS / 2); i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		if (i % 100000 == 0)
			log_debug("deleted up to %llu th key", i);

		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		if (krc_delete(k->key_size, k->key_buf) != KRC_SUCCESS) {
			log_fatal("key %s not found failed to delete test failed!");
			exit(EXIT_FAILURE);
		}
	}
	log_debug("Verifying delete outcome");
	tuples = 0;
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		if (i % 100000 == 0)
			log_debug("looked up to %llu th key", i);

		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		if (krc_exists(k->key_size, k->key_buf))
			++tuples;
	}
	if (tuples != NUM_KEYS / 2) {
		log_fatal("Test failed found %u keys should find %u", tuples, NUM_KEYS / 2);
		exit(EXIT_FAILURE);
	}
	log_debug("Delete test success! :-)");
#endif
	log_debug("Testing prefix match scans");
	log_debug("loading keys %u keys with prefix %s", PREFIX_TEST_KEYS, PREFIX_1);
	memcpy(k->key_buf, PREFIX_1, strlen(PREFIX_1));
	for (i = 0; i < PREFIX_TEST_KEYS; i++) {
		sprintf(k->key_buf + strlen(PREFIX_1), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
		v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
		memset(v->value_buf, 0xDD, v->value_size);
		krc_put(k->key_size, k->key_buf, v->value_size, v->value_buf);
	}
	log_debug("Done loading keys %u keys with prefix %s", PREFIX_TEST_KEYS, PREFIX_1);
	log_debug("loading keys %u keys with prefix %s", PREFIX_TEST_KEYS, PREFIX_2);
	memcpy(k->key_buf, PREFIX_2, strlen(PREFIX_2));
	for (i = 0; i < PREFIX_TEST_KEYS; i++) {
		sprintf(k->key_buf + strlen(PREFIX_2), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
		v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
		memset(v->value_buf, 0xDD, v->value_size);
		krc_put(k->key_size, k->key_buf, v->value_size, v->value_buf);
	}
	log_debug("Done loading keys %u keys with prefix %s", PREFIX_TEST_KEYS, PREFIX_2);
	log_debug("loading keys %u keys with prefix %s", PREFIX_TEST_KEYS, PREFIX_3);
	memcpy(k->key_buf, PREFIX_3, strlen(PREFIX_3));
	for (i = 0; i < PREFIX_TEST_KEYS; i++) {
		sprintf(k->key_buf + strlen(PREFIX_3), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
		v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
		memset(v->value_buf, 0xDD, v->value_size);
		krc_put(k->key_size, k->key_buf, v->value_size, v->value_buf);
	}
	log_debug("Done loading keys %u keys with prefix %s", PREFIX_TEST_KEYS, PREFIX_3);
	char *prefix_start;
	char *prefix_stop;

	for (j = 0; j < 3; j++) {
		log_debug("Testing j = %d", j);
		switch (j) {
		case 0:
			prefix_start = PREFIX_1;
			prefix_stop = PREFIX_2;
			break;
		case 1:
			prefix_start = PREFIX_2;
			prefix_stop = PREFIX_3;
			break;
		case 2:
			prefix_start = PREFIX_3;
			prefix_stop = infinity;
			break;
		}
		sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);
		strcpy(k->key_buf, prefix_start);
		k->key_size = strlen(k->key_buf);
		krc_scan_set_start(sc, k->key_size, k->key_buf, KRC_GREATER_OR_EQUAL);
		strcpy(k->key_buf, prefix_stop);
		k->key_size = strlen(k->key_buf);
		krc_scan_set_stop(sc, k->key_size, k->key_buf, KRC_GREATER_OR_EQUAL);
		tuples = 0;
		while (krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
			log_debug("Got key %s", s_key);
			++tuples;
		}
		if (tuples != PREFIX_TEST_KEYS) {
			log_fatal("Test failed got %u expected %u", tuples, PREFIX_TEST_KEYS);
			exit(EXIT_FAILURE);
		}
		log_debug("Ok for start %s stop %s", prefix_start, prefix_stop);
	}
	log_info("prefix test scans successfull");
	krc_scan_close(sc);
#if 0
	log_debug("Testing reading large objects");
	log_debug("inserting value of 1MB");
	free(k);
	k = (key *)malloc(2 * 1024 * 1024);
	strcpy(k->key_buf, "large_key_1MB");
	k->key_size = strlen(k->key_buf);
	v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
	v->value_size = 1024 * 1024;
	uint32_t bytes_written = 0;
	i = 0;
	while (bytes_written < v->value_size) {
		memcpy(v->value_buf + bytes_written, &i, sizeof(uint64_t));
		bytes_written += sizeof(uint64_t);
		++i;
	}

	krc_put(k->key_size, k->key_buf, v->value_size, v->value_buf);
	log_debug("Uploaded to kreon value of 1 MB now read it");
	get_buffer = NULL;
	error_code = krc_get(k->key_size, k->key_buf, &get_buffer, &get_size, 0);
	if (error_code != KRC_SUCCESS) {
		log_fatal("Could not retrieve large key");
		exit(EXIT_FAILURE);
	}
	if (get_size != (1024 * 1024)) {
		log_fatal("corrupted size got %u expected %u", get_size, (1024 * 1024));
		exit(EXIT_FAILURE);
	}
	i = 0;
	bytes_written = 0;
	while (bytes_written < get_size) {
		if (i != *(uint64_t *)(get_buffer + bytes_written)) {
			log_fatal("Corrupted value got %lu expected %lu", *(uint64_t *)(get_buffer + bytes_written), i);
			exit(EXIT_FAILURE);
		}
		++i;
		bytes_written += sizeof(uint64_t);
	}

	free(get_buffer);

	log_debug("Testing zero sided value keys populating");
	for (i = BASE; i < (BASE + NUM_KEYS); i++) {
		memcpy(k->key_buf, ZERO_VALUE_PREFIX, strlen(ZERO_VALUE_PREFIX));
		if (i % 100000 == 0)
			log_debug("inserted up to %llu th key", i);

		sprintf(k->key_buf + strlen(ZERO_VALUE_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
		v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
		memset(v->value_buf, 0xDD, v->value_size);
		krc_put(k->key_size, k->key_buf, 0, NULL);
	}
	k->key_size = strlen(TOMBSTONE);
	sprintf(k->key_buf, "%s", TOMBSTONE);
	krc_put(k->key_size, k->key_buf, 0, NULL);
	sprintf(k->key_buf, "%s", ZERO_VALUE_PREFIX);

	log_debug("Scanning now zero sided value keys");
	krc_scannerp zero_sc = krc_scan_init(16, 64 * 1024);
	krc_scan_set_prefix_filter(zero_sc, strlen(ZERO_VALUE_PREFIX), ZERO_VALUE_PREFIX);
	i = BASE;
	int zeroed_value_keys = 0;
	while (krc_scan_get_next(zero_sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
		sprintf(k->key_buf + strlen(ZERO_VALUE_PREFIX), "%llu", (long long unsigned)i);
		if (s_key_size != strlen(k->key_buf)) {
			log_fatal("Corrupted len expected %u got %u", strlen(k->key_buf), s_key_size);
			exit(EXIT_FAILURE);
		}
		if (memcmp(s_key, k->key_buf, s_key_size) != 0) {
			log_fatal("Corrupted key got %s expected %s", s_key, k->key_buf);
			exit(EXIT_FAILURE);
		}
		assert(s_value_size == 0);
		++zeroed_value_keys;
		++i;
	}
	if (zeroed_value_keys != NUM_KEYS) {
		log_fatal("Failed did not retrieve all keys got %u expected %u", zeroed_value_keys, NUM_KEYS);
		exit(EXIT_FAILURE);
	}
	log_debug("Zeroed value keys success");
	for (i = BASE; i < BASE + 20; i++) {
		log_debug("Testing scan after delete scenario");
		sprintf(k->key_buf + strlen(ZERO_VALUE_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf);
		assert(krc_delete(k->key_size, k->key_buf) == KRC_SUCCESS);
		sc = krc_scan_init(PREFETCH_ENTRIES, PREFETCH_MEM_SIZE);
		krc_scan_set_prefix_filter(sc, k->key_size, k->key_buf);
		if (krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
			log_fatal("Test failed key %s invalid scanner should have returned zero "
				  "instead got key %s",
				  s_key);
			exit(EXIT_FAILURE);
		}
		krc_scan_close(sc);
	}
#endif
	krc_close();
	log_info("************ ALL TESTS SUCCESSFULL! ************");
	return 1;
}
