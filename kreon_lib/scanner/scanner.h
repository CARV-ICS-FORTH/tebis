#pragma once
#include "stack.h"
#include "../allocator/allocator.h"
#include "../btree/btree.h"

#define MAX_FREE_SPILL_BUFFER_SCANNER_SIZE 128
#define MAX_PREFETCH_SIZE 511

#define STOP_ROW_REACHED 1
#define END_OF_DATABASE 2
#define ROW_CHANGED 3
#define KREON_BUFFER_OVERFLOW 0x0F

#define MAX_LONG 9223372036854775807L

enum sh_heap_status { EMPTY_MIN_HEAP = 4, GOT_MIN_HEAP = 5, HEAP_SIZE = 32 };

struct sc_full_kv {
	uint64_t kv_log_offt;
	struct bt_kv_log_address L;
	uint8_t level_id;
	uint8_t deleted;
};

struct sh_heap_node {
	union {
		struct sc_full_kv key_value;
		struct kv_prefix kv_prefix;
	};
	struct db_descriptor *db_desc;
	uint8_t level_id;
	uint8_t active_tree;
	uint8_t duplicate;
	enum KV_type type;
};

struct sh_min_heap {
	struct sh_heap_node elem[HEAP_SIZE];
	int size;
	// int active_tree;
};

typedef enum SEEK_SCANNER_MODE { GREATER = 5, GREATER_OR_EQUAL = 6, FETCH_FIRST } SEEK_SCANNER_MODE;
enum scanner_type { FULL_SCANNER = 1, SPILL_BUFFER_SCANNER, LEVEL_SCANNER };

typedef struct level_scanner {
	union {
		struct kv_prefix kv_prefix;
		struct sc_full_kv key_value;
	};
	db_handle *db;
	stackT stack;
	node_header *root; /*root of the tree when the cursor was initialized/reset, related to CPAAS-188*/
	uint32_t level_id;
	int32_t type;
	uint8_t valid : 1;
	uint8_t dirty : 1;

} level_scanner;

typedef struct scannerHandle {
	level_scanner LEVEL_SCANNERS[MAX_LEVELS][NUM_TREES_PER_LEVEL];
	struct sh_min_heap heap;
	struct sc_full_kv key_value;
	db_handle *db;
	int32_t type; /*to be removed also*/
} scannerHandle;

struct Kreoniterator {
	scannerHandle *sc;
};

/*
 * Standalone version
 *
 * Example use to print all the database in sorted order:
 *
 * scannerHandle *scanner = initScanner(db, NULL);
 * while(isValid(scanner)){
 * 		std::cout << "[" << entries
 *							<< "][" << getKeySize(scanner)
 *							<< "][" << (char *)getKeyPtr(scanner)
 *							<< "][" << getValueSize(scanner)
 *							<< "][" << (char *)getValuePtr(scanner)
 *							<< "]"
 *							<< std::endl;
 *		getNextKV(scanner);
 * }
 * closeScanner(scanner);
 */
void init_dirty_scanner(scannerHandle *sc, db_handle *handle, void *start_key, char seek_flag);
scannerHandle *initScanner(scannerHandle *sc, db_handle *handle, void *key, char seek_mode);
void closeScanner(scannerHandle *sc);

int32_t getNext(scannerHandle *sc);

int isValid(scannerHandle *sc);
int32_t getKeySize(scannerHandle *sc);
uint64_t getKeyOfft(scannerHandle *sc);

int32_t getValueSize(scannerHandle *sc);
uint64_t getValueOfft(scannerHandle *sc);

void *get_kv_pointer(scannerHandle *sc);
void done_with_kv_pointer(scannerHandle *sc);

level_scanner *_init_spill_buffer_scanner(db_handle *handle, node_header *node, void *start_key);
int32_t _get_next_KV(level_scanner *sc);
void _close_spill_buffer_scanner(level_scanner *sc, node_header *root);

int32_t _get_next_KV(level_scanner *sc);
int32_t _get_prev_KV(level_scanner *sc);

void seek_to_first(db_handle *, struct Kreoniterator *);

void seek_to_last(db_handle *, struct Kreoniterator *);

int get_next(struct Kreoniterator *it);

int get_prev(struct Kreoniterator *it);

int Seek(db_handle *hd, void *Keyname, struct Kreoniterator *it);

//scanner min heap staff
void sh_init_heap(struct sh_min_heap *heap, int active_tree);
void sh_insert_heap_node(struct sh_min_heap *hp, struct sh_heap_node *nd);
enum sh_heap_status sh_remove_min(struct sh_min_heap *hp, struct sh_heap_node *heap_node);
