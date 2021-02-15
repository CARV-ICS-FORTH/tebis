/*macros staff*/
#pragma once
#define SPINLOCK_INIT(L, attr) pthread_spin_init(L, attr)
#define SPIN_LOCK(L) pthread_spin_lock(L)
#define SPIN_UNLOCK(L) pthread_spin_unlock(L)

/*Important note Condition variables are not defined*/
#define MUTEX_INIT(L, attr) pthread_mutex_init(L, attr)
#define MUTEX_LOCK(L) pthread_mutex_lock(L)
#define MUTEX_TRYLOCK(L) pthread_mutex_trylock(L)
#define MUTEX_UNLOCK(L) pthread_mutex_unlock(L)

#define RWLOCK_INIT(L, attr) pthread_rwlock_init(L, attr)
#define RWLOCK_WRLOCK(L) pthread_rwlock_wrlock(L)
#define RWLOCK_RDLOCK(L) pthread_rwlock_rdlock(L)
#define RWLOCK_UNLOCK(L) pthread_rwlock_unlock(L)

/*DEBUG operation FLAGS*/
#define DEBUG_ALLOCATOR_NO
#define DEBUG_CLEANER_NO
#define DEBUG_SNAPSHOT_NO

#define MAX_DB_NAME_SIZE 64
/*hierarchy of trees parameters*/
#define MAX_LEVELS 8
#define NUM_TREES_PER_LEVEL 4
#define GROUP_SIZE 2
#define NUM_OF_DB_GROUPS 506
#define DEVICE_BLOCK_SIZE 4096

/*for allocator.c*/
#define DEV_NAME_MAX 512 /* Length of the device name */
#define DATA_PER_BITMAP_BLOCK ((DEVICE_BLOCK_SIZE - 8) * 8) /*Data blocks addressed per bitmap block*/
#define FREE_LOG_SIZE 512000 /*size in 4KB blocks of the log used for marking the free ops*/

#define MAX_HANDLES 5090
#define CLEAN_SIZE 500000

#define SEC (1000000L)

#define CLEAN_INTERVAL (10 * SEC)
//#define COMMIT_KV_LOG_INTERVAL (500 * SEC)
#define SNAPSHOT_INTERVAL (1500 * SEC)
#define GC_INTERVAL (50 * SEC)

#define WORD_SIZE 64
#define BREAKPOINT asm volatile("int3;");

#define KB (1024)
#define PG (4 * KB)
#define MB (KB * KB)

#define KEY_BLOCK_SIZE 8192 // 4KB

/*from scan.c*/
#define MAX_SIZE 64
#define COUNTERS_no /*enables counter stats*/

/*various*/
#define LLU long long unsigned

#define SEGMENT_SIZE (2 * 1024 * 1024)
#define GROWTH_FACTOR 4
#define L0_SIZE 512000
#define EXPLICIT_IO 1
#define ENABLE_BLOOM_FILTERS 0
#define COMPACTION_UNIT_OF_WORK 131072
/*Buffering related tunables*/
#define AGGRESIVE_FREE_POLICY
#define TO_SPILL_KEYS 16384

#define GB(x) (x * 1024LU * 1024LU * 1024LU)

#define SEGMENT_MEMORY_THREASHOLD 511 /*Careful, in number of pages -1 used for chaining*/
#define MAX_ALLOCATION_TRIES 2
