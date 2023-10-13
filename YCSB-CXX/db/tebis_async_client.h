#pragma once

#include "../core/ycsbdb.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <time.h>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../core/properties.h"
#include "workload_gen.h"
// FIXME definition of COMPUTE_TAIL(_ASYNC) should be done in cmake
//#define COMPUTE_TAIL_ASYNC
#ifdef COMPUTE_TAIL_ASYNC
#include "../core/Measurements.hpp"
#endif

extern "C" {
#include "../../tebis_rdma_client/client_utils.h"
#include "../../tebis_rdma_client/tebis_rdma_client.h"
#include "../../utilities/queue.h"
#include <btree/btree.h>
#include <log.h>

__thread int kv_count = 0;

typedef void (*op_callback_function)(void *);

#ifdef COMPUTE_TAIL_ASYNC
struct compute_tail_callback_args {
	struct timespec start;
	int id;
	Op opcode;
	void *op_callback_args;
	op_callback_function op_callback;
};

void timespec_diff(struct timespec *start, struct timespec *stop, struct timespec *result)
{
	if ((stop->tv_nsec - start->tv_nsec) < 0) {
		result->tv_sec = stop->tv_sec - start->tv_sec - 1;
		result->tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000;
	} else {
		result->tv_sec = stop->tv_sec - start->tv_sec;
		result->tv_nsec = stop->tv_nsec - start->tv_nsec;
	}

	return;
}

void compute_tail_callback(void *args)
{
	extern Measurements *tail;
	struct compute_tail_callback_args *ar = (struct compute_tail_callback_args *)args;
	struct timespec end, diff;
	clock_gettime(CLOCK_MONOTONIC, &end);
	timespec_diff(&ar->start, &end, &diff);
	tail->addLatency(ar->id, ar->opcode, 1000000 * diff.tv_sec + diff.tv_nsec / 1000);
}
#endif

void put_callback(void *cnxt)
{
	uint64_t *counter = (uint64_t *)cnxt;
	++(*counter);
	//log_info("Done with put callback, counter %llu", *counter);
}

struct get_cnxt {
	uint64_t *counter;
	uint32_t buf_size;
	char *buf;
};

void get_callback(void *cnxt)
{
	struct get_cnxt *g = (struct get_cnxt *)cnxt;
	++(*g->counter);
	//log_info("Done with get callback, counter %llu", *(g->counter));
	free(g->buf);
	free(g);
}

unsigned long djb2_hash(unsigned char *buf, uint32_t length)
{
	unsigned long hash = 5381;

	for (unsigned i = 0; i < length; ++i) {
		hash = ((hash << 5) + hash) + buf[i]; /* hash * 33 + c */
	}

	return hash;
}

static uint64_t reply_counter;
extern struct cu_regions client_regions;
}

#define FIELD_COUNT 10
#define MAX_THREADS 128
using std::cout;
using std::endl;

extern std::unordered_map<std::string, int> ops_per_server;
int pending_requests[MAX_THREADS];
int served_requests[MAX_THREADS];
int num_of_batch_operations_per_thread[MAX_THREADS];
#ifdef COUNT_REQUESTS_PER_REGION
extern int *region_requests;
#endif

namespace ycsbc
{
class tebisAsyncClientDB : public YCSBDB {
    private:
	int field_count;
	std::vector<db_handle *> dbs;
	double tinit, t1, t2;
	struct timeval tim;
	pthread_mutex_t mutex_num;
	std::string custom_workload;
	char **region_prefixes_map;
	uint32_t regions_total;

    public:
	tebisAsyncClientDB(int num, utils::Properties &props)
		: field_count(std::stoi(
			  props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT)))
		, dbs()
	{
		struct timeval start;

		std::string zookeeper = props.GetProperty("zookeeperEndpoint", "localhost:2181");
		if (krc_init((char *)zookeeper.c_str()) != KRC_SUCCESS) {
			log_fatal("Failed to init client at zookeeper host %s", zookeeper.c_str());
			exit(EXIT_FAILURE);
		}
		reply_counter = 0;
		pthread_mutex_init(&mutex_num, NULL);
		gettimeofday(&start, NULL);
		tinit = start.tv_sec + (start.tv_usec / 1000000.0);
		custom_workload = props.GetProperty("workloadType", "undefined");
		if (custom_workload.compare("undefined") == 0) {
			std::cerr << "Error: missing argument -w" << std::endl;
			exit(EXIT_FAILURE);
		}
		std::cout << "Workload Type: " << custom_workload << std::endl;
		// Create a prefixes map that maps each region to a specific prefix
		// By hashing a key to pick a region you can eliminate load balancing issues
		// The prefix is necessary to make sure that the key that will be sent to
		// the server will match the region's range
		regions_total = client_regions.num_regions;
		region_prefixes_map = new char *[regions_total];
#ifdef COUNT_REQUESTS_PER_REGION
		region_requests = new int[regions_total]();
#endif
		char first = 'A';
		char second = 'A';
		for (unsigned i = 0; i < regions_total; ++i) {
			region_prefixes_map[i] = new char[3];
			region_prefixes_map[i][0] = first;
			region_prefixes_map[i][1] = second;
			region_prefixes_map[i][2] = '\0';
			if (first == 'Z') {
				std::cerr << "Error: exceeded maximum supported regions for this benchmark (576)"
					  << std::endl;
				exit(EXIT_FAILURE);
			} else if (second == 'Z') {
				second = 'A';
				++first;
			} else {
				++second;
			}
		}
	}

	virtual ~tebisAsyncClientDB()
	{
		cout << "Calling ~tebisRAsyncClientDB()..." << endl;
		gettimeofday(&tim, NULL);
		t2 = tim.tv_sec + (tim.tv_usec / 1000000.0);
		fprintf(stderr, "ycsb=[%lf]sec\n", (t2 - t1));
		fprintf(stderr, "start=[%lf]sec\n", (t1 - tinit));

		// Client_Flush_Volume( client_regions );
		// Client_Flush_Volume_MultipleServers( client_regions );
	}

    public:
	void Init()
	{
		krc_start_async_thread();
	}

	void Close()
	{
		krc_close();
		log_info("Done Bye Bye!");
		// exit(EXIT_SUCCESS);
	}

	int Read(int id, const std::string &table, const std::string &key, const std::vector<std::string> *fields,
		 std::vector<KVPair> &result)
	{
		if (fields) {
			return __read(id, table, key, fields, result);
		} else {
			std::vector<std::string> __fields;
			for (int i = 0; i < field_count; ++i)
				__fields.push_back("field" + std::to_string(i));
			return __read(id, table, key, &__fields, result);
		}

		return 0;
	}

	int __read(int id, const std::string &table, const std::string &key, const std::vector<std::string> *fields,
		   std::vector<KVPair> &result)
	{
		struct get_cnxt *g = (struct get_cnxt *)malloc(sizeof(struct get_cnxt));
		g->counter = &reply_counter;
		g->buf_size = 1500;
		g->buf = (char *)malloc(g->buf_size);

		int region_id = djb2_hash((unsigned char *)key.c_str(), key.length()) % regions_total;
		std::string prefix_key = std::string(region_prefixes_map[region_id]) + key;
#ifdef COUNT_REQUESTS_PER_REGION
		__sync_fetch_and_add(&region_requests[region_id], 1);
#endif
		op_callback_function callback = get_callback;
		void *callback_args = g;
#ifdef COMPUTE_TAIL_ASYNC
		struct compute_tail_callback_args *args = new struct compute_tail_callback_args;
		args->id = id;
		args->opcode = (enum Op)1; // Op::READ
		args->op_callback_args = (void *)g;
		args->op_callback = get_callback;
		callback = compute_tail_callback;
		callback_args = args;
		clock_gettime(CLOCK_MONOTONIC, &args->start);
#endif
		enum krc_ret_code code = krc_aget(prefix_key.length(), (char *)prefix_key.c_str(), &g->buf_size, g->buf,
						  callback, callback_args);
		if (code != KRC_SUCCESS) {
			log_fatal("problem with key %s", key.c_str());
			exit(EXIT_FAILURE);
		}

#if 0 //VALUE_CHECK
		std::string value(val, len_val);
		std::vector<std::string> tokens;
		boost::split(tokens, value, boost::is_any_of(" "));
		int cnt = 0;
		for (std::map<std::string, std::string>::size_type i = 0; i + 1 < tokens.size(); i += 2) {
			vmap.insert(std::pair<std::string, std::string>(tokens[i], tokens[i + 1]));
			++cnt;
		}
		if (cnt != field_count) {
			std::cout << "ERROR IN VALUE!" << std::endl
				  << " cnt is = " << cnt << "value len " << len_val << "field count is " << field_count
				  << "\n";
			std::cout << "[" << value << "]" << std::endl << "\n";
			// printf("[%s:%s:%d] rest is
			// %s\n",__FILE__,__func__,__LINE__,val+strlen(val)+4);
			exit(EXIT_FAILURE);
		}
		for (auto f : *fields) {
			std::map<std::string, std::string>::iterator it = vmap.find(f);
			if (it == vmap.end()) {
				std::cout << "[2]cannot find : " << f << " in DB " << db_id << std::endl;
				printf("Value %d %s\n", len_val, val);
				fflush(stdout);
				// exit(EXIT_FAILURE);
				break;
			}
			KVPair k = std::make_pair(f, it->second);
			result.push_back(k);
		}
#endif
		return 0;
	}

	int Scan(int id /*ignore*/, const std::string &table /*ignore*/, const std::string &key, int record_count,
		 const std::vector<std::string> *fields /*ignore*/, std::vector<KVPair> &result)
	{
		struct get_cnxt *mget_cnxt = (struct get_cnxt *)malloc(sizeof(struct get_cnxt));
		mget_cnxt->counter = &reply_counter;
		mget_cnxt->buf_size = 30 * 1024; //30 KB
		mget_cnxt->buf = (char *)calloc(1UL, mget_cnxt->buf_size);

		int region_id = djb2_hash((unsigned char *)key.c_str(), key.length()) % regions_total;
		std::string prefix_key = std::string(region_prefixes_map[region_id]) + key;
		op_callback_function callback = get_callback;

		uint32_t *value_buf_size = &mget_cnxt->buf_size;
		enum krc_ret_code code = krc_amget(prefix_key.length(), prefix_key.c_str(), value_buf_size,
						   mget_cnxt->buf, callback, mget_cnxt, record_count);
		if (code != KRC_SUCCESS) {
			log_fatal("problem with key %s", key.c_str());
			_exit(EXIT_FAILURE);
		}
		return 0;
	}

	void CreateKey(const std::string *key, std::string *qual, char *okey)
	{
		*(int32_t *)okey = key->length() + qual->length() + 1;
		memcpy(okey + sizeof(int32_t), key->c_str(), key->length());
		memcpy(okey + sizeof(int32_t) + key->length(), qual->c_str(), qual->length());
		okey[sizeof(int32_t) + key->length() + qual->length()] = '\0';
	}

	int Update(int id, const std::string &table, const std::string &key, std::vector<KVPair> &values)
	{
		static std::string value3(1200, 'a');
		static std::string value2(120, 'a');
		static std::string value(5, 'a');
		int y = kv_count++ % 10;

		const char *value_buf = NULL;
		uint32_t value_size = 0;

		switch (choose_wl(custom_workload, y)) {
		case 0:
			value_size = value.size();
			value_buf = value.c_str();
			break;
		case 1:
			value_size = value2.size();
			value_buf = value2.c_str();
			break;
		case 2:
			value_size = value3.size();
			value_buf = value3.c_str();
			break;
		default:
			assert(0);
			std::cout << "Got Unknown value" << std::endl;
			exit(EXIT_FAILURE);
		}

		int region_id = djb2_hash((unsigned char *)key.c_str(), key.length()) % regions_total;
		std::string prefix_key = std::string(region_prefixes_map[region_id]) + key;
		op_callback_function callback = put_callback;
		void *callback_args = &reply_counter;
#ifdef COMPUTE_TAIL_ASYNC
		struct compute_tail_callback_args *args = new struct compute_tail_callback_args;
		args->id = id;
		args->opcode = (enum Op)2; // Op::Update
		args->op_callback_args = (void *)&reply_counter;
		args->op_callback = put_callback;
		callback = compute_tail_callback;
		callback_args = args;
		clock_gettime(CLOCK_MONOTONIC, &args->start);
#endif
		if (krc_aput_if_exists(prefix_key.length(), (void *)prefix_key.c_str(), value_size, (void *)value_buf,
				       callback, callback_args) != KRC_SUCCESS) {
			log_fatal("Put failed for key %s", key.c_str());
			exit(EXIT_FAILURE);
		}
		return 0;
	}

	int Insert(int id, const std::string &table /*ignored*/, const std::string &key, std::vector<KVPair> &values)
	{
		static std::string value3(1200, 'a');
		static std::string value2(120, 'a');
		static std::string value(10, 'a');
		int y = kv_count++ % 10;

		const char *value_buf = NULL;
		uint32_t value_size = 0;

		switch (choose_wl(custom_workload, y)) {
		case 0:
			value_size = value.size();
			value_buf = value.c_str();
			break;
		case 1:
			value_size = value2.size();
			value_buf = value2.c_str();
			break;
		case 2:
			value_size = value3.size();
			value_buf = value3.c_str();
			break;
		default:
			assert(0);
			std::cout << "Got Unknown value" << std::endl;
			exit(EXIT_FAILURE);
		}

		int region_id = djb2_hash((unsigned char *)key.c_str(), key.length()) % regions_total;
		std::string prefix_key = std::string(region_prefixes_map[region_id]) + key;
#ifdef COUNT_REQUESTS_PER_REGION
		__sync_fetch_and_add(&region_requests[region_id], 1);
#endif
		op_callback_function callback = put_callback;
		void *callback_args = &reply_counter;
#ifdef COMPUTE_TAIL_ASYNC
		struct compute_tail_callback_args *args = new struct compute_tail_callback_args;
		args->id = id;
		args->opcode = (enum Op)3; // Op::INSERT
		args->op_callback_args = (void *)&reply_counter;
		args->op_callback = put_callback;
		callback = compute_tail_callback;
		callback_args = args;
		clock_gettime(CLOCK_MONOTONIC, &args->start);
#endif
		if (krc_aput(prefix_key.length(), (void *)prefix_key.c_str(), value_size, (void *)value_buf, callback,
			     callback_args) != KRC_SUCCESS) {
			log_fatal("Put failed for key %s", key.c_str());
			exit(EXIT_FAILURE);
		}
		return 0;
	}

	int Delete(int id, const std::string &table, const std::string &key)
	{
		std::cerr << "DELETE " << table << ' ' << key << std::endl;
		std::cerr << "Delete() not implemented [" << __FILE__ << ":" << __func__ << "():" << __LINE__ << "]"
			  << std::endl;
		exit(EXIT_FAILURE);
		return 0;
	}
};
} // namespace ycsbc
