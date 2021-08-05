/**
 * eutropia_db.h
 *  YCSB-C
 * Created by Anastasios Papagiannis on 17/11/15.
 * Copyright (c) 2015 Anastasios Papagiannis <apapag@ics.forth.gr>.
**/

#pragma once

#include "../core/ycsbdb.h"

#include <iostream>
#include <string>
#include <mutex>
#include <algorithm>
#include <atomic>
#include <functional>
#include <unordered_map>

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <boost/algorithm/string.hpp>

#include "../core/properties.h"
#include "workload_gen.h"

extern "C" {
#include "../../kreon_rdma_client/kreon_rdma_client.h"
#include "../../kreon_lib/btree/btree.h"
#include "../../utilities/queue.h"
#include <log.h>

__thread int kv_count = 0;

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
}

#define FIELD_COUNT 10
#define MAX_THREADS 128
using std::cout;
using std::endl;

extern std::unordered_map<std::string, int> ops_per_server;
int pending_requests[MAX_THREADS];
int served_requests[MAX_THREADS];
int num_of_batch_operations_per_thread[MAX_THREADS];
extern std::string zk_host;
extern int zk_port;
extern int regions_total;
#ifdef COUNT_REQUESTS_PER_REGION
extern int *region_requests;
#endif

namespace ycsbc
{
class kreonRAsyncClientDB : public YCSBDB {
    private:
	int db_num;
	int field_count;
	std::vector<db_handle *> dbs;
	double tinit, t1, t2;
	struct timeval tim;
	long long how_many = 0;
	int cu_num;
	pthread_mutex_t mutex_num;
	std::string custom_workload;
	char **region_prefixes_map;

    public:
	kreonRAsyncClientDB(int num, utils::Properties &props)
		: db_num(num), field_count(std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
								       CoreWorkload::FIELD_COUNT_DEFAULT))),
		  dbs()
	{
		struct timeval start;

		if (krc_init((char *)zk_host.c_str(), zk_port) != KRC_SUCCESS) {
			log_fatal("Failed to init client at zookeeper host %s port %d", zk_host.c_str(), zk_port);
			exit(EXIT_FAILURE);
		}
		cu_num = 0;
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
		regions_total = strtol(props.GetProperty("totalRegions", "-1").c_str(), NULL, 10);
		if (regions_total == -1) {
			std::cerr << "Error: missing argument -r" << std::endl;
			exit(EXIT_FAILURE);
		}
		// Create a prefixes map that maps each region to a specific prefix
		// By hashing a key to pick a region you can eliminate load balancing issues
		// The prefix is necessary to make sure that the key that will be sent to
		// the server will match the region's range
		region_prefixes_map = new char *[regions_total];
#ifdef COUNT_REQUESTS_PER_REGION
		region_requests = new int[regions_total]();
#endif
		char first = 'A';
		char second = 'A';
		for (int i = 0; i < regions_total; ++i) {
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

	virtual ~kreonRAsyncClientDB()
	{
		cout << "Calling ~kreonRAsyncClientDB()..." << endl;
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
		krc_start_async_thread(16, UTILS_QUEUE_CAPACITY);
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
		enum krc_ret_code code = krc_aget(prefix_key.length(), (char *)prefix_key.c_str(), &g->buf_size, g->buf,
						  get_callback, g);
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
		log_fatal("Sorry still unsupported");
		exit(EXIT_FAILURE);
#if 0
		size_t s_key_size = 0;
		char *s_key = NULL;
		size_t s_value_size = 0;
		char *s_value = NULL;
		krc_scannerp sc = krc_scan_init(32, (16 * 1024));

		krc_scan_set_start(sc, key.length(), (void *)key.c_str(), KRC_GREATER_OR_EQUAL);
		int i = 0;

		while (i < record_count && krc_scan_get_next(sc, &s_key, &s_key_size, &s_value, &s_value_size)) {
			KVPair k = std::make_pair(std::string(s_key, s_key_size), std::string(s_value, s_value_size));
			result.push_back(k);
			++i;
		}
		krc_scan_close(sc);
#endif
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
#if 1
		return Insert(id, table, key, values);
#else
		static std::string value3(1000, 'a');
		static std::string value2(100, 'a');
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
		if (krc_aput_if_exists(prefix_key.length(), (void *)prefix_key.c_str(), value_size, (void *)value_buf,
				       put_callback, &reply_counter) != KRC_SUCCESS) {
			log_fatal("Put failed for key %s", key.c_str());
			exit(EXIT_FAILURE);
		}
		return 0;
#endif
	}

	int Insert(int id, const std::string &table /*ignored*/, const std::string &key, std::vector<KVPair> &values)
	{
		static std::string value3(1000, 'a');
		static std::string value2(100, 'a');
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
		if (krc_aput(prefix_key.length(), (void *)prefix_key.c_str(), value_size, (void *)value_buf,
			     put_callback, &reply_counter) != KRC_SUCCESS) {
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
