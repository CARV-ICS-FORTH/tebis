#include "tcp_db.hpp"
#include "db_factory.h"
#include "workload_gen.h"
#include <cstring>
#include <future>
#include <iostream>
#include <stdlib.h>

extern "C" {
#include <log.h>
}

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT "8080"

using namespace ycsbc;
/** PRIVATE **/
__thread int kv_count = 0;

int tcpDB::serialize_values(std::vector<KVPair> &values, char *buf)
{
	for (KVPair p : values) {
		memcpy(buf, p.first.c_str(), p.first.size());
		buf += p.first.size();

		memcpy(buf, p.second.c_str(), p.second.size());
		buf += p.second.size();
	}

	return EXIT_SUCCESS;
}

size_t tcpDB::values_size(std::vector<KVPair> &values)
{
	size_t tsize = 0UL;

	for (KVPair p : values)
		tsize += p.first.size() + p.second.size();

	return tsize;
}

/** PUBLIC **/

tcpDB::tcpDB(int num, utils::Properties &props) /* OK */
{
	this->threads = stoi(props.GetProperty("threadcount", "1"));

	printf("\033[1;31mthreads = %d\033[0m\n", this->threads);

	if (!(this->chandle = (void **)malloc(this->threads *
					      (sizeof(*this->chandle) + sizeof(*this->req) + sizeof(*this->rep))))) {
		log_error("calloc() failed: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	this->req = (typeof(this->req))((char *)(this->chandle) + (this->threads * sizeof(*(this->chandle))));
	this->rep = (typeof(this->rep))((char *)(this->req) + (this->threads * sizeof(*(this->req))));

	custom_workload = props.GetProperty("workloadType", "undefined");
	if (custom_workload.compare("undefined") == 0) {
		std::cerr << "Error: missing argument -w" << std::endl;
		exit(EXIT_FAILURE);
	}
	std::cout << "Workload Type: " << custom_workload << std::endl;
	uint32_t ip_size = strlen(props.GetProperty("par_server_ip", DEFAULT_HOST).c_str()) + 1;
	uint32_t port_size = strlen(props.GetProperty("par_server_port", DEFAULT_PORT).c_str()) + 1;
	char *ip = (char *)calloc(1, ip_size);
	char *port = (char *)calloc(1, port_size);
	memcpy(ip, props.GetProperty("par_server_ip", DEFAULT_HOST).c_str(), ip_size);
	memcpy(port, props.GetProperty("par_server_port", DEFAULT_PORT).c_str(), port_size);
	for (int i = 0; i < this->threads; ++i) {
		if (chandle_init(this->chandle + i, ip, port) < 0) {
			perror("chandle_init()");
			exit(EXIT_FAILURE);
		}

		if (!(this->req[i] = c_tcp_req_factory(NULL, REQ_GET, TT_DEF_KEYSZ, TT_DEF_PAYSZ))) {
			perror("c_tcp_req_new() failed >");
			exit(EXIT_FAILURE);
		}

		if (!(this->rep[i] = c_tcp_rep_new(TT_DEF_REPSZ))) {
			perror("c_tcp_rep_new() failed >");
			exit(EXIT_FAILURE);
		}
	}
}

tcpDB::~tcpDB() /* OK */
{
	for (uint i = 0U; i < this->threads; ++i) {
		chandle_destroy(this->chandle[i]);
		c_tcp_req_destroy(this->req[i]);
		c_tcp_rep_destroy(this->rep[i]);
	}

	free(this->chandle);
}

int tcpDB::Read(int id, const std::string &table, const std::string &key, const std::vector<std::string> *fields,
		std::vector<KVPair> &result) /* OK */
{
	c_tcp_req_factory(&this->req[id], REQ_GET, key.size(), 0UL);

	char *__key = (char *)c_tcp_req_expose_key(this->req[id]);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	/* my client implementation has no need for this copy bellow! */

	memcpy(__key, key.c_str(), key.size());

	if (c_tcp_send_req(this->chandle[id], this->req[id]) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle[id], &this->rep[id]) < 0) {
		perror("c_tcp_recv_rep() failed >");
		return -(EXIT_FAILURE);
	}

	return YCSBDB::kOK;
}

int tcpDB::Scan(int id, const std::string &table, const std::string &key, int record_count,
		const std::vector<std::string> *fields, std::vector<KVPair> &result) /* Under Construction */
{
	throw "not supported!";

	c_tcp_req_factory(&this->req[id], REQ_SCAN, key.size(), record_count);

	char *__key = (char *)c_tcp_req_expose_key(this->req[id]);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req[id]);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return YCSBDB::kErrorNoData;
	}

	/* my client implementation has no need for this copy bellow! */

	memcpy(__key, key.c_str(), key.size());
	memcpy(__pay, &record_count, sizeof(record_count));

	if (c_tcp_send_req(this->chandle[id], this->req[id]) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle[id], &this->rep[id]) < 0) {
		perror("c_tcp_recv_rep() failed >");
		return -(EXIT_FAILURE);
	}

	generic_data_t val;

	c_tcp_rep_pop_value(rep, &val); // "saloustros"

	return YCSBDB::kOK;
}

int tcpDB::Update(int id, const std::string &table, const std::string &key, std::vector<KVPair> &values) /* OK */
{
	c_tcp_req_factory(&this->req[id], REQ_PUT, key.size(), this->values_size(values));

	char *__key = (char *)c_tcp_req_expose_key(this->req[id]);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req[id]);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return YCSBDB::kErrorNoData;
	}

	memcpy(__key, key.c_str(), key.size());
	this->serialize_values(values, __pay);

	if (c_tcp_send_req(this->chandle[id], this->req[id]) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle[id], &this->rep[id]) < 0) {
		perror("c_tcp_recv_rep() failed >");
		return -(EXIT_FAILURE);
	}

	return YCSBDB::kOK;
}

int tcpDB::Insert(int id, const std::string &table, const std::string &key, std::vector<KVPair> &values) /* OK */
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
	// printf("\033[1;31mID = %d\033[0m\n", id);
	c_tcp_req_factory(&this->req[id], REQ_PUT, key.size(), value_size);

	char *__key = (char *)c_tcp_req_expose_key(this->req[id]);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req[id]);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return YCSBDB::kErrorNoData;
	}

	// extra copy below happens only in YCSB

	memcpy(__key, key.c_str(), key.size());
	this->serialize_values(values, __pay);

	if (c_tcp_send_req(this->chandle[id], this->req[id]) < 0) {
		log_error("c_tcp_send_req() failed");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle[id], &this->rep[id]) < 0) {
		log_error("c_tcp_recv_rep(%d) failed\n", id);
		perror("recv_rep()");
		return YCSBDB::kErrorNoData;
	}

	return YCSBDB::kOK;
}

int tcpDB::Delete(int id, const std::string &table, const std::string &key) /* OK */
{
	c_tcp_req_factory(&this->req[id], REQ_DEL, key.size(), 0UL);

	char *__key = (char *)c_tcp_req_expose_key(this->req[id]);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	/* my client implementation has no need for this copy below! */

	memcpy(__key, key.c_str(), key.size());

	if (c_tcp_send_req(this->chandle, this->req[id]) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle[id], &this->rep[id]) < 0) {
		perror("c_tcp_recv_rep() failed >");
		return -(EXIT_FAILURE);
	}

	return YCSBDB::kOK;
}
