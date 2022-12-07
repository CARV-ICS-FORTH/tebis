#include "tcp_db.hpp"
#include "db_factory.h"

#include <cstring>
#include <future>
#include <stdlib.h>

extern "C" {
#include <log.h>
}

#define SITH3_IP_56G
#define SITH4_IP_56G
#define SITH5_IP_56G "192.168.2.125"
#define SITH6_IP_56G

using namespace ycsbc;

/** PRIVATE **/

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

tcpDB::tcpDB(int num) /* OK */
{
	for (uint i = 0U; i < NUM_OF_THR; ++i) {
		if (chandle_init(this->chandle + i, "139.91.92.134", "25565") < 0) {
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
	for (uint i = 0U; i < NUM_OF_THR; ++i) {
		chandle_destroy(this->chandle[i]);
		c_tcp_req_destroy(this->req[i]);
		c_tcp_rep_destroy(this->rep[i]);
	}
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

	generic_data_t val;

	c_tcp_rep_pop_value(rep, &val); // value="saloustros"

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
	c_tcp_req_factory(&this->req[id], REQ_PUT_IFEX, key.size(), this->values_size(values));

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
		return -(EXIT_FAILURE);
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
