#include "tcp_db.hpp"
#include <cstring>

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

tcpDB::tcpDB(cHandle __restrict__ *__restrict__ chandle, const char *__restrict__ addr,
	     const char *__restrict__ port) /* OK */
{
	chandle_init(chandle, addr, port);

	if (!(this->req = c_tcp_req_factory(NULL, REQ_GET, TT_DEF_KEYSZ, TT_DEF_PAYSZ))) {
		perror("c_tcp_req_new() failed >");
		exit(EXIT_FAILURE);
	}

	if (!(this->rep = c_tcp_rep_new(TT_DEF_REPSZ))) {
		perror("c_tcp_rep_new() failed >");
		exit(EXIT_FAILURE);
	}
}

tcpDB::~tcpDB() /* OK */
{
	chandle_destroy(this->chandle);
	c_tcp_req_destroy(this->req);
	c_tcp_rep_destroy(this->rep);
}

/** TODO: lots of duplicate code, fix that! */

int tcpDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
		std::vector<KVPair> &result) /* OK */
{
	c_tcp_req_factory(&this->req, REQ_GET, key.size(), 0UL);

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	/* my client implementation has no need for this copy bellow! */

	memcpy(__key, key.c_str(), key.size());

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle, this->rep) < 0) {
		perror("c_tcp_recv_rep() failed >");
		return -(EXIT_FAILURE);
	}

	generic_data_t val;

	c_tcp_rep_pop_value(rep, &val); // "saloustros"

	return YCSBDB::kOK;
}

int tcpDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
		std::vector<std::vector<KVPair> > &result) /* Under Construction */
{
	c_tcp_req_factory(&this->req, REQ_SCAN, key.size(), sizeof(len));

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return YCSBDB::kErrorNoData;
	}

	/* my client implementation has no need for this copy bellow! */

	memcpy(__key, key.c_str(), key.size());
	memcpy(__pay, &len, sizeof(len));

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	/* wait for the reply from the server */

	if (c_tcp_recv_rep(this->chandle, this->rep) < 0) {
		perror("c_tcp_recv_rep() failed >");
		return -(EXIT_FAILURE);
	}

	generic_data_t val;

	c_tcp_rep_pop_value(rep, &val); // "saloustros"

	return YCSBDB::kOK;
}

int tcpDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) /* OK */
{
	c_tcp_req_factory(&this->req, REQ_PUT_IFEX, key.size(), this->values_size(values));

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return YCSBDB::kErrorNoData;
	}

	memcpy(__key, key.c_str(), key.size());
	this->serialize_values(values, __pay);

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	return YCSBDB::kOK;
}

int tcpDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) /* OK */
{
	c_tcp_req_factory(&this->req, REQ_PUT, key.size(), this->values_size(values));

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return YCSBDB::kErrorNoData;
	}

	memcpy(__key, key.c_str(), key.size());
	this->serialize_values(values, __pay);

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	return YCSBDB::kOK;
}

int tcpDB::Delete(const std::string &table, const std::string &key) /* OK */
{
	c_tcp_req_factory(&this->req, REQ_DEL, key.size(), 0UL);

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return YCSBDB::kErrorNoData;
	}

	/* my client implementation has no need for this copy below! */

	memcpy(__key, key.c_str(), key.size());

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	return YCSBDB::kOK;
}
