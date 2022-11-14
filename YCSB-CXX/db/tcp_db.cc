#include "tcp_db.hpp"
#include <cstring>

using namespace ycsbc;

tcpDB::tcpDB(cHandle __restrict__ *__restrict__ chandle, const char *__restrict__ addr,
	     const char *__restrict__ port) /* OK */
{
	chandle_init(chandle, addr, port);

	if (!(this->req = c_tcp_req_new(REQ_GET, TT_DEF_KEYSZ, TT_DEF_PAYSZ))) {
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
	c_tcp_req_update(&this->req, REQ_GET, key.size(), 0UL);

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return DB::kErrorNoData;
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

	const char *value = (const char *)c_tcp_rep_expose(this->rep);
	result.push_back(std::make_pair(value, ""));

	return DB::kOK;
}

int tcpDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
		std::vector<std::vector<KVPair> > &result) /* Under Construction */
{
	throw "Scan not implemented yet!";

	// char *__key = (char *) c_tcp_req_expose_key(this->req);

	// if ( !__key )
	// {
	//     perror("c_tcp_req_expose_key() failed >");
	//     return DB::kErrorNoData;
	// }

	// c_tcp_req_update(&this->req, REQ_SCAN, key.size(), len);

	// /* my client implementation has no need for this copy bellow! */

	// memcpy(__key, key.c_str(), key.size());

	// if ( c_tcp_send_req(this->chandle, this->req) < 0 )
	// {
	//     perror("c_tcp_send_req() failed >");
	//     return -(EXIT_FAILURE);
	// }

	// /* wait for the reply from the server */

	// if ( c_tcp_recv_rep(this->chandle, this->rep) < 0 )
	// {
	//     perror("c_tcp_recv_rep() failed >");
	//     return -(EXIT_FAILURE);
	// }

	// const char *value = (const char *) c_tcp_rep_expose(this->rep);
	// /** TODO: tebis_tcp_types.h --> make changes to the reply-buffer to support scan */

	// return DB::kOK;
}

int tcpDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) /* OK */
{
	c_tcp_req_update(&this->req, REQ_PUT_IFEX, key.size(), values[0].second.size());
	/** TODO: saloustro to 'values' giati exei duo fields. To 'second' einai pragmatiko to value? */
	/** TODO: epishs, giati Vector apo values? Exei support gia Scatter Gather IO ?*/

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return DB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return DB::kErrorNoData;
	}

	memcpy(__key, key.c_str(), key.size());
	memcpy(__pay, values.at(0).second.c_str(), values.at(0).second.size());

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	return DB::kOK;
	return DB::kOK;
}

int tcpDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) /* OK */
{
	c_tcp_req_update(&this->req, REQ_PUT, key.size(), values[0].second.size());
	/** TODO: saloustro to 'values' giati exei duo fields. To 'second' einai pragmatiko to value? */
	/** TODO: epishs, giati Vector apo values? Exei support gia Scatter Gather IO ?*/

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return DB::kErrorNoData;
	}

	char *__pay = (char *)c_tcp_req_expose_payload(this->req);

	if (!__pay) {
		perror("c_tcp_req_expose_payload() failed >");
		return DB::kErrorNoData;
	}

	memcpy(__key, key.c_str(), key.size());
	memcpy(__pay, values.at(0).second.c_str(), values.at(0).second.size());

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	return DB::kOK;
}

int tcpDB::Delete(const std::string &table, const std::string &key) /* OK */
{
	c_tcp_req_update(&this->req, REQ_DEL, key.size(), 0UL);

	char *__key = (char *)c_tcp_req_expose_key(this->req);

	if (!__key) {
		perror("c_tcp_req_expose_key() failed >");
		return DB::kErrorNoData;
	}

	/* my client implementation has no need for this copy below! */

	memcpy(__key, key.c_str(), key.size());

	if (c_tcp_send_req(this->chandle, this->req) < 0) {
		perror("c_tcp_send_req() failed >");
		return -(EXIT_FAILURE);
	}

	return DB::kOK;
}
