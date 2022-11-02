#include "tcp_db.hpp"
#include <cstring>

using namespace ycsbc;


tcpDB::tcpDB(cHandle __restrict__ * __restrict__ chandle, const char *__restrict__ addr, const char *__restrict__ port)
{
    chandle_init(chandle, addr, port);

    if ( !(req = c_tcp_req_new(REQ_GET, TT_DEF_KEYSZ, TT_DEF_PAYSZ)) )
	{
		perror("c_tcp_req_new() failed >");
		exit(EXIT_FAILURE);
	}

	if ( !(rep = c_tcp_rep_new(8192UL)) )
	{
		perror("c_tcp_rep_new() failed >");
		exit(EXIT_FAILURE);
	}
}

int tcpDB::Read(const std::string &table, const std::string &key,
         const std::vector<std::string> *fields,
         std::vector<KVPair> &result)
{
    char *__key = (char *) c_tcp_req_expose_key(req);

    if ( !__key )
    {
        perror("c_tcp_req_expose_key() failed >");
        return DB::kErrorNoData;
    }

    /* my client implementation has no need for this copy bellow! */

    memcpy(__key, key.c_str(), key.size());

    if ( c_tcp_send_req(this->chandle, req) < 0 )
    {
        perror("c_tcp_send_req() failed >");
        return -(EXIT_FAILURE);
    }

    /* wait for the reply from the server */

    if ( c_tcp_recv_rep(this->chandle, this->rep) < 0 )
    {
        perror("c_tcp_recv_rep() failed >");
        return -(EXIT_FAILURE);
    }
    
    return DB::kOK;
}

int tcpDB::Scan(const std::string &table, const std::string &key,
         int len, const std::vector<std::string> *fields,
         std::vector<std::vector<KVPair>> &result)
{
    return DB::kOK;
}

int tcpDB::Update(const std::string &table, const std::string &key,
            std::vector<KVPair> &values)
{
    return DB::kOK;
}

int tcpDB::Insert(const std::string &table, const std::string &key,
            std::vector<KVPair> &values)
{
    return DB::kOK;
}

int tcpDB::Delete(const std::string &table, const std::string &key)
{
    return DB::kOK;
}

