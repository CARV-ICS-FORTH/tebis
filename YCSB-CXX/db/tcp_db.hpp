#ifndef YCSB_C_TCP_DB_H
#define YCSB_C_TCP_DB_H

extern "C" {
#include "tcp_client.h"
}

#include "properties.h"
#include "ycsbdb.h"

namespace ycsbc
{

class tcpDB : public YCSBDB {
    public:
	std::string custom_workload;
	tcpDB(int num, utils::Properties &props);

	~tcpDB();

	int Read(int id, const std::string &table, const std::string &key, const std::vector<std::string> *fields,
		 std::vector<KVPair> &result) override;

	int Scan(int id, const std::string &table, const std::string &key, int record_count,
		 const std::vector<std::string> *fields, std::vector<KVPair> &result) override;

	int Update(int id, const std::string &table, const std::string &key, std::vector<KVPair> &values) override;

	int Insert(int id, const std::string &table, const std::string &key, std::vector<KVPair> &values) override;

	int Delete(int id, const std::string &table, const std::string &key) override;

	void Init(void) override
	{
		return;
	};

	void Close(void) override
	{
		return;
	};

    private:
	int threads;
	cHandle *chandle;
	c_tcp_req *req;
	c_tcp_rep *rep;

	size_t values_size(std::vector<KVPair> &values);
	int serialize_values(std::vector<KVPair> &values, char *buf);
};

}

#endif /* YCSB_C_TCP_DB_H */
