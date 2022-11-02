#ifndef YCSB_C_TCP_DB_H
#define YCSB_C_TCP_DB_H

#include "../core/db.h"
#include "../../tcp_client/tcp_client.h"


namespace ycsbc
{

class tcpDB : public DB {

    public:

        tcpDB(cHandle __restrict__ * __restrict__ chandle, const char *__restrict__ addr, const char *__restrict__ port);

        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values);

        int Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

    private:

        cHandle chandle;
        c_tcp_req req;
        c_tcp_rep rep;
};

}

#endif /* YCSB_C_TCP_DB_H */