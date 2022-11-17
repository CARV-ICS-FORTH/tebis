#include "db_factory.h"
#include "tcp_db.hpp"

using namespace ycsbc;

YCSBDB *DBFactory::CreateDB(int num, utils::Properties &props)
{
    return new tcpDB(num);
}
