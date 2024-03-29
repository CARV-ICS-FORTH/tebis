//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"

#include "tebis_blocking_client.h"

using ycsbc::YCSBDB;
using ycsbc::DBFactory;

YCSBDB *DBFactory::CreateDB(int num, utils::Properties &props)
{
	return new tebisBlockingClientDB(num, props);
}
