#!/bin/python3
import os
import sys

prefix = "./" + sys.argv[1]
files = [
    "/usr/local/include/kreon_rdma_client.h",
    "/usr/local/lib/libkreon_rdma.a",
    "/usr/local/lib/libkreonr.a",
    "/usr/local/lib/libkreon.a",
    "/usr/local/lib/libkreon_client.a",
    "/usr/local/bin/mkfs.kreon",
    "/usr/local/bin/kreon_server",
]

for f in files:
    filetocheck = prefix + f
    if not os.path.isfile(filetocheck):
        print("File {0} was not found".format(filetocheck))
        sys.exit(1)
