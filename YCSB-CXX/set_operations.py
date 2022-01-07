#!/bin/python3

import sys, os

workloads_folder = "workloads"

if len(sys.argv) != 4:
    print(
        "\033[1;31m"
        + "E: Usage: ./set_operations.py <record_count> <operation_count> <clients>"
        + "\033[1;39m"
    )
    sys.exit(-1)

record_count = sys.argv[1]
ops = sys.argv[2]
clients = int(sys.argv[3])
ops = str(int(int(ops) / clients))
record_count_load = str(int(int(sys.argv[1]) / clients))

for wfilename in ["workloada", "workloadb", "workloadc", "workloadd", "workloadf"]:
    wfile = open(workloads_folder + "/" + wfilename, "r")
    newwfile = open("tmp_" + wfilename, "w")
    for line in wfile:
        if "recordcount" in line:
            newwfile.write("recordcount=" + record_count + "\n")
        elif "operationcount" in line:
            newwfile.write("operationcount=" + ops + "\n")
        else:
            newwfile.write(line)
    wfile.close()
    newwfile.close()
    os.system("mv " + "tmp_" + wfilename + " " + workloads_folder + "/" + wfilename)

wfilename = "workloada"
wfile = open(workloads_folder + "/" + wfilename, "r")
newwfile = open("tmp_" + wfilename, "w")
for line in wfile:
    if "recordcount" in line:
        newwfile.write("recordcount=" + record_count_load + "\n")
    elif "operationcount" in line:
        newwfile.write("operationcount=" + record_count_load + "\n")
    else:
        newwfile.write(line)
wfile.close()
newwfile.close()
os.system("mv " + "tmp_" + wfilename + " " + workloads_folder + "/" + wfilename)
