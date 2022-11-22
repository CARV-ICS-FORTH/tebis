#!/bin/bash

YCSB_DIR=build/YCSB-CXX
YCSB_ARGS="-e ./execution_plan.txt -threads 1 -dbnum 1 -w s -outFile "
DRIVER=ycsb-tcp
INSTANCES=2
YCSB_LOG_FILE="ops.txt"

echo "ycsb-dir = $YCSB_DIR"
cd $YCSB_DIR || exit
pwd

if [ $# -eq 0 ]; then
	printf "no number of ycsb-tcp instances provided, choosing default (%d)\n\n" $INSTANCES
else
	INSTANCES=$1
	echo "new-INSTANCES = $INSTANCES"
fi

for i in $(seq "$INSTANCES"); do
	YCSB_LOG_FILE="ops_$i.txt"
	echo "run: \e[1m$DRIVER\e[0;3m $YCSB_ARGS\e[0m" &&
		./$DRIVER "$YCSB_ARGS" "$YCSB_LOG_FILE" &
done
