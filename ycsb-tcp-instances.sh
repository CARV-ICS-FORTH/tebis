#!/bin/bash

YCSB_DIR=./build/YCSB-CXX
YCSB_ARGS="-e ./execution_plan.txt -threads 1 -dbnum 1 -w s -outFile "
DRIVER=$YCSB_DIR/ycsb-tcp
INSTANCES=2
YCSB_LOG_FILE="ops.txt"

echo "ycsb-dir = $YCSB_DIR"
cd $YCSB_DIR || exit

if [ $# -eq 0 ]; then
	printf "no number of ycsb-tcp instances provided, choosing default (%d)\n\n" $INSTANCES
else
	INSTANCES=$1
	echo "new-INSTANCES = $INSTANCES"
fi

pwd

for i in $(seq "$INSTANCES"); do
	YCSB_LOG_FILE="ops_"$i.txt
	EXEC="$DRIVER" "$YCSB_ARGS" "$YCSB_LOG_FILE"
	echo -e "run: ./$DRIVER $YCSB_ARGS $YCSB_LOG_FILE"
	./$DRIVER "$YCSB_ARGS" "$YCSB_LOG_FILE"
done
#
#
#
#
