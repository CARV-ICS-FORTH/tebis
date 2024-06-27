#!/bin/zsh -x

if [ $# -eq 0 ]
  then
    echo "No arguments supplied, Usage ./start_ycsb_clients.sh  <execution plan (a,a2d)>  <workload type <s,m,l,sd,md,ld>"
    exit
fi

#execution_plans=( execution_plan_la.txt execution_plan_ra.txt execution_plan_rb.txt execution_plan_rc.txt execution_plan_rd.txt )
execution_plans=( execution_plan_la.txt execution_plan_ra.txt )
if [ "$1" = "a" ]
then
	execution_plans=( execution_plan_la.txt execution_plan_ra.txt )
elif [ "$1" = "a2d" ]
then
	execution_plans=( execution_plan_la.txt execution_plan_ra.txt execution_plan_rb.txt execution_plan_rc.txt execution_plan_rd.txt )
else
	echo "Unknown execution plan possible values a, a2d"
	exit
fi
workload_type=$2

WORKING_DIR=$(pwd)
TEBIS_HOME=/home1/public/geostyl/tebis

host=$(hostname)
workload_folder=( load_a run_a run_b run_c run_d )
for i in $(seq ${#execution_plans[@]}); do
  jemalloc.sh $TEBIS_HOME/build/YCSB-CXX/ycsb-tcp -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -ip 192.168.2.123 -port 8080 -threads 128 -o $WORKING_DIR/RESULTS_$host-1 -w $workload_type
	# Wait for all active YCSB clients to finish
  wait
done
wait
