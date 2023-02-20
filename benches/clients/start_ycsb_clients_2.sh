#!/bin/zsh -x

rm /tmp/cli1.txt
rm /tmp/cli2.txt
rm /tmp/cli3.txt
rm /tmp/cli4.txt
if [ $# -eq 0 ]
  then
    echo "No arguments supplied, Usage ./start_ycsb_clients.sh  <execution plan (a,a2d)>  <workload type <s,m,l,sd,md,ld>"
    exit
fi

#execution_plans=( execution_plan_la.txt execution_plan_ra.txt execution_plan_rb.txt execution_plan_rc.txt execution_plan_rd.txt )
execution_plans=( execution_plan_la.txt execution_plan_ra.txt )
if [ "$1" = "a" ]
then
	execution_plans=( execution_plan_la.txt ) #execution_plan_ra.txt )
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
BARRIER=$WORKING_DIR/barrier.sh


host=$(hostname)
zk_host=sith6.cluster.ics.forth.gr
workload_folder=( load_a run_a run_b run_c run_d )
# 2 NODES
#insertstart=( 8000000 10000000 12000000 14000000 )
#insertstart=( 25000000 31250000 37500000 43750000 ) # 50M 2 clients
insertstart=( 50000000 62500000 75000000 87500000 ) # 100M 2 clients
#insertstart=( 100000000 125000000 150000000 175000000 ) # 200M 2 clients
#insertstart=( 200000000 250000000 300000000 350000000 ) # 400M 2 clients

for i in $(seq ${#execution_plans[@]}); do
  sleep 6
  jemalloc.sh $TEBIS_HOME/remote_build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[1]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-1 -zookeeper $zk_host:2181 -w $workload_type &
  jemalloc.sh $TEBIS_HOME/remote_build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[2]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-2 -zookeeper $zk_host:2181 -w $workload_type &
  jemalloc.sh $TEBIS_HOME/remote_build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[3]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-3 -zookeeper $zk_host:2181 -w $workload_type &
  jemalloc.sh $TEBIS_HOME/remote_build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[4]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-4 -zookeeper $zk_host:2181 -w $workload_type
	# Wait for all active YCSB clients to finish
  wait
	$BARRIER 2 2
	echo "Cleared the barrier"
done
wait
