#!/bin/zsh -x

if [ $# -ne 2 ]
  then
    echo "No arguments supplied, Usage ./start_ycsb_clients.sh  <execution plan (a,a2d)>  <workload type <s,m,l,sd,md,ld>"
    exit
fi
if [ "$1" = "a" ]
then
	execution_plans=( execution_plan_la.txt) # execution_plan_ra.txt )
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
rm /tmp/cli*

host=$(hostname)
zk_host=sith1.cluster.ics.forth.gr:2181

# YCSB parameters
workload_folder=( load_a run_a run_b run_c run_d )
#insertstart=( 0 2500000 5000000 7500000 )
#insertstart=( 0 6250000 12500000 18750000 ) # 50M 2 clients
insertstart=( 0 12500000 25000000 37500000 ) # 100M 2 clients
#insertstart=( 0 25000000 50000000 75000000 ) # 200M 2 clients
#insertstart=( 0 50000000 100000000 150000000 ) # 400M 2 clients

# Tebis host parameters
tebis_hosts=( sith2.cluster.ics.forth.gr sith3.cluster.ics.forth.gr )
net_iface=( ens10 ens10 ens10 )

rm -rf .barrier
rm -rf $WORKING_DIR/clients_group1_network_traffic
mkdir $WORKING_DIR/clients_group1_network_traffic
for i in $(seq ${#execution_plans[@]}); do
	mkdir $WORKING_DIR/clients_group1_network_traffic/${workload_folder[$i]}
	ethtool -S ens10d1 > $WORKING_DIR/clients_group1_network_traffic/${workload_folder[$i]}/counters_start
	for j in $(seq ${#tebis_hosts[@]}); do
		#Required for the servers STATS
		mkdir $WORKING_DIR/STATS-${tebis_hosts[$j]}
		mkdir $WORKING_DIR/STATS-${tebis_hosts[$j]}/${workload_folder[$i]}
		#Acquire the network counters for all the servers
		ssh ${tebis_hosts[$j]} /usr/sbin/ethtool -S ens10d1 > $WORKING_DIR/STATS-${tebis_hosts[$j]}/${workload_folder[$i]}/counters_start
		#ssh to server and start the statistics
		ssh ${tebis_hosts[$j]} nohup $WORKING_DIR/start_statistics.sh $WORKING_DIR/STATS-${tebis_hosts[$j]}/${workload_folder[$i]}
	done
	jemalloc.sh $TEBIS_HOME/build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[1]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-1 -zookeeper $zk_host -w $workload_type  &
	jemalloc.sh $TEBIS_HOME/build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[2]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-2 -zookeeper $zk_host -w $workload_type  &
	jemalloc.sh $TEBIS_HOME/build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[3]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-3 -zookeeper $zk_host -w $workload_type  &
	jemalloc.sh $TEBIS_HOME/build/YCSB-CXX/ycsb-async-tebis -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -insertStart ${insertstart[4]} -threads 2 -dbnum 1 -o $WORKING_DIR/RESULTS_$host-4 -zookeeper $zk_host -w $workload_type
	# Wait for all active YCSB clients to finish
  wait
	$BARRIER 2 1
	echo "Cleared the barrier"
	for tebis_host in ${tebis_hosts[@]}; do
		ssh $tebis_host $WORKING_DIR/stop_statistics.sh $WORKING_DIR/STATS-$tebis_host/${workload_folder[$i]}
		#measure the network counters again for the servers
		ssh $tebis_host /usr/sbin/ethtool -S ens10d1 > $WORKING_DIR/STATS-$tebis_host/${workload_folder[$i]}/counters_end
	done
	ethtool -S ens10d1 > $WORKING_DIR/clients_group1_network_traffic/${workload_folder[$i]}/counters_end
done

printf "\a" # Ring the bell
