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

host=$(hostname)

# YCSB parameters
workload_folder=( load_a run_a run_b run_c run_d )

# Tebis host parameters
tebis_hosts=( sith3.cluster.ics.forth.gr )
net_iface=( ens10 ens10 ens10 )

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
	jemalloc.sh $TEBIS_HOME/build/YCSB-CXX/ycsb-tcp -e $WORKING_DIR/ycsb_execution_plans/${execution_plans[$i]} -ip 192.168.2.123 -port 8080 -threads 128 -o $WORKING_DIR/RESULTS_$host -w $workload_type 
	# Wait for all active YCSB clients to finish
  wait
	for tebis_host in ${tebis_hosts[@]}; do
		ssh $tebis_host $WORKING_DIR/stop_statistics.sh $WORKING_DIR/STATS-$tebis_host/${workload_folder[$i]}
		#measure the network counters again for the servers
		ssh $tebis_host /usr/sbin/ethtool -S ens10d1 > $WORKING_DIR/STATS-$tebis_host/${workload_folder[$i]}/counters_end
	done
	ethtool -S ens10d1 > $WORKING_DIR/clients_group1_network_traffic/${workload_folder[$i]}/counters_end
done

printf "\a" # Ring the bell
