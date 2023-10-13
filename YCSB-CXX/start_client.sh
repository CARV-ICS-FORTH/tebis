#!/bin/bash -x
# Ensure we have the correct number of arguments
if [ "$#" -lt 6 ] || [ "$#" -gt 8 ]; then
    echo "Usage: $0 client_id zookeeper_host execution_plan output_folder barrier_folder insert_start [--skip-load / --skip-scan]"
    exit 1
fi



echo "SKIP_LOAD is $SKIP_LOAD"
echo "SKIP_SCAN is $SKIP_SCAN"
# Assign arguments to named variables for clarity
client_id="$1"
zookeeper_host="$2"
execution_plan="$3"
output_folder="$4"
barrier_folder="$5"
base_insert_start="$6"

SKIP_LOAD=0
SKIP_SCAN=0
if [ "$#" -ge 7 ] && [ "$7" == "--skip-load" ]; then
    SKIP_LOAD=1
    shift
fi
if [ "$#" -ge 7 ] && [ "$7" == "--skip-scan" ]; then
    SKIP_SCAN=1
    shift
fi

execute_ycsb_processes() {
	local e_plan="$1"

	for i in {1..4}; do
		insertStart=$((base_insert_start + (i - 1) * 12500000))
		client_folder="${output_folder}/client_$((client_id + i - 1))"

		# Create/clean the client folder
		rm -rf "$client_folder"
		mkdir -p "$client_folder"

		./ycsb-async-tebis -threads 1 -w sd -zookeeper "$zookeeper_host" -dbnum 1 -e "$e_plan" -insertStart "$insertStart" -o "$client_folder" &
		pids[i]=$! # store the process ID
	done

	# Wait for all background processes to finish
	for pid in "${pids[@]}"; do
		wait "$pid"
	done
}

if [ $SKIP_LOAD -eq 0 ]; then
	# Start the initial background processes with the load execution plan
	execute_ycsb_processes "${execution_plan}_load"

	# Write to the barrier file to indicate this client has finished the initial phase
	echo "Client $client_id load phase finished" >"$barrier_folder/client_{$client_id}_load"

	# Wait for the barrier to have two files
	while [ "$(find "$barrier_folder" -maxdepth 1 -type f | wc -l)" -lt 2 ]; do
		sleep 5 # Check every 5 seconds
		echo "Waiting at the barrier"
	done
fi

if [ $SKIP_SCAN -eq 0 ]; then
    # Start the background processes again with the main execution plan
    execute_ycsb_processes "$execution_plan"

    # Write to the barrier file to indicate this client has finished the main phase
    echo "Client $client_id main phase finished" >"$barrier_folder/client_$client_id"
fi


# Write to the barrier file to indicate this client has finished the main phase
echo "Client $client_id main phase finished" >"$barrier_folder/client_$client_id"


