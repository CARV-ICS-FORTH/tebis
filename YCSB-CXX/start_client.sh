#!/bin/bash

# Ensure we have the correct number of arguments
if [ "$#" -lt 6 ] || [ "$#" -gt 7 ]; then
	echo "Usage: $0 client_id zookeeper_host execution_plan output_folder barrier_folder insert_start [--skip-load]"
	exit 1
fi

SKIP_LOAD=0
if [ "$#" -eq 7 ]; then
	if [ "$7" == "--skip-load" ]; then
		SKIP_LOAD=1
	else
		echo "Invalid 7th argument. If provided, it must be '--skip-load' and it is $7"
		exit 1
	fi
fi
echo "SKIP_LOAD is $SKIP_LOAD"
# Assign arguments to named variables for clarity
client_id="$1"
zookeeper_host="$2"
execution_plan="$3"
output_folder="$4"
barrier_folder="$5"
base_insert_start="$6"

execute_ycsb_processes() {
	local e_plan="$1"

	for i in {1..4}; do
		insertStart=$((base_insert_start + (i - 1) * 12500000))
		client_folder="${output_folder}/client_$((client_id + i - 1))"

		# Create/clean the client folder
		rm -rf "$client_folder"
		mkdir -p "$client_folder"

		./ycsb-async-tebis -threads 2 -w sd -zookeeper "$zookeeper_host" -dbnum 1 -e "$e_plan" -insertStart "$insertStart" -o "$client_folder" &
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

# Start the background processes again with the main execution plan
execute_ycsb_processes "$execution_plan"

# Write to the barrier file to indicate this client has finished the main phase
echo "Client $client_id main phase finished" >"$barrier_folder/client_$client_id"

echo "Script completed for client $client_id"
