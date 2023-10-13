#!/bin/bash

# Check if at least 3 arguments are provided
if [ "$#" -lt 3 ]; then
	echo "Usage: $0 <nvme_device_name> <net_interface> <host1> [<host2>...]"
	exit 1
fi

# Parse the arguments
device_name="$1"
net_interface="$2"
shift 2 # Remove the first two arguments (device_name and net_interface)

execute_on_host() {
	local host="$1"

	# Kill CPU monitoring processes
	ssh "$host" "pkill -9 mpstat"
	ssh "$host" "pkill -9 monitor_cpu"

	# Dump counters
	ssh "$host" "/tmp/dump_nvme_counters.sh \"$device_name\" /tmp/stats stop"
	ssh "$host" "/tmp/dump_net_counters.sh \"$net_interface\" /tmp/stats stop"

	# Download the stats
	local local_stats_path="/tmp/stats/$host"
	mkdir -p "$local_stats_path" # ensure directory exists
	scp -r "$host:/tmp/stats/*" "$local_stats_path/"
}

analyze_data() {
	local host="$1"

	local local_stats_path="/tmp/stats/$host"

	# Analyze NVMe IO
	./calc_nvme_IO.sh "$local_stats_path" "$device_name" "start" "stop"

	# Analyze Network IO
	python3 calc_net_IO.py "$local_stats_path/net_$net_interface"_start "$local_stats_path/net_$net_interface"_stop

	# Analyze Average CPU
	./calc_avg_cpu.sh "$local_stats_path/cpu_$host.txt"
}

# Ensure local /tmp/stats directory exists or create and clean it
mkdir -p /tmp/stats
rm -rf /tmp/stats/*

# Iterate over each host, stop monitoring, download data, and analyze
for host in "$@"; do
	execute_on_host "$host"
	analyze_data "$host"
done

echo "Monitoring stopped, stats downloaded, and analysis completed for all specified hosts."
