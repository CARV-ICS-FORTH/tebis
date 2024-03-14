#!/bin/bash

if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <start_file> <stop_file>"
	exit 1
fi

start_file="$1"
stop_file="$2"

# Extract the timestamps
start_timestamp=$(head -1 "$start_file")
stop_timestamp=$(head -1 "$stop_file")

# Calculate time difference
time_difference=$((stop_timestamp - start_timestamp))

echo "Time elapsed: $time_difference seconds"

# Process each file and get the counters into associative arrays
declare -A start_counters
declare -A stop_counters

# Ignore the timestamp (using tail command) and read the rest of the counters
while IFS=':' read -r key value; do
	start_counters["$key"]=$(echo "$value" | tr -d ' ')
done < <(tail -n +2 "$start_file")

while IFS=':' read -r key value; do
	stop_counters["$key"]=$(echo "$value" | tr -d ' ')
done < <(tail -n +2 "$stop_file")

# Report differences
echo "Counter Differences (stop - start):"
for key in "${!start_counters[@]}"; do
	if [[ -n "${stop_counters[$key]}" && "${start_counters[$key]}" =~ ^-?[0-9]+$ && "${stop_counters[$key]}" =~ ^-?[0-9]+$ ]]; then
		difference=$((${stop_counters[$key]} - ${start_counters[$key]}))
		echo "$key: $difference"
	fi
done

# Calculate and print the transmit throughput
if [[ -n "${stop_counters['tx_prio_3_bytes']}" && "${start_counters['tx_prio_3_bytes']}" =~ ^-?[0-9]+$ && "${stop_counters['tx_prio3_bytes']}" =~ ^-?[0-9]+$ ]]; then
	tx_difference_bytes=$((${stop_counters['tx_prio3_bytes']} - ${start_counters['tx_prio3_bytes']}))
	tx_throughput_mbps=$(echo "scale=2; ($tx_difference_bytes / 1048576) / $time_difference" | bc)
	echo "Transmit Throughput: $tx_throughput_mbps MB/s"
fi

# Calculate and print the receive throughput
if [[ -n "${stop_counters['rx_prio_3_bytes']}" && "${start_counters['rx_prio_3_bytes']}" =~ ^-?[0-9]+$ && "${stop_counters['rx_prio_3_bytes']}" =~ ^-?[0-9]+$ ]]; then
	rx_difference_bytes=$((${stop_counters['rx_prio_3_bytes']} - ${start_counters['rx_prio_3_bytes']}))
	rx_throughput_mbps=$(echo "scale=2; ($rx_difference_bytes / 1048576) / $time_difference" | bc)
	echo "Receive Throughput: $rx_throughput_mbps MB/s"
fi
