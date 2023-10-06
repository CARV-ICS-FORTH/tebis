#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <nvme_device> <folder_path>"
	exit 1
fi

# Device path and output directory from arguments
NVME_PATH="/sys/block/$1"
OUTPUT_DIR="$2"

# Interval for monitoring is set to a constant value of 1 second
INTERVAL=1

# Duration for which the script will run
DURATION=1200
END_TIME=$(($(date +%s) + DURATION))

# Check if the provided NVMe path exists
if [ ! -d "$NVME_PATH" ]; then
	echo "Error: The provided NVMe device path does not exist."
	exit 1
fi

# Ensure the provided folder path exists, otherwise create it
if [ ! -d "$OUTPUT_DIR" ]; then
	mkdir -p "$OUTPUT_DIR"
fi

# File to store the average queue depths and request sizes
OUTPUT_FILE="${OUTPUT_DIR}/${1}_queue_depth.txt"
rm -f "$OUTPUT_FILE"

# Function to get the current inflight values
get_inflight() {
	if [ -f "${NVME_PATH}/inflight" ]; then
		read -r inflight_read inflight_write <"${NVME_PATH}/inflight"
		echo $((inflight_read + inflight_write))
	else
		echo "Error: inflight file not found"
		exit 1
	fi
}

# Function to get total bytes read, bytes written, read operations, and write operations
get_stats() {
	if [ -f "${NVME_PATH}/stat" ]; then
		read -r read_ops _ _ bytes_read write_ops _ _ bytes_written <"${NVME_PATH}/stat"
		echo "$bytes_read $bytes_written $read_ops $write_ops"
	else
		echo "Error: stat file not found"
		exit 1
	fi
}

while [ "$(date +%s)" -lt "$END_TIME" ]; do
	start_inflight=$(get_inflight)
	start_stats=($(get_stats))
	start_bytes_read=${start_stats[0]}
	start_bytes_written=${start_stats[1]}
	start_read_ops=${start_stats[2]}
	start_write_ops=${start_stats[3]}

	# Wait for the interval of 1 second
	sleep $INTERVAL

	end_inflight=$(get_inflight)
	end_stats=($(get_stats))
	end_bytes_read=${end_stats[0]}
	end_bytes_written=${end_stats[1]}
	end_read_ops=${end_stats[2]}
	end_write_ops=${end_stats[3]}

	# Calculate the average queue depth over the interval
	avg_queue_depth=$(((start_inflight + end_inflight) / 2))

	# Calculate average request sizes
	if [ "$((end_read_ops - start_read_ops))" -eq 0 ]; then
		avg_read_request_size=0
	else
		avg_read_request_size=$(((end_bytes_read - start_bytes_read) / (end_read_ops - start_read_ops)))
	fi

	if [ "$((end_write_ops - start_write_ops))" -eq 0 ]; then
		avg_write_request_size=0
	else
		avg_write_request_size=$(((end_bytes_written - start_bytes_written) / (end_write_ops - start_write_ops)))
	fi

	# Append the results to the output file
	echo "$(date) - Average Queue Depth over $INTERVAL seconds: $avg_queue_depth, Average Read Request Size: $avg_read_request_size bytes, Average Write Request Size: $avg_write_request_size bytes" >>"$OUTPUT_FILE"
done
