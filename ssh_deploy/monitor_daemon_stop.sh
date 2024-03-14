#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
	echo "Usage: $0 <nvme_device_name> <path_to_save_directory> <file_suffix>"
	exit 1
fi

pkill -9 monitor_nvme
pkill -9 mpstat
pkill -9 monitor_cpu

NVME_DEVICE="$1"
PATH_TO_SAVE_DIRECTORY="$2"
FILE_SUFFIX="$3"
OUTPUT_FILE="${PATH_TO_SAVE_DIRECTORY}/${NVME_DEVICE}_queue_depth.txt"

# First, dump the NVMe counters
./dump_nvme_counters.sh "$NVME_DEVICE" "$PATH_TO_SAVE_DIRECTORY" "$FILE_SUFFIX"

# Call the average queue script
./calc_nvme_avg_queue.sh "$OUTPUT_FILE"
hostname_var=$(hostname)
# Call the IO calculation script
./calc_nvme_IO.sh "$PATH_TO_SAVE_DIRECTORY" "$NVME_DEVICE" "start" "$FILE_SUFFIX" # Assuming "start" as first suffix
./calc_avg_cpu.sh "$PATH_TO_SAVE_DIRECTORY/cpu_$hostname_var.txt"
