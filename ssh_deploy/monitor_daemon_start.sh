#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
	echo "Usage: $0 <nvme_device_name> <path_to_save_directory> <file_suffix>"
	exit 1
fi

NVME_DEVICE="$1"
PATH_TO_SAVE_DIRECTORY="$2"
FILE_SUFFIX="$3"

# First, dump the NVMe counters
./dump_nvme_counters.sh "$NVME_DEVICE" "$PATH_TO_SAVE_DIRECTORY" "$FILE_SUFFIX"

# Start the NVMe queue depth monitoring script in the background
./monitor_nvme_queue_depth.sh "$NVME_DEVICE" "$PATH_TO_SAVE_DIRECTORY" &
# Start the CPU monitoring script in the background
./monitor_cpu.sh "$PATH_TO_SAVE_DIRECTORY" &
