#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
	echo "Usage: $0 <nvme_device_name> <path_to_save_directory> <file_suffix>"
	exit 1
fi

DEVICE_NAME="$1"
SAVE_PATH="$2"
FILE_SUFFIX="$3"

# Construct the full path for saving the file
OUTPUT_FILE="${SAVE_PATH}/${DEVICE_NAME}_stats_${FILE_SUFFIX}.txt"

# Paths to kernel stats for the device
STATS_PATH="/sys/block/${DEVICE_NAME}/stat"
INFLIGHT_PATH="/sys/block/${DEVICE_NAME}/inflight"

# Ensure the paths exist
if [ ! -f "$STATS_PATH" ]; then
	echo "Error: Stats file for $DEVICE_NAME does not exist."
	exit 1
fi

if [ ! -f "$INFLIGHT_PATH" ]; then
	echo "Error: Inflight file for $DEVICE_NAME does not exist."
	exit 1
fi

# Extract values from stats file
read -r reads writes sectors_read sectors_written < <(awk '{ print $1, $5, $3, $7 }' "$STATS_PATH")

# Convert sectors to bytes (assuming 512 bytes per sector)
bytes_read=$((sectors_read * 512))
bytes_written=$((sectors_written * 512))

# Convert bytes to MB (assuming 1MB = 1048576 bytes)
mb_read=$(awk "BEGIN {printf \"%.2f\", $bytes_read / 1048576}")
mb_written=$(awk "BEGIN {printf \"%.2f\", $bytes_written / 1048576}")

# Get inflight values for IOPs
read -r inflight_read inflight_write <"$INFLIGHT_PATH"

# Assuming 512 bytes per sector for request size (this is a simplistic assumption)
request_size_read=$((sectors_read * 512 / (reads + 1)))      # +1 to avoid division by zero
request_size_write=$((sectors_written * 512 / (writes + 1))) # +1 to avoid division by zero

# Save the extracted data to the output file
{
	echo "Bytes Read: $bytes_read B ($mb_read MB)"
	echo "Bytes Written: $bytes_written B ($mb_written MB)"
	echo "IOPs Read: $inflight_read"
	echo "IOPs Write: $inflight_write"
	echo "Request Size Read (bytes): $request_size_read"
	echo "Request Size Write (bytes): $request_size_write"
} >"$OUTPUT_FILE"

echo "Stats saved to $OUTPUT_FILE"
