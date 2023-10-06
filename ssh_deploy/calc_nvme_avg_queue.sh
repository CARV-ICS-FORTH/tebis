#!/bin/bash

# Check for mandatory parameter
if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <path_to_output_file>"
	exit 1
fi

FILE_PATH="$1"

# Check if the provided file exists
if [ ! -f "$FILE_PATH" ]; then
	echo "Error: The provided file does not exist."
	exit 1
fi

# Extract queue depth values and compute their average
average_queue_depth=$(awk -F': ' '{sum += $3; count++} END {if(count > 0) print sum/count; else print 0}' "$FILE_PATH")

# Extract average read request size values and compute their average, then convert to KB
average_read_size=$(awk -F': ' '/Average Read Request Size/ {sum += $5; count++} END {if(count > 0) print sum/count/1024; else print 0}' "$FILE_PATH")

# Extract average write request size values and compute their average, then convert to KB
average_write_size=$(awk -F': ' '/Average Write Request Size/ {sum += $8; count++} END {if(count > 0) print sum/count/1024; else print 0}' "$FILE_PATH")

echo "Average Queue Depth: $average_queue_depth"
echo "Average Read Request Size: ${average_read_size} KB"
echo "Average Write Request Size: ${average_write_size} KB"
