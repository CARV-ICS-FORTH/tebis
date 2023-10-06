#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 4 ]; then
	echo "Usage: $0 <directory_path> <device_name> <first_suffix> <second_suffix>"
	exit 1
fi

DIR_PATH="$1"
DEVICE_NAME="$2"
SUFFIX_1="$3"
SUFFIX_2="$4"

FILE_1="${DIR_PATH}/${DEVICE_NAME}_stats_${SUFFIX_1}.txt"
FILE_2="${DIR_PATH}/${DEVICE_NAME}_stats_${SUFFIX_2}.txt"

# Function to extract value from file based on a keyword
extract_value() {
	local file=$1
	local keyword=$2
	grep "$keyword" "$file" | awk -F': ' '{print $2}' | awk '{print $1}'
}

# Check if both files exist
if [[ ! -f "$FILE_1" || ! -f "$FILE_2" ]]; then
	echo "Error: One of the files doesn't exist."
	exit 1
fi

# Extract values for the first file
bytes_read_1=$(extract_value "$FILE_1" "Bytes Read")
bytes_written_1=$(extract_value "$FILE_1" "Bytes Written")
iops_read_1=$(extract_value "$FILE_1" "IOPs Read")
iops_write_1=$(extract_value "$FILE_1" "IOPs Write")

# Extract values for the second file
bytes_read_2=$(extract_value "$FILE_2" "Bytes Read")
bytes_written_2=$(extract_value "$FILE_2" "Bytes Written")
iops_read_2=$(extract_value "$FILE_2" "IOPs Read")
iops_write_2=$(extract_value "$FILE_2" "IOPs Write")

# Compute differences
diff_bytes_read=$((bytes_read_2 - bytes_read_1))
diff_bytes_written=$((bytes_written_2 - bytes_written_1))
diff_iops_read=$((iops_read_2 - iops_read_1))
diff_iops_write=$((iops_write_2 - iops_write_1))

# Report the differences
{
	echo "===== Differences for $DEVICE_NAME ====="
	echo "Difference in Bytes Read: $diff_bytes_read B"
	echo "Difference in Bytes Written: $diff_bytes_written B"
	echo "Difference in IOPs Read: $diff_iops_read"
	echo "Difference in IOPs Write: $diff_iops_write"
}
