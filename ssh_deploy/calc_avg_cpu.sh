#!/bin/bash

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <mpstat_output_file>"
	exit 1
fi

mpstat_output_file="$1"

# Check if the specified file exists
if [ ! -f "$mpstat_output_file" ]; then
	echo "Error: File not found: $mpstat_output_file"
	exit 1
fi

# Calculate the averages
total_usr=0
total_sys=0
total_iowait=0
total_idle=0

num_samples=0

while read -r line; do
	if [[ "$line" == *" all "* && ! "$line" == "Average"* ]]; then
		num_samples=$((num_samples + 1))
		usr_line=$(echo "$line" | awk '{gsub(",", ".", $4); print $4}')
		sys_line=$(echo "$line" | awk '{gsub(",", ".", $6); print $6}')
		iowait_line=$(echo "$line" | awk '{gsub(",", ".", $7); print $7}')
		idle_line=$(echo "$line" | awk '{gsub(",", ".", $13); print $13}')

		total_usr=$(awk "BEGIN {print $total_usr + $usr_line}")
		total_sys=$(awk "BEGIN {print $total_sys + $sys_line}")
		total_iowait=$(awk "BEGIN {print $total_iowait + $iowait_line}")
		total_idle=$(awk "BEGIN {print $total_idle + $idle_line}")
	fi
done <"$mpstat_output_file"

# Calculate the averages
avg_usr=$(awk "BEGIN {print $total_usr / $num_samples}")
avg_sys=$(awk "BEGIN {print $total_sys / $num_samples}")
avg_iowait=$(awk "BEGIN {print $total_iowait / $num_samples}")
avg_idle=$(awk "BEGIN {print $total_idle / $num_samples}")

echo "Average User Time: $avg_usr%"
echo "Average System Time: $avg_sys%"
echo "Average I/O Wait Time: $avg_iowait%"
echo "Average Idle Time: $avg_idle%"
