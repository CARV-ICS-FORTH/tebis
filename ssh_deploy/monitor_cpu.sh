#!/bin/bash

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <file_dir>"
	exit 1
fi

file_dir="$1"

# Get the hostname
hostname=$(hostname)

# Generate the output filename
output_filename="cpu_${hostname}.txt"
output_path="${file_dir}/${output_filename}"

# Start mpstat in the background and save its output to the generated filename
mpstat 1 1200 >"$output_path" &

# Get the PID of the mpstat process
mpstat_pid=$!

# Sleep for 1200 seconds (20 minutes)
sleep 1200

# Kill the mpstat process
kill "$mpstat_pid"

echo "mpstat process killed. Output saved to: $output_path"

# Optionally, you can add additional cleanup or processing here
