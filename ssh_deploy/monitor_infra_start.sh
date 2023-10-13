#!/bin/bash -x

# Check the minimum required arguments
if [ "$#" -lt 3 ]; then
	echo "Usage: $0 <device_name> <net_interface> <host1> [<host2> ...]"
	exit 1
fi

# Assign the first two arguments
device_name="$1"
net_interface="$2"
shift 2 # Remove these two arguments

# Function to execute the commands on the remote host
execute_on_host() {
	local host="$1"

	# Create/clean stats directory
	ssh "$host" "mkdir -p /tmp/stats && rm -rf /tmp/stats/*"

	# Execute NVMe counters script

	ssh "$host" "/tmp/dump_nvme_counters.sh \"$device_name\" /tmp/stats start"

	# Execute network counters script
	ssh "$host" "/tmp/dump_net_counters.sh \"$net_interface\" /tmp/stats start"

	# Start CPU monitoring script in the background
	ssh "$host" "nohup /tmp/monitor_cpu.sh /tmp/stats > /dev/null 2>&1 &"
}

# Iterate over each host and execute the function
for host in "$@"; do
	execute_on_host "$host"
done

echo "Monitoring started on all specified hosts."
