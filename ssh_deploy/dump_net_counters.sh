#!/bin/bash

if [ "$#" -ne 3 ]; then
	echo "Usage: $0 <net_interface> <folder> <suffix>"
	exit 1
fi

net_interface="$1"
folder="$2"
suffix="$3"
output_file="${folder}/net_${net_interface}_${suffix}"

# Dump timestamp
date '+%s' >"$output_file"

# Append network counters
/usr/sbin/ethtool -S "$net_interface" >>"$output_file"
