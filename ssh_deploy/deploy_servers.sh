#!/bin/bash

# Check if at least 1 argument is provided
if [ "$#" -lt 1 ]; then
	echo "Usage: $0 <script_folder> [-b bastion_host] <server1> [<server2>...]"
	exit 1
fi

# Parse the arguments
SCRIPT_FOLDER="$1"
shift # Remove the first argument (script_folder)

# Check if a bastion host is provided as an optional argument
if [ "$1" == "-b" ]; then
	if [ -z "$2" ]; then
		echo "Error: Bastion host argument is empty."
		exit 1
	fi
	BASTION_HOST="$2"
	shift 2 # Remove -b flag and its value
fi

echo "Bastion host = $BASTION_HOST"

# Iterate over all server arguments
for SERVER in "$@"; do
	# Transfer all .sh scripts from the specified folder to /tmp on the remote machine
	if [ -n "$BASTION_HOST" ]; then
		scp -o ProxyJump="$BASTION_HOST" "$SCRIPT_FOLDER"/*.sh "$SERVER:/tmp"
	else
		scp "$SCRIPT_FOLDER"/*.sh "$SERVER:/tmp"
	fi

	if [ $? -eq 0 ]; then
		echo "Successfully transferred .sh scripts to $SERVER:/tmp."
	else
		echo "Transfer of .sh scripts to $SERVER:/tmp failed."
	fi
done

echo "Deployment done."
