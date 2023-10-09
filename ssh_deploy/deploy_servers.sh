#!/bin/bash

# Check if at least 2 arguments are provided
if [ "$#" -lt 2 ]; then
	echo "Usage: $0 <executable_path> <script_folder> [bastion_host] <server1> [<server2>...]"
	exit 1
fi

# Parse the arguments
EXECUTABLE_PATH="$1"
SCRIPT_FOLDER="$2"
shift 2 # Remove the first two arguments (executable_path and script_folder)

# Check if a bastion host is provided as an optional argument
if [ "$1" == "bastion_host" ]; then
	if [ -z "$2" ]; then
		echo "Error: Bastion host argument is empty."
		exit 1
	fi
	BASTION_HOST="$2"
	shift 2 # Remove bastion_host argument and its value
fi

echo "Bastion host = $BASTION_HOST"
# Iterate over all server arguments
for SERVER in "$@"; do
	# Transfer the executable to the server
	if [ -n "$BASTION_HOST" ]; then
		scp -o ProxyJump="$BASTION_HOST" "$EXECUTABLE_PATH" "$SERVER:"
	else
		scp "$EXECUTABLE_PATH" "$SERVER:"
	fi

	if [ $? -eq 0 ]; then
		echo "Successfully transferred $EXECUTABLE_PATH to $SERVER."
	else
		echo "Transfer of $EXECUTABLE_PATH to $SERVER failed."
	fi

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
