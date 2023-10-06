#!/bin/bash

# Check if at least 3 arguments are provided
if [ "$#" -lt 3 ]; then
	echo "Usage: $0 <executable> <path> <server1> [<server2>...]"
	exit 1
fi

EXECUTABLE="$1"
DEST_PATH="$2"
shift
shift # Remove first two arguments to get only the list of servers

# Iterate over all server arguments
for SERVER in "$@"; do
	echo "Transferring $EXECUTABLE to $SERVER:$DEST_PATH ..."
	scp "$EXECUTABLE" "$SERVER:$DEST_PATH"
	if [ $? -eq 0 ]; then
		echo "Successfully transferred to $SERVER."
	else
		echo "Transfer to $SERVER failed."
	fi
done

echo "Deployment done."
