#!/bin/bash

DEFAULT_ZKHOST="zk-cs:2181"
DEFAULT_PORT=8080
DEFAULT_THREADS=3

usage() {
	echo "Usage: $0 <partial-pod-name> <device_name> <rdma> [-z <zkhost>] [-p <port>] [-t <threads>]"
	exit 1
}

if [ $# -lt 3 ]; then
	usage
fi

PARTIAL_POD_NAME=$1
DEVNAME=$2
RDMA=$3
shift 3

ZKHOST=$DEFAULT_ZKHOST
PORT=$DEFAULT_PORT
THREADS=$DEFAULT_THREADS

while getopts ":z:p:t:" opt; do
	case ${opt} in
	z)
		ZKHOST=$OPTARG
		;;
	p)
		PORT=$OPTARG
		;;
	t)
		THREADS=$OPTARG
		;;
	\?)
		usage
		;;
	esac
done

# Get the full pod name matching the partial pod name
POD_NAME=$(sudo kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "$PARTIAL_POD_NAME" | head -n 1)

if [ -z "$POD_NAME" ]; then
	echo "No pod found with name containing '$PARTIAL_POD_NAME'"
	exit 1
fi

COMMAND=("./build/tebis_server/tebis_server" "-d" "$DEVNAME" "-z" "$ZKHOST" "-r" "$RDMA" "-p" "$PORT" "-c" "$THREADS")

sudo kubectl exec -it "$POD_NAME" -- "${COMMAND[@]}"
