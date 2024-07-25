#!/bin/bash

function remove_zookeeper() {
	echo "Zookeeper remove from Kubernetes..."
	if sudo kubectl delete -f zookeeper/zookeeper.yaml; then
		echo "Zookeeper remove complete."
	else
		echo "Zookeeper remove failed."
		exit 1
	fi
}

function remove_tebis() {
	echo "Tebis remove from Kubernetes..."
	if sudo kubectl delete -f tebis/tebis-1.yaml \
		-f tebis/tebis-2.yaml; then
		echo "Tebis remove complete."
	else
		echo "Tebis remove failed."
		exit 1
	fi
}

function remove_ycsb() {
	echo "YCSB remove from Kubernetes..."
	if sudo kubectl delete -f YCSB_CXX/ycsb-pod.yaml; then
		echo "YCSB remove complete."
	else
		echo "YCSB remove failed."
		exit 1
	fi
}

# Flags to track which functions to call
DO_REMOVE_ZOOKEEPER=false
DO_REMOVE_TEBIS=false
DO_REMOVE_YCSB=false

# Process input arguments
if [ "$#" -eq 0 ]; then
	DO_REMOVE_ZOOKEEPER=true
	DO_REMOVE_TEBIS=true
	DO_REMOVE_YCSB=true
else
	for arg in "$@"; do
		case $arg in
		zoo)
			DO_REMOVE_ZOOKEEPER=true
			;;
		tebis)
			DO_REMOVE_TEBIS=true
			;;
		ycsb)
			DO_REMOVE_YCSB=true
			;;
		*)
			echo "Invalid parameter: $arg. Use 'zoo', 'tebis', 'ycsb', or no parameter for all."
			exit 1
			;;
		esac
	done
fi

# Call functions based on flags
if [ "$DO_REMOVE_ZOOKEEPER" = true ]; then
	remove_zookeeper
fi

if [ "$DO_REMOVE_TEBIS" = true ]; then
	remove_tebis
fi

if [ "$DO_REMOVE_YCSB" = true ]; then
	remove_ycsb
fi
