#!/bin/bash

function remove_zookeeper() {
	echo "Zookeeper remove from Kubernetes..."
	if sudo kubectl delete -f zookeeper/zookeeper-deployment.yaml \
		-f zookeeper/zookeeper-service.yaml; then
		echo "Zookeeper remove complete."
	else
		echo "Zookeeper remove failed."
		exit 1
	fi
}

function remove_pvs() {
	echo "Zookeeper PVs remove from Kubernetes..."
	if sudo kubectl delete -f zookeeper/PVs/zookeeper-datalog-pvc.yaml \
		-f zookeeper/PVs/zookeeper-data-pvc.yaml \
		-f zookeeper/PVs/zookeeper-datalog-pv.yaml \
		-f zookeeper/PVs/zookeeper-data-pv.yaml; then
		echo "Zookeeper PVs remove complete."
	else
		echo "Zookeeper PVs remove failed."
		exit 1
	fi
}

function remove_tebis() {
	echo "Tebis remove from Kubernetes..."
	if sudo kubectl delete -f tebis/tebis-deployment-1.yaml \
		-f tebis/tebis-service-1.yaml \
		-f tebis/tebis-deployment-2.yaml \
		-f tebis/tebis-service-2.yaml; then
		echo "Tebis remove complete."
	else
		echo "Tebis remove failed."
		exit 1
	fi
}

# Flags to track which functions to call
DO_REMOVE_ZOOKEEPER=false
DO_REMOVE_PVS=false
DO_REMOVE_TEBIS=false

# Process input arguments
if [ "$#" -eq 0 ]; then
	DO_REMOVE_ZOOKEEPER=true
	DO_REMOVE_PVS=true
	DO_REMOVE_TEBIS=true
else
	for arg in "$@"; do
		case $arg in
		zoo)
			DO_REMOVE_ZOOKEEPER=true
			;;
		pv)
			DO_REMOVE_PVS=true
			;;
		tebis)
			DO_REMOVE_TEBIS=true
			;;
		*)
			echo "Invalid parameter: $arg. Use 'zoo', 'pv', 'tebis', or no parameter for all."
			exit 1
			;;
		esac
	done
fi

# Call functions based on flags
if [ "$DO_REMOVE_ZOOKEEPER" = true ]; then
	remove_zookeeper
fi

if [ "$DO_REMOVE_PVS" = true ]; then
	remove_pvs
fi

if [ "$DO_REMOVE_TEBIS" = true ]; then
	remove_tebis
fi
