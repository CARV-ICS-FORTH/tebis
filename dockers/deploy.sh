#!/bin/bash

function deploy_zookeeper() {
	echo "Zookeeper deploy from Kubernetes..."
	if sudo kubectl apply -f zookeeper/zookeeper-deployment.yaml \
		-f zookeeper/zookeeper-service.yaml; then
		echo "Zookeeper deploy complete."
	else
		echo "Zookeeper deploy failed."
		exit 1
	fi
}

function deploy_pvs() {
	echo "Zookeeper PVs deploy from Kubernetes..."
	if sudo kubectl apply -f zookeeper/PVs/zookeeper-datalog-pvc.yaml \
		-f zookeeper/PVs/zookeeper-data-pvc.yaml \
		-f zookeeper/PVs/zookeeper-datalog-pv.yaml \
		-f zookeeper/PVs/zookeeper-data-pv.yaml; then
		echo "Zookeeper PVs deploy complete."
	else
		echo "Zookeeper PVs deploy failed."
		exit 1
	fi
}

function deploy_tebis() {
	echo "Tebis deploy from Kubernetes..."
	if sudo kubectl apply -f tebis/tebis-deployment-1.yaml \
		-f tebis/tebis-service-1.yaml \
		-f tebis/tebis-deployment-2.yaml \
		-f tebis/tebis-service-2.yaml; then
		echo "Tebis deploy complete."
	else
		echo "Tebis deploy failed."
		exit 1
	fi
}

# Flags to track which functions to call
DO_deploy_ZOOKEEPER=false
DO_deploy_PVS=false
DO_deploy_TEBIS=false

# Process input arguments
if [ "$#" -eq 0 ]; then
	DO_deploy_ZOOKEEPER=true
	DO_deploy_PVS=true
	DO_deploy_TEBIS=true
else
	for arg in "$@"; do
		case $arg in
		zoo)
			DO_deploy_ZOOKEEPER=true
			;;
		pv)
			DO_deploy_PVS=true
			;;
		tebis)
			DO_deploy_TEBIS=true
			;;
		*)
			echo "Invalid parameter: $arg. Use 'zoo', 'pv', 'tebis', or no parameter for all."
			exit 1
			;;
		esac
	done
fi

# Call functions based on flags
if [ "$DO_deploy_ZOOKEEPER" = true ]; then
	deploy_zookeeper
fi

if [ "$DO_deploy_PVS" = true ]; then
	deploy_pvs
fi

if [ "$DO_deploy_TEBIS" = true ]; then
	deploy_tebis
fi
