#!/bin/bash

function deploy_zookeeper() {
	echo "Zookeeper deploy from Kubernetes..."
	if sudo kubectl apply -f zookeeper/zookeeper.yaml; then
		echo "Zookeeper deploy complete."
	else
		echo "Zookeeper deploy failed."
		exit 1
	fi
}

function deploy_script() {
	echo "Zookeeper Script..."
	if sudo kubectl delete job.batch/zookeeper-init-job && sudo kubectl apply -f zookeeper/zookeeper.yaml; then
		echo "Zookeeper Scripts re-deploy complete."
	else
		echo "Zookeeper Scripts re-deploy failed."
		exit 1
	fi
}

function deploy_tebis() {
	echo "Tebis deploy from Kubernetes..."
	if sudo kubectl apply -f tebis/tebis-1.yaml \
		-f tebis/tebis-2.yaml; then
		echo "Tebis deploy complete."
	else
		echo "Tebis deploy failed."
		exit 1
	fi
}

function deploy_ycsb() {
	echo "YCSB deploy from Kubernetes..."
	if sudo kubectl apply -f YCSB_CXX/ycsb-pod.yaml; then
		echo "YCSB deploy complete."
	else
		echo "YCSB deploy failed."
		exit 1
	fi
}

# Flags to track which functions to call
DO_deploy_ZOOKEEPER=false
DO_deploy_script=false
DO_deploy_TEBIS=false
DO_deploy_YCSB=false

# Process input arguments
if [ "$#" -eq 0 ]; then
	DO_deploy_ZOOKEEPER=true
	DO_deploy_TEBIS=true
	DO_deploy_YCSB=true
else
	for arg in "$@"; do
		case $arg in
		zoo)
			DO_deploy_ZOOKEEPER=true
			;;
		script)
			DO_deploy_script=true
			;;
		tebis)
			DO_deploy_TEBIS=true
			;;
		ycsb)
			DO_deploy_YCSB=true
			;;
		*)
			echo "Invalid parameter: $arg. Use 'zoo', 'script', 'tebis', 'ycsb', or no parameter for all."
			exit 1
			;;
		esac
	done
fi

# Call functions based on flags
if [ "$DO_deploy_ZOOKEEPER" = true ]; then
	deploy_zookeeper
fi

if [ "$DO_deploy_script" = true ]; then
	deploy_script
fi

if [ "$DO_deploy_TEBIS" = true ]; then
	deploy_tebis
fi

if [ "$DO_deploy_YCSB" = true ]; then
	deploy_ycsb
fi
