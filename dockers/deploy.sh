#!/bin/bash

echo "Zookeeper Deployment to Kubernetes..."
if sudo kubectl apply -f zookeeper/zookeeper-deployment.yaml \
	-f zookeeper/zookeeper-service.yaml; then
	echo "Zookeeper Deployment complete."
else
	echo "Zookeeper Deployment failed."
	exit 1
fi

echo "Zookeeper PVs to Kubernetes..."
if sudo kubectl apply -f zookeeper/PVs/zookeeper-datalog-pvc.yaml \
	-f zookeeper/PVs/zookeeper-data-pvc.yaml \
	-f zookeeper/PVs/zookeeper-datalog-pv.yaml \
	-f zookeeper/PVs/zookeeper-data-pv.yaml; then
	echo "Zookeeper PVs complete."
else
	echo "Zookeeper PVs failed."
	exit 1
fi

echo "Tebis Deployment to Kubernetes..."
if sudo kubectl apply -f tebis/tebis-deployment-1.yaml \
	-f tebis/tebis-service-1.yaml \
	-f tebis/tebis-deployment-2.yaml \
	-f tebis/tebis-service-2.yaml; then
	echo "Tebis Deployment complete."
else
	echo "Tebis Deployment failed."
	exit 1
fi
