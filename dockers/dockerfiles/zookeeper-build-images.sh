#!/bin/bash

ZOOKEEPER_IMAGE="dstath/tebis_test_bench:zookeeper"

# Build Docker images
echo "Building ZooKeeper image..."
if docker build -t $ZOOKEEPER_IMAGE -f dockerfile-zookeeper/Dockerfile.zookeeper dockerfile-zookeeper/; then
	docker push $ZOOKEEPER_IMAGE
	echo "ZooKeeper image built and pushed successfully."
else
	echo "Failed to build ZooKeeper image."
	exit 1
fi
