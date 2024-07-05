#!/bin/bash

echo "Building Tebis images..."
if docker-compose -f dockerfile-tebis/docker-compose.yml up --build; then
	docker push dstath/tebis_test_bench:tebis-base
	docker push dstath/tebis_test_bench:tebis-zookeeper
	docker push dstath/tebis_test_bench:tebis-app

	echo "Tebis Base image built and pushed successfully."
else
	echo "Failed to build Tebis Base image."
	exit 1
fi
