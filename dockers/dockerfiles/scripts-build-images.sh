#!/bin/bash

IMAGE="dstath/tebis_test_bench:zookeeper-init-script"

SCRIPT_SRC="../../scripts/kreonR/tebis_zk_init.py"
SCRIPT_DEST="dockerfile-scripts/tebis_zk_init.py"

if [ ! -f "$SCRIPT_SRC" ]; then
	echo "Source script $SCRIPT_SRC not found!"
	exit 1
fi

cp "$SCRIPT_SRC" "$SCRIPT_DEST"

echo "Building ZooKeeper Init Script image..."
if docker build -t "$IMAGE" -f dockerfile-scripts/Dockerfile dockerfile-scripts/; then
	docker push "$IMAGE"
	echo "ZooKeeper Init Script image built and pushed successfully."
else
	echo "Failed to build ZooKeeper Init Script image."
	rm -f "$SCRIPT_DEST"
	exit 1
fi

rm -f "$SCRIPT_DEST"
