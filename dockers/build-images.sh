#!/bin/bash

if cd dockerfiles; then
	echo "Changed directory to dockerfiles."
else
	echo "No dockerfiles dir"
	exit 1
fi

# Function to build images
build_images() {
	local script_name=$1
	local image_name=$2

	echo "Building $image_name images..."
	if "./$script_name"; then
		echo "Succeeded to build & push $image_name images."
	else
		echo "Failed to build $image_name images."
		exit 1
	fi
}

build_images "tebis-build-images.sh" "Tebis"

build_images "zookeeper-build-images.sh" "Zookeeper"

build_images "scripts-build-images.sh" "Scripts"

cd ..
echo "All images built successfully."
