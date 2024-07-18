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

# Flags to track which functions to call
DO_deploy_script=false
DO_deploy_TEBIS=false

# Process input arguments
if [ "$#" -eq 0 ]; then
	DO_deploy_script=true
	DO_deploy_TEBIS=true
else
	for arg in "$@"; do
		case $arg in
		script)
			DO_deploy_script=true
			;;
		tebis)
			DO_deploy_TEBIS=true
			;;
		*)
			echo "Invalid parameter: $arg. Use 'zoo', 'script', 'tebis', or no parameter for all."
			exit 1
			;;
		esac
	done
fi

if [ "$DO_deploy_script" = true ]; then
	build_images "scripts-build-images.sh" "Scripts"
fi

if [ "$DO_deploy_TEBIS" = true ]; then
	build_images "tebis-build-images.sh" "Tebis"
fi

cd ..
echo "All images built successfully."
