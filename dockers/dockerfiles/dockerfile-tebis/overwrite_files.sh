#!/bin/bash

if [ ! -d "/usr/src/test" ]; then
	echo "The test directory does not exist."
	exit 1
fi

# Function to copy files recursively
copy_files() {
	local src_dir="$1"
	local dest_dir="$2"

	for file in "$src_dir"/*; do
		if [ -d "$file" ]; then
			dir_name=$(basename "$file")
			local dir_name
			mkdir -p "$dest_dir/$dir_name"
			copy_files "$file" "$dest_dir/$dir_name"
		else
			cp -f "$file" "$dest_dir"
		fi
	done
}

copy_files "/usr/src/test" "/usr/src/app"
