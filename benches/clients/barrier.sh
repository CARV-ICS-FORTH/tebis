#!/bin/bash

if [[ $1 == "--help" || $1 == "-h" ]]; then
	echo "Usage: $0 <processes> <process_id>"
	echo "    Process id has to be [1..processes]"
	exit 0
elif [[ $# -ne 2 ]]; then
	echo "Usage: $0 <processes> <process_id>"
	echo "    Process id has to be [1..processes]"
	exit 1
fi

peers=$1
me=$2

echo "There are $peers clients. I am client $me"

mkdir -p .barrier
touch .barrier/wait_$me # FIXME if the file exists wait until it doesn't and then create it

waiting=$(ls -l .barrier/wait_* | wc -l)
while [ $waiting -ne $peers ]; do
	sleep 1
	waiting=$(ls -l .barrier/wait_* | wc -l)
done

touch .barrier/pass_$me

if [[ $me -eq 1 ]]; then
	waiting=$(ls -l .barrier/pass_* | wc -l)
	while [ $waiting -ne $peers ]; do
		sleep 1
		waiting=$(ls -l .barrier/pass_* | wc -l)
	done
	rm -r .barrier
fi

exit 0
