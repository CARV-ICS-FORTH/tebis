#!/bin/bash -xe

TEBIS_PATH="/home1/public/orchiot/tebis"
CLIENT_HOSTS=""
SERVER_HOST=""
SERVER_PORT=25565

printf "bash arguments = %d\n" $#

while [ $# -gt 0 ]; do
	case "$1" in
	--server)

		if [[ -z $2 ]]; then
			printf "no server host provided\n"
			exit 1

		fi

		SERVER_HOST="$2"
		shift 2
		;;
	--clients)

		if [[ -z $2 ]]; then
			printf "no client host(s) provided\n"
			exit 1

		fi
		CLIENT_HOSTS="$2"
		shift 2
		;;
	--help)
		printf "Usage: $(basename $0) [--server shost] [--clients chosts]\n"
		shift
		;;
	*)
		printf "Usage: $(basename $0) [--server shost] [--clients chosts]\n"
		exit 1
		;;
	esac
done

### Launching Server ###

# ./tcp-server --bind 192.168.2.122 --port 25565 --threads 16 --file /tmp/nvme/orestis.dat
ssh ${SERVER_HOST} -- "(cd ${TEBIS_PATH}/build/tcp_server/ && ${TEBIS_PATH}/build/tcp_server/tcp-server --bind 192.168.2.12${SERVER_HOST//[!0-9]/} --port ${SERVER_PORT} --threads 16 --file /tmp/nvme/orestis.dat) &"
#SERVER_PID=$!
#
printf "\033[1;31mserver is running...\033[0m\n"

### Launching Clients ###

YCSB_CLIENT_COMMAND=$TEBIS_PATH"/build/YCSB-CXX/ycsb-tcp -threads 16 -w l -e execution_plan.txt"
YCSB_CLIENT_RESULT_PATH=$TEBIS_PATH"/build/YCSB-CXX/RESULTS/"
CLIENT_PID_ARRAY=()

for client in ${CLIENT_HOSTS}; do
	ssh ${client} -- "(${YCSB_CLIENT_COMMAND} -outFile ${host}_ops.txt && scp ${host}:${YCSB_CLIENT_RESULT_PATH}/${host}_ops.txt ./total_results)" &
	CLIENT_PID_ARRAY+=($!)
	#CLIENT_PID_ARRAY[${CLIENT_PID_ARRAY[@]}]=...
done

for pid in "${CLIENT_PID_ARRAY[@]}"; do
	wait $pid
done

### Terminating Server ###

kill ${SERVER_PID}

### Now, all results from clients are stored to total_results/ (load_a, run_a etc)
