#!/bin/bash -xe

TEBIS_PATH="/home1/public/orchiot/tebis"
CLIENT_HOSTS=""
SERVER_HOST=""

printf "bash arguments = %d\n" $#

while [ $# -gt 0 ];
do
	case "$1" in
		--server)
			
			if [[ -z $2 ]];
			then
				printf "no server host provided\n"
				exit 1

			fi

			SERVER_HOST="$2"
			shift 2
			;;
		--clients)

			if [[ -z $2 ]];
			then
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

ssh ${SERVER_HOST} -- ${TEBIS_PATH}/build/tcp_server/tcp-server &
SERVER_PID=$!

### Launching Clients ###

YCSB_CLIENT_COMMAND=$TEBIS_PATH"/build/ycsb-tcp -threads 16 -w l -e execution_plan.txt" 
YCSB_CLIENT_RESULT_PATH=$TEBIS_PATH"/build/YCSB-CXX/RESULTS/"
CLIENT_PID_ARRAY=()

exit 0

for client in ${CLIENT_HOSTS};
do
	ssh ${client} -- "(${YCSB_CLIENT_COMMAND} -outFile ${host}_ops.txt && scp ${host}:${YCSB_CLIENT_RESULT_PATH}/${host}_ops.txt ./total_results)" &
	CLIENT_PID_ARRAY+=($!)
	#CLIENT_PID_ARRAY[${CLIENT_PID_ARRAY[@]}]=...
done

for pid in ${CLIENT_PID_ARRAY[@]};
do
	wait $pid
done

### Terminating Server ###

kill ${SERVER_PID}

### Now, all results from clients are stored to total_results/ (load_a, run_a etc)
