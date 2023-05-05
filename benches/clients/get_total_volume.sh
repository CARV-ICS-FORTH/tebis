#!/bin/bash

if [ $# -ne 2 ]; then
	echo 'Usage: get_total_volume.sh <diskstats before> <diskstats after>'
	exit 1
fi

DSTATS_BEFORE=$1
DSTATS_AFTER=$2

R_SEC_BEFORE_NVM=$(grep nvme0n1 ${DSTATS_BEFORE} | awk '{ print $6 }')

W_SEC_BEFORE_NVM=$(grep nvme0n1 ${DSTATS_BEFORE} | awk '{ print $10 }')

R_SEC_AFTER_NVM=$(grep nvme0n1 ${DSTATS_AFTER} | awk '{ print $6 }')

W_SEC_AFTER_NVM=$(grep nvme0n1 ${DSTATS_AFTER} | awk '{ print $10 }')

DIFF_SEC_READ=$(expr ${R_SEC_BEFORE_NVM} - ${R_SEC_AFTER_NVM})
DIFF_SEC_WRITE=$(expr ${W_SEC_BEFORE_NVM} - ${W_SEC_AFTER_NVM})

DIFF_BYTES_READ=$(expr ${DIFF_SEC_READ} \* 512)
DIFF_BYTES_WRITE=$(expr ${DIFF_SEC_WRITE} \* 512)

DIFF_MB_READ=$(expr ${DIFF_BYTES_READ} / 1024 / 1024)
DIFF_MB_WRITE=$(expr ${DIFF_BYTES_WRITE} / 1024 / 1024)

echo 'Total writes' ${DIFF_MB_WRITE} 'MBs'
echo 'Total reads' ${DIFF_MB_READ} 'MBs'
