#!/bin/bash
for i in {16..23}; do
	echo 0 | sudo tee /sys/devices/system/cpu/cpu"$i"/online
done

for i in {24..31}; do
	echo 0 | sudo tee /sys/devices/system/cpu/cpu"$i"/online
done
