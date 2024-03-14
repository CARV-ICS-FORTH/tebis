sudo bash -c "for cpu in /sys/devices/system/cpu/cpu[0-15]*; do echo performance > $cpu/cpufreq/scaling_governor; done"
