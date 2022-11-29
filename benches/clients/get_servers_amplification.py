import argparse
import os

class colors:
    RED = "\033[31m"
    ENDC = "\033[m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    PURPLE = "\033[35m"
    LIGHT_BLUE = "\033[36m"

# create an argparser for the calculator
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Execute calculator with appropriate parameters"
    )
    parser.add_argument("workload", type=str, help="workload to acquire the network traffic (load_a, run_a, run_b, run_c, run_d")
    return parser.parse_args()

def fetch_diskstats_files(dir):
    diskstats_before = ""
    diskstats_after = ""
    for file in os.listdir(dir):
        if file.startswith("diskstats-before"):
            diskstats_before = file
        if file.startswith("diskstats-after"):
            diskstats_after = file

    return diskstats_before, diskstats_after

def count_lines(filename):
    file = open(filename, "r")
    count = 0
    line = file.readline()
    while line:
        count +=1
        line = file.readline()

    file.close()
    return count

def fetch_device_name(line):
    str_line_list = line.split()
    return str_line_list[2]

def fetch_read_traffic_value(line):
    str_line_list = line.split()
    return str_line_list[5]

def fetch_write_traffic_value(line):
    str_line_list = line.split()
    return str_line_list[9]

def fetch_read_traffic(filename):
    file = open(filename, "r")
    num_of_lines_in_file = count_lines(filename)
    count  = 0
    line = file.readline()
    while line:
        if count == num_of_lines_in_file - 1:
            # this is the last null line, exit
            break
        # in tebis we have only 1 device per server, so we only care about the nvme
        # TODO: (geostyl) make this more robust
        if fetch_device_name(line) == "nvme0n1":
            file.close()
            return fetch_read_traffic_value(line)
        line = file.readline()

    print(colors.RED + "not an nvme device found for calcualting the amplification" + colors.ENDC)
    file.close()
    exit()

def fetch_write_traffic(filename):
    file = open(filename, "r")
    num_of_lines_in_file = count_lines(filename)
    count  = 0
    line = file.readline()
    while line:
        if count == num_of_lines_in_file - 1:
            # this is the last null line, exit
            break
        # in tebis we have only 1 device per server, so we only care about the nvme
        # TODO: (geostyl) make this more robust
        if fetch_device_name(line) == "nvme0n1":
            file.close()
            return fetch_write_traffic_value(line)
        line = file.readline()

    print(colors.RED + "not an nvme device found for calcualting the amplification" + colors.ENDC)
    file.close()
    exit()

def calculate_disk_read_traffic(diskstats_before, diskstats_after):
    read_traffic_before = fetch_read_traffic(diskstats_before)
    read_traffic_after = fetch_read_traffic(diskstats_after)
    
    diff = int(read_traffic_after) - int(read_traffic_before)
    diff_bytes = diff * 512
    diff_in_MB = diff_bytes / 1024 / 1024
    return diff_in_MB

def calculate_disk_write_traffic(diskstats_before, diskstats_after):
    write_traffic_before = fetch_write_traffic(diskstats_before)
    write_traffic_after = fetch_write_traffic(diskstats_after)
    
    diff = int(write_traffic_after) - int(write_traffic_before)
    diff_bytes = diff * 512
    diff_in_MB = diff_bytes / 1024 / 1024
    return diff_in_MB

def main():
    read_traffic = 0
    write_traffic = 0
    dir = "./"
    args = parse_arguments()
    for file in os.listdir(dir):
        if file.startswith("STATS"): #stats file for a specific server
            print(colors.YELLOW + file + colors.ENDC)
            diskstats_file_dir_path = file + "/" + args.workload + "/"

            diskstats_before, diskstats_after = fetch_diskstats_files(diskstats_file_dir_path)
            diskstats_before = diskstats_file_dir_path + diskstats_before
            diskstats_after = diskstats_file_dir_path + diskstats_after
            read_traffic += calculate_disk_read_traffic(diskstats_before, diskstats_after)
            write_traffic += calculate_disk_write_traffic(diskstats_before, diskstats_after)

    print(colors.GREEN + " read traffic is " + str(read_traffic) + colors.ENDC)
    print(colors.GREEN + " write traffic is " + str(write_traffic) + colors.ENDC)

if __name__ == "__main__":
    main()
