# Throughtput calculator
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

def change_color(color):
    if color == colors.RED:
        color = colors.YELLOW
    elif color == colors.YELLOW:
        color = colors.BLUE
    elif color == colors.BLUE:
        color = colors.PURPLE
    elif color == colors.PURPLE:
        color = colors.LIGHT_BLUE
    elif color == colors.LIGHT_BLUE:
        color = colors.RED

    return color


def calculate_throughput(workload_name):
    total_throughput = 0
    dir = "./"
    for file in os.listdir(dir):
            if file.startswith("RESULT"):
                print(colors.YELLOW + file + colors.ENDC)
                result_ops_file = file + "/" + workload_name + "/ops.txt"
                ops_file = open(result_ops_file)
                for line in ops_file:
                    last_line = line
                print(colors.LIGHT_BLUE + last_line + colors.ENDC)
                last_line_str = last_line.split()
                total_throughput += float(last_line_str[2])
                ops_file.close()
    return total_throughput

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Execute calculator with appropriate parameters"
    )
    parser.add_argument("workload", type=str, help="throughput of a specific workload (load_a, run_a, run_b, run_c, run_d)")
    return parser.parse_args()

def main():
    args = parse_arguments()
    total_throughput = calculate_throughput(args.workload)
    print(colors.GREEN + "total throughput for workload " + args.workload + " is equal to " + str(total_throughput) + colors.ENDC)
    
if __name__ == "__main__":
    main()
