# Throughtput calculator
import argparse
import os

def calculate_throughput(workload_name):
    total_throughput = 0
    dir = "./"
    for file in os.listdir(dir):
            if file.startswith("RESULT"):
                print(file)
                result_ops_file = file + "/" + workload_name + "/ops.txt"
                ops_file = open(result_ops_file)
                for line in ops_file:
                    last_line = line
                print(last_line)
                last_line_str = last_line.split()
                total_throughput += int(last_line_str[2])
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
    print("total throughput for workload " + args.workload + "is equal to " + str(total_throughput))
    
if __name__ == "__main__":
    main()
