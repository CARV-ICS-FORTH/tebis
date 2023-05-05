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

def calculate_diff_in_counters(starting_counters_list, ending_counters_list):
    #its asserted that the lists have the same size
    i = 0
    for (counter_name, counter_value) in starting_counters_list:
        if counter_value == ending_counters_list[i][1]:
            i+=1
            continue
        diff_in_counters = int(ending_counters_list[i][1]) - int(counter_value)
        print(colors.GREEN + counter_name + " " + str(diff_in_counters) + colors.ENDC)
        i+=1

def fetch_counter_name(line):
    str_line_list = line.split()
    return str_line_list[0]

def fetch_counter_value(line):
    str_line_list = line.split()
    return str_line_list[1]


def retrieve_counters(filename, num_of_lines_in_file):
    file_with_counters = open(filename, "r")
    skip_first_line = True
    counters_list = []
    count = 0
    line = file_with_counters.readline()
    while line:
        if skip_first_line == True:
            skip_first_line = False
            continue
        if count == num_of_lines_in_file -1:
            #this is the last null line, exit
            break

        line = file_with_counters.readline()
        counter_name = fetch_counter_name(line)
        counter_value = fetch_counter_value(line)
        counters_list.append((counter_name, counter_value))
        count +=1
    file_with_counters.close()
    return counters_list


# create an argparser for the calculator
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Execute calculator with appropriate parameters"
    )
    parser.add_argument("workload", type=str, help="workload to acquire the network traffic (load_a, run_a, run_b, run_c, run_d")
    return parser.parse_args()

def count_lines(filename):
    file = open(filename, "r")
    count = 0
    line = file.readline()
    while line:
        count +=1
        line = file.readline()

    file.close()
    return count

def main():
    args = parse_arguments()
    dir = "./"
    for file in os.listdir(dir):
        if file.startswith("STATS") or file.startswith("clients_group"):
            print(colors.YELLOW + file + colors.ENDC)
            counters_start = file + "/" + args.workload + "/counters_start"
            counters_end = file + "/" + args.workload + "/counters_end"
            num_of_lines_starting_counters_file = count_lines(counters_start)
            num_of_lines_ending_counters_file = count_lines(counters_end)
            if num_of_lines_starting_counters_file != num_of_lines_ending_counters_file:
                print("Ethtool outputs must be the same across the input files")
                exit()

            starting_counters_list = retrieve_counters(counters_start, num_of_lines_starting_counters_file)
            ending_counters_list = retrieve_counters(counters_end, num_of_lines_ending_counters_file)
            calculate_diff_in_counters(starting_counters_list, ending_counters_list)

if __name__ == "__main__":
    main()
