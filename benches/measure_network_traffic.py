# network output calculator
# By providing 2 files containing the ethtool -s ${RDMA_NIC} output, this scripts calculates
# and produces the diff on ethtool counters. If the diff between two counters is 0, the counter is skipped
# and not shown in the output
import argparse

def calculate_diff_in_counters(starting_counters_list, ending_counters_list):
    #its asserted that the lists have the same size
    i = 0
    for (counter_name, counter_value) in starting_counters_list:
        if counter_value == ending_counters_list[i][1]:
            i+=1
            continue
        diff_in_counters = int(ending_counters_list[i][1]) - int(counter_value)
        print(counter_name + " " + str(diff_in_counters))
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
    parser.add_argument("counters_before", type=str, help="starting ethtool output file to be parsed")
    parser.add_argument("counters_after", type=str, help="ending ethtool output file to be parsed")
    return parser.parse_args()

def count_lines(filename):
    file_with_counters = open(filename, "r")
    count = 0
    line = file_with_counters.readline()
    while line:
        count +=1
        line = file_with_counters.readline()

    file_with_counters.close()
    return count


def main():
    args = parse_arguments()
    num_of_lines_starting_counters_file = count_lines(args.counters_before)
    num_of_lines_ending_counters_file = count_lines(args.counters_after)
    if num_of_lines_starting_counters_file != num_of_lines_ending_counters_file:
        print("Ethtool outputs must be the same across the input files")
        exit()

    starting_counters_list = retrieve_counters(args.counters_before, num_of_lines_starting_counters_file)
    ending_counters_list = retrieve_counters(args.counters_after, num_of_lines_ending_counters_file)
    calculate_diff_in_counters(starting_counters_list, ending_counters_list)

if __name__ == "__main__":
    main()
