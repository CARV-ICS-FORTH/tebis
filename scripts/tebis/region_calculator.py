#!/bin/python3

import sys

letters = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

def generate_combinations(letters):
    combinations = []
    for l1 in letters:
        for l2 in letters:
            combinations.append(l1 + l2)
    return combinations

def divide_alphabet(n_regions):
    combinations = generate_combinations(letters)
    regions = []

    # If only one region, it spans the entire range
    if n_regions == 1:
        return [('-oo', '+oo')]

    # Start key for the first region is always '-oo'
    regions.append(('-oo', combinations[0]))

    # Define middle regions
    for i in range(1, n_regions - 1):  # -1 because we are manually adding the first and last regions
        start = combinations[i - 1]
        end = combinations[i]
        regions.append((start, end))

    # End key for the last region is always '+oo'
    regions.append((combinations[n_regions - 2], '+oo'))

    return regions

def assign_servers(hosts, n_replicas, n_regions):
    distinct_hostnames = len(set([host.split(":")[0] for host in hosts]))

    if distinct_hostnames < n_replicas + 1:  # +1 for the primary
        print(f"Error: Not enough distinct hostnames. Need at least {n_replicas + 1} but found {distinct_hostnames}.")
        sys.exit(1)

    regions = divide_alphabet(n_regions)
    assignments = []

    for i, (start, end) in enumerate(regions):
        primary_host = hosts[i % len(hosts)]
        replicas = [primary_host]

        if n_replicas > 0:
            j = 1
            while len(replicas) < n_replicas + 1:
                next_host = hosts[(i + j) % len(hosts)]
                if next_host.split(':')[0] not in [replica.split(':')[0] for replica in replicas]:
                    replicas.append(next_host)
                j += 1

        assignments.append((i, start, end, replicas))

    return assignments

def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <n_regions> <n_replicas> <path_to_hostnames_file>")
        sys.exit(1)

    n_regions = int(sys.argv[1])
    n_replicas = int(sys.argv[2])
    if n_replicas < 0:
        print("Replica number must be >= 0.")
        sys.exit(1)

    with open(sys.argv[3], 'r') as f:
        hosts = [line.strip() for line in f.readlines()]

    assignments = assign_servers(hosts, n_replicas, n_regions)

    with open("regions", "w") as out_file:
        for region_id, start, end, replicas in assignments:
            replicas_str = ' '.join(replicas)
            line = f"{region_id} {start} {end} {replicas_str}"
            print(line)  # Print to screen
            out_file.write(line + "\n")  # Write to file

    print("Regions and replicas have also been written to 'regions'.")

if __name__ == "__main__":
    main()
