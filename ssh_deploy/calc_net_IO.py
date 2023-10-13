import sys

def parse_file(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
    timestamp = int(lines[0].strip())
    counters = {}
    # print (f"Timestap is {timestamp}")
    for line in lines[2:]:
        parts = line.strip().split(":")
        key = parts[0].strip()
        value = int(parts[1].strip())
        counters[key] = value
        # print (f"key: {key} is {value}")

    return timestamp, counters

def main():
    if len(sys.argv) != 3:
        print("Usage: script.py <start_file> <stop_file>")
        sys.exit(1)

    start_file = sys.argv[1]
    stop_file = sys.argv[2]

    start_timestamp, start_counters = parse_file(start_file)
    stop_timestamp, stop_counters = parse_file(stop_file)

    time_difference = stop_timestamp - start_timestamp
    print(f"Time elapsed: {time_difference} seconds")

    print("Counter Differences (stop - start):")
    for key in start_counters:
        if key in stop_counters:
            difference = stop_counters[key] - start_counters[key]
            # print(f"{key}: {difference}")

    # Calculate throughput for specific counters
    if 'tx_prio_3_bytes' in start_counters and 'tx_prio_3_bytes' in stop_counters:
        tx_diff = (stop_counters['tx_prio_3_bytes'] - start_counters['tx_prio_3_bytes']) / (1024*1024)  # Convert from B to MB
        tx_throughput = tx_diff / time_difference  # MB/s
        print(f"Transmit Throughput (tx_prio_3_bytes): {tx_throughput:.2f} MB/s")

    if 'rx_prio_3_bytes' in start_counters and 'rx_prio_3_bytes' in stop_counters:
        rx_diff = (stop_counters['rx_prio_3_bytes'] - start_counters['rx_prio_3_bytes']) / (1024*1024)  # Convert from B to MB
        rx_throughput = rx_diff / time_difference  # MB/s
        print(f"Receive Throughput (rx_prio_3_bytes): {rx_throughput:.2f} MB/s")
    # Calculate packets per second
    tx_pps = (stop_counters['tx_prio_3_packets'] - start_counters['tx_prio_3_packets']) / time_difference
    rx_pps = (stop_counters['rx_prio_3_packets'] - start_counters['rx_prio_3_packets']) / time_difference

    # Print results
    print(f"Transmit Packets/s: {tx_pps:.2f} pps")
    print(f"Receive Packets/s: {rx_pps:.2f} pps")

if __name__ == "__main__":
    main()
