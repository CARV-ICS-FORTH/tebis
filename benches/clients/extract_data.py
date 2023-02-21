#!/bin/python3
""" Parse YCSB and server stastistics to generate a report."""
# authors: Michalis Vardoulakis

import os
import sys
import copy
import json

workload_bytes_per_op = {
  "s": 29,
  "m": 119,
  "l": 1219,
  "sd": 0.6*30 + 0.2*120 + 0.2*1020,
  "md": 0.2*30 + 0.6*120 + 0.2*1020,
  "ld": 0.2*30 + 0.2*120 + 0.2*1020,
  "sd40": 0.4*30 + 0.3*120 + 0.3*1020,
  "sd80": 0.8*30 + 0.1*120 + 0.1*1020
}

cycles_sithx = 2.4 * 10**9
cores_sithx = 32
cycles_jedix = 2.1 * 10**9
cores_jedix = 24
cycles_tiex = 2.0 * 10**9
cores_tiex = 32

server_stats_prefix = "STATS-"
ycsb_ops_prefix = "RESULTS_"

workload_labels = {
  "load_a": "Workload A Load",
  "run_a": "Workload A Run",
  "run_b": "Workload B Run",
  "run_c": "Workload C Run",
  "run_d": "Workload D Run"
}

workload_res_dirs = []
server_stats_dirs = []
server_info = {}
results = {}

JSON_OUTPUT = False

def get_iostat_data(server, workload_dir):
  server_name = server[0]
  server_device = server[1]
  server_stats_folder = os.path.join(server[2], workload_dir)

  iostat_file = ""
  ls = os.listdir(server_stats_folder)
  for f in ls:
    if f.startswith("iostat"):
      iostat_file = os.path.join(server_stats_folder, f)
      break

  if not os.path.isfile(iostat_file):
    print("Error: could not find iostat output file for server " + server_name)
    sys.exit(1)

  samples = 0
  samples_dev = 0
  cpu_user = .0
  cpu_system = .0
  cpu_iowait = .0
  cpu_idle = .0
  dev_rkb = .0
  dev_wkb = .0
  dev_avgrq_sz = .0
  dev_avgqu_sz = .0
  dev_util = .0
  cpu_per_second = []
  dev_per_second = []
  with open(iostat_file, "r") as f:
    lines = f.readlines()
    for i in range(len(lines)):
      line = lines[i]
      if line.startswith("avg-cpu"):
        cols = lines[i + 1].split()
        cpu_user += float(cols[0])
        cpu_system += float(cols[2])
        cpu_iowait += float(cols[3])
        cpu_idle += float(cols[5])
        cpu_per_second.append(100 - float(cols[5]) - float(cols[3]))
        samples += 1
      elif line.startswith(server_device):
        cols = line.split()
        dev_rkb += float(cols[5])
        dev_wkb += float(cols[6])
        dev_avgrq_sz += float(cols[7])
        dev_avgqu_sz += float(cols[8])
        dev_util += float(cols[-1])
        dev_per_second.append(float(cols[-1]))
        samples_dev += 1

  assert samples == samples_dev
  cpu_user /= samples
  cpu_system /= samples
  cpu_iowait /= samples
  cpu_idle /= samples
  cpu_util = 100 - cpu_idle - cpu_iowait

  dev_rkb /= samples
  dev_wkb /= samples
  dev_avgrq_sz /= samples
  dev_avgqu_sz /= samples
  dev_util /= samples

  cpu = {
    'util': cpu_util,
    'user': cpu_user,
    'system': cpu_system,
    'iowait': cpu_iowait,
    'idle': cpu_idle#,
    # 'util_per_second': cpu_per_second
  }

  dev = {
    'rkb': dev_rkb,
    'wkb': dev_wkb,
    'avgrq_sz': dev_avgrq_sz,
    'avgqu_sz': dev_avgqu_sz,
    'util': dev_util#,
    # 'util_per_second': dev_per_second
  }

  return cpu, dev

def add_to_overall(overall, cpu, device, network):
  for key in cpu:
    if key == "util_per_second":
      continue
    elif key in overall["cpu"]:
      overall["cpu"][key] += cpu[key]
    else:
      overall["cpu"][key] = cpu[key]
  for key in device:
    if key == "util_per_second":
      continue
    elif key in overall["device"]:
      overall["device"][key] += device[key]
    else:
      overall["device"][key] = device[key]
  for key in network:
    if key not in overall["network"]:
      overall["network"][key] = network[key]
    else:
      overall["network"][key] += network[key]

def average_overall_cpu_device(overall, servers_len):
  for key in overall["cpu"]:
    overall["cpu"][key] /= servers_len
  for key in overall["device"]:
    if key == "mb_written" or key == "mb_read":
      continue
    overall["device"][key] /= servers_len
  for key in overall["network"]:
    overall["network"][key] /= servers_len

def get_diskstats(server, workload):
  ls = os.listdir(os.path.join(server[2], workload))
  before_filename = None
  after_filename = None
  for f in ls:
    if f.startswith("diskstats-before"):
      before_filename = os.path.join(server[2], workload, f)
    elif f.startswith("diskstats-after"):
      after_filename = os.path.join(server[2], workload, f)
  if before_filename is None or after_filename is None:
    return 0, 0
  with open(before_filename, "r") as f:
    for line in f.readlines():
      cols = line.split()
      if cols[2] == server[1]:
        sectors_read_before = int(cols[5])
        sectors_written_before = int(cols[9])
        break
  with open(after_filename, "r") as f:
    for line in f.readlines():
      cols = line.split()
      if cols[2] == server[1]:
        sectors_read_after = int(cols[5])
        sectors_written_after = int(cols[9])
        break
  return sectors_read_after - sectors_read_before, sectors_written_after - sectors_written_before

def generate_report(working_dir, workload_type, record_count, operations, servers):
  """Generate report"""
  global workload_res_dirs
  workload_res_dirs = os.listdir(os.path.join(working_dir, server_stats_prefix + servers[0][0]))
  workload_res_dirs.sort()
  workload_res_dirs.sort(key=lambda x: x.split('_')[1])
  bytes_load = workload_bytes_per_op[workload_type] * record_count
  bytes_run = workload_bytes_per_op[workload_type] * operations

  server_stats_folders, ycsb_folders = get_stats_and_result_dirs(working_dir, servers)
  # Create list of tuples (server, device, stats folder)
  server_with_stats_folder = list(map(lambda x: (x[0][0], x[0][1], x[1]), zip(servers, server_stats_folders)))
  stats = {}
  for workload in workload_res_dirs:
    stats[workload] = {}
    stats[workload]["overall"] = {}
    overall_stats = stats[workload]["overall"]
    overall_stats["ops"] = calculate_ops(ycsb_folders, workload)
    overall_stats["latency"] = calculate_latency(ycsb_folders, workload)
    overall_stats["cpu"] = {}
    overall_stats["device"] = {}
    overall_stats["network"] = {}
    cycles_per_second = 0
    for server in server_with_stats_folder:
      cpu, device = get_iostat_data(server, workload)
      network = get_network_data(server, workload)
      stats[workload][server[0]] = {}
      stats[workload][server[0]]["cpu"] = cpu
      stats[workload][server[0]]["device"] = device
      sectors_read, sectors_written = get_diskstats(server, workload)
      stats[workload][server[0]]["device"]["mb_read"] = (sectors_read * 512) / (2**20)
      stats[workload][server[0]]["device"]["mb_written"] = (sectors_written * 512) / (2**20)
      stats[workload][server[0]]["network"] = network
      add_to_overall(overall_stats, cpu, device, network)
      if server[0].startswith("jedi"):
        cycles_per_second += stats[workload][server[0]]["cpu"]["util"] * cycles_jedix * cores_jedix / 100
      elif server[0].startswith("sith"):
        cycles_per_second += stats[workload][server[0]]["cpu"]["util"] * cycles_sithx * cores_sithx / 100
      elif server[0].startswith("tie"):
        cycles_per_second += stats[workload][server[0]]["cpu"]["util"] * cycles_tiex * cores_tiex / 100
      else:
        print("Warning: cycles per operation calculation is wrong! Cycles per second for server "
            + server[0] + " are not defined in the script", file=sys.stderr)

    overall_stats["cycles_per_op"] = cycles_per_second / overall_stats["ops"]
    if "load" in workload:
      overall_stats["io_amplification"] = (overall_stats["device"]["mb_written"] +
          overall_stats["device"]["mb_read"]) * 2**20 / bytes_load
      if overall_stats["network"] != {}:
        overall_stats["network"]["amplification"] = (overall_stats["network"]["tx_bytes"] +
            overall_stats["network"]["rx_bytes"]) / bytes_load
    else:
      overall_stats["io_amplification"] = (overall_stats["device"]["mb_written"] +
          overall_stats["device"]["mb_read"]) * 2**20 / bytes_run
      if overall_stats["network"] != {}:
        overall_stats["network"]["amplification"] = (overall_stats["network"]["tx_bytes"] +
            overall_stats["network"]["rx_bytes"]) / bytes_run
    if len(server_with_stats_folder) > 1:
      average_overall_cpu_device(overall_stats, len(servers))

  return stats

number = "{:9.2f}"

def print_workload(stats_workload, workload, servers):
  print(workload_labels[workload])
  print("------------------------------------------------")
  print("OVERALL (Average)")
  print(number.format(stats_workload["overall"]["ops"]) + "  Ops/sec")
  print(number.format(stats_workload["overall"]["cycles_per_op"]) + "  Cycles/op")
  print(number.format(stats_workload["overall"]["io_amplification"]) + " IO Amplification")
  print_stats(stats_workload["overall"])
  for server in servers:
    print()
    print(server[0] + " | Device: " + server[1])
    print_stats(stats_workload[server[0]])

def print_stats(stats):
  print(number.format(stats["cpu"]["util"]) + "% Average CPU Util")
  print(number.format(stats["cpu"]["user"]) + "% User CPU Util")
  print(number.format(stats["cpu"]["system"]) + "% System CPU Util")
  print(number.format(stats["cpu"]["iowait"]) + "% IO-Wait CPU Util")
  print(number.format(stats["cpu"]["idle"]) + "% Idle CPU Util")
  print(number.format(stats["device"]["mb_read"]) + "  MB Read (Total)")
  print(number.format(stats["device"]["mb_written"]) + "  MB Written (Total)")
  print(number.format(stats["device"]["util"]) + "% Device Utilization")
  print(number.format(stats["device"]["avgqu_sz"]) + "  Average Queue Depth")
  print(number.format(stats["device"]["avgrq_sz"]) + "  KB Average Request Size")

def get_stats_and_result_dirs(working_dir, servers):
  ycsb_folders = []
  ls = os.listdir(working_dir)
  # Find YCSB folders
  for f in ls:
    path = os.path.join(working_dir, f)
    if os.path.isdir(path) and f.startswith(ycsb_ops_prefix):
      ycsb_folders.append(path)
  # Calculate server stats folders
  server_stats_folders = []
  for server in servers:
    server_stats_folders.append(os.path.join(working_dir, server_stats_prefix + server[0]))
    if not os.path.isdir(server_stats_folders[-1]):
      print("Cannot find folder " + server_stats_folders[-1], file=sys.stderr)
      sys.exit(1)

  return server_stats_folders, ycsb_folders

def get_overall_ops(ycsb_ops_filename):
  with open(ycsb_ops_filename, "r") as f:
    for line in f.readlines():
      if line.startswith("[OVERALL]"):
        return float(line.split()[2])

def calculate_ops(ycsb_folders, workload):
  current_ops = 0
  for ycsb_folder in ycsb_folders:
    print(ycsb_folder + " " + workload + "LALA")
    current_ops += get_overall_ops(os.path.join(ycsb_folder, workload, "ops.txt"))
  return current_ops

def get_latency(ycsb_ops_filename):
  lat = {}
  with open(ycsb_ops_filename, "r") as f:
    for line in f.readlines():
      if line.startswith("[TAIL]"):
        if "[INSERT]" in line or "[LOAD]" in line:
          op = "INSERT"
        elif "[UPDATE]" in line:
          op = "UPDATE"
        elif "[READ]" in line:
          op = "READ"
        lat[op] = {}
        cols = line.split()[1:]
        for i in range(0, len(cols), 2):
          lat[op][cols[i]] = int(cols[i + 1])
  return lat

def calculate_latency(ycsb_folders, workload):
  overall_lat = {}
  for ycsb_folder in ycsb_folders:
    lat = get_latency(os.path.join(ycsb_folder, workload, "ops.txt"))
    for op in lat:
      if op not in overall_lat:
        overall_lat[op] = {"min": 0, "max": 0, "avg": 0, "lat50": 0, "lat70": 0, "lat90": 0, "lat99": 0,
            "lat999": 0, "lat9999": 0}
      for key in overall_lat[op]:
        overall_lat[op][key] += lat[op][key]
  for op in overall_lat:
    for key in overall_lat[op]:
      overall_lat[op][key] /= len(ycsb_folders)
  return overall_lat

def get_network_data(server, workload_dir):
  tx_packets = "tx_prio_3_packets" # Counter name for packets sent
  rx_packets = "rx_prio_3_packets" # Counter name for packets received
  tx_bytes = "tx_prio_3_bytes"
  rx_bytes = "rx_prio_3_bytes"
  server_name = server[0]
  server_stats_folder = os.path.join(server[2], workload_dir)

  network_stats_file = ""
  for f in os.listdir(server_stats_folder):
    if f.startswith("nicmon"):
      network_stats_file = os.path.join(server_stats_folder, f)
      break

  if not os.path.isfile(network_stats_file):
    print("Error: could not find network monitor output for server " + server_name, file=sys.stderr)
    return {}

  import pandas as pd
  network_stats = pd.read_csv(network_stats_file)
  avg_tx_pps = sum(network_stats[tx_packets])/len(network_stats[tx_packets])
  avg_rx_pps = sum(network_stats[rx_packets])/len(network_stats[rx_packets])
  total_tx_bytes = sum(network_stats[tx_bytes])
  total_rx_bytes = sum(network_stats[rx_bytes])
  tx_throughput = total_tx_bytes / len(network_stats[tx_bytes])
  rx_throughput = total_rx_bytes / len(network_stats[rx_bytes])
  return {
    "tx_pps": avg_tx_pps,
    "rx_pps": avg_rx_pps,
    "tx_bytes": total_tx_bytes,
    "rx_bytes": total_rx_bytes,
    "tx_throughput": tx_throughput,
    "rx_throughput": rx_throughput
  }

def print_usage(outfile):
  print("Usage: " + sys.argv[0] + " [OPTIONS] <working_dir> <workload_type> <record_count> "
      + "<operation_count> <server:device> [<server:device> ...]", file=outfile)
  print("Options:", file=outfile)
  print("  -j, --json print json object", file=outfile)
  print("  -h, --help print this message", file=outfile)

def parse_arguments(arglist):
  if len(arglist) < 2:
    print_usage(sys.stderr)
    sys.exit(1)

  argstart = 0
  while arglist[argstart].startswith("-"):
    if arglist[argstart] == "-j" or arglist[argstart] == "--json":
      global JSON_OUTPUT
      JSON_OUTPUT = True
    elif arglist[argstart] == "-h" or arglist[argstart] == "--help":
      print_usage(sys.stdout)
      sys.exit(0)
    argstart += 1

  working_dir = arglist[argstart]
  workload_type = arglist[argstart + 1]
  record_count = int(arglist[argstart + 2])
  operations = int(arglist[argstart + 3])
  servers = []
  for server_and_dev in arglist[argstart + 4:]:
    fields = server_and_dev.split(":")
    servers.append((fields[0], fields[1]))

  return working_dir, workload_type, record_count, operations, servers

# add parameters for cycles per second
if __name__ == '__main__':
  working_dir, workload_type, record_count, operations, servers = parse_arguments(sys.argv[1:])
  stats = generate_report(working_dir, workload_type, record_count, operations, servers)

  if not JSON_OUTPUT:
    for workload in workload_res_dirs:
      print_workload(stats[workload], workload, servers)
      print()
  else:
    json_object = json.dumps(stats)
    print(json_object)

  sys.exit(0)
