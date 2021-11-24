#!/bin/python3

import sys
from os.path import exists as file_exists
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.security import OPEN_ACL_UNSAFE
import json

ROOT_PATH = "/kreonR"
SERVERS_PATH = ROOT_PATH + "/servers"
MAILBOX_PATH = ROOT_PATH + "/mailbox"
REGIONS_PATH = ROOT_PATH + "/regions"
LEADER_PATH = ROOT_PATH + "/leader"
ALIVE_LEADER_PATH = ROOT_PATH + "/alive_leader"
ALIVE_DATASERVERS_PATH = ROOT_PATH + "/alive_dataservers"


def parse_arguments():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print(
            "Usage: " + sys.argv[0] + " hosts_file regions_file [zookeeper_host]",
            file=sys.stderr,
        )
        sys.exit(1)

    hosts_file = sys.argv[1]
    regions_file = sys.argv[2]
    zk_host = "localhost:2181"
    if len(sys.argv) == 4:
        zk_host = sys.argv[3]

    # Check zk_host string
    parts = zk_host.split(":")
    if len(parts) != 2:
        print("E: zookeeper_host should be in IP:PORT format", file=sys.stderr)
        sys.exit(1)

    if not file_exists(hosts_file):
        print("E: hosts file " + hosts_file + " does not exist", file=sys.stderr)
        sys.exit(1)

    if not file_exists(regions_file):
        print("E: regions file " + regions_file + " does not exist", file=sys.stderr)
        sys.exit(1)

    return hosts_file, regions_file, zk_host


def main():
    hosts_file, regions_file, zk_host = parse_arguments()
    zk = KazooClient(hosts=zk_host)
    zk.start()
    if zk.state != KazooState.CONNECTED:
        print("E: Cannot connect to host " + zk_host, file=sys.stderr)

    # Cleanup existing data
    zk.delete(ROOT_PATH, -1, recursive=True)
    # Recreate node structure
    # kreonR
    # |- servers mailbox regions leader alive_leader alive_dataservers
    zk.create(ROOT_PATH, acl=OPEN_ACL_UNSAFE)
    zk.create(SERVERS_PATH, acl=OPEN_ACL_UNSAFE)
    zk.create(MAILBOX_PATH, acl=OPEN_ACL_UNSAFE)
    zk.create(REGIONS_PATH, acl=OPEN_ACL_UNSAFE)
    zk.create(LEADER_PATH, acl=OPEN_ACL_UNSAFE)
    zk.create(ALIVE_LEADER_PATH, acl=OPEN_ACL_UNSAFE)
    zk.create(ALIVE_DATASERVERS_PATH, acl=OPEN_ACL_UNSAFE)

    # Create server nodes
    leader = None
    with open(hosts_file, "r") as hosts:
        hosts_file_lines = hosts.readlines()
        hosts_file_lines = list(
            filter(lambda x: not x.startswith("#"), hosts_file_lines)
        )
        leader = hosts_file_lines[0].split()[0]
        for line in hosts_file_lines:
            cols = line.split()
            if len(cols) > 2 or len(cols) == 2 and cols[1].lower() != "leader":
                print(
                    "E: wrong line format in hosts file " + hosts_file, file=sys.stderr
                )
                print("Correct format is: hostname [leader]", file=sys.stderr)
                sys.exit(1)
            elif len(cols) == 2:
                assert cols[1].lower() == "leader"
                leader = cols[0]
            zk.create(MAILBOX_PATH + "/" + cols[0], acl=OPEN_ACL_UNSAFE)
            # build/kreon_server/create_server_node zk_host cols[0]
            server_path = SERVERS_PATH + "/" + cols[0]
            server_name = {
                "hostname": cols[0].split("-")[0],
                "dataserver_name": cols[0],
                "leader": "",
                "RDMA_IP_addr": "",
                "epoch": 0,
            }
            zk.create(
                server_path,
                acl=OPEN_ACL_UNSAFE,
                value=bytes(json.dumps(server_name), "utf8"),
            )

    # Create leader node
    zk.create(LEADER_PATH + "/" + leader, acl=OPEN_ACL_UNSAFE)

    # Create regions
    with open(regions_file, "r") as regions:
        regions_file_lines = regions.readlines()
        regions_file_lines = list(
            filter(lambda x: not x.startswith("#"), regions_file_lines)
        )
        for line in regions_file_lines:
            cols = line.split()
            if len(cols) < 4:
                print(
                    "E: wrong line format in regions file " + regions_file,
                    file=sys.stderr,
                )
                print(
                    "Correct format is: region_id min_key max_key primary [backup_1 backup_2 backup_n]",
                    file=sys.stderr,
                )
                sys.exit(1)

            region_entry = {
                "id": cols[0],
                "min_key": cols[1],
                "max_key": cols[2],
                "primary": cols[3],
                "backups": cols[4:] if len(cols) > 4 else [],
                "status": 2,  # KRM_FRESH
            }

            zk.create(
                REGIONS_PATH + "/" + region_entry["id"],
                acl=OPEN_ACL_UNSAFE,
                value=bytes(json.dumps(region_entry), "utf8"),
            )

    zk.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
