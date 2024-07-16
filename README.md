# Tebis
Tebis is a persistent LSM key value (KV) store desinged for fast storage devices and RDMA networks. Tebis uses two main technologies :
 1.  Hybrid KV placement via its [Parallax](https://github.com/CARV-ICS-FORTH/parallax) LSM KV store to reduce I/O amplification and increase CPU efficiency.
  2.  Index shipping via CPU efficient RDMA bulk network transfers to reduce the compaction overhead in replicas. Instead of repeating the compaction in replicas primary ships the index which replicas rewrite to be valid for their storage address space.

More details can be found in the Eurosys '22 paper [Tebis: Index Shipping for Efficient Replication in LSM Key-Value Stores](https://dl.acm.org/doi/abs/10.1145/3492321.3519572).

# Project structure
## The following folders contain
- YCSB-CXX contains the C++ version of the YCSB benchmark along with a Tebis driver
- tebis_rdma contains code rdma utilities used in the project
- tebis_server contains all the server related code
- tebis_rdma_client contains the client side code of Tebis
- File  tebis_rdma_client/tebis_rdma_client.h  contains the public API of the client API
- tcp_server contains the code for a standalone tcp_server over Parallax, it is a separate cmake project. Detailed information are in the tcp_server folder README.md
- tcp_client contains the code for the TCP client

# Building Tebis
**Note: It has been tested with gcc 10.1.0**

## Build Dependencies

To build Tebis, the following libraries have to be installed on your system:
* `libnuma` - Allocations with NUMA policy
* `libibverbs` - Infiniband verbs
* `librdmacm` - RDMA Connection Manager
* `libzookeeper_mt` - Zookeeper client bindings for C

For Mellanox cards, the Infiniband and RDMA libraries are included in the software package provided by the vendor.
Additionally, Tebis uses cmake for its build system and the gcc and g++ compilers for its compilation.

### Installing Dependencies on Ubuntu 18.04 LTS

Tebis requires CMake version >= 3.11.0. On Ubuntu, you need to add the
following repository to get the latest stable version of CMake:

	wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | sudo apt-key add -
	sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main'
	sudo apt update

Run the following command with superuser privileges:

	sudo apt install libnuma-dev libibverbs-dev librdmacm-dev libzookeeper-mt-dev

For the build tools and compiler:

	sudo apt install cmake build-essential

### Installing Depedencies on Centos/RHEL 7

Tebis requires CMake version >= 3.11.0. On Centos/RHEL this is supplied from the
EPEL repository and can be installed with:

	sudo yum install cmake3 kernel-devel gcc-c++


#### Dependencies for Single Node Tebis

	sudo yum install numactl-devel boost-devel


For RDMA:

	sudo yum install libibverbs-devel librdmacm-devel

You also need to install ZooKeeper. Ready-made packages are available from
Cloudera.
1. Add Cloudera's Centos 7 repository as described
[here](https://docs.cloudera.com/documentation/enterprise/5-14-x/topics/cdh_ig_cdh5_install.html)
2. Install the Zookeeper C binding for clients:

	    yum install zookeeper-native

## Build Configuration

Compilation is done using the clang compiler, provided by the clang package in
most Linux distributions. To configure the build system of Tebis and build it run
the commands:

	mkdir build
	cd build
	cmake .. -DTEBIS_FORMAT=ON
	make

On Centos/RHEL 7, replace the `cmake` command with the `cmake3` command supplied
from the EPEL package of the same name.

In the case of the standalone tcp-server you need to allocate a file for Parallax via the command


`fallocate -l <size in GB>G <file name>`

Then, you need to initialize it with the kv_format tool of Parallax

`<BUILD_FOLDER>_deps/parallax-build/lib/kv_format.parallax --device <file name> --max_regions_num <number of regions>`

Finally, execute

`<BUILD_FOLDER>/tcp_server/tcp-server -t <threads num> -b <IP address> -p <port number> -f <file name> -L0 <L0 size in MB> -GF <growth factor>`



## Build Configuration Parameters

The CMake scripts provided support two build configurations; "Release" and
"Debug". The Debug configuration enables the "-g" option during compilation to
allow debugging. The build configuration can be defined as a parameter to the
cmake call as follows:

	cmake3 .. -DCMAKE_BUILD_TYPE="Debug|Release"

The default build configuration is "Debug".

The "Release" build disables warnings and enables optimizations.

## Build Targets
* build/tebis_server/libtebis_client.a - Client library for applications to interact with Tebis
* build/tebis_server/tebis_server - The executable of Tebis server
* build/YCSB-CXXX/ycsb-async-tebis - YCSB that uses Tebis as its storage



# Static Analyzer

Install the clang static analyzer with the command:

	sudo pip install scan-build

Before running the analyzer, make sure to delete any object files and
executables from previous build by running in the root of the repository:

	rm -r build

Then generate a report using:

	scan-build --intercept-first make

The last line of the above command's output will mention the folder where the
newly created report resides in. For example:

	"scan-build: Run 'scan-view /tmp/scan-build-2018-09-05-16-21-31-978968-9HK0UO'
	to examine bug reports."

To view the report you can run the above command, assuming you have a graphical
environment or just copy the folder mentioned to a computer that does and open
the index.html file in that folder.

<!-- Development in cluster

For development in the cluster append the paths below in your PATH environment variable:

	export PATH=/archive/users/gxanth/llvm-project/build/bin:$PATH
	export PATH=/archive/users/gxanth/git/bin:$PATH
	export PATH=/archive/users/gxanth/gcc-9.1/bin:$PATH
	export LD_LIBRARY_PATH=/archive/users/gxanth/gcc-9.1/lib64:$LD_LIBRARY_PATH
	export PATH=$PATH:/archive/users/gxanth/go/bin
	export PATH=/archive/users/gxanth/shellcheck-stable:$PATH
	export PATH=$PATH:/archive/users/gxanth/go/bin
	export PATH=$PATH:$HOME/go/bin
	export CC=gcc-9.1
	export CXX=g++-9.1-->

<!--# Install shfmt

To install shfmt run the command below in your shell:

	GO111MODULE=on go get mvdan.cc/sh/v3/cmd/shfmt


# Generating compile_commands.json for Tebis

Install compdb for header awareness in compile_commands.json:

	pip install --user git+https://github.com/Sarcasm/compdb.git#egg=compdb

After running cmake .. in the build directory run:

	cd ..
	compdb -p build/ list > compile_commands.json
	mv compile_commands.json build
	cd tebis
	ln -sf ../build/compile_commands.json-->

# Pre commit hooks using pre-commit

<!--Add the path below to your PATH environment variable:

    	PATH=/archive/users/gxanth/git/bin:$PATH-->

To install pre-commit:

	pip install pre-commit --user
	pre-commit --version
	2.2.0

If the above command does not print 2.2.0 you need to update python using:

	sudo yum update python3

Then try upgrading pre-commit:

	pip install -U pre-commit --user

To install pre-commit hooks:

	cd tebis
	pre-commit install
    pre-commit install --hook-type commit-msg

If everything worked as it should then the following message should be printed:
	pre-commit installed at .git/hooks/pre-commit

If you want to run a specific hook with a specific file run:

	pre-commit run hook-id --files filename
	pre-commit run cmake-format --files CMakeLists.txt

## Running Tebis
Tebis uses RDMA for all network communication, which requires support from the
network interface to run. A software implementation (soft-RoCE) exists and can
run on all network interfaces.

### Enabling soft-RoCE
soft-RoCE is part of the mainline Linux kernel versions since version 4.9
through the `rdma_rxe` kernel module. To enable it for a network adapter the
following steps are required:

#### 1. Install dependencies
The `ibverbs-utils` and `rdma-core` packages are required to enable soft-RoCE.
These packages should be in most distirbutions' repositories

##### Installing Dependencies on Ubuntu 18.04 LTS
Run the following command form a terminal with root access (or use sudo):
```
# apt install ibverbs-utils rdma-core perftest
```

##### Enable soft-RoCE
To enable soft-RoCE on a network command run the following commands with
superuser privileges:
```
rxe_cfg start
rxe_cfg add eth_interface
```
where `eth_interface` is the name of an ethernet network adapter interface. To
view available network adapters run `ip a`.

The command `rxe_cfg start` has to be run at every boot to use RDMA features.

##### Verify soft-RoCE is working
To verify that soft-RoCE is working, we can run a simple RDMA Write throuhgput
benchmark.

First, open two shells, one to act as the server and one to act as the client.
Then run the following commands:
* On the server: `ib_write_bw`
* On the client: `ib_write_bw eth_interface_ip`, where `eth_interface_ip` is
the IP address of a soft-RoCE enabled ethernet interface.

Example output:
* Server process:
```
************************************
* Waiting for client to connect... *
************************************
---------------------------------------------------------------------------------------
                    RDMA_Write BW Test
 Dual-port       : OFF		Device         : rxe0
 Number of qps   : 1		Transport type : IB
 Connection type : RC		Using SRQ      : OFF
 CQ Moderation   : 100
 Mtu             : 1024[B]
 Link type       : Ethernet
 GID index       : 1
 Max inline data : 0[B]
 rdma_cm QPs	 : OFF
 Data ex. method : Ethernet
---------------------------------------------------------------------------------------
 local address: LID 0000 QPN 0x0011 PSN 0x3341fd RKey 0x000204 VAddr 0x007f7e1b8fa000
 GID: 00:00:00:00:00:00:00:00:00:00:255:255:192:168:122:205
 remote address: LID 0000 QPN 0x0012 PSN 0xbfbac5 RKey 0x000308 VAddr 0x007f70f5843000
 GID: 00:00:00:00:00:00:00:00:00:00:255:255:192:168:122:205
---------------------------------------------------------------------------------------
 #bytes     #iterations    BW peak[MB/sec]    BW average[MB/sec]   MsgRate[Mpps]
 65536      5000             847.44             827.84 		   0.013245
---------------------------------------------------------------------------------------
```

* Client process:
```
---------------------------------------------------------------------------------------
                    RDMA_Write BW Test
 Dual-port       : OFF		Device         : rxe0
 Number of qps   : 1		Transport type : IB
 Connection type : RC		Using SRQ      : OFF
 TX depth        : 128
 CQ Moderation   : 100
 Mtu             : 1024[B]
 Link type       : Ethernet
 GID index       : 1
 Max inline data : 0[B]
 rdma_cm QPs	 : OFF
 Data ex. method : Ethernet
---------------------------------------------------------------------------------------
 local address: LID 0000 QPN 0x0012 PSN 0xbfbac5 RKey 0x000308 VAddr 0x007f70f5843000
 GID: 00:00:00:00:00:00:00:00:00:00:255:255:192:168:122:205
 remote address: LID 0000 QPN 0x0011 PSN 0x3341fd RKey 0x000204 VAddr 0x007f7e1b8fa000
 GID: 00:00:00:00:00:00:00:00:00:00:255:255:192:168:122:205
---------------------------------------------------------------------------------------
 #bytes     #iterations    BW peak[MB/sec]    BW average[MB/sec]   MsgRate[Mpps]
 65536      5000             847.44             827.84 		   0.013245
---------------------------------------------------------------------------------------
```

Git commit message template
--------------------------------------------------------------------------------

	git config commit.template .git-commit-template



# Using cgroups to Limit Available Memory
You can use `cgroups` to limit the memory available to a process by running the
process using `systemd-run`. In addition to memory allocations, pages in the Linux
kernel's buffer cache count towards the `cgroups` memory limit. Example usage:
```
# systemd-run --unit=unit0 --scope --slice=slice0 --property MemoryLimit=16G <command>
```
Starting a `systemd` unit requires root privileges. The above example will limit
the memory available to a command (including pages in the buffer cache) to 16GB.

# Running Tebis on a two server machine configuration
First we need a Zookeeper server. For simplicity we assume that the Zookeeper service runs at zoo:2181. Then we
need to initialize Tebis metadata. This can be done through the command
<tebis_root_folder>/scripts/kreonR/tebis_zk_init.py <hosts_file> <regions_file> <zookeeper_host>

- **Hosts_file:** Contains the servers of the cluster in the form <host1:port_for_incoming_rdma_connections:0> <role leader or empty>
 Example:
- sith2.cluster.ics.forth.gr:8080:0 leader (so sith2.cluster.ics.forth.gr:8080 will be the initial leader of the system)
- sith3.cluster.ics.forth.gr:8080:0
- sith6.cluster.ics.forth.gr:8080:0
-**Regions file** Contains the region info in which we split the key space
<region_id> <min_key_range> <max_key_range> <server1:port:1 (primary)> <server2:port:1 (backup)>
*Example of regions file*

0 -oo MM sith2.cluster.ics.forth.gr:8080:1 sith3.cluster.ics.forth.gr:8080:1
1 MM  ZZ sith3.cluster.ics.forth.gr:8080:1 sith6.cluster.ics.forth.gr:8080:1
2 ZZ +oo sith6.cluster.ics.forth.gr:8080:1 sith2.cluster.ics.forth.gr:8080:1

In each tebis server we need a allocated file where Tebis will store its data. Each server's storage capacity will be equal
to the size of the file provided. The server will create its own file (or use dd or fallocate).
Example
fallocate -l 100G /path/to/file

Then we need to boot first the leader of the Tebis rack
<tebis_build_root folder>/tebis_server/tebis_server -d <path to tebis file> -z <zk_host:zk_port> -r <RDMA IP subnet> -p <server port>
-c <num of threads> [-t <LSM L0 size in keys>] [-g <growth factor>] [-i <"send_index" | "build_index">] [-s <device size in GB>]

example: build/tebis_server/tebis_server -d /nvme/par1.dat -z sith2:2181 -r 192.168.4 -p 8080 -c 3
example: build/tebis_server/tebis_server -d /nvme/par1.dat -z sith2:2181 -r 192.168.4 -p 8080 -c 3 -t 16 -g 10 -i send_index -s 100

# Tests
cd into folder <BUILD_ROOT_FOLDER>/tests/ and type
test_krc_api zk_host:zk_port

## Acknowledgements
We thankfully acknowledge the support of the European Commission under the Horizon 2020 Framework Programme for Research and Innovation through the projects EVOLVE (Grant Agreement ID: 825061). This work is (also) partly supported by project EUPEX, which has received funding from the European High-Performance Computing Joint Undertaking (JU) under grant agreement No 101033975. The JU receives support from the European Union's Horizon 2020 re-search and innovation programme and France, Germany, Italy, Greece, United Kingdom, Czech Republic, Croatia.
