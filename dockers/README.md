# Kubernetes Implementation
Tebis and ZooKeeper implemented in K3s.

# Project Structure
## The following folders contain:
- **build-images.sh**: Script to build Docker images for the application.
- **deploy.sh**: Script to deploy the application in K3s.
- **dockerfiles**: Directory containing Dockerfiles and related scripts for building Docker images.
- **remove_sources.sh**: Script to remove the application from K3s.
- **tebis**: Kubernetes deployment and service configuration files for the Tebis component.
- **zookeeper**: Kubernetes deployment and service configuration files for the ZooKeeper component.
- **YCSB_CXX**: Kubernetes deployment files for Tebis Pod to run YCSB-CXX.
- **exec_tebis.sh**: Script to run Tebis inside the pod.

# Run Application
## Run Dependencies
To run the application, the following must be installed on your system:
* `K3s` - Lightweight Kubernetes (without VM)

### Installing K3s
To install K3s:

    curl -sfL https://get.k3s.io | sh -

## Run Configuration
Start the K3s daemon:

    sudo systemctl start k3s

Run deploy.sh (requires sudo access):

    ./deploy.sh [zoo] [tebis] [script] [ycsb]

- **./deploy.sh**: Deploys ZooKeeper, Tebis, YCSB and re-deploy script.
- **./deploy.sh zoo**: Deploys only ZooKeeper.
- **./deploy.sh tebis**: Deploys only Tebis.
- **./deploy.sh script**: Re-Deploys only Script.
- **./deploy.sh ycsb**: Deploys only YCSB.

Run exec_tebis.sh (requires sudo access):

    ./exec_tebis.sh <partial-pod-name> <device_name> <rdma> [-z <zkhost>] [-p <port>] [-t <threads>]

- **partial-pod-name**: Partial pod-name, ex. "tebis-1".
- **device_name**: Device file name path, ex. "/mnt/nvme/par.dat".
- **rdma**: RDMA subnet, ex. "192.168.5".
- **-z <zkhost>**: ZooKeeper Entrypoint, default(zk-pod): "zk-cs:2181".
- **-p <port>**: Tebis port, default: "8080".
- **-t <threads>**: Number of threads, default: "3".

## Run YCSB Tests
Configure hosts_file and regions_file and build the script image. Afterwards, deploy everything:

```
./deploy.sh
```

Run your Tebis pods:

```
./exec_tebis.sh tebis-1 /mnt/nvme/par.dat 192.168.5
```

Finally deploy YCSB job:

```
./deploy.sh ycsb
```

Start Tebis in the pod

## Stop Configuration
Run remove_sources.sh (requires sudo access):

    ./remove_sources.sh [zoo] [tebis] [ycsb]

- **./remove_sources.sh**: Removes ZooKeeper, PVs, and Tebis.
- **./remove_sources.sh zoo**: Removes only ZooKeeper.
- **./remove_sources.sh tebis**: Removes only Tebis.
- **./remove_sources.sh ycsb**: Removes only YCSB.

>You can use any combination of these arguments.

Stop the K3s daemon:

    sudo systemctl stop k3s

## Test changes

To test changes in the code within a Kubernetes environment, follow these steps:

1. **Create a Test Directory**:

   - Navigate to the `dockerfiles/dockerfile-tebis/` directory.
   - Create a new directory named `test`.

2. **Copy and Modify Files**:

   - Within the `test` directory, replicate the directory structure of the files you intend to change.
   - Create the corresponding directory structure inside `test`
   - Copy the files you want to change into this directory
   - Make your changes to the copied files.
   - Finally, the overwrite_files script will do the rest.

   >For example, if you want to modify `master.c` located in `tebis_server/master/`, so make `test/tebis_server/master/` and copy `master.c` there.
