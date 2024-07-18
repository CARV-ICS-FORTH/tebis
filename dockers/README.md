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

    ./deploy.sh [zoo] [tebis] [script]

- **./deploy.sh**: Deploys ZooKeeper, PVs, and Tebis.
- **./deploy.sh zoo**: Deploys only ZooKeeper.
- **./deploy.sh tebis**: Deploys only Tebis.
- **./deploy.sh script**: Re-Deploys only Script.

>You can use any combination of these arguments.

## Stop Configuration
Run remove_sources.sh (requires sudo access):

    ./remove_sources.sh [zoo] [tebis]

- **./remove_sources.sh**: Removes ZooKeeper, PVs, and Tebis.
- **./remove_sources.sh zoo**: Removes only ZooKeeper.
- **./remove_sources.sh tebis**: Removes only Tebis.

>You can use any combination of these arguments.

Stop the K3s daemon:

    sudo systemctl stop k3s
