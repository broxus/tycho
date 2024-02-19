# network sim
## Prerequisites

Before you start using `network_sim.py`, ensure you have the following installed on your system:
- Python 3.6 or later
- Docker
- Docker Compose

or 

- podman
- podman-compose


##  Usage

To start the network simulation with the default configuration:
```
./scripts/network_sim.py  start
```

To build the Docker image used in the simulation:
```
./scripts/network_sim.py  build
```

To execute a specific command in a node:
```
./scripts/network_sim.py  exec <node-name> <command>
```

To stream logs from a specific node:
```
./scripts/network_sim.py  logs --node <node-index>
```
To follow the log output
```
./scripts/network_sim.py  logs --node <node-index> -f
```

To add a new node to the already running network simulation:
```
./scripts/network_sim.py  add-node
```