#!/usr/bin/env python3
import yaml
import subprocess
import os
import json


def generate_entrypoint_script(service_name, start_delay, params: str = ""):
    script_content = f"""#!/bin/bash
    # Introduce startup delay
    sleep {start_delay}
    export RUST_LOG="info,tycho_network=trace"
    /app/network-node {params}
    """
    script_path = f".scratch/entrypoints/{service_name}_entrypoint.sh"
    os.makedirs(os.path.dirname(script_path), exist_ok=True)
    with open(script_path, "w") as file:
        file.write(script_content)
    os.chmod(script_path, 0o755)  # Make the script executable


def generate_docker_compose(services):
    """
    Generates a Docker Compose file with specified services, IPs, and entrypoints.
    """
    compose_dict = {"version": "3.7", "services": {}}

    for service, details in services.items():
        compose_dict["services"][service] = {
            "image": details["image"],
            "entrypoint": "/entrypoints/entrypoint.sh",
            "volumes": [
                f"./entrypoints/{service}_entrypoint.sh:/entrypoints/entrypoint.sh",
                "./global-config.json:/app/global-config.json",
            ],
            "networks": {"default": {"ipv4_address": details["ip"]}},
        }

    networks_dict = {
        "networks": {"default": {"ipam": {"config": [{"subnet": "172.30.0.0/24"}]}}}
    }

    compose_dict.update(networks_dict)

    with open(".scratch/docker-compose.yml", "w") as file:
        yaml.dump(compose_dict, file)

    print("Docker Compose file and entrypoint scripts generated.")


def execute_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout


def run_docker_compose(services):
    """
    Runs the Docker Compose file and applies the specified start delays.
    """
    os.system("docker compose -f .scratch/docker-compose.yml up")

    # for service, details in services.items():
    #     latency = details.get("latency", 0)
    #     if latency:
    #         print(f"Applying {latency}ms latency to {service}...")
    #         # Assuming eth0 as the default network interface inside the container
    #         container_id = (
    #             subprocess.check_output(["docker", "ps", "-qf", f"name={service}"])
    #             .decode()
    #             .strip()
    #         )
    #         os.system(
    #             f"docker exec {container_id} tc qdisc add dev eth0 root netem delay {latency}ms"
    #         )

    print("Docker Compose services started with specified delays and latencies.")


def main():
    # Example input
    node_count = 5
    node_port = 25565

    services = {}
    bootstrap_peers = []

    for i in range(node_count):
        key = os.urandom(32).hex()
        ip = f"172.30.0.{i + 10}"

        cmd = (
            f"cargo run --example network-node -- gendht '{ip}:{node_port}' --key {key}"
        )
        dht_entry = json.loads(execute_command(cmd))
        bootstrap_peers.append(dht_entry)

        node_name = f"node-{i}"
        services[node_name] = {
            "image": "tycho-network",
            "ip": ip,
        }
        generate_entrypoint_script(
            node_name,
            start_delay=0,
            params=f"run '{ip}:{node_port}' --key {key} --global-config /app/global-config.json",
        )

    with open(".scratch/global-config.json", "w") as f:
        json.dump({"bootstrap_peers": bootstrap_peers}, f, indent=2)

    generate_docker_compose(services)
    print("To manually test the setup, run the following commands:")
    print("docker compose -f .scratch/docker-compose.yml up")
    run_docker_compose(services)


if __name__ == "__main__":
    main()
