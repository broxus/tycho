#!/usr/bin/env python3
import yaml
import time
import subprocess
import os


def generate_entrypoint_script(service_name, start_delay, params: str = ""):
    script_content = f"""#!/bin/bash
    # Introduce startup delay
    sleep {start_delay}
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
        # Generate entrypoint script for each service
        generate_entrypoint_script(
            service, details.get("start_delay", 0), details.get("latency", 0)
        )

        compose_dict["services"][service] = {
            "image": details["image"],
            "entrypoint": f"/entrypoints/{service}_entrypoint.sh",
            "volumes": [
                f"./entrypoints/{service}_entrypoint.sh:/entrypoints/{service}_entrypoint.sh"
            ],
            "networks": {"default": {"ipv4_address": details["ip"]}},
        }

    networks_dict = {
        "networks": {"default": {"ipam": {"config": [{"subnet": "172.20.0.0/16"}]}}}
    }

    compose_dict.update(networks_dict)

    with open(".scratch/docker-compose.yml", "w") as file:
        yaml.dump(compose_dict, file)

    print("Docker Compose file and entrypoint scripts generated.")


def run_docker_compose(services):
    """
    Runs the Docker Compose file and applies the specified start delays.
    """
    os.system("docker compose up  -f .scratch/docker-compose.yml -d")

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
    services = {
        "node-1": {
            "image": "tycho-network",
            "ip": "172.20.0.2",
            "start_delay": 5,
            "latency": 100,
        },
        "node-2": {
            "image": "tycho-network",
            "ip": "172.20.0.3",
            "start_delay": 10,
            "latency": 50,
        },
    }

    generate_entrypoint_script("node-1", 5, "--help")
    generate_entrypoint_script("node-2", 10, "--help")
    generate_docker_compose(services)
    print("To manually test the setup, run the following commands:")
    print("docker-compose up -f .scratch/docker-compose.yml -d")
    run_docker_compose(services)


if __name__ == "__main__":
    main()
