#!/usr/bin/env bash

set -euo pipefail

# Constants
NODE_COUNT=3
STABILIZE_DURATION=30

# Function to clean up and exit
cleanup() {
    echo "Cleaning up..."
    pkill -f "just node" || true
    exit 0
}

# Set a trap to call cleanup function on Ctrl+C
trap 'cleanup' INT

start_nodes() {
    # Remove temporary files
    rm -rf .temp/*

    # Generate network
    if ! just gen_network "$NODE_COUNT"; then
        echo "Failed to generate network."
        exit 1
    fi

    # Start nodes in background
    for i in $(seq 1 "$NODE_COUNT"); do
        just node "$i" > /dev/null 2>&1 &
    done

    echo "Waiting $STABILIZE_DURATION seconds for nodes to start and stabilize..."
    sleep "$STABILIZE_DURATION"
}

main() {
    start_nodes
    echo "Nodes are running. Press Ctrl+C to stop."
    
    # Wait indefinitely
    while true; do
        sleep 86400 &  # Sleep for 24 hours
        wait $!        # Wait for the sleep command to finish or be interrupted
    done
}

main "$@"

