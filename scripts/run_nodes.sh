#!/usr/bin/env bash

set -euo pipefail

NODE_COUNT=3
STABILIZE_DURATION=30
SAVE_GIVER_KEYS=""
GIVER_BALANCE=""
SAVE_LOGS=false

cleanup() {
    echo "Cleaning up..."
    pkill -f "just node" || true
    exit 0
}

trap cleanup INT

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --save-giver-keys)
            SAVE_GIVER_KEYS="$2"
            shift 2
            ;;
        --giver-balance)
            GIVER_BALANCE="$2"
            shift 2
            ;;
        --save-logs)
            SAVE_LOGS=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Generate network configuration
rm -rf .temp/*
args=()
$SAVE_LOGS && args+=(--save-logs)
[ -n "$SAVE_GIVER_KEYS" ] && args+=(--save-giver-keys "$SAVE_GIVER_KEYS")
[ -n "$GIVER_BALANCE" ] && args+=(--giver-balance "$GIVER_BALANCE")

if ! cargo xtask gen-network "${args[@]}" "$NODE_COUNT"; then
    echo "Failed to generate network."
    exit 1
fi

# Start nodes in background
for ((i=1; i<=NODE_COUNT; i++)); do
    just node "$i" > /dev/null 2>&1 &
done

echo "Nodes started in background"

echo "Nodes are running. Press Ctrl+C to stop."
sleep infinity