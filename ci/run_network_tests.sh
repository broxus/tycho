#!/usr/bin/env bash

set -euo pipefail

# Constants
NODE_COUNT=3
STABILIZE_CHECK_ATTEMPTS=30
TEST_IMAGE="ghcr.io/broxus/tycho-tests/tycho-tests-destroyable:latest"
GIVER_BALANCE="10000000000000"
KEYS_FILE=".temp/giver_keys.json"
SAVE_LOGS=true

# Cleanup function for background processes
cleanup() {
    echo "Cleaning up..."
    pkill tycho -9 || true
    rm -rf .temp
}

# Register cleanup trap
trap cleanup EXIT

# Go to project root
cd "$(dirname "$0")/.."

mkdir -p .temp

just init_zerostate_config || true
just init_node_config || true
echo "Generating network configuration..."

args=()
${SAVE_LOGS} && args+=(--save-logs)
[ -n "$KEYS_FILE" ] && args+=(--save-giver-keys "$KEYS_FILE")
[ -n "$GIVER_BALANCE" ] && args+=(--giver-balance "$GIVER_BALANCE")

if ! cargo xtask gen-network --force "${args[@]}" "$NODE_COUNT"; then
    echo "Failed to generate network configuration"
    exit 1
fi

echo "Starting ${NODE_COUNT} Tycho nodes..."
for ((i=1; i<=NODE_COUNT; i++)); do
    just node "$i" >/dev/null 2>&1 &
done

echo "Waiting for giver keys to be generated..."
timeout=60
while [ ! -f "$KEYS_FILE" ] && [ $timeout -gt 0 ]; do
    sleep 1
    ((timeout--))
done
[[ -f "$KEYS_FILE" ]] || { echo "Giver keys file missing"; exit 1; }

echo "Initializing environment..."
GIVER_ADDRESS="-1:$(jq -r '.accounts | keys[0]' .temp/zerostate.json)"
GIVER_SECRET=$(jq -r '.secret' "$KEYS_FILE")

export CI_JRPC_ENDPOINT="http://127.0.0.1:8001"
export CI_GIVER_ADDRESS="$GIVER_ADDRESS"
export CI_GIVER_KEY="$GIVER_SECRET"

echo "Waiting for node readiness..."
for _ in $(seq 1 ${STABILIZE_CHECK_ATTEMPTS}); do
    if curl -sSf "${CI_JRPC_ENDPOINT}" >/dev/null; then
        break
    fi
    sleep 2
done

echo "Running network tests..."
# Set container command based on environment
CONTAINER_CMD="docker"
if [ -n "${CI:-}" ]; then
    CONTAINER_CMD="podman"
    echo "Using podman in CI environment..."
fi

$CONTAINER_CMD pull "$TEST_IMAGE"
$CONTAINER_CMD run --network host \
    -e CI_JRPC_ENDPOINT \
    -e CI_GIVER_ADDRESS \
    -e CI_GIVER_KEY \
    -e NETWORK=ci \
    "$TEST_IMAGE"

exit $?
