#!/usr/bin/env bash

set -euo pipefail

# Constants
NODE_COUNT=3
STABILIZE_CHECK_ATTEMPTS=30
TEST_IMAGE="${1:-ghcr.io/broxus/tycho-tests/tycho-tests-destroyable:latest}"
GIVER_BALANCE="10000000000000"
VALIDATOR_BALANCE="100000"
VALIDATOR_STAKE="30000"

# Get the absolute path to the project directory
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEMP_DIR="${PROJECT_DIR}/.temp"
KEYS_FILE="$PROJECT_DIR/keys.json"

TYCHO_CLI=$("${PROJECT_DIR}/scripts/build-node.sh")
CONTAINER_CMD="docker"
[ -n "${CI:-}" ] && CONTAINER_CMD="podman"

cleanup() {
    echo "Cleaning up..."
    pkill tycho -9 || true
    rm -rf "${TEMP_DIR}" || true
}

init_node_configs() {
    echo "Updating node $1 configuration..."
    local i=$1
    mkdir -p "${TEMP_DIR}/logs$i"
    jq --arg logs_dir "${TEMP_DIR}/logs$i" \
        '.logger = {
            outputs: [
                { type: "Stderr" },
                {
                    type: "File",
                    dir: $logs_dir,
                    file_prefix: "node",
                    human_readable: true,
                    max_files: 1
                }
            ]
        } | .blockchain_rpc_client.too_new_archive_threshold = 1' \
        "${TEMP_DIR}/config$i.json" > "${TEMP_DIR}/tmpcfg" && mv "${TEMP_DIR}/tmpcfg" "${TEMP_DIR}/config$i.json"
}

wait_for_stabilization() {
    echo "Waiting for nodes to stabilize..."
    for _ in $(seq 1 ${STABILIZE_CHECK_ATTEMPTS}); do
        if curl -sSf "${CI_JRPC_ENDPOINT}" >/dev/null; then
            break
        fi
        sleep 2
    done
}

trap cleanup EXIT

# Main execution
mkdir -p "${TEMP_DIR}"
just init_zerostate_config --force || true
just init_node_config || true

# Setup giver account
giver_pk=$(jq -r '.public' "$KEYS_FILE")
giver_sk=$(jq -r '.secret' "$KEYS_FILE")
giver_account=$("${TYCHO_CLI}" tool gen-account wallet --pubkey "$giver_pk" --balance $GIVER_BALANCE)
jq --arg addr "$(jq -r '.account' <<< "$giver_account")" --arg boc "$(jq -r '.boc' <<< "$giver_account")" \
    '.accounts[$addr] = $boc' "${PROJECT_DIR}/zerostate.json" > "${TEMP_DIR}/zs.tmp"
mv "${TEMP_DIR}/zs.tmp" "${PROJECT_DIR}/zerostate.json"

# Generate network configuration
"${PROJECT_DIR}/scripts/gen-network.sh" \
    --dir "${TEMP_DIR}" \
    --validator-balance "${VALIDATOR_BALANCE}" \
    --validator-stake "${VALIDATOR_STAKE}" \
    --force \
    "${NODE_COUNT}"

# Configure and start nodes
for i in $(seq 1 $NODE_COUNT); do
    init_node_configs "$i"
    just node --dir "${TEMP_DIR}" "$i" >/dev/null 2>&1 &
done

# Setup test environment
export CI_JRPC_ENDPOINT="http://127.0.0.1:8001"
export CI_GIVER_ADDRESS="-1:$(jq -r '.account' <<< "$giver_account")"
export CI_GIVER_KEY="$giver_sk"

wait_for_stabilization

# Execute tests
"${CONTAINER_CMD}" pull "$TEST_IMAGE"
"${CONTAINER_CMD}" run --network host \
    -e CI_JRPC_ENDPOINT \
    -e CI_GIVER_ADDRESS \
    -e CI_GIVER_KEY \
    -e NETWORK=ci \
    "$TEST_IMAGE"