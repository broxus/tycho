#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

base_dir="${root_dir}/.temp"
N=3
giver_balance="10000000000000"
validator_balance="100000"
validator_stake="30000"
image_name=""
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      --dir)
        base_dir="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected directory path'
          exit 1
        fi
      ;;
      --giver-balance)
        giver_balance="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected giver balance'
          exit 1
        fi
      ;;
      --validator-balance)
        validator_balance="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected validator balance'
          exit 1
        fi
      ;;
      --validator-stake)
        validator_stake="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected validator stake'
          exit 1
        fi
      ;;
      --nodes)
        N="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected node count'
          exit 1
        fi
      ;;
      *) # positional
        if ! [ -z "$image_name" ]; then
            echo "ERROR: Too many args"
            exit 1
        fi

        image_name="${key}"
        shift # past argument
      ;;
  esac
done

source "${script_dir}/common.sh"
if ! is_number "$N" || ((N < 1)); then
    echo "ERROR: Expected a non-zero positive number of nodes."
    exit 1
fi

if [ -z "$image_name" ]; then
    echo "ERROR: Expected test image name."
    exit 1
fi


# Constants
stabilize_check_attempts=30
test_image="${image_name:-ghcr.io/broxus/tycho-tests/tycho-tests-destroyable:latest}"

keys_path="${root_dir}/keys.json"

tycho_bin=$(/usr/bin/env bash "${script_dir}/build-node.sh")

docker_bin="docker"
[ -n "${CI:-}" ] && docker_bin="podman"

cleanup() {
    echo "INFO: Cleaning up..."
    pkill tycho -9 || true
}
trap cleanup EXIT

# Prepare config templates
mkdir -p "${base_dir}"
"${script_dir}/init-zerostate-config.sh" || true
"${script_dir}/init-node-config.sh" || true

# Setup giver account
giver_pk=$(jq -r '.public' "$keys_path")
giver_sk=$(jq -r '.secret' "$keys_path")
giver_account=$("${tycho_bin}" tool gen-account wallet --pubkey "$giver_pk" --balance $giver_balance)
giver_addr=$(jq -r '.account' <<< "$giver_account")

jq_inplace "${root_dir}/zerostate.json" \
    --arg addr "$giver_addr" \
    --arg boc "$(jq -r '.boc' <<< "$giver_account")" \
    '.accounts[$addr] = $boc'

# Generate network configuration
"${script_dir}/gen-network.sh" \
    --dir "${base_dir}" \
    --validator-balance "${validator_balance}" \
    --validator-stake "${validator_stake}" \
    --force \
    "$N"

# Configure and start nodes
for i in $(seq $N); do
    echo "INFO: Starting node $i..."
    mkdir -p "${base_dir}/logs$i"

    jq_inplace "${base_dir}/config$i.json" \
        --arg logs_dir "${base_dir}/logs$i" \
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
        } | .blockchain_rpc_client.too_new_archive_threshold = 1'

    "${script_dir}/run-node.sh" --dir "${base_dir}" "$i" >/dev/null 2>&1 &
done

# Setup test environment
export CI_JRPC_ENDPOINT="http://127.0.0.1:8001"
export CI_GIVER_ADDRESS="-1:$giver_addr"
export CI_GIVER_KEY="$giver_sk"

# Wait until rpc is available
echo "INFO: Waiting for nodes to stabilize..."
for _ in $(seq $stabilize_check_attempts); do
    if curl -sSf "${CI_JRPC_ENDPOINT}" >/dev/null; then
        break
    fi
    sleep 2
done

# Execute tests
"${docker_bin}" pull "$test_image"
"${docker_bin}" run --network host \
    -e CI_JRPC_ENDPOINT \
    -e CI_GIVER_ADDRESS \
    -e CI_GIVER_KEY \
    -e NETWORK=ci \
    "$test_image"
