#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

force=""
base_dir="${root_dir}/.temp"
N=""
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      --force)
        force="true"
        shift # past argument
      ;;
      --dir)
        base_dir="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected directory path'
          echo ''
          print_help
          exit 1
        fi
      ;;
      *) # positional
        if ! [ -z "$N" ]; then
            echo "ERROR: Too many args"
            exit 1
        fi

        N="${key}"
        shift # past argument
      ;;
  esac
done

source "${script_dir}/common.sh"
if ! is_number "$N" || ((N < 1)); then
    echo "ERROR: Expected a non-zero positive number of nodes as the first argument."
    exit 1
fi

if [ "$force" != "true" ] && [ -f "${base_dir}/zerostate.boc" ]; then
    echo "ERROR: Network state already exists, use --force to overwrite"
    exit 1
fi

rm -rf "${base_dir}"
mkdir -p "${base_dir}"

tycho_bin=$(/usr/bin/env bash "${script_dir}/build-node.sh")

base_node_port="20000"
base_rpc_port="8000"
base_metrics_port="10000"

global_config="{}"
node_config=$(cat "${root_dir}/config.json")

for i in $(seq $N);
do
    $tycho_bin tool gen-key > "${base_dir}/keys${i}.json"

    node_port=$((base_node_port + i))
    rpc_listen_addr="0.0.0.0:$((base_rpc_port + i))"
    metrics_listen_addr="0.0.0.0:$((base_metrics_port + i))"

    storage_root_dir="${base_dir}/db${i}"
    control_socket_path="${base_dir}/control-${i}.sock"

    key=$(jq -r .secret < "${base_dir}/keys${i}.json")
    dht_entry=$($tycho_bin tool gen-dht "127.0.0.1:${node_port}" --key "${key}")

    global_config=$(echo "${global_config}" | jq ".bootstrap_peers += [${dht_entry}]")

    node_config=$(echo "${node_config}" | jq ".port = ${node_port} | .storage.root_dir = \"${storage_root_dir}\"")
    node_config=$(echo "${node_config}" | jq "if .rpc.listen_addr? then .rpc.listen_addr = \"${rpc_listen_addr}\" else . end")
    node_config=$(echo "${node_config}" | jq "if .metrics.listen_addr? then .metrics.listen_addr = \"${metrics_listen_addr}\" else . end")
    node_config=$(echo "${node_config}" | jq "if .control.socket_path? then .control.socket_path = \"${control_socket_path}\" else . end")
    echo "${node_config}" > "${base_dir}/config${i}.json"
done

zerostate=$(cat "${root_dir}/zerostate.json" | jq ".validators = []")
for i in $(seq $N);
do
    pubkey=$(jq .public < "${base_dir}/keys${i}.json")
    zerostate=$(echo "${zerostate}" | jq ".validators += [$pubkey]")
done

echo "${zerostate}" > "${base_dir}/zerostate.json"
zerostate_id=$(
    $tycho_bin tool gen-zerostate "${base_dir}/zerostate.json" \
        --output "${base_dir}/zerostate.boc" \
        --force
)

global_config=$(echo "${global_config}" | jq ".zerostate = $zerostate_id")

mempool='{
  "clock_skew": 5000,
  "commit_depth": 20,
  "genesis_round": 0,
  "payload_batch_size": 786432,
  "deduplicate_rounds": 140,
  "max_anchor_distance": 210
}'

global_config=$(echo "${global_config}" | jq ".mempool = $mempool")

echo "${global_config}" > "${base_dir}/global-config.json"
