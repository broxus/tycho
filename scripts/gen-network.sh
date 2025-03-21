#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

force=""
base_dir="${root_dir}/.temp"
N=""
validator_balance="100000"
validator_stake="30000"
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

zerostate=$(cat "${root_dir}/zerostate.json" | jq ".validators = []")

for i in $(seq $N);
do
    $tycho_bin tool gen-key > "${base_dir}/keys${i}.json"

    # Generate validator wallet
    wallet_keys=$($tycho_bin tool gen-key)
    wallet_secret=$(echo "$wallet_keys" | jq -r ".secret")
    wallet_public=$(echo "$wallet_keys" | jq -r ".public")
    wallet=$($tycho_bin tool gen-account wallet --pubkey "$wallet_public" --balance "$validator_balance")
    wallet_address=$(echo "$wallet" | jq -r ".account")
    wallet_boc=$(echo "$wallet" | jq -r ".boc")
    zerostate=$(echo "${zerostate}" | jq ".accounts[\"$wallet_address\"] = \"$wallet_boc\"")

    # Generate node configs
    node_port=$((base_node_port + i))
    rpc_listen_addr="0.0.0.0:$((base_rpc_port + i))"
    metrics_listen_addr="0.0.0.0:$((base_metrics_port + i))"

    storage_root_dir="${base_dir}/db${i}"
    control_socket_path="${base_dir}/control${i}.sock"

    key=$(jq -r .secret < "${base_dir}/keys${i}.json")
    dht_entry=$($tycho_bin tool gen-dht "127.0.0.1:${node_port}" --key "${key}")

    global_config=$(echo "${global_config}" | jq ".bootstrap_peers += [${dht_entry}]")

    node_config=$(echo "${node_config}" | jq ".port = ${node_port} | .storage.root_dir = \"${storage_root_dir}\"")
    node_config=$(echo "${node_config}" | jq "if .rpc.listen_addr? then .rpc.listen_addr = \"${rpc_listen_addr}\" else . end")
    node_config=$(echo "${node_config}" | jq "if .metrics.listen_addr? then .metrics.listen_addr = \"${metrics_listen_addr}\" else . end")
    node_config=$(echo "${node_config}" | jq "if .control.socket_path? then .control.socket_path = \"${control_socket_path}\" else . end")
    echo "${node_config}" > "${base_dir}/config${i}.json"

    elections_config=$(
      echo '{"ty":"Simple"}' | \
      jq ".wallet_secret = \"$wallet_secret\" | .wallet_address = \"-1:$wallet_address\" | .stake = \"$validator_stake\""
    )
    echo "${elections_config}" > "${base_dir}/elections${i}.json"
done

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
echo "${global_config}" > "${base_dir}/global-config.json"
