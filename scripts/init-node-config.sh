#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

config_path="$root_dir/config.json"
logger_path="$root_dir/logger.json"

tycho_bin=$(/usr/bin/env bash "$script_dir/build-node.sh")

$tycho_bin init config "$config_path" "$@"

config=$(jq '
    .public_ip = "127.0.0.1" |
    .blockchain_rpc_client.too_new_archive_threshold = 1
' "$config_path")
echo "$config" > "$config_path"

if ! [ -f "$logger_path" ]; then
    cat << EOF > "$logger_path"
{
    "tycho": "info",
    "tycho_core": "debug",
    "tycho_network": "info",
    "async_queued_dispatcher": "error",
    "collation_manager": "debug",
    "mempool_adapter": "debug",
    "state_node_adapter": "debug",
    "mq_adapter": "debug",
    "validator": "debug",
    "collator": "debug",
    "collator::read_next_externals": "warn",
    "exec_manager": "error",
    "tycho_consensus::engine::input_buffer": "error"
}
EOF
fi
