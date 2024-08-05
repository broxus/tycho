#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

keys_path="$root_dir/keys.json"
config_path="$root_dir/zerostate.json"

tycho_bin=$(/usr/bin/env bash "$script_dir/build-node.sh")

$tycho_bin tool gen-zerostate --init-config "$config_path" "$@"

if ! [ -f "$keys_path" ]; then
    $tycho_bin tool gen-key > "$keys_path"
fi

public_key=$(jq '.public' "$keys_path")
config=$(jq ".config_public_key = $public_key | .minter_public_key = $public_key" "$config_path")
echo "$config" > "$config_path"
