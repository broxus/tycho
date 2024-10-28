#!/usr/bin/env bash
set -eE

if [ -z "${TYCHO_BUILD_PROFILE}" ]; then
    profile="debug"
else
    profile="${TYCHO_BUILD_PROFILE}"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)
tycho_bin="${root_dir}/target/${profile}/tycho"

echo "script_dir: $script_dir"
echo "root_dir: $root_dir"
echo "tycho_bin: $tycho_bin"

source "${script_dir}/common.sh"

next_time=$(date +%s)
next_time=$((next_time + 30))

output=$($tycho_bin tool bc get-param --rpc http://localhost:8001/rpc 34)

modified_vset=$(echo "$output" | jq ".param | .utime_since = ${next_time} | .utime_until = ${next_time} | .main = 3")
modified_vset=$(echo "$modified_vset" | jq -c '.')

echo "modified next vset: $modified_vset"

$tycho_bin tool bc set-param --rpc http://localhost:8001/rpc --key $(jq -r .secret ${root_dir}/keys.json) 36 "${modified_vset}"
