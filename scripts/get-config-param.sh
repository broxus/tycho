#!/usr/bin/env bash
set -eE

if [ -z "${TYCHO_BUILD_PROFILE}" ]; then
    profile="debug"
else
    profile="${TYCHO_BUILD_PROFILE}"
fi

N=$1
RPC=$2

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)
tycho_bin="${root_dir}/target/${profile}/tycho"

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <N> <RPC>"
    echo "Get config from BC into ${root_dir}/.temp/param-N.json file if file does not exist"
    echo "Where:"
    echo "  <N> - number of config param"
    echo " [RPC] - RPC address. Default: http://localhost:8001/rpc"
    exit 1
fi

if [ ! -n "$RPC" ]; then
    RPC="http://localhost:8001/rpc"
fi

echo "script_dir: $script_dir"
echo "root_dir: $root_dir"
echo "tycho_bin: $tycho_bin"
echo "rpc: $RPC"

source "${script_dir}/common.sh"

param_config_file="${root_dir}/.temp/param-${N}.json"

if [ -f "$param_config_file" ]; then
    echo "file ${param_config_file} already exists"
    exit 1
else
    curr_config=$($tycho_bin tool bc get-param ${N} --rpc ${RPC})
    param_config=$(echo "$curr_config" | jq ".param")
    echo $param_config > $param_config_file
    echo "param ${N} config:"
    echo "$param_config"
fi