#!/usr/bin/env bash
set -eE

if [ -z "${TYCHO_BUILD_PROFILE}" ]; then
    profile="debug"
else
    profile="${TYCHO_BUILD_PROFILE}"
fi

N=$1
RPC=$2
KEY=$3

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)
tycho_bin="${root_dir}/target/${profile}/tycho"

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <N> <RPC>"
    echo "Set BC config from file ${root_dir}/.temp/param-N.json if file exists"
    echo "Where:"
    echo "  <N> - number of config param"
    echo " [RPC] - RPC address. Default: http://localhost:8001/rpc"
    echo " [KEY] - Network secret. Default: from '${root_dir}/keys.json'"
    exit 1
fi

if [ ! -n "$RPC" ]; then
    RPC="http://localhost:8001/rpc"
fi

if [ ! -n "$KEY" ]; then
    KEY=$(jq -r .secret ${root_dir}/keys.json)
fi

echo "script_dir: ${script_dir}"
echo "root_dir: ${root_dir}"
echo "tycho_bin: ${tycho_bin}"
echo "rpc: ${RPC}"
echo "key: ${KEY}"

param_config_file="${root_dir}/.temp/param-${N}.json"
echo "config file: ${param_config_file}"

if [ -f "${param_config_file}" ]; then
    param_config=$(cat ${param_config_file})
    echo "${param_config}"
else
    RED='\033[0;31m'
    NORMAL='\033[0m'
    echo -e "${RED}ERROR${NORMAL}: config file does not exists"
    exit 1
fi

source "${script_dir}/common.sh"

$tycho_bin tool bc set-param --rpc ${RPC} --key ${KEY} ${N} "${param_config}"
