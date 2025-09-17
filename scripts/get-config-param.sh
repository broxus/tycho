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

echo "rpc: ${RPC}"
echo "script_dir: ${script_dir}"
echo "root_dir: ${root_dir}"

source "${script_dir}/common.sh"
tycho_bin=$(/usr/bin/env bash "${script_dir}/build-node.sh")

echo "tycho_bin: ${tycho_bin}"

curr_config=$($tycho_bin tool bc get-param ${N} --rpc ${RPC})
param_config=$(echo "${curr_config}" | jq ".param")
echo "param ${N} config:"
echo "${param_config}"

mkdir -p "${root_dir}/.temp"
param_config_file="${root_dir}/.temp/param-${N}.json"
echo "config file: ${param_config_file}"

if [ -f "${param_config_file}" ]; then
    RED='\033[0;31m'
    NORMAL='\033[0m'
    echo -e "${RED}ERROR${NORMAL}: config file already exists and won't be changed"
    exit 1
else
    echo "${param_config}" > ${param_config_file}
fi