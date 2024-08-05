#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

base_dir="${root_dir}/.temp"
N=""
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
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
    echo "ERROR: Expected a non-zero positive number of node as the first argument."
    exit 1
fi

tycho_bin=$(/usr/bin/env bash "${script_dir}/build-node.sh")

$tycho_bin node run \
    --keys "${base_dir}/keys${N}.json" \
    --config "${base_dir}/config${N}.json" \
    --global-config "${base_dir}/global-config.json" \
    --import-zerostate "${base_dir}/zerostate.boc" \
    --logger-config "${root_dir}/logger.json"
