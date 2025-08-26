#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

contracts_dir="$root_dir/contracts"
build_dir="$contracts_dir/build"

function copy_code() {
    jq -r .hex "$build_dir/$1.compiled.json" | xxd -r -p > "$2"
}

# TODO: Add `sed` here to overwrite contract constants.
cd "${contracts_dir}"
yarn build --all

copy_code Elector "$root_dir/cli/res/elector_code.boc"
copy_code Config "$root_dir/cli/res/config_code.boc"
