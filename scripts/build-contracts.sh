#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

contracts_dir="$root_dir/contracts"
temp_dir="$contracts_dir/dist"
mkdir -p "$temp_dir"

export TOLK_STDLIB="$contracts_dir/tolk-stdlib"

function compile() {
    local contract_name="$1"
    local output_path="$2"

    echo "Building '$contract_name' contract"

    local fift_path="$temp_dir/$contract_name.fif"

    tolk "$contracts_dir/$contract_name.tolk" \
        -b "$output_path" \
        > "$fift_path"

    # TODO: Remove `|| true` when exit code is fixed.
    fift "$contracts_dir/lib/util.fif" "$fift_path" || true

    echo "Built code into: $output_path"
}

export ELECTOR_MSG_VALUE_UPDATE_VSET=1000000000
export ELECTOR_MSG_VALUE_STAKE_ACCEPTED=1000000000
compile elector "$root_dir/cli/res/elector_code.boc"
