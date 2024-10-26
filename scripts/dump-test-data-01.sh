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

# dump zero state
cp -f "${root_dir}/.temp/zerostate.boc" "${root_dir}/core/tests/data/zerostate.boc"
cp -f "${root_dir}/.temp/zerostate.boc" "${root_dir}/test/data/zerostate.boc"

# dump first empty block
output=$($tycho_bin node list-blocks --sock .temp/control-1.sock)
block_id=$(echo "$output" | jq -r '.blocks[0]')

echo ${block_id} > "${root_dir}/test/data/first_block_id.txt"
$tycho_bin node dump-block -b "${block_id}" --sock .temp/control-1.sock "${root_dir}/test/data/first_block.bin"
$tycho_bin node dump-queue-diff -b "${block_id}" --sock .temp/control-1.sock "${root_dir}/test/data/first_block_queue_diff.bin"

cp -f "${root_dir}/test/data/first_block.bin" "${root_dir}/core/tests/data/empty_block.bin"
