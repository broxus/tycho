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

# dump not empty block #10 from shard 0:80
CHECK_INTERVAL=10
TARGET_COUNT=20
while true; do
    output=$($tycho_bin node list-blocks --control-socket .temp/control1.sock)
    if [ $? -ne 0 ]; then
        echo "Command 'node list-blocks' returned error. Repeat attempt in $CHECK_INTERVAL seconds..."
    else
        matching_blocks=$(echo "$output" | jq -r '.blocks[] | select(startswith("0:80"))')
        count=$(echo "$matching_blocks" | wc -l)

        if ! is_number "$count"; then
            echo "Unable to detect blocks count from shard 0:80. Repeat attempt in $CHECK_INTERVAL seconds..."
        elif [ "$count" -ge "$TARGET_COUNT" ]; then
            block_id=$(echo "$matching_blocks" | sed -n "${TARGET_COUNT}p")
            echo "Found block '$block_id'. Will dump it."
            break
        else
            echo "Found $count/$TARGET_COUNT blocks. Repeat attempt in $CHECK_INTERVAL seconds..."
        fi
    fi
    sleep "$CHECK_INTERVAL"
done

echo ${block_id} > "${root_dir}/core/tests/data/block_id.txt"
$tycho_bin node dump-block -b "${block_id}" --control-socket .temp/control1.sock "${root_dir}/core/tests/data/block.bin"
$tycho_bin node dump-queue-diff -b "${block_id}" --control-socket .temp/control1.sock "${root_dir}/core/tests/data/block_queue_diff.bin"
