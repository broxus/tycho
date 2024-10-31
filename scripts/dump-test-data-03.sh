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

# dump first 3 archives
CHECK_INTERVAL=10
TARGET_COUNT=3
while true; do
    output=$($tycho_bin node list-archives --control-socket .temp/control1.sock)
    echo "$output"

    if [ $? -ne 0 ]; then
        echo "Command 'node list-archives' returned error. Repeat attempt in $CHECK_INTERVAL seconds..."
    else
        count=$(echo "$output" | jq 'length')

        if ! is_number "$count"; then
            echo "Unable to detect archives count. Repeat attempt in $CHECK_INTERVAL seconds..."
        elif [ "$count" -ge "$TARGET_COUNT" ]; then
            echo "Found $count/$TARGET_COUNT archives. Will dump them."
            break
        else
            echo "Found $count/$TARGET_COUNT archives. Repeat attempt in $CHECK_INTERVAL seconds..."
        fi
    fi
    sleep "$CHECK_INTERVAL"
done

output_dir="${root_dir}/core/tests/data"
ids=$(echo "$output" | jq -r '.[].id')
counter=1
for id in $ids; do
    output="$output_dir/archive_${counter}.bin"
    $tycho_bin node dump-archive --control-socket .temp/control1.sock --seqno $id $output

    echo "Archive '$id' dumped into '$output'."

    counter=$((counter + 1))
    if [ "$counter" -gt "$TARGET_COUNT" ]; then
        break
    fi
done
