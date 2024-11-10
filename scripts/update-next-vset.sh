#!/usr/bin/env bash
set -eE

if [ -z "${TYCHO_BUILD_PROFILE}" ]; then
    profile="debug"
else
    profile="${TYCHO_BUILD_PROFILE}"
fi

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <N> <I> <RPC> [KEY]"
    echo "Where:"
    echo "  <N> - Num of validators to include in the next vset from 'zerostate.validators'"
    echo "  <I> - Start index to read vset from 'zerostate.validators'"
    echo " [RPC] - RPC address. Default: http://localhost:8001/rpc"
    echo " [KEY] - Network secret. Default: from 'keys.json'"
    exit 1
fi

N=$1
I=$2
RPC=$3
KEY=$4

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)
tycho_bin="${root_dir}/target/${profile}/tycho"

if [ ! -n "$RPC" ]; then
    RPC="http://localhost:8001/rpc"
fi

if [ ! -n "$KEY" ]; then
    KEY=$(jq -r .secret ${root_dir}/keys.json)
fi

echo "script_dir: $script_dir"
echo "root_dir: $root_dir"
echo "tycho_bin: $tycho_bin"
echo "rpc: $RPC"
echo "key: $KEY"

source "${script_dir}/common.sh"

validators_file="${script_dir}/../.temp/validators.json"

if [ -f "$validators_file" ]; then
    validators=$(cat $validators_file)
else
    curr_set=$($tycho_bin tool bc get-param 34 --rpc ${RPC})
    validators=$(echo "$curr_set" | jq "{ list: [.param.list[].public_key] }")
    echo $validators > $validators_file
fi

validators_length=$(echo "$validators" | jq ".list | length")

echo "validators: $validators"
echo "validators_length: $validators_length"

vset_pkeys=$(echo "$validators" | jq --argjson N "$N" --argjson I "$I" --argjson length "$validators_length" '
    .list as $original_list |
    .list = [
        range(0; $N) |
        (. + $I) % $length |
        $original_list[.]
    ]
')

echo "vset_pkeys: $vset_pkeys"


vset="{}"
for k in $(seq $N);
do
    idx=$k-1
    pkey=$(echo "$vset_pkeys" | jq -r ".list[$idx]")
    validator_entry='{
        "weight": 1,
        "mc_seqno_since": 0
    }'
    validator_entry=$(echo "$validator_entry" | jq ".public_key = \"${pkey}\" | .adnl_addr = \"${pkey}\"")
    vset=$(echo "$vset" | jq ".list += [${validator_entry}]")
done

now=$(date +%s)
utime_since=$((now + 100))
utime_until=$((now + 100))

vset=$(echo "$vset" | jq ".main = ${N} | .total_weight = ${N} | .utime_since = ${utime_since} | .utime_until = ${utime_until}")

echo "vset: $vset"

$tycho_bin tool bc set-param --rpc ${RPC} --key ${KEY} 36 "${vset}"
