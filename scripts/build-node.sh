#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

if [ -z "${TYCHO_BUILD_PROFILE}" ]; then
    profile_arg=""
    profile="debug"
else
    profile_arg="--profile ${TYCHO_BUILD_PROFILE}"
    profile="${TYCHO_BUILD_PROFILE}"
fi

cargo build --bin tycho $profile_arg

echo "${root_dir}/target/${profile}/tycho"