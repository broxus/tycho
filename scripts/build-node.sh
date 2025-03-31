#!/usr/bin/env bash
set -eE


# If TYCHO_BIN_PATH env is provided and valid, use it instead of building
if [ -n "$TYCHO_BIN_PATH" ]; then
    if [ -x "$TYCHO_BIN_PATH" ]; then
        echo "$TYCHO_BIN_PATH"
        exit 0
    else
        echo "ERROR: TYCHO_BIN_PATH ('$TYCHO_BIN_PATH') is set but is not executable or not found." >&2
        exit 1
    fi
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

source "${script_dir}/common.sh"

if [ -z "${TYCHO_BUILD_PROFILE}" ]; then
    profile_arg=""
    profile="debug"
else
    profile_arg="--profile ${TYCHO_BUILD_PROFILE}"
    profile="${TYCHO_BUILD_PROFILE}"
fi

if set_clang_env 19; then
    : # dont exit
fi

cargo build --bin tycho $profile_arg

echo "${root_dir}/target/${profile}/tycho"
