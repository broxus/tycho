#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)
venv_dir="${root_dir}/.venv"
python_bin="${venv_dir}/bin/python"

if [ ! -x "${python_bin}" ]; then
    "${script_dir}/install-python-deps.sh"
fi

if [ -x "${python_bin}" ]; then
    exec "${python_bin}" "$@"
fi

exec python "$@"
