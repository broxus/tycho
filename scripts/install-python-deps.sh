#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

venv_dir="${root_dir}/.venv"

python -m venv "${venv_dir}"
source "${venv_dir}/bin/activate"

pip install -r "${script_dir}/requirements.txt" --ignore-installed
