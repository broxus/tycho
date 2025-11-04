#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

venv_dir="${root_dir}/.venv"
requirements_file="${script_dir}/requirements.txt"

# Skip reinstall if deps already match
if [ -x "${venv_dir}/bin/python" ] && [ -f "${venv_dir}/.deps-hash" ]; then
    current_hash=$(REQUIREMENTS_FILE="${requirements_file}" python - <<'PY'
import hashlib, os
from pathlib import Path

req = Path(os.environ["REQUIREMENTS_FILE"])
print(hashlib.sha256(req.read_bytes()).hexdigest())
PY
)
    stored_hash=$(cat "${venv_dir}/.deps-hash")
    if [ "${current_hash}" = "${stored_hash}" ]; then
        echo "Python dependencies up to date; skipping install."
        exit 0
    fi
fi

if command -v uv >/dev/null 2>&1; then
    uv venv "${venv_dir}"
    uv pip install --python "${venv_dir}/bin/python" -r "${requirements_file}"
else
    python3 -m venv "${venv_dir}"
    source "${venv_dir}/bin/activate"
    python3 -m pip install -r "${requirements_file}" --ignore-installed
fi

REQUIREMENTS_FILE="${requirements_file}" DEPS_HASH_FILE="${venv_dir}/.deps-hash" "${venv_dir}/bin/python" - <<'PY'
import hashlib
import os
from pathlib import Path

req = Path(os.environ["REQUIREMENTS_FILE"])
digest = hashlib.sha256(req.read_bytes()).hexdigest()
Path(os.environ["DEPS_HASH_FILE"]).write_text(digest)
PY
