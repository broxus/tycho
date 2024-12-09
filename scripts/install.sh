#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

source "${script_dir}/common.sh"

if ! command -v cargo 2>&1 >/dev/null; then
    cat << EOF
ERROR: \`cargo\` could not be found. You need to install the Rust compiler first:

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

And add its environment variables:

source ~/.cargo/env

EOF
    exit 1
fi

features=""
if set_clang_env 19 18; then
  if ! command -v cargo 2>&1 >/dev/null; then
    echo "ERROR: \`lld\` could not be found."
    exit 1
  fi

  features="--features lto"
  echo "INFO: Building node with lto"
  export RUSTFLAGS="-Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=lld"
fi

cargo install --path ./cli --locked $features

cat << EOF
Node installed successfully. Run the following to configure it:

tycho init --systemd --global-config https://testnet.tychoprotocol.com/global-config.json \\
  --validator --stake 500000

Where
  --global-config: file path or URL to the network global config
  --stake: stake value per round
EOF
