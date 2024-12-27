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

. ~/.cargo/env

EOF
    exit 1
fi

# Combine rustflags
features=""
base_rustflags="-Ctarget_cpu=native -Cforce-frame-pointers=true"

if set_clang_env 19; then
  features="$features --features lto"
  echo "INFO: Building node with lto"
  export RUSTFLAGS="$base_rustflags -Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=$LD"
fi

echo "RUSTFLAGS: $RUSTFLAGS"
# shellcheck disable=SC2086 # we want to expand the flags
cargo install $features --path ./cli --locked

cat << EOF
Node installed successfully. Run the following to configure it:

tycho init --systemd --global-config https://testnet.tychoprotocol.com/global-config.json \\
  --validator --stake 500000

Where
  --global-config: file path or URL to the network global config
  --stake: stake value per round
EOF