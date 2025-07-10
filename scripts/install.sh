#!/usr/bin/env bash
set -eE

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)

source "${script_dir}/common.sh"

# Parse command line arguments
cargo_profile="release"
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --profile requires a value" >&2
                exit 1
            fi
            cargo_profile="$2"
            shift 2
            ;;
        *)
            echo "ERROR: Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

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

if set_clang_env auto; then
  features="$features --features lto"
  echo "INFO: Building node with lto"
  export RUSTFLAGS="$base_rustflags -Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=$LD"
fi

echo "RUSTFLAGS: $RUSTFLAGS"
# shellcheck disable=SC2086 # we want to expand the flags
cargo install $features --profile "$cargo_profile" --path ./cli --locked

cat << EOF
Node installed successfully. Run the following to configure it:

tycho init --systemd --global-config https://testnet.tychoprotocol.com/global-config.json \\
  --validator --stake 500000

Where
  --global-config: file path or URL to the network global config
  --stake: stake value per round
EOF
