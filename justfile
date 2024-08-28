# === Config ===

integration_test_dir := justfile_directory() / ".scratch/integration_tests"
integration_test_base_url := "https://tycho-test.broxus.cc"

local_network_dir := justfile_directory() / ".temp"

# === Simple commands ===

default:
   @just --choose

# Installs the required version of `rustfmt`.
install_fmt:
    rustup component add rustfmt --toolchain nightly

# Installs the required version of links checker. See https://github.com/lycheeverse/lychee.
install_lychee:
    cargo install lychee

# Creates venv and installs all required script dependencies.
install_python_deps:
    ./scripts/install-python-deps.sh

# Formats the whole project.
fmt: install_fmt
    cargo +nightly fmt --all

# CI checks.
ci: check_dev_docs check_format lint test

# Checks links in the `/docs` directory.
check_dev_docs:
    lychee {{justfile_directory()}}/docs

# Checks whether the code is formatted.
check_format: install_fmt
    cargo +nightly fmt --all -- --check

# Clippy go brr.
lint:
    #cargo clippy --all-targets --all-features --workspace # update when clippy is fixed
    cargo clippy --all-targets --all-features -p tycho-block-util -p tycho-core -p tycho-network -p tycho-rpc -p tycho-storage -p tycho-consensus -- -D warnings

# Generates cargo docs.
docs:
    export RUSTDOCFLAGS=-D warnings
    cargo doc --no-deps --document-private-items --all-features --workspace

# Runs all tests.
test:
    cargo nextest run -p tycho-block-util -p tycho-core -p tycho-network -p tycho-rpc -p tycho-storage -p tycho-consensus

# Generates a Grafana panel JSON.
gen_dashboard:
    #!/usr/bin/env bash
    if ! [ -d ".venv" ]; then
        ./scripts/install-python-deps.sh
    fi
    /usr/bin/env python ./scripts/gen-dashboard.py

# === Integration tests stuff ===

# Runs all tests including ignored. Will take a lot of time to run.
run_integration_tests: prepare_integration_tests
    ./scripts/run-integration-tests.sh

# Synchronizes files for integration tests.
prepare_integration_tests:
    ./scripts/prepare-integration-tests.sh \
        --dir {{integration_test_dir}} \
        --base-url {{integration_test_base_url}}

# Removes all files for integration tests.
clean_integration_tests:
    rm -rf {{integration_test_dir}}

# === Local network stuff ===

# Builds the node and prints a path to the binary. Use `TYCHO_BUILD_PROFILE` env to explicitly set cargo profile.
build *flags:
    ./scripts/build-node.sh {{flags}}

# Creates a node config template with all defaults. Use `--force` to overwrite.
init_node_config *flags:
    ./scripts/init-node-config.sh {{flags}}

# Creates a zerostate config template with all defaults. Use `--force` to overwrite.
init_zerostate_config *flags:
    ./scripts/init-zerostate-config.sh {{flags}}

# Creates a network of `N` nodes. Use `--force` to reset the state.
gen_network *flags:
    ./scripts/gen-network.sh --dir {{local_network_dir}} {{flags}}

# Runs the Nth node.
node n:
    ./scripts/run-node.sh --dir {{local_network_dir}} {{n}}
