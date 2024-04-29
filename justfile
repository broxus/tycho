# Define default recipe
default: fmt lint docs test

install_fmt:
    rustup component add rustfmt --toolchain nightly

# helpers
fmt: install_fmt
    cargo +nightly fmt --all

# ci checks
check_format: install_fmt
    cargo +nightly fmt --all -- --check

lint: check_format
    cargo clippy --all-targets --all-features --workspace

docs: check_format
    export RUSTDOCFLAGS=-D warnings
    cargo doc --no-deps --document-private-items --all-features --workspace

test: lint
    cargo test --all-targets --all-features --workspace