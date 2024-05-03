default:
   @just --choose

install_fmt:
    rustup component add rustfmt --toolchain nightly


integration_test_dir := justfile_directory() / ".scratch/integration_tests"
integration_test_base_url := "https://tycho-test.broxus.cc"

prepare_integration_tests:
    #!/usr/bin/env bash
    # Create the integration test directory if it does not exist
    echo "Integration test directory: {{integration_test_dir}}"
    mkdir -p {{integration_test_dir}}

    # Always download the checksum file first to ensure it's the latest
    echo "Downloading checksum file..."
    curl --request GET -sL --url {{integration_test_base_url}}/states.tar.zst.sha256 --output {{integration_test_dir}}/states.tar.zst.sha256

    # Check if the archive file exists
    if [ -f {{integration_test_dir}}/states.tar.zst ]; then
      # Verify the archive against the checksum
      echo "Verifying existing archive against checksum..."
      cd {{integration_test_dir}}
      if sha256sum -c states.tar.zst.sha256; then
          echo "Checksum matches. No need to download the archive."
      else
          echo "Checksum does not match. Downloading the archive..."
          just _download_archive
      fi
      else
          echo "Archive file does not exist. Downloading the archive..."
          just _download_archive
    fi

_download_archive:
    curl --request GET -L --url {{integration_test_base_url}}/states.tar.zst --output {{integration_test_dir}}/states.tar.zst

clean_integration_tests:
    rm -rf {{integration_test_dir}}


fmt: install_fmt
    cargo +nightly fmt --all

# ci checks
ci: fmt lint docs test

check_format: install_fmt
    cargo +nightly fmt --all -- --check

lint:
    cargo clippy --all-targets --all-features --workspace

docs:
    export RUSTDOCFLAGS=-D warnings
    cargo doc --no-deps --document-private-items --all-features --workspace

test:
    cargo test --all-targets --all-features --workspace

# runs all tests including ignored. Will take a lot of time to run
integration_test: prepare_integration_tests
    export RUST_BACKTRACE=1
    export RUST_LIB_BACKTRACE=1
    #cargo test -r --all-targets --all-features --workspace -- --ignored #uncomment this when all crates will compile ˙◠˙
    # for now add tests one by one
    RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=1 cargo test -r --package tycho-storage --lib store::shard_state::replace_transaction::test::insert_and_delete_of_several_shards -- --ignored --exact --nocapture