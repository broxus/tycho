#!/usr/bin/env bash
set -eE

#cargo test -r --all-targets --all-features --workspace -- --ignored #uncomment this when all crates will compile ˙◠˙
# for now add tests one by one
RUST_BACKTRACE=1 cargo test \
  --package tycho-core \
  --profile=release_check \
  --features=test \
  --test archives heavy_archives \
  -- --ignored --exact --nocapture

#RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=1 cargo test -r --package tycho-storage \
#    --lib store::shard_state::store_state_raw::test::insert_and_delete_of_several_shards \
#    -- --ignored --exact --nocapture

#RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=1 \
# cargo run --example engine -- --nodes 4 --workers-per-node 2 --payload_step 0 --duration 1m
