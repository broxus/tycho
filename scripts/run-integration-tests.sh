#!/usr/bin/env bash
set -eE

#cargo test -r --all-targets --all-features --workspace -- --ignored #uncomment this when all crates will compile ˙◠˙
# for now add tests one by one
RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=1 cargo test -r --package tycho-storage \
    --lib store::shard_state::store_state_raw::test::insert_and_delete_of_several_shards \
    -- --ignored --exact --nocapture
