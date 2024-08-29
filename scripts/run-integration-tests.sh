#!/usr/bin/env bash
set -eE

#cargo test -r --all-targets --all-features --workspace -- --ignored #uncomment this when all crates will compile ˙◠˙
# for now add tests one by one
RUST_BACKTRACE=1 cargo test \
#  --lib store::shard_state::store_state_raw::test::insert_and_delete_of_several_shards \ TODO: should be fixed
  --package tycho-core --test archives heavy_archives \
  -- --ignored --exact --nocapture
