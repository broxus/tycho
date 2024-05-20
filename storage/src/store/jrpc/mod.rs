use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::cell::*;
use everscale_types::dict::Dict;
use everscale_types::models::*;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::FastHashMap;
use weedb::rocksdb;

use crate::db::*;

pub struct JrpcStorage {
    db: JrpcDb,
}

impl JrpcStorage {
    pub fn new(db: JrpcDb) -> Self {
        Self { db }
    }

    #[tracing::instrument(
        level = "info",
        name = "reset_accounts",
        skip_all,
        fields(shard = %shard_state.block_id().shard)
    )]
    pub async fn reset_accounts(&self, shard_state: ShardStateStuff) -> Result<()> {
        let shard_ident = shard_state.block_id().shard;
        let Ok(workchain) = i8::try_from(shard_ident.workchain()) else {
            return Ok(());
        };

        tracing::info!("clearing old code hash indices");
        let started_at = Instant::now();
        self.remove_code_hashes(&shard_ident).await?;
        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "cleared old code hash indices"
        );

        // Split on virtual shards
        let (_state_guard, virtual_shards) = {
            let guard = shard_state.ref_mc_state_handle().clone();

            let mut virtual_shards = FastHashMap::default();
            split_shard(
                &shard_ident,
                shard_state.state().load_accounts()?.dict(),
                4,
                &mut virtual_shards,
            )
            .context("failed to split shard state into virtual shards")?;

            // NOTE: Ensure that the root cell is dropped.
            drop(shard_state);
            (guard, virtual_shards)
        };

        // Rebuild code hashes
        let db = self.db.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _guard = span.enter();

            tracing::info!("started building new code hash indices");
            let started_at = Instant::now();

            let raw = db.rocksdb().as_ref();
            let code_hashes_cf = &db.code_hashes.cf();
            let code_hashes_by_address_cf = &db.code_hashes_by_address.cf();

            let mut non_empty_batch = false;
            let mut write_batch = rocksdb::WriteBatch::default();

            // Prepare buffer for code hashes ids
            let mut code_hashes_key = [0u8; { tables::CodeHashes::KEY_LEN }];
            code_hashes_key[32] = workchain as u8;

            let mut code_hashes_by_address_key = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            code_hashes_by_address_key[0] = workchain as u8;

            // Iterate all accounts
            for (virtual_shard, accounts) in virtual_shards {
                tracing::info!(%virtual_shard, "started collecting code hashes");
                let started_at = Instant::now();

                for entry in accounts.iter() {
                    let (id, (_, account)) = entry?;
                    let Some(code_hash) = extract_code_hash(&account)? else {
                        continue;
                    };

                    non_empty_batch |= true;

                    // Fill account address in the key buffer
                    code_hashes_key[..32].copy_from_slice(code_hash.as_slice());
                    code_hashes_key[33..65].copy_from_slice(id.as_slice());

                    code_hashes_by_address_key[1..33].copy_from_slice(id.as_slice());

                    // Write tx data and indices
                    write_batch.put_cf(code_hashes_cf, code_hashes_key.as_slice(), []);
                    write_batch.put_cf(
                        code_hashes_by_address_cf,
                        code_hashes_by_address_key.as_slice(),
                        code_hash.as_slice(),
                    );
                }

                tracing::info!(
                    %virtual_shard,
                    elapsed = %humantime::format_duration(started_at.elapsed()),
                    "finished collecting code hashes",
                );
            }

            if non_empty_batch {
                raw.write_opt(write_batch, db.code_hashes.write_config())?;
            }

            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "finished building new code hash indices"
            );

            // Flush indices after delete/insert
            tracing::info!("started flushing code hash indices");
            let started_at = Instant::now();

            let bound = Option::<[u8; 0]>::None;
            raw.compact_range_cf(code_hashes_cf, bound, bound);
            raw.compact_range_cf(code_hashes_by_address_cf, bound, bound);

            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "finished flushing code hash indices"
            );
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn remove_code_hashes(&self, shard: &ShardIdent) -> Result<(), rocksdb::Error> {
        let workchain = shard.workchain() as u8;

        // Remove from the secondary index first
        {
            let mut from = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            from[0] = workchain;

            {
                let [_, from @ ..] = &mut from;
                extend_account_prefix(shard, false, from);
            }

            let mut to = from;
            {
                let [_, to @ ..] = &mut to;
                extend_account_prefix(shard, true, to);
            }

            let raw = self.db.rocksdb();
            let cf = &self.db.code_hashes_by_address.cf();
            let writeopts = self.db.code_hashes_by_address.write_config();

            // Remove `[from; to)`
            raw.delete_range_cf_opt(cf, &from, &to, writeopts)?;
            // Remove `to`, (-1:ffff..ffff might be a valid existing address)
            raw.delete_cf_opt(cf, to, writeopts)?;
        }

        // Full scan the main code hashes index and remove all entires for the shard
        let db = self.db.clone();
        let shard = *shard;

        tokio::task::spawn_blocking(move || {
            let cf = &db.code_hashes.cf();

            let raw = db.rocksdb().as_ref();
            let snapshot = raw.snapshot();
            let mut readopts = db.code_hashes.new_read_config();
            readopts.set_snapshot(&snapshot);

            let writeopts = db.code_hashes.write_config();

            let mut iter = raw.raw_iterator_cf_opt(cf, readopts);
            iter.seek_to_first();

            let mut prefix = shard.prefix();
            let tag = extract_tag(&shard);
            prefix -= tag; // Remove tag from the prefix

            // For the prefix 1010000 the mask is 1100000
            let prefix_mask = !(tag | (tag - 1));

            loop {
                let key = match iter.key() {
                    Some(key) => key,
                    None => return iter.status(),
                };

                if key.len() != tables::CodeHashes::KEY_LEN
                    || key[32] == workchain
                        && (shard.is_full() || {
                            // Filter only the keys with the same prefix
                            let key = u64::from_be_bytes(key[33..41].try_into().unwrap());
                            (key ^ prefix) & prefix_mask == 0
                        })
                {
                    raw.delete_cf_opt(cf, key, writeopts)?;
                }

                iter.next();
            }
        })
        .await
        .unwrap()
    }
}

fn extract_code_hash(account: &ShardAccount) -> Result<Option<HashBytes>> {
    if let Some(account) = account.load_account()? {
        if let AccountState::Active(state_init) = &account.state {
            if let Some(code) = &state_init.code {
                return Ok(Some(*code.repr_hash()));
            }
        }
    }
    Ok(None)
}

fn split_shard(
    shard: &ShardIdent,
    accounts: &ShardAccountsDict,
    depth: u8,
    shards: &mut FastHashMap<ShardIdent, ShardAccountsDict>,
) -> Result<()> {
    fn split_shard_impl(
        shard: &ShardIdent,
        accounts: &ShardAccountsDict,
        depth: u8,
        shards: &mut FastHashMap<ShardIdent, ShardAccountsDict>,
        builder: &mut CellBuilder,
    ) -> Result<()> {
        let (left_shard_ident, right_shard_ident) = 'split: {
            if depth > 0 {
                if let Some((left, right)) = shard.split() {
                    break 'split (left, right);
                }
            }
            shards.insert(*shard, accounts.clone());
            return Ok(());
        };

        let (left_accounts, right_accounts) = {
            let prefix_len = shard.prefix_len();
            builder.rewind(builder.bit_len()).unwrap();
            builder.store_uint(shard.prefix() >> (64 - prefix_len), prefix_len)?;
            accounts.split_by_prefix(&builder.as_data_slice())?
        };

        split_shard_impl(
            &left_shard_ident,
            &left_accounts,
            depth - 1,
            shards,
            builder,
        )?;
        split_shard_impl(
            &right_shard_ident,
            &right_accounts,
            depth - 1,
            shards,
            builder,
        )
    }

    split_shard_impl(shard, accounts, depth, shards, &mut CellBuilder::new())
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;

fn extend_account_prefix(shard: &ShardIdent, max: bool, target: &mut [u8; 32]) {
    let mut prefix = shard.prefix();
    if max {
        // Fill remaining bits after the trailing bit
        // 1010000:
        // 1010000 | (1010000 - 1) = 1010000 | 1001111 = 1011111
        prefix |= prefix - 1;
    } else {
        // Remove the trailing bit
        // 1010000:
        // (!1010000 + 1) = 0101111 + 1 = 0110000
        // 1010000 & 0110000 = 0010000 // only trailing bit
        prefix -= extract_tag(shard);
    };
    target[..8].copy_from_slice(&prefix.to_be_bytes());
    target[8..].fill(0xff * max as u8);
}

const fn extract_tag(shard: &ShardIdent) -> u64 {
    let prefix = shard.prefix();
    prefix & (!prefix).wrapping_add(1)
}
