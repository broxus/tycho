use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use everscale_types::cell::Lazy;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::{rocksdb, OwnedSnapshot};

use crate::db::*;
use crate::tables::Transactions;
use crate::util::*;

#[derive(Default, Clone)]
pub struct BlacklistedAccounts {
    inner: Arc<BlacklistedAccountsInner>,
}

impl BlacklistedAccounts {
    pub fn update<I: IntoIterator<Item = StdAddr>>(&self, items: I) {
        let items = items
            .into_iter()
            .map(|item| {
                let mut key = [0; 33];
                key[0] = item.workchain as u8;
                key[1..33].copy_from_slice(item.address.as_array());
                key
            })
            .collect::<FastHashSet<_>>();

        self.inner.accounts.store(Arc::new(items));
    }

    fn load(&self) -> Arc<FastHashSet<AddressKey>> {
        self.inner.accounts.load_full()
    }
}

#[derive(Default)]
struct BlacklistedAccountsInner {
    accounts: ArcSwap<FastHashSet<AddressKey>>,
}

pub struct RpcStorage {
    db: RpcDb,
    min_tx_lt: AtomicU64,
    min_tx_lt_guard: tokio::sync::Mutex<()>,
    snapshot: ArcSwapOption<OwnedSnapshot>,
}

impl RpcStorage {
    pub fn new(db: RpcDb) -> Self {
        let this = Self {
            db,
            min_tx_lt: AtomicU64::new(u64::MAX),
            min_tx_lt_guard: Default::default(),
            snapshot: Default::default(),
        };

        let state = &this.db.state;
        if state.get(INSTANCE_ID).unwrap().is_none() {
            state
                .insert(INSTANCE_ID, rand::random::<InstanceId>())
                .unwrap();
        }

        let min_lt = match state.get(TX_MIN_LT).unwrap() {
            Some(value) if value.is_empty() => None,
            Some(value) => Some(u64::from_le_bytes(value.as_ref().try_into().unwrap())),
            None => None,
        };

        this.min_tx_lt
            .store(min_lt.unwrap_or(u64::MAX), Ordering::Release);

        tracing::debug!(?min_lt, "rpc storage initialized");

        this
    }

    pub fn db(&self) -> &RpcDb {
        &self.db
    }

    pub fn min_tx_lt(&self) -> u64 {
        self.min_tx_lt.load(Ordering::Acquire)
    }

    pub fn update_snapshot(&self) {
        let snapshot = Arc::new(self.db.owned_snapshot());
        self.snapshot.store(Some(snapshot));
    }

    pub fn store_instance_id(&self, id: InstanceId) {
        let rpc_states = &self.db.state;
        rpc_states.insert(INSTANCE_ID, id).unwrap();
    }

    pub fn load_instance_id(&self) -> InstanceId {
        let id = self.db.state.get(INSTANCE_ID).unwrap().unwrap();
        InstanceId::from_slice(id.as_ref())
    }

    pub fn get_brief_shards_descr(&self, mc_seqno: u32) -> Result<Option<Vec<BriefShardDescr>>> {
        let mut key = [0x00; tables::BlocksByMcSeqno::KEY_LEN];
        key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
        key[4] = -1i8 as u8;
        key[5..13].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
        key[13..17].copy_from_slice(&mc_seqno.to_be_bytes());

        let Some(value) = self.db.blocks_by_mc_seqno.get(key)? else {
            return Ok(None);
        };
        let value = value.as_ref();

        let shard_count = u32::from_le_bytes(
            value[tables::BlocksByMcSeqno::VALUE_LEN..tables::BlocksByMcSeqno::VALUE_LEN + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut result = Vec::with_capacity(shard_count);
        for i in 0..shard_count {
            let offset =
                tables::BlocksByMcSeqno::DESCR_OFFSET + i * tables::BlocksByMcSeqno::DESCR_LEN;
            let descr = &value[offset..offset + tables::BlocksByMcSeqno::DESCR_LEN];

            result.push(BriefShardDescr {
                shard_ident: ShardIdent::new(
                    descr[0] as i8 as i32,
                    u64::from_le_bytes(descr[1..9].try_into().unwrap()),
                )
                .context("invalid top shard ident")?,
                seqno: u32::from_le_bytes(descr[9..13].try_into().unwrap()),
                root_hash: HashBytes::from_slice(&descr[13..45]),
                file_hash: HashBytes::from_slice(&descr[45..77]),
                start_lt: u64::from_le_bytes(descr[77..85].try_into().unwrap()),
                end_lt: u64::from_le_bytes(descr[85..93].try_into().unwrap()),
            });
        }

        Ok(Some(result))
    }

    pub fn get_accounts_by_code_hash(
        &self,
        code_hash: &HashBytes,
        continuation: Option<&StdAddr>,
    ) -> Result<CodeHashesIter<'_>> {
        let mut key = [0u8; tables::CodeHashes::KEY_LEN];
        key[0..32].copy_from_slice(code_hash.as_ref());
        if let Some(continuation) = continuation {
            key[32] = continuation.workchain as u8;
            key[33..65].copy_from_slice(continuation.address.as_ref());
        }

        let mut upper_bound = Vec::with_capacity(tables::CodeHashes::KEY_LEN);
        upper_bound.extend_from_slice(&key[..32]);
        upper_bound.extend_from_slice(&[0xff; 33]);

        let mut readopts = self.db.code_hashes.new_read_config();
        // TODO: somehow make the range inclusive since
        // upper_bound is not included in the range
        readopts.set_iterate_upper_bound(upper_bound);

        let snapshot = self.snapshot.load_full();
        if let Some(snapshot) = &snapshot {
            readopts.set_snapshot(snapshot);
        }

        let rocksdb = self.db.rocksdb();
        let code_hashes_cf = self.db.code_hashes.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&code_hashes_cf, readopts);

        iter.seek(key);
        if continuation.is_some() {
            iter.next();
        }

        Ok(CodeHashesIter {
            inner: iter,
            snapshot,
        })
    }

    pub fn get_block_transactions(
        &self,
        block_id: &BlockIdShort,
    ) -> Result<Option<BlockTransactionIdsIter<'_>>> {
        let Ok(workchain) = i8::try_from(block_id.shard.workchain()) else {
            return Ok(None);
        };

        let Some(snapshot) = self.snapshot.load_full() else {
            // TODO: Somehow always use snapshot.
            anyhow::bail!("No snapshot available");
        };

        let mut range_from = [0x00; tables::BlockTransactions::KEY_LEN];
        range_from[0] = workchain as u8;
        range_from[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        range_from[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

        if !self.db.known_blocks.contains_key(&range_from[0..13])? {
            return Ok(None);
        }

        let mut range_to = [0xff; tables::BlockTransactions::KEY_LEN];
        range_to[0..13].copy_from_slice(&range_from[0..13]);

        let mut readopts = self.db.block_transactions.new_read_config();
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());
        readopts.set_snapshot(&snapshot);

        let rocksdb = self.db.rocksdb();
        let block_transactions_cf = self.db.block_transactions.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&block_transactions_cf, readopts);
        iter.seek(range_from);

        Ok(Some(BlockTransactionIdsIter {
            inner: iter,
            snapshot,
        }))
    }

    pub fn get_transactions(
        &self,
        account: &StdAddr,
        mut last_lt: Option<u64>,
        mut to_lt: u64,
    ) -> Result<TransactionsIterBuilder<'_>> {
        if matches!(last_lt, Some(last_lt) if last_lt < to_lt) {
            // Make empty iterator if `last_lt < to_lt`.
            last_lt = Some(u64::MAX);
            to_lt = u64::MAX - 1;
        }

        let mut key = [0u8; tables::Transactions::KEY_LEN];
        key[0] = account.workchain as u8;
        key[1..33].copy_from_slice(account.address.as_ref());
        key[33..].copy_from_slice(&last_lt.unwrap_or(u64::MAX).to_be_bytes());

        let mut lower_bound = Vec::with_capacity(tables::Transactions::KEY_LEN);
        lower_bound.extend_from_slice(&key[..33]);
        lower_bound.extend_from_slice(&to_lt.to_be_bytes());

        let mut readopts = self.db.transactions.new_read_config();
        readopts.set_iterate_lower_bound(lower_bound);

        let snapshot = self.snapshot.load_full();
        if let Some(snapshot) = &snapshot {
            readopts.set_snapshot(snapshot);
        }

        let rocksdb = self.db.rocksdb();
        let transactions_cf = self.db.transactions.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&transactions_cf, readopts);
        iter.seek_for_prev(key);

        Ok(TransactionsIterBuilder {
            inner: iter,
            snapshot,
        })
    }

    pub fn get_transaction(&self, hash: &HashBytes) -> Result<Option<TransactionData<'_>>> {
        let Some(tx_info) = self.db.transactions_by_hash.get(hash)? else {
            return Ok(None);
        };
        let tx_info = &tx_info.as_ref()[..Transactions::KEY_LEN];

        let tx = self.db.transactions.get(tx_info)?;
        Ok(tx.map(TransactionData::new))
    }

    pub fn get_transaction_block_id(&self, hash: &HashBytes) -> Result<Option<BlockId>> {
        let Some(tx_info) = self.db.transactions_by_hash.get(hash)? else {
            return Ok(None);
        };
        let tx_info = tx_info.as_ref();
        if tx_info.len() < tables::TransactionsByHash::VALUE_FULL_LEN {
            return Ok(None);
        }

        let prefix_len = tx_info[41];
        debug_assert!(prefix_len < 64);

        let account_prefix = u64::from_be_bytes(tx_info[1..9].try_into().unwrap());
        let tail_mask = 1u64 << (63 - prefix_len);

        // TODO: Move into types?
        let Some(shard) = ShardIdent::new(
            tx_info[0] as i8 as i32,
            (account_prefix | tail_mask) & !(tail_mask - 1),
        ) else {
            // TODO: unwrap?
            return Ok(None);
        };

        Ok(Some(BlockId {
            shard,
            seqno: u32::from_le_bytes(tx_info[42..46].try_into().unwrap()),
            root_hash: HashBytes::from_slice(&tx_info[46..78]),
            file_hash: HashBytes::from_slice(&tx_info[78..110]),
        }))
    }

    pub fn get_dst_transaction(
        &self,
        in_msg_hash: &HashBytes,
    ) -> Result<Option<TransactionData<'_>>> {
        let Some(key) = self.db.transactions_by_in_msg.get(in_msg_hash)? else {
            return Ok(None);
        };

        let tx = self.db.transactions.get(key)?;
        Ok(tx.map(TransactionData::new))
    }

    #[tracing::instrument(
        level = "info",
        name = "reset_accounts",
        skip_all,
        fields(shard = %shard_state.block_id().shard)
    )]
    pub async fn reset_accounts(
        &self,
        shard_state: ShardStateStuff,
        split_depth: u8,
    ) -> Result<()> {
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
                split_depth,
                &mut virtual_shards,
            )
            .context("failed to split shard state into virtual shards")?;

            // NOTE: Ensure that the root cell is dropped.
            drop(shard_state);
            (guard, virtual_shards)
        };

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        // Rebuild code hashes
        let db = self.db.clone();
        let mut cancelled = cancelled.debounce(10000);
        let span = tracing::Span::current();

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            tracing::info!(split_depth, "started building new code hash indices");
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
                    if cancelled.check() {
                        anyhow::bail!("accounts reset cancelled");
                    }

                    let (id, (_, account)) = entry?;

                    let code_hash = match extract_code_hash(&account)? {
                        ExtractedCodeHash::Exact(Some(code_hash)) => code_hash,
                        ExtractedCodeHash::Exact(None) => continue,
                        ExtractedCodeHash::Skip => anyhow::bail!("code in account state is pruned"),
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

            // Done
            scopeguard::ScopeGuard::into_inner(guard);
            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "finished flushing code hash indices"
            );
            Ok(())
        })
        .await?
    }

    #[tracing::instrument(level = "info", name = "remove_old_transactions", skip(self))]
    pub async fn remove_old_transactions(
        &self,
        mc_seqno: u32,
        min_lt: u64,
        keep_tx_per_account: usize,
    ) -> Result<()> {
        const ITEMS_PER_BATCH: usize = 100000;

        type TxKey = [u8; tables::Transactions::KEY_LEN];

        enum PendingDelete {
            Single,
            Range,
        }

        struct GcState<'a> {
            raw: &'a rocksdb::DB,
            writeopt: &'a rocksdb::WriteOptions,
            tx_cf: weedb::BoundedCfHandle<'a>,
            tx_by_hash: weedb::BoundedCfHandle<'a>,
            tx_by_in_msg: weedb::BoundedCfHandle<'a>,
            key_range_begin: TxKey,
            key_range_end: TxKey,
            pending_delete: Option<PendingDelete>,
            batch: rocksdb::WriteBatch,
            total_tx: usize,
            total_tx_by_hash: usize,
            total_tx_by_in_msg: usize,
        }

        impl<'a> GcState<'a> {
            fn new(db: &'a RpcDb) -> Self {
                Self {
                    raw: db.rocksdb(),
                    writeopt: db.transactions.write_config(),
                    tx_cf: db.transactions.cf(),
                    tx_by_hash: db.transactions_by_hash.cf(),
                    tx_by_in_msg: db.transactions_by_in_msg.cf(),
                    key_range_begin: [0u8; tables::Transactions::KEY_LEN],
                    key_range_end: [0u8; tables::Transactions::KEY_LEN],
                    pending_delete: None,
                    batch: Default::default(),
                    total_tx: 0,
                    total_tx_by_hash: 0,
                    total_tx_by_in_msg: 0,
                }
            }

            fn delete_tx(&mut self, key: &TxKey, value: &[u8]) {
                // Batch multiple deletes for the primary table
                self.pending_delete = Some(if self.pending_delete.is_none() {
                    self.key_range_end.copy_from_slice(key);
                    PendingDelete::Single
                } else {
                    self.key_range_begin.copy_from_slice(key);
                    PendingDelete::Range
                });
                self.total_tx += 1;

                // Must contain at least mask and tx hash
                assert!(value.len() >= 33);

                let mask = TransactionMask::from_bits_retain(value[0]);

                // Delete transaction by hash index entry
                let tx_hash = &value[1..33];
                self.batch.delete_cf(&self.tx_by_hash, tx_hash);
                self.total_tx_by_hash += 1;

                // Delete transaction by incoming message hash index entry
                if mask.has_msg_hash() {
                    assert!(value.len() >= 65);

                    let in_msg_hash = &value[33..65];
                    self.batch.delete_cf(&self.tx_by_in_msg, in_msg_hash);
                    self.total_tx_by_in_msg += 1;
                }
            }

            fn end_account(&mut self) {
                // Flush pending batch
                if let Some(pending) = self.pending_delete.take() {
                    match pending {
                        PendingDelete::Single => self
                            .batch
                            .delete_cf(&self.tx_cf, self.key_range_end.as_slice()),
                        PendingDelete::Range => {
                            // Remove `[begin; end)`
                            self.batch.delete_range_cf(
                                &self.tx_cf,
                                self.key_range_begin.as_slice(),
                                self.key_range_end.as_slice(),
                            );
                            // Remove `end`
                            self.batch
                                .delete_cf(&self.tx_cf, self.key_range_end.as_slice());
                        }
                    }
                }
            }

            fn flush(&mut self) -> Result<()> {
                self.raw
                    .write_opt(std::mem::take(&mut self.batch), self.writeopt)?;
                Ok(())
            }
        }

        if let Some(known_min_lt) = self.db.state.get(TX_MIN_LT)? {
            let known_min_lt = u64::from_le_bytes(known_min_lt.as_ref().try_into().unwrap());
            let was_running = matches!(
                self.db.state.get(TX_GC_RUNNING)?,
                Some(status) if !status.is_empty()
            );

            if !was_running && min_lt <= known_min_lt {
                tracing::info!(known_min_lt, "skipping removal of old transactions");
                return Ok(());
            }
        }

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        // Force update min lt and gc flag
        self.min_tx_lt.store(min_lt, Ordering::Release);

        let db = self.db.clone();
        let mut cancelled = cancelled.debounce(10000);
        let span = tracing::Span::current();

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let raw = db.rocksdb().as_ref();

            tracing::info!("started removing old transactions");
            let started_at = Instant::now();

            // Prepare snapshot and iterator
            let snapshot = raw.snapshot();

            // Delete block transactions.
            'block: {
                let mc_seqno = match mc_seqno.checked_sub(1) {
                    None | Some(0) => break 'block,
                    Some(seqno) => seqno,
                };

                let known_blocks = &db.known_blocks;
                let blocks_by_mc_seqno = &db.blocks_by_mc_seqno;
                let block_transactions = &db.block_transactions;

                // Get masterchain block entry.
                let mut key = [0u8; tables::BlocksByMcSeqno::KEY_LEN];
                key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
                key[4] = -1i8 as u8;
                key[5..13].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
                key[13..17].copy_from_slice(&mc_seqno.to_be_bytes());

                let Some(value) = snapshot.get_pinned_cf_opt(
                    &blocks_by_mc_seqno.cf(),
                    key,
                    blocks_by_mc_seqno.new_read_config(),
                )?
                else {
                    break 'block;
                };
                let value = value.as_ref();
                debug_assert!(value.len() >= tables::BlocksByMcSeqno::VALUE_LEN + 4);

                // Parse top shard block ids (short).
                let shard_count = u32::from_le_bytes(
                    value[tables::BlocksByMcSeqno::VALUE_LEN
                        ..tables::BlocksByMcSeqno::VALUE_LEN + 4]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let mut top_block_ids = Vec::with_capacity(1 + shard_count);
                top_block_ids.push(BlockIdShort {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: mc_seqno,
                });
                for i in 0..shard_count {
                    let offset = tables::BlocksByMcSeqno::DESCR_OFFSET
                        + i * tables::BlocksByMcSeqno::DESCR_LEN;
                    let descr = &value[offset..offset + tables::BlocksByMcSeqno::DESCR_LEN];
                    top_block_ids.push(BlockIdShort {
                        shard: ShardIdent::new(
                            descr[0] as i8 as i32,
                            u64::from_le_bytes(descr[1..9].try_into().unwrap()),
                        )
                        .context("invalid top shard ident")?,
                        seqno: u32::from_le_bytes(descr[9..13].try_into().unwrap()),
                    });
                }

                // Prepare batch.
                let mut batch = rocksdb::WriteBatch::new();

                // Delete `blocks_by_mc_seqno` range before the mc block.
                let range_from = [0x00; tables::BlocksByMcSeqno::KEY_LEN];
                let mut range_to = [0xff; tables::BlocksByMcSeqno::KEY_LEN];
                range_to[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
                batch.delete_range_cf(&blocks_by_mc_seqno.cf(), range_from, range_to);
                batch.delete_cf(&blocks_by_mc_seqno.cf(), range_to);

                // Delete `known_blocks` and `block_transactions` ranges for each shard
                // (including masterchain).
                let mut range_from = [0x00; tables::BlockTransactions::KEY_LEN];
                let mut range_to = [0xff; tables::BlockTransactions::KEY_LEN];
                for block_id in top_block_ids {
                    range_from[0] = block_id.shard.workchain() as i8 as u8;
                    range_from[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
                    range_from[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());
                    range_to[0..13].copy_from_slice(&range_from[0..13]);
                    batch.delete_range_cf(&block_transactions.cf(), range_from, range_to);
                    batch.delete_cf(&block_transactions.cf(), range_to);

                    let range_from = &range_from[0..tables::KnownBlocks::KEY_LEY];
                    let range_to = &range_to[0..tables::KnownBlocks::KEY_LEY];
                    batch.delete_range_cf(&known_blocks.cf(), range_from, range_to);
                    batch.delete_cf(&known_blocks.cf(), range_to);
                }

                // Apply batch.
                db.rocksdb()
                    .write(batch)
                    .context("failed to remove block transactions")?;
            }

            // Remove transactions
            let mut readopts = db.transactions.new_read_config();
            readopts.set_snapshot(&snapshot);
            let mut iter = raw.raw_iterator_cf_opt(&db.transactions.cf(), readopts);
            iter.seek_to_last();

            // Prepare GC state
            let mut gc = GcState::new(&db);

            // `last_account` buffer is used to track the last processed account.
            //
            // The buffer is also used to seek to the beginning of the tx range.
            // Its last 8 bytes are `min_lt`. It forces the `seek_prev` method
            // to jump right to the last tx that is needed to be deleted.
            let mut last_account: TxKey = [0u8; tables::Transactions::KEY_LEN];
            last_account[33..41].copy_from_slice(&min_lt.to_be_bytes());

            let mut items = 0usize;
            let mut total_invalid = 0usize;
            let mut iteration = 0usize;
            let mut tx_count = 0usize;
            loop {
                let Some((key, value)) = iter.item() else {
                    break iter.status()?;
                };
                iteration += 1;

                if cancelled.check() {
                    anyhow::bail!("transactions GC cancelled");
                }

                let Ok::<&TxKey, _>(key) = key.try_into() else {
                    // Remove invalid entires from the primary index only
                    items += 1;
                    total_invalid += 1;
                    gc.batch.delete_cf(&gc.tx_cf, key);
                    iter.prev();
                    continue;
                };

                // Check whether the prev account is processed
                let item_account = &key[..33];
                let is_prev_account = item_account != &last_account[..33];
                if is_prev_account {
                    // Update last account address
                    last_account[..33].copy_from_slice(item_account);

                    // Add pending delete into batch
                    gc.end_account();

                    tx_count = 0;
                }

                // Get lt from the key
                let lt = u64::from_be_bytes(key[33..41].try_into().unwrap());

                if tx_count < keep_tx_per_account {
                    // Keep last `keep_tx_per_account` transactions for account
                    tx_count += 1;
                    iter.prev();
                } else if lt < min_lt {
                    // Add tx and its secondary indices into the batch
                    items += 1;
                    gc.delete_tx(key, value);
                    iter.prev();
                } else if lt > 0 {
                    // Seek to the end of the removed range
                    // (to start removing it backwards).
                    iter.seek_for_prev(last_account.as_slice());
                } else {
                    // Just seek to the previous account.
                    iter.prev();
                }

                // Write batch
                if items >= ITEMS_PER_BATCH {
                    tracing::info!(iteration, "flushing batch");
                    gc.flush()?;
                    items = 0;
                }
            }

            // Add final pending delete into batch
            gc.end_account();

            // Write remaining batch
            if items != 0 {
                gc.flush()?;
            }

            // Reset gc flag
            raw.put(TX_GC_RUNNING, [])?;

            // Done
            scopeguard::ScopeGuard::into_inner(guard);
            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                total_invalid,
                total_tx = gc.total_tx,
                total_tx_by_hash = gc.total_tx_by_hash,
                total_tx_by_in_msg = gc.total_tx_by_in_msg,
                "finished removing old transactions"
            );
            Ok(())
        })
        .await?
    }

    #[tracing::instrument(level = "info", name = "update", skip_all, fields(block_id = %block.id()))]
    pub async fn update(
        &self,
        mc_block_id: &BlockId,
        block: BlockStuff,
        rpc_blacklist: Option<&BlacklistedAccounts>,
    ) -> Result<()> {
        let Ok(workchain) = i8::try_from(block.id().shard.workchain()) else {
            return Ok(());
        };

        let is_masterchain = block.id().is_masterchain();
        let mc_seqno = mc_block_id.seqno;
        let start_lt;
        let end_lt;
        {
            let info = block.load_info()?;
            start_lt = info.start_lt;
            end_lt = info.end_lt;
        }

        let shard_hashes = is_masterchain
            .then(|| {
                let custom = block.load_custom()?;
                Ok::<_, anyhow::Error>(custom.shards.clone())
            })
            .transpose()?;

        let span = tracing::Span::current();
        let db = self.db.clone();

        let rpc_blacklist = rpc_blacklist.map(|x| x.load());

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        tokio::task::spawn_blocking(move || {
            let prepare_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_prepare_batch_time");

            let _span = span.enter();

            let extra = block.load_extra()?;
            let account_blocks = extra.account_blocks.load()?;

            let accounts = if account_blocks.is_empty() {
                Dict::new()
            } else {
                let merkle_update = block.as_ref().state_update.load()?;

                // Accounts dict is stored in the second cell.
                let get_accounts = |cell: Cell| {
                    let mut cs = cell.as_slice()?;
                    cs.skip_first(0, 1)?;
                    cs.load_reference_cloned().map(Cell::virtualize)
                };

                let old_accounts = get_accounts(merkle_update.old)?;
                let new_accounts = get_accounts(merkle_update.new)?;

                if old_accounts.repr_hash() == new_accounts.repr_hash() {
                    Dict::new()
                } else {
                    let accounts = Lazy::<ShardAccounts>::from_raw(new_accounts)?.load()?;
                    let (accounts, _) = accounts.into_parts();
                    accounts
                }
            };

            let mut write_batch = rocksdb::WriteBatch::default();
            let tx_cf = &db.transactions.cf();
            let tx_by_hash_cf = &db.transactions_by_hash.cf();
            let tx_by_in_msg_cf = &db.transactions_by_in_msg.cf();
            let block_txs_cf = &db.block_transactions.cf();

            // Prepare buffer for full tx id
            let mut tx_info = [0u8; tables::TransactionsByHash::VALUE_FULL_LEN];
            tx_info[0] = workchain as u8;

            let block_id = block.id();
            tx_info[41] = block_id.shard.prefix_len() as u8;
            tx_info[42..46].copy_from_slice(&block_id.seqno.to_le_bytes());
            tx_info[46..78].copy_from_slice(block_id.root_hash.as_slice());
            tx_info[78..110].copy_from_slice(block_id.file_hash.as_slice());

            let mut block_tx = [0u8; tables::BlockTransactions::KEY_LEN];
            block_tx[0] = workchain as u8;
            block_tx[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
            block_tx[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

            // Add known block id.
            write_batch.put_cf(
                &db.known_blocks.cf(),
                &block_tx[0..tables::KnownBlocks::KEY_LEY],
                [],
            );

            // Write block info.
            {
                let mut key = [0u8; tables::BlocksByMcSeqno::KEY_LEN];
                key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
                key[4] = workchain as u8;
                key[5..13].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
                key[13..17].copy_from_slice(&block_id.seqno.to_be_bytes());

                let mut value = Vec::with_capacity(tables::BlocksByMcSeqno::VALUE_LEN);
                value.extend_from_slice(block_id.root_hash.as_slice()); // 0..32
                value.extend_from_slice(block_id.file_hash.as_slice()); // 32..64
                value.extend_from_slice(&start_lt.to_le_bytes()); // 64..72
                value.extend_from_slice(&end_lt.to_le_bytes()); // 72..80
                if let Some(shard_hashes) = shard_hashes {
                    let shards = shard_hashes
                        .iter()
                        .filter_map(|item| {
                            let (shard_ident, descr) = match item {
                                Ok(item) => item,
                                Err(e) => return Some(Err(e)),
                            };
                            if i8::try_from(shard_ident.workchain()).is_err() {
                                return None;
                            }

                            Some(Ok(BriefShardDescr {
                                shard_ident,
                                seqno: descr.seqno,
                                root_hash: descr.root_hash,
                                file_hash: descr.file_hash,
                                start_lt: descr.start_lt,
                                end_lt: descr.end_lt,
                            }))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    value.reserve(4 + tables::BlocksByMcSeqno::DESCR_LEN * shards.len());
                    value.extend_from_slice(&(shards.len() as u32).to_le_bytes());
                    for shard in shards {
                        value.push(shard.shard_ident.workchain() as i8 as u8);
                        value.extend_from_slice(&shard.shard_ident.prefix().to_le_bytes());
                        value.extend_from_slice(&shard.seqno.to_le_bytes());
                        value.extend_from_slice(shard.root_hash.as_slice());
                        value.extend_from_slice(shard.file_hash.as_slice());
                        value.extend_from_slice(&shard.start_lt.to_le_bytes());
                        value.extend_from_slice(&shard.end_lt.to_le_bytes());
                    }
                }

                write_batch.put_cf(&db.blocks_by_mc_seqno.cf(), key, value);
            }

            let mut tx_buffer = Vec::with_capacity(1024);

            let rpc_blacklist = rpc_blacklist.as_deref();

            // Iterate through all changed accounts in the block.
            for item in account_blocks.iter() {
                let (account, _, account_block) = item?;

                // Fill account address in the key buffer
                tx_info[1..33].copy_from_slice(account.as_slice());
                block_tx[13..45].copy_from_slice(account.as_slice());

                // Flag to update code hash
                let mut has_special_actions = false;
                let mut was_active = false;
                let mut is_active = false;

                // Process account transactions
                let mut first_tx = true;
                for item in account_block.transactions.values() {
                    let (_, tx_cell) = item?;

                    let tx = tx_cell.load()?;

                    tx_info[33..41].copy_from_slice(&tx.lt.to_be_bytes());
                    block_tx[45..53].copy_from_slice(&tx.lt.to_be_bytes());

                    // Update flags
                    if first_tx {
                        // Remember the original status from the first transaction
                        was_active = tx.orig_status == AccountStatus::Active;
                        first_tx = false;
                    }
                    if was_active && tx.orig_status != AccountStatus::Active {
                        // Handle the case when an account (with some updated code) was deleted,
                        // and then deployed with the initial code (end status).
                        // Treat this situation as a special action.
                        has_special_actions = true;
                    }
                    is_active = tx.end_status == AccountStatus::Active;

                    if !has_special_actions {
                        // Search for special actions (might be code hash update)
                        let info = tx.load_info()?;
                        let action_phase = match &info {
                            TxInfo::Ordinary(info) => info.action_phase.as_ref(),
                            TxInfo::TickTock(info) => info.action_phase.as_ref(),
                        };
                        if let Some(action_phase) = action_phase {
                            has_special_actions |= action_phase.special_actions > 0;
                        }
                    }

                    // Don't write tx for account from blacklist
                    if let Some(blacklist) = &rpc_blacklist {
                        if blacklist.contains(&tx_info[..33]) {
                            continue;
                        }
                    }

                    let tx_hash = tx_cell.inner().repr_hash();
                    let (tx_mask, msg_hash) = match &tx.in_msg {
                        Some(in_msg) => {
                            let hash = Some(in_msg.repr_hash());
                            let mask = TransactionMask::HAS_MSG_HASH;
                            (mask, hash)
                        }
                        None => (TransactionMask::empty(), None),
                    };

                    // Collect transaction data to `tx_buffer`
                    tx_buffer.clear();
                    tx_buffer.push(tx_mask.bits());
                    tx_buffer.extend_from_slice(tx_hash.as_slice());
                    if let Some(msg_hash) = msg_hash {
                        tx_buffer.extend_from_slice(msg_hash.as_slice());
                    }
                    everscale_types::boc::ser::BocHeader::<ahash::RandomState>::with_root(
                        tx_cell.inner().as_ref(),
                    )
                    .encode(&mut tx_buffer);

                    // Write tx data and indices
                    write_batch.put_cf(tx_by_hash_cf, tx_hash.as_slice(), tx_info.as_slice());
                    write_batch.put_cf(block_txs_cf, block_tx.as_slice(), tx_hash.as_slice());

                    if let Some(msg_hash) = msg_hash {
                        write_batch.put_cf(
                            tx_by_in_msg_cf,
                            msg_hash,
                            &tx_info[..tables::Transactions::KEY_LEN],
                        );
                    }

                    write_batch.put_cf(
                        tx_cf,
                        &tx_info[..tables::Transactions::KEY_LEN],
                        &tx_buffer,
                    );
                }

                // Update code hash
                let update = if is_active && (!was_active || has_special_actions) {
                    // Account is active after this block and this is either a new account,
                    // or it was an existing account which possibly changed its code.
                    // Update: just store the code hash.
                    Some(false)
                } else if was_active && !is_active {
                    // Account was active before this block and is not active after the block.
                    // Update: remove the code hash.
                    Some(true)
                } else {
                    // No update for other cases
                    None
                };

                // Apply the update if any
                if let Some(remove) = update {
                    Self::update_code_hash(
                        &db,
                        workchain,
                        &account,
                        &accounts,
                        remove,
                        &mut write_batch,
                    )?;
                }
            }

            drop(prepare_batch_histogram);

            let _execute_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_execute_batch_time");

            db.rocksdb()
                .write_opt(write_batch, db.transactions.write_config())?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        // Update min lt after a successful block processing.
        'min_lt: {
            // Update the runtime value first. Load is relaxed since we just need
            // to know that the value was updated.
            if start_lt < self.min_tx_lt.fetch_min(start_lt, Ordering::Release) {
                // Acquire the operation guard to ensure that there is only one writer.
                let _guard = self.min_tx_lt_guard.lock().await;

                // Do nothing if the value was already updated while we were waiting.
                // Load is Acquire since we need to see the most recent value.
                if start_lt > self.min_tx_lt.load(Ordering::Acquire) {
                    break 'min_lt;
                }

                // Update the value in the database.
                self.db.state.insert(TX_MIN_LT, start_lt.to_le_bytes())?;
            }
        }

        Ok(())
    }

    fn update_code_hash(
        db: &RpcDb,
        workchain: i8,
        account: &HashBytes,
        accounts: &ShardAccountsDict,
        remove: bool,
        write_batch: &mut rocksdb::WriteBatch,
    ) -> Result<()> {
        // Find the new code hash
        let new_code_hash = 'code_hash: {
            if !remove {
                if let Some((_, account)) = accounts.get(account)? {
                    match extract_code_hash(&account)? {
                        ExtractedCodeHash::Exact(hash) => break 'code_hash hash,
                        ExtractedCodeHash::Skip => return Ok(()),
                    }
                }
            }
            None
        };

        // Prepare column families
        let code_hashes_cf = &db.code_hashes.cf();
        let code_hashes_by_address_cf = &db.code_hashes_by_address.cf();

        // Check the secondary index first
        let mut code_hashes_by_address_id = [0u8; tables::CodeHashesByAddress::KEY_LEN];
        code_hashes_by_address_id[0] = workchain as u8;
        code_hashes_by_address_id[1..33].copy_from_slice(account.as_slice());

        // Find the old code hash
        let old_code_hash = db
            .code_hashes_by_address
            .get(code_hashes_by_address_id.as_slice())?;

        if remove && old_code_hash.is_none()
            || matches!(
                (&old_code_hash, &new_code_hash),
                (Some(old), Some(new)) if old.as_ref() == new.as_slice()
            )
        {
            // Code hash should not be changed.
            return Ok(());
        }

        let mut code_hashes_id = [0u8; tables::CodeHashes::KEY_LEN];
        code_hashes_id[32] = workchain as u8;
        code_hashes_id[33..65].copy_from_slice(account.as_slice());

        // Remove entry from the primary index
        if let Some(old_code_hash) = old_code_hash {
            code_hashes_id[..32].copy_from_slice(&old_code_hash);
            write_batch.delete_cf(code_hashes_cf, code_hashes_id.as_slice());
        }

        match new_code_hash {
            Some(new_code_hash) => {
                // Update primary index
                code_hashes_id[..32].copy_from_slice(new_code_hash.as_slice());
                write_batch.put_cf(
                    code_hashes_cf,
                    code_hashes_id.as_slice(),
                    new_code_hash.as_slice(),
                );

                // Update secondary index
                write_batch.put_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                    new_code_hash.as_slice(),
                );
            }
            None => {
                // Remove entry from the secondary index
                write_batch.delete_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                );
            }
        }

        Ok(())
    }

    async fn remove_code_hashes(&self, shard: &ShardIdent) -> Result<()> {
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

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        // Full scan the main code hashes index and remove all entires for the shard
        let db = self.db.clone();
        let mut cancelled = cancelled.debounce(1000);
        let shard = *shard;
        let span = tracing::Span::current();

        // NOTE: `spawn_blocking` is used here instead of `rayon_run` as it is IO-bound task.
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

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
                    None => break iter.status()?,
                };

                if cancelled.check() {
                    anyhow::bail!("remove_code_hashes cancelled");
                }

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

            scopeguard::ScopeGuard::into_inner(guard);
            Ok(())
        })
        .await?
    }
}

pub struct BriefShardDescr {
    pub shard_ident: ShardIdent,
    pub seqno: u32,
    pub root_hash: HashBytes,
    pub file_hash: HashBytes,
    pub start_lt: u64,
    pub end_lt: u64,
}

pub struct CodeHashesIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    snapshot: Option<Arc<OwnedSnapshot>>,
}

impl<'a> CodeHashesIter<'a> {
    pub fn into_raw(self) -> RawCodeHashesIter<'a> {
        RawCodeHashesIter {
            inner: self.inner,
            _snapshot: self.snapshot,
        }
    }
}

impl Iterator for CodeHashesIter<'_> {
    type Item = StdAddr;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.key()?;
        debug_assert!(value.len() == tables::CodeHashes::KEY_LEN);

        let result = Some(StdAddr {
            anycast: None,
            workchain: value[32] as i8,
            address: HashBytes(value[33..65].try_into().unwrap()),
        });
        self.inner.next();
        result
    }
}

pub struct RawCodeHashesIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    _snapshot: Option<Arc<OwnedSnapshot>>,
}

impl Iterator for RawCodeHashesIter<'_> {
    type Item = [u8; 33];

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.key()?;
        debug_assert!(value.len() == tables::CodeHashes::KEY_LEN);

        let result = Some(value[32..65].try_into().unwrap());
        self.inner.next();
        result
    }
}

pub struct BlockTransactionIdsIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    snapshot: Arc<OwnedSnapshot>,
}

impl BlockTransactionIdsIter<'_> {
    #[inline]
    pub fn snapshow(&self) -> &Arc<OwnedSnapshot> {
        &self.snapshot
    }
}

impl Iterator for BlockTransactionIdsIter<'_> {
    type Item = FullTransactionId;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.item()?;
        let res = Some(FullTransactionId {
            account: StdAddr::new(key[0] as i8, HashBytes::from_slice(&key[13..45])),
            lt: u64::from_be_bytes(key[45..53].try_into().unwrap()),
            hash: HashBytes::from_slice(&value[0..32]),
        });
        self.inner.next();
        res
    }
}

#[derive(Debug, Clone)]
pub struct FullTransactionId {
    pub account: StdAddr,
    pub lt: u64,
    pub hash: HashBytes,
}

pub struct TransactionsIterBuilder<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    // NOTE: We must store the snapshot for as long as iterator is alive.
    snapshot: Option<Arc<OwnedSnapshot>>,
}

impl<'a> TransactionsIterBuilder<'a> {
    pub fn map<F, R>(self, map: F) -> TransactionsIter<'a, F>
    where
        for<'s> F: FnMut(&'s [u8]) -> R,
    {
        TransactionsIter {
            inner: self.inner,
            map,
            _snapshot: self.snapshot,
        }
    }
}

pub struct TransactionsIter<'a, F> {
    inner: rocksdb::DBRawIterator<'a>,
    map: F,
    _snapshot: Option<Arc<OwnedSnapshot>>,
}

impl<F, R> Iterator for TransactionsIter<'_, F>
where
    for<'a> F: FnMut(&'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.value()?;
        let result = (self.map)(TransactionData::read_transaction(value))?;
        self.inner.prev();
        Some(result)
    }
}

pub struct TransactionData<'a> {
    data: rocksdb::DBPinnableSlice<'a>,
}

impl<'a> TransactionData<'a> {
    pub fn new(data: rocksdb::DBPinnableSlice<'a>) -> Self {
        Self { data }
    }

    fn read_transaction<T: AsRef<[u8]> + ?Sized>(value: &T) -> &[u8] {
        let value = value.as_ref();
        assert!(!value.is_empty());

        let mask = TransactionMask::from_bits_retain(value[0]);
        let boc_start = if mask.has_msg_hash() { 65 } else { 33 }; // 1 + 32 + (32)

        assert!(boc_start < value.len());

        value[boc_start..].as_ref()
    }
}

impl AsRef<[u8]> for TransactionData<'_> {
    fn as_ref(&self) -> &[u8] {
        Self::read_transaction(self.data.as_ref())
    }
}

enum ExtractedCodeHash {
    Exact(Option<HashBytes>),
    Skip,
}

fn extract_code_hash(account: &ShardAccount) -> Result<ExtractedCodeHash> {
    if account.account.inner().descriptor().is_pruned_branch() {
        return Ok(ExtractedCodeHash::Skip);
    }

    if let Some(account) = account.load_account()? {
        if let AccountState::Active(state_init) = &account.state {
            if let Some(code) = &state_init.code {
                return Ok(ExtractedCodeHash::Exact(Some(*code.repr_hash())));
            }
        }
    }

    Ok(ExtractedCodeHash::Exact(None))
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
            builder.clear_bits();
            let prefix_len = shard.prefix_len();
            if prefix_len > 0 {
                builder.store_uint(shard.prefix() >> (64 - prefix_len), prefix_len)?;
            }
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

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct TransactionMask: u8 {
        const HAS_MSG_HASH = 1 << 0;
    }
}

impl TransactionMask {
    pub fn has_msg_hash(&self) -> bool {
        self.contains(TransactionMask::HAS_MSG_HASH)
    }
}

type AddressKey = [u8; 33];

const TX_MIN_LT: &[u8] = b"tx_min_lt";
const TX_GC_RUNNING: &[u8] = b"tx_gc_running";
const INSTANCE_ID: &[u8] = b"instance_id";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_prefix() {
        let prefix_len = 10;

        let account_prefix = 0xabccdeadaaaaaaaa;
        let tail_mask = 1u64 << (63 - prefix_len);

        let shard = ShardIdent::new(0, (account_prefix | tail_mask) & !(tail_mask - 1)).unwrap();
        assert_eq!(shard, unsafe {
            ShardIdent::new_unchecked(0, 0xabe0000000000000)
        });
    }
}
