use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::kv::InstanceId;
use tycho_types::cell::Lazy;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb;

use super::db::RpcDb;
use super::tables::{self, Transactions};

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
    snapshot: ArcSwapOption<weedb::OwnedSnapshot>,
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

    pub fn load_snapshot(&self) -> Option<RpcSnapshot> {
        self.snapshot.load_full().map(RpcSnapshot)
    }

    pub fn store_instance_id(&self, id: InstanceId) {
        let rpc_states = &self.db.state;
        rpc_states.insert(INSTANCE_ID, id).unwrap();
    }

    pub fn load_instance_id(&self) -> InstanceId {
        let id = self.db.state.get(INSTANCE_ID).unwrap().unwrap();
        InstanceId::from_slice(id.as_ref())
    }

    pub fn get_known_mc_blocks_range(
        &self,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<(u32, u32)>> {
        let mut snapshot = snapshot.cloned();
        if snapshot.is_none() {
            snapshot = self.snapshot.load_full().map(RpcSnapshot);
        }

        let table = &self.db.known_blocks;

        let mut range_from = [0x00; tables::KnownBlocks::KEY_LEN];
        range_from[0] = -1i8 as u8;
        range_from[1..9].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
        let mut range_to = [0xff; tables::KnownBlocks::KEY_LEN];
        range_to[0..9].clone_from_slice(&range_from[0..9]);

        let mut readopts = table.new_read_config();
        if let Some(snapshot) = &snapshot {
            readopts.set_snapshot(snapshot);
        }
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&table.cf(), readopts);

        iter.seek(range_from.as_slice());
        Ok(if let Some(key) = iter.key() {
            let from_seqno = u32::from_be_bytes(key[9..13].try_into().unwrap());
            let mut to_seqno = from_seqno;

            iter.seek_for_prev(range_to.as_slice());
            if let Some(key) = iter.key() {
                let seqno = u32::from_be_bytes(key[9..13].try_into().unwrap());
                to_seqno = std::cmp::max(to_seqno, seqno);
            }

            Some((from_seqno, to_seqno))
        } else {
            iter.status()?;
            None
        })
    }

    pub fn get_blocks_by_mc_seqno(
        &self,
        mc_seqno: u32,
        mut snapshot: Option<RpcSnapshot>,
    ) -> Result<Option<BlocksByMcSeqnoIter>> {
        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = -1i8 as u8;
        key[1..9].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
        key[9..13].copy_from_slice(&mc_seqno.to_be_bytes());

        if snapshot.is_none() {
            snapshot = self.snapshot.load_full().map(RpcSnapshot);
        }
        let Some(snapshot) = snapshot else {
            // TODO: Somehow always use snapshot.
            anyhow::bail!("No snapshot available");
        };

        let table = &self.db.known_blocks;
        if table.get_ext(key, Some(&snapshot))?.is_none() {
            return Ok(None);
        };

        let mut range_from = [0x00; tables::BlocksByMcSeqno::KEY_LEN];
        range_from[0..4].clone_from_slice(&mc_seqno.to_be_bytes());
        let mut range_to = [0xff; tables::BlocksByMcSeqno::KEY_LEN];
        range_to[0..4].clone_from_slice(&mc_seqno.to_be_bytes());

        let table = &self.db.blocks_by_mc_seqno;
        let mut readopts = table.new_read_config();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());

        let rocksdb = self.db.rocksdb();
        let mut iter = rocksdb.raw_iterator_cf_opt(&table.cf(), readopts);
        iter.seek(range_from.as_slice());

        Ok(Some(BlocksByMcSeqnoIter {
            mc_seqno,
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { weedb::OwnedRawIterator::new(rocksdb.clone(), iter) },
            snapshot,
        }))
    }

    pub fn get_brief_block_info(
        &self,
        block_id: &BlockIdShort,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<(BlockId, u32, BriefBlockInfo)>> {
        let Ok(workchain) = i8::try_from(block_id.shard.workchain()) else {
            return Ok(None);
        };
        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = workchain as u8;
        key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

        let table = &self.db.known_blocks;
        let Some(value) = table.get_ext(key, snapshot)? else {
            return Ok(None);
        };
        let value = value.as_ref();

        let brief_info = BriefBlockInfo::load_from_bytes(workchain as i32, &value[68..])
            .context("invalid brief info")?;

        let block_id = BlockId {
            shard: block_id.shard,
            seqno: block_id.seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        };
        let mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());

        Ok(Some((block_id, mc_seqno, brief_info)))
    }

    pub fn get_brief_shards_descr(
        &self,
        mc_seqno: u32,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<Vec<BriefShardDescr>>> {
        let mut key = [0x00; tables::BlocksByMcSeqno::KEY_LEN];
        key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
        key[4] = -1i8 as u8;
        key[5..13].copy_from_slice(&ShardIdent::PREFIX_FULL.to_be_bytes());
        key[13..17].copy_from_slice(&mc_seqno.to_be_bytes());

        let table = &self.db.blocks_by_mc_seqno;
        let Some(value) = table.get_ext(key, snapshot)? else {
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
        mut snapshot: Option<RpcSnapshot>,
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

        if snapshot.is_none() {
            snapshot = self.snapshot.load_full().map(RpcSnapshot);
        }
        let snapshot = snapshot.unwrap_or_else(|| RpcSnapshot(Arc::new(self.db.owned_snapshot())));
        readopts.set_snapshot(&snapshot);

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
        reverse: bool,
        cursor: Option<&BlockTransactionsCursor>,
        snapshot: Option<RpcSnapshot>,
    ) -> Result<Option<BlockTransactionsIterBuilder>> {
        let Some(ids) = self.get_block_transaction_ids(block_id, reverse, cursor, snapshot)? else {
            return Ok(None);
        };

        Ok(Some(BlockTransactionsIterBuilder {
            ids,
            transactions_cf: self.db.transactions.get_unbounded_cf(),
        }))
    }

    pub fn get_block_transaction_ids(
        &self,
        block_id: &BlockIdShort,
        reverse: bool,
        cursor: Option<&BlockTransactionsCursor>,
        mut snapshot: Option<RpcSnapshot>,
    ) -> Result<Option<BlockTransactionIdsIter>> {
        let Ok(workchain) = i8::try_from(block_id.shard.workchain()) else {
            return Ok(None);
        };

        if snapshot.is_none() {
            snapshot = self.snapshot.load_full().map(RpcSnapshot);
        }
        let Some(snapshot) = snapshot else {
            // TODO: Somehow always use snapshot.
            anyhow::bail!("No snapshot available");
        };

        let mut range_from = [0x00; tables::BlockTransactions::KEY_LEN];
        range_from[0] = workchain as u8;
        range_from[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
        range_from[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

        if let Some(cursor) = cursor {
            range_from[13..45].copy_from_slice(cursor.hash.as_slice());
            range_from[45..53].copy_from_slice(&cursor.lt.to_be_bytes());
        }

        let table = &self.db.known_blocks;
        let ref_by_mc_seqno;
        let block_id = match table.get_ext(&range_from[0..13], Some(&snapshot))? {
            Some(value) => {
                let value = value.as_ref();
                ref_by_mc_seqno = u32::from_le_bytes(value[64..68].try_into().unwrap());
                BlockId {
                    shard: block_id.shard,
                    seqno: block_id.seqno,
                    root_hash: HashBytes::from_slice(&value[0..32]),
                    file_hash: HashBytes::from_slice(&value[32..64]),
                }
            }
            None => return Ok(None),
        };

        let mut range_to = [0xff; tables::BlockTransactions::KEY_LEN];
        range_to[0..13].copy_from_slice(&range_from[0..13]);

        let mut readopts = self.db.block_transactions.new_read_config();
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());
        readopts.set_snapshot(&snapshot);

        let rocksdb = self.db.rocksdb();
        let block_transactions_cf = self.db.block_transactions.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&block_transactions_cf, readopts);

        if reverse {
            iter.seek_for_prev(range_to);
        } else {
            iter.seek(range_from);
        }

        if cursor.is_some()
            && let Some(key) = iter.key()
            && key == range_from.as_slice()
        {
            if reverse {
                iter.prev();
            } else {
                iter.next();
            }
        }

        Ok(Some(BlockTransactionIdsIter {
            block_id,
            ref_by_mc_seqno,
            is_reversed: reverse,
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { weedb::OwnedRawIterator::new(rocksdb.clone(), iter) },
            snapshot,
        }))
    }

    pub fn get_transactions(
        &self,
        account: &StdAddr,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        reverse: bool,
        mut snapshot: Option<RpcSnapshot>,
    ) -> Result<TransactionsIterBuilder> {
        let mut start_lt = start_lt.unwrap_or_default();
        let mut end_lt = end_lt.unwrap_or(u64::MAX);
        if end_lt < start_lt {
            // Make empty iterator if `end_lt < start_lt`.
            start_lt = u64::MAX - 1;
            end_lt = u64::MAX;
        }

        if snapshot.is_none() {
            snapshot = self.snapshot.load_full().map(RpcSnapshot);
        }
        let snapshot = snapshot.unwrap_or_else(|| RpcSnapshot(Arc::new(self.db.owned_snapshot())));

        let mut range_from = [0u8; tables::Transactions::KEY_LEN];
        range_from[0] = account.workchain as u8;
        range_from[1..33].copy_from_slice(account.address.as_ref());
        range_from[33..41].copy_from_slice(&start_lt.to_be_bytes());
        let mut range_to = range_from;
        // NOTE: Compute upper bound as `end_lt + 1` since it will
        // not be included in the iteration result.
        range_to[33..41].copy_from_slice(&end_lt.saturating_add(1).to_be_bytes());

        let mut readopts = self.db.transactions.new_read_config();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_lower_bound(range_from.as_slice());
        readopts.set_iterate_upper_bound(range_to.as_slice());

        let rocksdb = self.db.rocksdb();
        let transactions_cf = self.db.transactions.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&transactions_cf, readopts);
        if reverse {
            iter.seek_for_prev(range_to.as_slice());
        } else {
            iter.seek(range_from.as_slice());
        }
        iter.status()?;

        Ok(TransactionsIterBuilder {
            is_reversed: reverse,
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { weedb::OwnedRawIterator::new(rocksdb.clone(), iter) },
            snapshot,
        })
    }

    pub fn get_transaction(
        &self,
        hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'_>>> {
        let table = &self.db.transactions_by_hash;
        let Some(tx_info) = table.get_ext(hash, snapshot)? else {
            return Ok(None);
        };
        let key = &tx_info.as_ref()[..Transactions::KEY_LEN];

        let table = &self.db.transactions;
        let tx = table.get_ext(key, snapshot)?;
        Ok(tx.map(TransactionData::new))
    }

    pub fn get_transaction_ext<'db>(
        &'db self,
        hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionDataExt<'db>>> {
        let table = &self.db.transactions_by_hash;
        let Some(tx_info) = table.get_ext(hash, snapshot)? else {
            return Ok(None);
        };
        let tx_info = tx_info.as_ref();
        let Some(info) = TransactionInfo::from_bytes(tx_info) else {
            return Ok(None);
        };

        let table = &self.db.transactions;
        let tx = table.get_ext(&tx_info[..Transactions::KEY_LEN], snapshot)?;

        Ok(tx.map(move |data| TransactionDataExt {
            info,
            data: TransactionData::new(data),
        }))
    }

    pub fn get_transaction_info(
        &self,
        hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionInfo>> {
        let table = &self.db.transactions_by_hash;
        let Some(tx_info) = table.get_ext(hash, snapshot)? else {
            return Ok(None);
        };
        Ok(TransactionInfo::from_bytes(&tx_info))
    }

    pub fn get_src_transaction<'db>(
        &'db self,
        account: &StdAddr,
        message_lt: u64,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'db>>> {
        let table = &self.db.transactions;

        let owned_snapshot;
        let snapshot = match snapshot {
            Some(snapshot) => snapshot,
            None => {
                owned_snapshot = self
                    .load_snapshot()
                    .unwrap_or_else(|| RpcSnapshot(Arc::new(self.db.owned_snapshot())));
                &owned_snapshot
            }
        };

        let mut key = [0u8; tables::Transactions::KEY_LEN];
        key[0] = account.workchain as u8;
        key[1..33].copy_from_slice(account.address.as_slice());

        let lower_bound = key;
        key[33..41].copy_from_slice(&message_lt.to_be_bytes());

        let mut readopts = table.new_read_config();
        readopts.set_iterate_lower_bound(lower_bound);
        readopts.set_iterate_upper_bound(key);
        readopts.set_snapshot(snapshot);
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&table.cf(), readopts);
        iter.seek_for_prev(key.as_slice());

        // TODO: Allow TransactionData to store iterator/data itself.
        let Some(tx_key) = iter.key() else {
            iter.status()?;
            return Ok(None);
        };
        if tx_key[0..33] != key[0..33] {
            return Ok(None);
        }

        let tx = table.get_ext(tx_key, Some(snapshot))?;

        Ok(tx.map(TransactionData::new))
    }

    pub fn get_dst_transaction<'db>(
        &'db self,
        in_msg_hash: &HashBytes,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<TransactionData<'db>>> {
        let table = &self.db.transactions_by_in_msg;
        let Some(key) = table.get_ext(in_msg_hash, snapshot)? else {
            return Ok(None);
        };

        let table = &self.db.transactions;
        let tx = table.get_ext(key, snapshot)?;
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

                    let range_from = &range_from[0..tables::KnownBlocks::KEY_LEN];
                    let range_to = &range_to[0..tables::KnownBlocks::KEY_LEN];
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
        let start_lt = tokio::task::spawn_blocking(move || {
            let prepare_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_prepare_batch_time");

            let _span = span.enter();

            let info = block.load_info()?;
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
            tx_info[110..114].copy_from_slice(&mc_seqno.to_le_bytes());

            let mut block_tx = [0u8; tables::BlockTransactions::KEY_LEN];
            block_tx[0] = workchain as u8;
            block_tx[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
            block_tx[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

            // Prepare buffer.
            let mut buffer = Vec::with_capacity(64 + BriefBlockInfo::MIN_BYTE_LEN);

            // Write block info.
            {
                let mut key = [0u8; tables::BlocksByMcSeqno::KEY_LEN];
                key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
                key[4] = workchain as u8;
                key[5..13].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
                key[13..17].copy_from_slice(&block_id.seqno.to_be_bytes());

                buffer.clear();
                buffer.extend_from_slice(block_id.root_hash.as_slice()); // 0..32
                buffer.extend_from_slice(block_id.file_hash.as_slice()); // 32..64
                buffer.extend_from_slice(&info.start_lt.to_le_bytes()); // 64..72
                buffer.extend_from_slice(&info.end_lt.to_le_bytes()); // 72..80
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

                    buffer.reserve(4 + tables::BlocksByMcSeqno::DESCR_LEN * shards.len());
                    buffer.extend_from_slice(&(shards.len() as u32).to_le_bytes());
                    for shard in shards {
                        buffer.push(shard.shard_ident.workchain() as i8 as u8);
                        buffer.extend_from_slice(&shard.shard_ident.prefix().to_le_bytes());
                        buffer.extend_from_slice(&shard.seqno.to_le_bytes());
                        buffer.extend_from_slice(shard.root_hash.as_slice());
                        buffer.extend_from_slice(shard.file_hash.as_slice());
                        buffer.extend_from_slice(&shard.start_lt.to_le_bytes());
                        buffer.extend_from_slice(&shard.end_lt.to_le_bytes());
                    }
                }

                write_batch.put_cf(&db.blocks_by_mc_seqno.cf(), key, buffer.as_slice());
            }

            let rpc_blacklist = rpc_blacklist.as_deref();

            // Iterate through all changed accounts in the block.
            let mut block_tx_count = 0usize;
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

                    // TODO: Should we increase this counter only for non-blacklisted accounts?
                    block_tx_count += 1;

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
                    if let Some(blacklist) = &rpc_blacklist
                        && blacklist.contains(&tx_info[..33])
                    {
                        continue;
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
                    buffer.clear();
                    buffer.push(tx_mask.bits());
                    buffer.extend_from_slice(tx_hash.as_slice());
                    if let Some(msg_hash) = msg_hash {
                        buffer.extend_from_slice(msg_hash.as_slice());
                    }
                    tycho_types::boc::ser::BocHeader::<ahash::RandomState>::with_root(
                        tx_cell.inner().as_ref(),
                    )
                    .encode(&mut buffer);

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

                    write_batch.put_cf(tx_cf, &tx_info[..tables::Transactions::KEY_LEN], &buffer);
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

            // Write block info.
            let brief_block_info =
                BriefBlockInfo::new(block.as_ref(), info, extra, block_tx_count)?;
            buffer.clear();
            buffer.extend_from_slice(&tx_info[46..114]); // root_hash + file_hash + mc_seqno
            brief_block_info.write_to_bytes(&mut buffer); // everything else

            write_batch.put_cf(
                &db.known_blocks.cf(),
                &block_tx[0..tables::KnownBlocks::KEY_LEN],
                buffer.as_slice(),
            );

            drop(prepare_batch_histogram);

            let _execute_batch_histogram =
                HistogramGuard::begin("tycho_storage_rpc_execute_batch_time");

            db.rocksdb()
                .write_opt(write_batch, db.transactions.write_config())?;

            Ok::<_, anyhow::Error>(info.start_lt)
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
            if !remove && let Some((_, account)) = accounts.get(account)? {
                match extract_code_hash(&account)? {
                    ExtractedCodeHash::Exact(hash) => break 'code_hash hash,
                    ExtractedCodeHash::Skip => return Ok(()),
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

trait TableExt {
    fn get_ext<'db, K: AsRef<[u8]>>(
        &'db self,
        key: K,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'db>>>;
}

impl<T: weedb::ColumnFamily> TableExt for weedb::Table<T> {
    fn get_ext<'db, K: AsRef<[u8]>>(
        &'db self,
        key: K,
        snapshot: Option<&RpcSnapshot>,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'db>>> {
        match snapshot {
            None => self.get(key),
            Some(snapshot) => {
                anyhow::ensure!(
                    Arc::ptr_eq(snapshot.db(), self.db()),
                    "snapshot must be made for the same DB instance"
                );

                let mut readopts = self.new_read_config();
                readopts.set_snapshot(snapshot);
                self.db().get_pinned_cf_opt(&self.cf(), key, &readopts)
            }
        }
        .map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct BriefShardDescr {
    pub shard_ident: ShardIdent,
    pub seqno: u32,
    pub root_hash: HashBytes,
    pub file_hash: HashBytes,
    pub start_lt: u64,
    pub end_lt: u64,
}

#[derive(Debug, Clone)]
pub struct BriefBlockInfo {
    pub global_id: i32,
    pub version: u32,
    pub flags: u8,
    pub after_merge: bool,
    pub after_split: bool,
    pub before_split: bool,
    pub want_merge: bool,
    pub want_split: bool,
    pub validator_list_hash_short: u32,
    pub catchain_seqno: u32,
    pub min_ref_mc_seqno: u32,
    pub is_key_block: bool,
    pub prev_key_block_seqno: u32,
    pub start_lt: u64,
    pub end_lt: u64,
    pub gen_utime: u32,
    pub vert_seqno: u32,
    pub rand_seed: HashBytes,
    pub tx_count: u32,
    pub master_ref: Option<BlockId>,
    pub prev_blocks: Vec<BlockId>,
}

impl BriefBlockInfo {
    const VERSION: u8 = 0;
    const MIN_BYTE_LEN: usize = 256;

    fn new(
        block: &Block,
        info: &BlockInfo,
        extra: &BlockExtra,
        tx_count: usize,
    ) -> Result<Self, tycho_types::error::Error> {
        let shard_ident = info.shard;
        let prev_blocks = match info.load_prev_ref()? {
            PrevBlockRef::Single(block_ref) => vec![block_ref.as_block_id(shard_ident)],
            PrevBlockRef::AfterMerge { left, right } => vec![
                left.as_block_id(shard_ident),
                right.as_block_id(shard_ident),
            ],
        };

        Ok(Self {
            global_id: block.global_id,
            version: info.version,
            flags: info.flags,
            after_merge: info.after_merge,
            after_split: info.after_split,
            before_split: info.before_split,
            want_merge: info.want_merge,
            want_split: info.want_split,
            validator_list_hash_short: info.gen_validator_list_hash_short,
            catchain_seqno: info.gen_catchain_seqno,
            min_ref_mc_seqno: info.min_ref_mc_seqno,
            is_key_block: info.key_block,
            prev_key_block_seqno: info.prev_key_block_seqno,
            start_lt: info.start_lt,
            end_lt: info.end_lt,
            gen_utime: info.gen_utime,
            vert_seqno: info.vert_seqno,
            rand_seed: extra.rand_seed,
            tx_count: tx_count.try_into().unwrap_or(u32::MAX),
            master_ref: info
                .load_master_ref()?
                .map(|r| r.as_block_id(ShardIdent::MASTERCHAIN)),
            prev_blocks,
        })
    }

    fn write_to_bytes(&self, target: &mut Vec<u8>) {
        // NOTE: Bit 0 is reserved for future.
        let packed_flags = ((self.master_ref.is_some() as u8) << 7)
            | ((self.after_merge as u8) << 6)
            | ((self.before_split as u8) << 5)
            | ((self.after_split as u8) << 4)
            | ((self.want_split as u8) << 3)
            | ((self.want_merge as u8) << 2)
            | ((self.is_key_block as u8) << 1);

        target.reserve(Self::MIN_BYTE_LEN);
        target.push(Self::VERSION);
        target.extend_from_slice(&self.global_id.to_le_bytes());
        target.extend_from_slice(&self.version.to_le_bytes());
        target.push(self.flags);
        target.push(packed_flags);
        target.extend_from_slice(&self.validator_list_hash_short.to_le_bytes());
        target.extend_from_slice(&self.catchain_seqno.to_le_bytes());
        target.extend_from_slice(&self.min_ref_mc_seqno.to_le_bytes());
        target.extend_from_slice(&self.prev_key_block_seqno.to_le_bytes());
        target.extend_from_slice(&self.start_lt.to_le_bytes());
        target.extend_from_slice(&self.end_lt.to_le_bytes());
        target.extend_from_slice(&self.gen_utime.to_le_bytes());
        target.extend_from_slice(&self.vert_seqno.to_le_bytes());
        target.extend_from_slice(self.rand_seed.as_slice());
        target.extend_from_slice(&self.tx_count.to_le_bytes());
        if let Some(block_id) = &self.master_ref {
            target.extend_from_slice(&block_id.seqno.to_le_bytes());
            target.extend_from_slice(block_id.root_hash.as_slice());
            target.extend_from_slice(block_id.file_hash.as_slice());
        }
        target.push(self.prev_blocks.len() as u8);
        for block_id in &self.prev_blocks {
            target.extend_from_slice(&block_id.shard.prefix().to_le_bytes());
            target.extend_from_slice(&block_id.seqno.to_le_bytes());
            target.extend_from_slice(block_id.root_hash.as_slice());
            target.extend_from_slice(block_id.file_hash.as_slice());
        }
    }

    fn load_from_bytes(workchain: i32, mut bytes: &[u8]) -> Option<Self> {
        use bytes::Buf;

        if bytes.get_u8() != Self::VERSION {
            return None;
        }

        let global_id = bytes.get_i32_le();
        let version = bytes.get_u32_le();
        let [flags, packed_flags] = bytes.get_u16().to_be_bytes();
        let validator_list_hash_short = bytes.get_u32_le();
        let catchain_seqno = bytes.get_u32_le();
        let min_ref_mc_seqno = bytes.get_u32_le();
        let prev_key_block_seqno = bytes.get_u32_le();
        let start_lt = bytes.get_u64_le();
        let end_lt = bytes.get_u64_le();
        let gen_utime = bytes.get_u32_le();
        let vert_seqno = bytes.get_u32_le();
        let rand_seed = HashBytes::from_slice(&bytes[..32]);
        bytes = &bytes[32..];
        let tx_count = bytes.get_u32_le();

        let master_ref = if packed_flags & 0b10000000 != 0 {
            let seqno = bytes.get_u32_le();
            let root_hash = HashBytes::from_slice(&bytes[0..32]);
            let file_hash = HashBytes::from_slice(&bytes[32..64]);
            bytes = &bytes[64..];
            Some(BlockId {
                shard: ShardIdent::MASTERCHAIN,
                seqno,
                root_hash,
                file_hash,
            })
        } else {
            None
        };

        let prev_block_count = bytes.get_u8();
        let mut prev_blocks = Vec::with_capacity(prev_block_count as _);
        for _ in 0..prev_block_count {
            prev_blocks.push(BlockId {
                shard: ShardIdent::new(
                    workchain,
                    u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                )
                .unwrap(),
                seqno: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                root_hash: HashBytes::from_slice(&bytes[12..44]),
                file_hash: HashBytes::from_slice(&bytes[44..76]),
            });
            bytes = &bytes[76..];
        }

        Some(Self {
            global_id,
            version,
            flags,
            after_merge: packed_flags & 0b01000000 != 0,
            after_split: packed_flags & 0b00010000 != 0,
            before_split: packed_flags & 0b00100000 != 0,
            want_merge: packed_flags & 0b00000100 != 0,
            want_split: packed_flags & 0b00001000 != 0,
            validator_list_hash_short,
            catchain_seqno,
            min_ref_mc_seqno,
            is_key_block: packed_flags & 0b00000010 != 0,
            prev_key_block_seqno,
            start_lt,
            end_lt,
            gen_utime,
            vert_seqno,
            rand_seed,
            tx_count,
            master_ref,
            prev_blocks,
        })
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct RpcSnapshot(Arc<weedb::OwnedSnapshot>);

impl std::ops::Deref for RpcSnapshot {
    type Target = weedb::OwnedSnapshot;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

pub struct BlocksByMcSeqnoIter {
    mc_seqno: u32,
    inner: weedb::OwnedRawIterator,
    snapshot: RpcSnapshot,
}

impl BlocksByMcSeqnoIter {
    pub fn mc_seqno(&self) -> u32 {
        self.mc_seqno
    }

    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }
}

impl Iterator for BlocksByMcSeqnoIter {
    // TODO: Extend with LT range?
    type Item = BlockId;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.item()?;
        let shard = ShardIdent::new(
            key[4] as i8 as i32,
            u64::from_be_bytes(key[5..13].try_into().unwrap()),
        )
        .expect("stored shard must have a valid prefix");
        let seqno = u32::from_be_bytes(key[13..17].try_into().unwrap());

        let block_id = BlockId {
            shard,
            seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        };
        self.inner.next();

        Some(block_id)
    }
}

pub struct CodeHashesIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
    snapshot: RpcSnapshot,
}

impl<'a> CodeHashesIter<'a> {
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }

    pub fn into_raw(self) -> RawCodeHashesIter<'a> {
        RawCodeHashesIter {
            inner: self.inner,
            snapshot: self.snapshot,
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
    snapshot: RpcSnapshot,
}

impl RawCodeHashesIter<'_> {
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockTransactionsCursor {
    pub hash: HashBytes,
    pub lt: u64,
}

pub struct BlockTransactionIdsIter {
    block_id: BlockId,
    ref_by_mc_seqno: u32,
    is_reversed: bool,
    inner: weedb::OwnedRawIterator,
    snapshot: RpcSnapshot,
}

impl BlockTransactionIdsIter {
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn ref_by_mc_seqno(&self) -> u32 {
        self.ref_by_mc_seqno
    }

    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }
}

impl Iterator for BlockTransactionIdsIter {
    type Item = FullTransactionId;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.item()?;
        let res = Some(FullTransactionId {
            account: StdAddr::new(key[0] as i8, HashBytes::from_slice(&key[13..45])),
            lt: u64::from_be_bytes(key[45..53].try_into().unwrap()),
            hash: HashBytes::from_slice(&value[0..32]),
        });
        if self.is_reversed {
            self.inner.prev();
        } else {
            self.inner.next();
        }
        res
    }
}

pub struct BlockTransactionsIterBuilder {
    ids: BlockTransactionIdsIter,
    transactions_cf: weedb::UnboundedCfHandle,
}

impl BlockTransactionsIterBuilder {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.ids.is_reversed()
    }

    #[inline]
    pub fn block_id(&self) -> &BlockId {
        self.ids.block_id()
    }

    #[inline]
    pub fn ref_by_mc_seqno(&self) -> u32 {
        self.ids.ref_by_mc_seqno()
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        self.ids.snapshot()
    }

    #[inline]
    pub fn into_ids(self) -> BlockTransactionIdsIter {
        self.ids
    }

    pub fn map<F, R>(self, map: F) -> BlockTransactionsIter<F>
    where
        for<'a> F: FnMut(&'a StdAddr, u64, &'a [u8]) -> R,
    {
        BlockTransactionsIter {
            ids: self.ids,
            transactions_cf: self.transactions_cf,
            map,
        }
    }
}

pub struct BlockTransactionsIter<F> {
    ids: BlockTransactionIdsIter,
    transactions_cf: weedb::UnboundedCfHandle,
    map: F,
}

impl<F> BlockTransactionsIter<F> {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.ids.is_reversed()
    }

    #[inline]
    pub fn block_id(&self) -> &BlockId {
        self.ids.block_id()
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        self.ids.snapshot()
    }

    #[inline]
    pub fn into_ids(self) -> BlockTransactionIdsIter {
        self.ids
    }
}

impl<F, R> Iterator for BlockTransactionsIter<F>
where
    for<'a> F: FnMut(&'a StdAddr, u64, &'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id = self.ids.next()?;

            let mut key = [0; tables::Transactions::KEY_LEN];
            key[0] = id.account.workchain as u8;
            key[1..33].copy_from_slice(id.account.address.as_slice());
            key[33..41].copy_from_slice(&id.lt.to_be_bytes());

            let cf = self.transactions_cf.bound();
            let value = match self.ids.snapshot.get_pinned_cf(&cf, key) {
                Ok(Some(value)) => value,
                // TODO: Maybe return error here?
                Ok(None) => continue,
                // TODO: Maybe return error here?
                Err(_) => return None,
            };
            break (self.map)(
                &id.account,
                id.lt,
                TransactionData::read_transaction(&value),
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullTransactionId {
    pub account: StdAddr,
    pub lt: u64,
    pub hash: HashBytes,
}

pub struct TransactionsIterBuilder {
    is_reversed: bool,
    inner: weedb::OwnedRawIterator,
    // NOTE: We must store the snapshot for as long as iterator is alive.
    snapshot: RpcSnapshot,
}

impl TransactionsIterBuilder {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }

    pub fn map<F, R>(self, map: F) -> TransactionsIter<F, false>
    where
        for<'a> F: FnMut(&'a [u8]) -> R,
    {
        TransactionsIter {
            is_reversed: self.is_reversed,
            inner: self.inner,
            map,
            snapshot: self.snapshot,
        }
    }

    pub fn map_ext<F, R>(self, map: F) -> TransactionsIter<F, true>
    where
        for<'a> F: FnMut(u64, &'a HashBytes, &'a [u8]) -> R,
    {
        TransactionsIter {
            is_reversed: self.is_reversed,
            inner: self.inner,
            map,
            snapshot: self.snapshot,
        }
    }
}

pub struct TransactionsIter<F, const EXT: bool> {
    is_reversed: bool,
    inner: weedb::OwnedRawIterator,
    map: F,
    snapshot: RpcSnapshot,
}

pub type TransactionsExtIter<F> = TransactionsIter<F, true>;

impl<F, const EXT: bool> TransactionsIter<F, EXT> {
    #[inline]
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    #[inline]
    pub fn snapshot(&self) -> &RpcSnapshot {
        &self.snapshot
    }
}

impl<F, R> Iterator for TransactionsIter<F, false>
where
    for<'a> F: FnMut(&'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.value()?;
        let result = (self.map)(TransactionData::read_transaction(value))?;
        if self.is_reversed {
            self.inner.prev();
        } else {
            self.inner.next();
        }
        Some(result)
    }
}

impl<F, R> Iterator for TransactionsIter<F, true>
where
    for<'a> F: FnMut(u64, &'a HashBytes, &'a [u8]) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.item()?;
        let result = (self.map)(
            u64::from_be_bytes(key[33..41].try_into().unwrap()),
            &TransactionData::read_tx_hash(value),
            TransactionData::read_transaction(value),
        )?;
        if self.is_reversed {
            self.inner.prev();
        } else {
            self.inner.next();
        }
        Some(result)
    }
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub account: StdAddr,
    pub lt: u64,
    pub block_id: BlockId,
    pub mc_seqno: u32,
}

impl TransactionInfo {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < tables::TransactionsByHash::VALUE_FULL_LEN {
            return None;
        }

        let account = StdAddr::new(bytes[0] as i8, HashBytes::from_slice(&bytes[1..33]));
        let lt = u64::from_be_bytes(bytes[33..41].try_into().unwrap());
        let prefix_len = bytes[41];
        debug_assert!(prefix_len < 64);

        let tail_mask = 1u64 << (63 - prefix_len);

        // TODO: Move into types?
        let Some(shard) = ShardIdent::new(
            bytes[0] as i8 as i32,
            (account.prefix() | tail_mask) & !(tail_mask - 1),
        ) else {
            // TODO: unwrap?
            return None;
        };

        let block_id = BlockId {
            shard,
            seqno: u32::from_le_bytes(bytes[42..46].try_into().unwrap()),
            root_hash: HashBytes::from_slice(&bytes[46..78]),
            file_hash: HashBytes::from_slice(&bytes[78..110]),
        };
        let mc_seqno = u32::from_le_bytes(bytes[110..114].try_into().unwrap());

        Some(Self {
            account,
            lt,
            block_id,
            mc_seqno,
        })
    }
}

pub struct TransactionDataExt<'a> {
    pub info: TransactionInfo,
    pub data: TransactionData<'a>,
}

pub struct TransactionData<'a> {
    data: rocksdb::DBPinnableSlice<'a>,
}

impl<'a> TransactionData<'a> {
    pub fn new(data: rocksdb::DBPinnableSlice<'a>) -> Self {
        Self { data }
    }

    pub fn tx_hash(&self) -> HashBytes {
        let value = self.data.as_ref();
        assert!(!value.is_empty());
        HashBytes::from_slice(&value[1..33])
    }

    pub fn in_msg_hash(&self) -> Option<HashBytes> {
        let value = self.data.as_ref();
        assert!(!value.is_empty());

        let mask = TransactionMask::from_bits_retain(value[0]);
        mask.has_msg_hash()
            .then(|| HashBytes::from_slice(&value[33..65]))
    }

    fn read_tx_hash(value: &[u8]) -> HashBytes {
        HashBytes::from_slice(&value[1..33])
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

    if let Some(account) = account.load_account()?
        && let AccountState::Active(state_init) = &account.state
        && let Some(code) = &state_init.code
    {
        return Ok(ExtractedCodeHash::Exact(Some(*code.repr_hash())));
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
            if depth > 0
                && let Some((left, right)) = shard.split()
            {
                break 'split (left, right);
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
