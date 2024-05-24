use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use everscale_types::models::*;
use everscale_types::prelude::*;
use metrics::atomics::AtomicU64;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::FastHashMap;
use weedb::{rocksdb, OwnedSnapshot};

use crate::db::*;

pub struct RpcStorage {
    db: RpcDb,
    min_tx_lt: AtomicU64,
    snapshot: ArcSwapOption<OwnedSnapshot>,
}

impl RpcStorage {
    pub fn new(db: RpcDb) -> Self {
        Self {
            db,
            min_tx_lt: AtomicU64::new(u64::MAX),
            snapshot: Default::default(),
        }
    }

    pub fn update_snapshot(&self) {
        let snapshot = Arc::new(self.db.owned_snapshot());
        self.snapshot.store(Some(snapshot));
    }

    pub fn get_accounts_by_code_hash(
        &self,
        code_hash: &HashBytes,
        continuation: Option<&StdAddr>,
    ) -> Result<CodeHashesIter<'_>> {
        let Some(snapshot) = self.snapshot.load_full() else {
            anyhow::bail!("not ready");
        };

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
        readopts.set_snapshot(&snapshot);
        // TODO: somehow make the range inclusive since
        // upper_bound is not included in the range
        readopts.set_iterate_upper_bound(upper_bound);

        let rocksdb = self.db.rocksdb();
        let code_hashes_cf = self.db.code_hashes.cf();
        let mut iter = rocksdb.raw_iterator_cf_opt(&code_hashes_cf, readopts);

        iter.seek(key);
        if continuation.is_some() {
            iter.next();
        }

        Ok(CodeHashesIter { inner: iter })
    }

    #[tracing::instrument(level = "info", name = "sync_min_tx_lt", skip_all)]
    pub async fn sync_min_tx_lt(&self) -> Result<()> {
        let min_lt = match self.db.state.get(TX_MIN_LT)? {
            Some(value) if value.is_empty() => None,
            Some(value) => Some(u64::from_le_bytes(value.as_ref().try_into().unwrap())),
            None => {
                let span = tracing::Span::current();
                let db = self.db.clone();
                tokio::task::spawn_blocking(move || {
                    let _span = span.enter();

                    tracing::info!("started searching for the minimum transaction LT");
                    let started_at = Instant::now();

                    let mut min_lt = None::<u64>;
                    for tx in db.transactions.iterator(rocksdb::IteratorMode::Start) {
                        let (key, _) = tx?;

                        let lt = u64::from_be_bytes(key[33..41].try_into().unwrap());
                        match &mut min_lt {
                            Some(min_lt) => *min_lt = (*min_lt).min(lt),
                            None => min_lt = Some(lt),
                        }
                    }

                    tracing::info!(
                        elapsed = %humantime::format_duration(started_at.elapsed()),
                        "finished searching for the minimum transaction LT"
                    );
                    Ok::<_, anyhow::Error>(min_lt)
                })
                .await
                .unwrap()?
            }
        };

        tracing::info!(?min_lt);

        self.min_tx_lt
            .store(min_lt.unwrap_or(u64::MAX), Ordering::Release);
        Ok(())
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

    #[tracing::instrument(
        level = "info",
        name = "remove_old_transactions",
        skip_all,
        fields(min_lt)
    )]
    pub async fn remove_old_transactions(&self, min_lt: u64) -> Result<()> {
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
                    self.key_range_begin.copy_from_slice(key);
                    PendingDelete::Single
                } else {
                    self.key_range_end.copy_from_slice(key);
                    PendingDelete::Range
                });
                self.total_tx += 1;

                // Delete secondary index entries
                if let Ok(tx_cell) = Boc::decode(value) {
                    // Delete transaction by hash index entry
                    self.batch
                        .delete_cf(&self.tx_by_hash, tx_cell.repr_hash().as_slice());
                    self.total_tx_by_hash += 1;

                    // Delete transaction by incoming message hash index entry
                    if let Ok(tx) = tx_cell.parse::<Transaction>() {
                        if let Some(in_msg) = &tx.in_msg {
                            self.batch
                                .delete_cf(&self.tx_by_in_msg, in_msg.repr_hash().as_slice());
                            self.total_tx_by_in_msg += 1;
                        }
                    }
                }
            }

            fn end_account(&mut self) {
                // Flush pending batch
                if let Some(pending) = self.pending_delete.take() {
                    match pending {
                        PendingDelete::Single => self
                            .batch
                            .delete_cf(&self.tx_cf, self.key_range_begin.as_slice()),
                        PendingDelete::Range => self.batch.delete_range_cf(
                            &self.tx_cf,
                            self.key_range_begin.as_slice(),
                            self.key_range_end.as_slice(),
                        ),
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

        // Force update min lt and gc flag
        self.min_tx_lt.store(min_lt, Ordering::Release);

        let db = self.db.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let raw = db.rocksdb().as_ref();

            tracing::info!("started removing old transactions");
            let started_at = Instant::now();

            // Prepare snapshot and iterator
            let snapshot = raw.snapshot();
            let mut readopts = db.transactions.new_read_config();
            readopts.set_snapshot(&snapshot);
            let mut iter = raw.raw_iterator_cf_opt(&db.transactions.cf(), readopts);
            iter.seek_to_first();

            // Prepare GC state
            let mut gc = GcState::new(&db);

            // `last_account` buffer is used to track the last processed account.
            //
            // The buffer is also used to seek for the next account. Its last
            // 8 bytes are `u64::MAX`. It forces the `seek` method to jump right
            // to the first tx of the next account (assuming that there are no
            // transactions with LT == u64::MAX).
            let mut last_account: TxKey = [0u8; tables::Transactions::KEY_LEN];
            last_account[33..41].copy_from_slice(&u64::MAX.to_be_bytes());

            let mut items = 0usize;
            let mut total_invalid = 0usize;
            let mut iteration = 0usize;
            loop {
                let Some((key, value)) = iter.item() else {
                    break iter.status()?;
                };
                iteration += 1;

                let Ok::<&TxKey, _>(key) = key.try_into() else {
                    // Remove invalid entires from the primary index only
                    items += 1;
                    total_invalid += 1;
                    gc.batch.delete_cf(&gc.tx_cf, key);
                    continue;
                };

                // Check whether the next account is processed
                let item_account = &key[..33];
                let is_next_account = item_account != &last_account[..33];
                if is_next_account {
                    // Update last account address
                    last_account[..33].copy_from_slice(item_account);

                    // Add pending delete into batch
                    gc.end_account();
                }

                // Get lt from the key
                let lt = u64::from_be_bytes(key[33..41].try_into().unwrap());

                if lt < min_lt {
                    // Add tx and its secondary indices into the batch
                    items += 1;
                    gc.delete_tx(key, value);
                    iter.next();
                } else {
                    // Seek to the next account
                    if lt < u64::MAX {
                        iter.seek(last_account.as_slice());
                    } else {
                        iter.next();
                    }
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
        .await
        .unwrap()
    }

    #[tracing::instrument(level = "info", name = "update", skip_all, fields(block_id = %block.id()))]
    pub async fn update(
        &self,
        block: BlockStuff,
        accounts: Option<ShardAccountsDict>,
    ) -> Result<()> {
        let Ok(workchain) = i8::try_from(block.id().shard.workchain()) else {
            return Ok(());
        };

        let span = tracing::Span::current();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let extra = block.block().load_extra()?;
            let account_blocks = extra.account_blocks.load()?;

            let mut write_batch = rocksdb::WriteBatch::default();
            let tx_cf = &db.transactions.cf();
            let tx_by_hash_cf = &db.transactions_by_hash.cf();
            let tx_by_in_msg_cf = &db.transactions_by_in_msg.cf();

            // Prepare buffer for full tx id
            let mut tx_key = [0u8; tables::Transactions::KEY_LEN];
            tx_key[0] = workchain as u8;

            // Iterate through all changed accounts in the block
            let mut non_empty_batch = false;
            for item in account_blocks.iter() {
                let (account, _, account_block) = item?;
                non_empty_batch |= true;

                // Fill account address in the key buffer
                tx_key[1..33].copy_from_slice(account.as_slice());

                // Flag to update code hash
                let mut has_special_actions = accounts.is_none(); // skip updates if no state provided
                let mut was_active = false;
                let mut is_active = false;

                // Process account transactions
                let mut first_tx = true;
                for item in account_block.transactions.values() {
                    let (_, tx_cell) = item?;

                    let tx = tx_cell.load()?;

                    tx_key[33..41].copy_from_slice(&tx.lt.to_be_bytes());

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

                    // Write tx data and indices
                    let tx_hash = tx_cell.inner().repr_hash();

                    write_batch.put_cf(tx_cf, tx_key.as_slice(), Boc::encode(tx_cell.inner()));
                    write_batch.put_cf(tx_by_hash_cf, tx_hash.as_slice(), tx_key.as_slice());
                    if let Some(in_msg) = &tx.in_msg {
                        write_batch.put_cf(tx_by_in_msg_cf, in_msg.repr_hash(), tx_key.as_slice());
                    }
                }

                // Update code hash
                if let Some(accounts) = &accounts {
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
                            accounts,
                            remove,
                            &mut write_batch,
                        )?;
                    }
                }
            }

            if non_empty_batch {
                db.rocksdb()
                    .write_opt(write_batch, db.transactions.write_config())?;
            }

            Ok(())
        })
        .await
        .unwrap()
    }

    fn update_code_hash(
        db: &RpcDb,
        workchain: i8,
        account: &HashBytes,
        accounts: &ShardAccountsDict,
        remove: bool,
        write_batch: &mut rocksdb::WriteBatch,
    ) -> Result<()> {
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

        // Find the new code hash
        let new_code_hash = 'code_hash: {
            if !remove {
                if let Some((_, account)) = accounts.get(account)? {
                    break 'code_hash extract_code_hash(&account)?;
                }
            }
            None
        };

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

pub struct CodeHashesIter<'a> {
    inner: rocksdb::DBRawIterator<'a>,
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

const TX_MIN_LT: &[u8] = b"tx_min_lt";
const TX_GC_RUNNING: &[u8] = b"tx_gc_running";
