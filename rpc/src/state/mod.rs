use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use everscale_types::models::*;
use everscale_types::prelude::*;
use futures_util::future::BoxFuture;
use parking_lot::RwLock;
use serde_json::value::RawValue;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_storage::{CodeHashesIter, KeyBlocksDirection, Storage, TransactionsIterBuilder};
use tycho_util::metrics::HistogramGuard;
use tycho_util::time::now_sec;
use tycho_util::FastHashMap;

use crate::config::{RpcConfig, TransactionsGcConfig};
use crate::endpoint::RpcEndpoint;
use crate::models::{GenTimings, LatestBlockchainConfigRef, LatestKeyBlockRef, StateTimings};

pub struct RpcStateBuilder<MandatoryFields = (Storage, BlockchainRpcClient)> {
    config: RpcConfig,
    mandatory_fields: MandatoryFields,
}

impl RpcStateBuilder {
    pub fn build(self) -> RpcState {
        let (storage, blockchain_rpc_client) = self.mandatory_fields;

        let gc_notify = Arc::new(Notify::new());
        let gc_handle = if let Some(config) = &self.config.transactions_gc {
            Some(tokio::spawn(transactions_gc(
                config.clone(),
                storage.clone(),
                gc_notify.clone(),
            )))
        } else {
            None
        };

        RpcState {
            inner: Arc::new(Inner {
                config: self.config,
                storage,
                blockchain_rpc_client,
                mc_accounts: Default::default(),
                sc_accounts: Default::default(),
                is_ready: AtomicBool::new(false),
                timings: ArcSwap::new(Default::default()),
                latest_key_block_json: ArcSwapOption::default(),
                blockchain_config_json: ArcSwapOption::default(),
                gc_notify,
                gc_handle,
            }),
        }
    }
}

impl<T2> RpcStateBuilder<((), T2)> {
    pub fn with_storage(self, storage: Storage) -> RpcStateBuilder<(Storage, T2)> {
        let (_, bc_rpc_client) = self.mandatory_fields;

        RpcStateBuilder {
            config: self.config,
            mandatory_fields: (storage, bc_rpc_client),
        }
    }
}

impl<T1> RpcStateBuilder<(T1, ())> {
    pub fn with_blockchain_rpc_client(
        self,
        client: BlockchainRpcClient,
    ) -> RpcStateBuilder<(T1, BlockchainRpcClient)> {
        let (storage, _) = self.mandatory_fields;

        RpcStateBuilder {
            config: self.config,
            mandatory_fields: (storage, client),
        }
    }
}

impl<T1, T2> RpcStateBuilder<(T1, T2)> {
    pub fn with_config(self, config: RpcConfig) -> RpcStateBuilder<(T1, T2)> {
        RpcStateBuilder { config, ..self }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct RpcState {
    inner: Arc<Inner>,
}

impl RpcState {
    pub fn builder() -> RpcStateBuilder<((), ())> {
        RpcStateBuilder {
            config: RpcConfig::default(),
            mandatory_fields: ((), ()),
        }
    }

    pub async fn init(&self, mc_block_id: &BlockId) -> Result<()> {
        self.inner.init(mc_block_id).await
    }

    pub async fn bind_endpoint(&self) -> Result<RpcEndpoint> {
        RpcEndpoint::bind(self.clone()).await
    }

    pub fn config(&self) -> &RpcConfig {
        &self.inner.config
    }

    pub fn is_ready(&self) -> bool {
        self.inner.is_ready.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        self.inner.storage.rpc_storage().is_some()
    }

    pub fn load_timings(&self) -> arc_swap::Guard<Arc<StateTimings>> {
        self.inner.timings.load()
    }

    pub fn load_latest_key_block_json(&self) -> arc_swap::Guard<Option<CachedJson>> {
        self.inner.latest_key_block_json.load()
    }

    pub fn load_blockchain_config_json(&self) -> arc_swap::Guard<Option<CachedJson>> {
        self.inner.blockchain_config_json.load()
    }

    pub async fn broadcast_external_message(&self, message: &[u8]) {
        self.inner
            .blockchain_rpc_client
            .broadcast_external_message(message)
            .await;
    }

    pub fn get_account_state(
        &self,
        address: &StdAddr,
    ) -> Result<LoadedAccountState, RpcStateError> {
        self.inner.get_account_state(address)
    }

    pub fn get_accounts_by_code_hash(
        &self,
        code_hash: &HashBytes,
        continuation: Option<&StdAddr>,
    ) -> Result<CodeHashesIter<'_>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_accounts_by_code_hash(code_hash, continuation)
            .map_err(RpcStateError::Internal)
    }

    pub fn get_transactions(
        &self,
        account: &StdAddr,
        last_lt: Option<u64>,
    ) -> Result<TransactionsIterBuilder<'_>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_transactions(account, last_lt)
            .map_err(RpcStateError::Internal)
    }

    pub fn get_transaction(
        &self,
        hash: &HashBytes,
    ) -> Result<Option<impl AsRef<[u8]> + '_>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_transaction(hash)
            .map_err(RpcStateError::Internal)
    }

    pub fn get_dst_transaction(
        &self,
        in_msg_hash: &HashBytes,
    ) -> Result<Option<impl AsRef<[u8]> + '_>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_dst_transaction(in_msg_hash)
            .map_err(RpcStateError::Internal)
    }
}

impl StateSubscriber for RpcState {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(self.inner.update(&cx.block, Some(&cx.state)))
    }
}

impl BlockSubscriber for RpcState {
    type Prepared = ();

    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(self.inner.update(&cx.block, None))
    }
}

struct Inner {
    config: RpcConfig,
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,
    mc_accounts: RwLock<Option<CachedAccounts>>,
    sc_accounts: RwLock<FastHashMap<ShardIdent, CachedAccounts>>,
    is_ready: AtomicBool,
    timings: ArcSwap<StateTimings>,
    latest_key_block_json: ArcSwapOption<Box<RawValue>>,
    blockchain_config_json: ArcSwapOption<Box<RawValue>>,
    // GC
    gc_notify: Arc<Notify>,
    gc_handle: Option<JoinHandle<()>>,
}

impl Inner {
    async fn init(self: &Arc<Self>, mc_block_id: &BlockId) -> Result<()> {
        anyhow::ensure!(mc_block_id.is_masterchain(), "not a masterchain state");

        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let shard_states = self.storage.shard_state_storage();

        // Try to init the latest known key block cache
        'key_block: {
            // NOTE: `+ 1` here because the `mc_block_id` might be a key block and we should use it
            let Some(handle) = block_handles.find_prev_key_block(mc_block_id.seqno + 1) else {
                break 'key_block;
            };

            if handle.meta().has_data() {
                let key_block = blocks.load_block_data(&handle).await?;
                self.update_mc_block_cache(&key_block)?;
            } else {
                let state = shard_states.load_state(handle.id()).await?;
                let state = state.as_ref();

                let Some(extra) = state.load_custom()? else {
                    anyhow::bail!("masterchain state without extra");
                };

                self.update_config(state.global_id, state.seqno, &extra.config);
                tracing::warn!("no key block found during initialization");
            }
        }

        let mc_state = shard_states.load_state(mc_block_id).await?;
        self.update_timings(mc_state.as_ref().gen_utime, mc_state.as_ref().seqno);

        if let Some(rpc_storage) = self.storage.rpc_storage() {
            rpc_storage.sync_min_tx_lt().await?;

            let make_cached_accounts = |state: &ShardStateStuff| -> Result<CachedAccounts> {
                let state_info = state.as_ref();
                Ok(CachedAccounts {
                    accounts: state_info.load_accounts()?.dict().clone(),
                    mc_ref_hanlde: state.ref_mc_state_handle().clone(),
                    gen_utime: state_info.gen_utime,
                })
            };

            let shards = mc_state.shards()?.clone();

            // Reset masterchain accounts and fill the cache
            *self.mc_accounts.write() = Some(make_cached_accounts(&mc_state)?);
            rpc_storage
                .reset_accounts(mc_state, self.config.shard_split_depth)
                .await?;

            for item in shards.latest_blocks() {
                let block_id = item?;
                let state = shard_states.load_state(&block_id).await?;

                // Reset shard accounts and fill the cache
                self.sc_accounts
                    .write()
                    .insert(block_id.shard, make_cached_accounts(&state)?);

                rpc_storage
                    .reset_accounts(state, self.config.shard_split_depth)
                    .await?;
            }
        }

        self.is_ready.store(true, Ordering::Release);
        Ok(())
    }

    fn get_account_state(&self, address: &StdAddr) -> Result<LoadedAccountState, RpcStateError> {
        let is_masterchain = address.is_masterchain();

        if is_masterchain {
            // Search in masterchain cache
            match &*self.mc_accounts.read() {
                None => Err(RpcStateError::NotReady),
                Some(cache) => cache.get(&address.address),
            }
        } else {
            let cache = self.sc_accounts.read();
            let mut state = Err(RpcStateError::NotReady);

            // Search in all shard caches
            let mut gen_utime = 0;
            let mut found = false;
            for (shard, cache) in &*cache {
                if !shard.contains_account(&address.address) || cache.gen_utime < gen_utime {
                    continue;
                }

                gen_utime = cache.gen_utime;
                state = cache.get(&address.address);
                found = true;
            }

            // Handle case when account is not found in any shard
            if !found && gen_utime > 0 {
                state = Ok(LoadedAccountState::NotFound {
                    timings: GenTimings {
                        gen_lt: 0,
                        gen_utime,
                    },
                });
            }

            // Done
            state
        }
    }

    async fn update(&self, block: &BlockStuff, state: Option<&ShardStateStuff>) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_rpc_state_update_time");

        let is_masterchain = block.id().is_masterchain();
        if is_masterchain {
            self.update_mc_block_cache(block)?;
        }

        let accounts = if let Some(state) = state {
            Some(self.update_accounts_cache(block, state)?)
        } else {
            None
        };

        if let Some(rpc_storage) = self.storage.rpc_storage() {
            rpc_storage.update(block.clone(), accounts).await?;

            if is_masterchain {
                // NOTE: Update snapshot only for masterchain because it is handled last
                rpc_storage.update_snapshot();
            }
        }
        Ok(())
    }

    fn update_mc_block_cache(&self, block: &BlockStuff) -> Result<()> {
        // Update timings
        {
            // TODO: Add `OnceLock` for block `info` and `custom``
            let info = block.load_info()?;
            self.update_timings(info.gen_utime, info.seqno);

            if !info.key_block {
                return Ok(());
            }
        }

        // Send a new KeyBlock notification to run GC
        if self.config.transactions_gc.is_some() {
            self.gc_notify.notify_waiters();
        }

        let custom = block.load_custom()?;

        // Try to update cached config:
        if let Some(ref config) = custom.config {
            self.update_config(block.as_ref().global_id, block.id().seqno, config);
        } else {
            tracing::error!("key block without config");
        }

        // Try to update cached key block:
        match serde_json::value::to_raw_value(&LatestKeyBlockRef {
            block: block.as_ref(),
        }) {
            Ok(value) => {
                self.latest_key_block_json.store(Some(Arc::new(value)));
            }
            Err(e) => {
                tracing::error!("failed to serialize key block: {e}");
            }
        }

        Ok(())
    }

    fn update_timings(&self, mc_gen_utime: u32, seqno: u32) {
        let time_diff = now_sec() as i64 - mc_gen_utime as i64;
        self.timings.store(Arc::new(StateTimings {
            last_mc_block_seqno: seqno,
            last_mc_utime: mc_gen_utime,
            mc_time_diff: time_diff,
            smallest_known_lt: self.storage.rpc_storage().map(|s| s.min_tx_lt()),
        }));
    }

    fn update_config(&self, global_id: i32, seqno: u32, config: &BlockchainConfig) {
        match serde_json::value::to_raw_value(&LatestBlockchainConfigRef {
            global_id,
            seqno,
            config,
        }) {
            Ok(value) => self.blockchain_config_json.store(Some(Arc::new(value))),
            Err(e) => {
                tracing::error!("failed to serialize blockchain config: {e}");
            }
        }
    }

    fn update_accounts_cache(
        &self,
        block: &BlockStuff,
        state: &ShardStateStuff,
    ) -> Result<ShardAccountsDict> {
        let shard = block.id().shard;

        // TODO: Get `gen_utime` from somewhere else.
        let block_info = block.load_info()?;

        let accounts = state.state().load_accounts()?.dict().clone();

        let cached = CachedAccounts {
            accounts: accounts.clone(),
            mc_ref_hanlde: state.ref_mc_state_handle().clone(),
            gen_utime: block_info.gen_utime,
        };

        if shard.is_masterchain() {
            *self.mc_accounts.write() = Some(cached);
        } else {
            let mut cache = self.sc_accounts.write();

            cache.insert(shard, cached);
            if block_info.after_merge || block_info.after_split {
                tracing::debug!("clearing shard states cache after shards merge/split");

                match block_info.load_prev_ref()? {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    PrevBlockRef::Single(..) => {
                        // Compute parent shard of the B' or B"
                        let parent = shard
                            .merge()
                            .ok_or(everscale_types::error::Error::InvalidData)?;

                        let opposite = shard.opposite().expect("after split");

                        // Remove parent shard state
                        if cache.contains_key(&shard) && cache.contains_key(&opposite) {
                            cache.remove(&parent);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    PrevBlockRef::AfterMerge { .. } => {
                        // Compute parent shard of the B' or B"
                        let (left, right) = shard
                            .split()
                            .ok_or(everscale_types::error::Error::InvalidData)?;

                        // Find and remove all parent shards
                        cache.remove(&left);
                        cache.remove(&right);
                    }
                }
            }
        }

        Ok(accounts)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self.gc_handle.take() {
            handle.abort();
        }
    }
}

pub enum LoadedAccountState {
    NotFound {
        timings: GenTimings,
    },
    Found {
        state: ShardAccount,
        mc_ref_handle: RefMcStateHandle,
        gen_utime: u32,
    },
}

struct CachedAccounts {
    accounts: ShardAccountsDict,
    mc_ref_hanlde: RefMcStateHandle,
    gen_utime: u32,
}

impl CachedAccounts {
    fn get(&self, account: &HashBytes) -> Result<LoadedAccountState, RpcStateError> {
        match self.accounts.get(account) {
            Ok(Some((_, state))) => Ok(LoadedAccountState::Found {
                state,
                mc_ref_handle: self.mc_ref_hanlde.clone(),
                gen_utime: self.gen_utime,
            }),
            Ok(None) => Ok(LoadedAccountState::NotFound {
                timings: GenTimings {
                    gen_lt: 0,
                    gen_utime: self.gen_utime,
                },
            }),
            Err(e) => Err(RpcStateError::Internal(e.into())),
        }
    }
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;

type CachedJson = Arc<Box<RawValue>>;

async fn transactions_gc(config: TransactionsGcConfig, storage: Storage, gc_notify: Arc<Notify>) {
    let Some(persistent_storage) = storage.rpc_storage() else {
        return;
    };

    let Ok(tx_ttl_sec) = config.tx_ttl.as_secs().try_into() else {
        return;
    };

    loop {
        // Wait for a new KeyBlock notification
        gc_notify.notified().await;

        let target_utime = now_sec().saturating_sub(tx_ttl_sec);
        let min_lt = match find_closest_key_block_lt(&storage, target_utime).await {
            Ok(lt) => lt,
            Err(e) => {
                tracing::error!(target_utime, "failed to find the closest key block lt: {e}");
                continue;
            }
        };

        if let Err(e) = persistent_storage.remove_old_transactions(min_lt).await {
            tracing::error!(
                target_utime,
                min_lt,
                "failed to remove old transactions: {e:?}"
            );
        }
    }
}

async fn find_closest_key_block_lt(storage: &Storage, utime: u32) -> Result<u64> {
    let block_handle_storage = storage.block_handle_storage();

    // Find the key block with max seqno which was preduced not later than `utime`
    let handle = 'last_key_block: {
        let iter = block_handle_storage.key_blocks_iterator(KeyBlocksDirection::Backward);
        for key_block_id in iter {
            let handle = block_handle_storage
                .load_handle(&key_block_id)
                .with_context(|| format!("key block not found: {key_block_id}"))?;

            if handle.meta().gen_utime() <= utime {
                break 'last_key_block handle;
            }
        }

        return Ok(0);
    };

    // Load block proof
    let block_proof = storage
        .block_storage()
        .load_block_proof(&handle, false)
        .await?;

    // Read `start_lt` from virtual block info
    let (virt_block, _) = block_proof.virtualize_block()?;
    let info = virt_block.info.load()?;
    Ok(info.start_lt)
}

#[derive(Debug, thiserror::Error)]
pub enum RpcStateError {
    #[error("not ready")]
    NotReady,
    #[error("not supported")]
    NotSupported,
    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
}
