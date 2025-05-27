use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use everscale_types::models::*;
use everscale_types::prelude::*;
use futures_util::future::BoxFuture;
use parking_lot::RwLock;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_storage::{
    BlacklistedAccounts, BlockTransactionIdsIter, BlockTransactionsCursor, BriefShardDescr,
    CodeHashesIter, KeyBlocksDirection, Storage, TransactionsIterBuilder,
};
use tycho_util::metrics::HistogramGuard;
use tycho_util::time::now_sec;
use tycho_util::FastHashMap;

use crate::config::{BlackListConfig, RpcConfig, RpcStorage, TransactionsGcConfig};
use crate::endpoint::{JrpcEndpointCache, ProtoEndpointCache, RpcEndpoint};
use crate::models::{GenTimings, StateTimings};

pub struct RpcStateBuilder<MandatoryFields = (Storage, BlockchainRpcClient)> {
    config: RpcConfig,
    mandatory_fields: MandatoryFields,
}

impl RpcStateBuilder {
    pub fn build(self) -> RpcState {
        let (storage, blockchain_rpc_client) = self.mandatory_fields;

        let gc_notify = Arc::new(Notify::new());

        let mut gc_handle = None;
        let mut blacklisted_accounts = None::<BlacklistedAccounts>;
        let mut blacklist_watcher_handle = None;

        if let RpcStorage::Full {
            gc, blacklist_path, ..
        } = &self.config.storage
        {
            if let Some(config) = gc {
                gc_handle = Some(tokio::spawn(transactions_gc(
                    config.clone(),
                    storage.clone(),
                    gc_notify.clone(),
                )));
            }

            if let Some(path) = blacklist_path {
                let accounts = BlacklistedAccounts::default();
                blacklisted_accounts = Some(accounts.clone());
                blacklist_watcher_handle = Some(tokio::spawn(watch_blacklisted_accounts(
                    path.clone(),
                    accounts,
                )));
            }
        }

        let download_block_semaphore =
            tokio::sync::Semaphore::new(self.config.max_parallel_block_downloads);

        let run_get_method_semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.run_get_method.max_vms,
        ));

        // NOTE: Only a stub here.
        let parsed_config = Arc::new(LatestBlockchainConfig::default());

        RpcState {
            inner: Arc::new(Inner {
                config: self.config,
                storage,
                blockchain_rpc_client,
                mc_info: RwLock::new(LatestMcInfo {
                    block_id: Arc::new(BlockId {
                        shard: ShardIdent::MASTERCHAIN,
                        seqno: 0,
                        ..Default::default()
                    }),
                    timings: GenTimings {
                        gen_lt: 0,
                        gen_utime: 0,
                    },
                }),
                mc_accounts: Default::default(),
                sc_accounts: Default::default(),
                run_get_method_semaphore,
                download_block_semaphore,
                is_ready: AtomicBool::new(false),
                timings: ArcSwap::new(Default::default()),
                blockchain_config: ArcSwap::new(parsed_config),
                jrpc_cache: Default::default(),
                proto_cache: Default::default(),
                gc_notify,
                gc_handle,
                blacklisted_accounts,
                blacklist_watcher_handle,
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

    pub fn split(self) -> (RpcBlockSubscriber, RpcStateSubscriber) {
        let block_subscriber = RpcBlockSubscriber {
            inner: self.inner.clone(),
        };

        let state_subscriber = RpcStateSubscriber { inner: self.inner };

        (block_subscriber, state_subscriber)
    }

    pub async fn init(&self, mc_block_id: &BlockId) -> Result<()> {
        self.inner.init(mc_block_id).await
    }

    pub async fn acquire_download_block_permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        // NOTE: We are not closing this semaphore explicitly.
        self.inner.download_block_semaphore.acquire().await.unwrap()
    }

    pub async fn acquire_run_get_method_permit(&self) -> RunGetMethodPermit {
        let config = &self.config().run_get_method;
        if config.max_vms == 0 {
            return RunGetMethodPermit::Disabled;
        }

        let fut = self.inner.run_get_method_semaphore.clone().acquire_owned();
        match tokio::time::timeout(config.max_wait_for_vm, fut).await {
            Ok(Ok(permit)) => RunGetMethodPermit::Acquired(permit),
            Ok(Err(_)) => RunGetMethodPermit::Disabled,
            Err(_) => RunGetMethodPermit::Timeout,
        }
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

    pub fn jrpc_cache(&self) -> &JrpcEndpointCache {
        &self.inner.jrpc_cache
    }

    pub fn proto_cache(&self) -> &ProtoEndpointCache {
        &self.inner.proto_cache
    }

    pub async fn broadcast_external_message(&self, message: &[u8]) {
        metrics::counter!("tycho_rpc_broadcast_external_message_tx_bytes_total")
            .increment(message.len() as u64);
        self.inner
            .blockchain_rpc_client
            .broadcast_external_message(message)
            .await;
    }

    pub fn get_unpacked_blockchain_config(&self) -> Arc<LatestBlockchainConfig> {
        self.inner.blockchain_config.load_full()
    }

    pub fn get_brief_shards_descr(
        &self,
        mc_seqno: u32,
    ) -> Result<Option<Vec<BriefShardDescr>>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_brief_shards_descr(mc_seqno)
            .map_err(RpcStateError::Internal)
    }

    pub fn get_libraries(&self) -> Dict<HashBytes, LibDescr> {
        match self.inner.mc_accounts.read().as_ref() {
            Some(cache) => cache.libraries.clone(),
            None => Dict::new(),
        }
    }

    pub fn get_raw_library(&self, hash: &HashBytes) -> Result<Option<Cell>> {
        let guard = self.inner.mc_accounts.read();
        match guard.as_ref() {
            Some(cache) => Ok(cache.libraries.get(hash)?.map(|x| x.lib)),
            None => Ok(None),
        }
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

    pub fn get_block_transaction_ids(
        &self,
        block_id: &BlockIdShort,
        cursor: Option<&BlockTransactionsCursor>,
    ) -> Result<Option<BlockTransactionIdsIter<'_>>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_block_transaction_ids(block_id, cursor)
            .map_err(RpcStateError::Internal)
    }

    pub fn get_transactions(
        &self,
        account: &StdAddr,
        last_lt: Option<u64>,
        to_lt: u64,
    ) -> Result<TransactionsIterBuilder<'_>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_transactions(account, last_lt, to_lt)
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

    pub fn get_transaction_block_id(
        &self,
        hash: &HashBytes,
    ) -> Result<Option<BlockId>, RpcStateError> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            return Err(RpcStateError::NotSupported);
        };
        storage
            .get_transaction_block_id(hash)
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

    pub async fn get_key_block_proof(
        &self,
        key_block_seqno: u32,
    ) -> Option<(BlockId, impl AsRef<[u8]> + Send + Sync + 'static)> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let handle = handles.load_key_block_handle(key_block_seqno)?;
        let data = blocks.load_block_proof_raw(&handle).await.ok()?;
        Some((*handle.id(), data))
    }

    // TODO: Remove.
    pub async fn get_block_proof(
        &self,
        block_id: &BlockId,
    ) -> Option<impl AsRef<[u8]> + Send + Sync + 'static> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let handle = handles.load_handle(block_id)?;
        blocks.load_block_proof_raw(&handle).await.ok()
    }

    // TODO: Remove.
    pub async fn get_block_data(
        &self,
        block_id: &BlockId,
    ) -> Option<impl AsRef<[u8]> + Send + Sync + 'static> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let handle = handles.load_handle(block_id)?;
        blocks.load_block_data_raw(&handle).await.ok()
    }
}

pub struct RpcStateSubscriber {
    inner: Arc<Inner>,
}

impl StateSubscriber for RpcStateSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        futures_util::future::ready(self.inner.update_accounts_cache(&cx.block, &cx.state))
    }
}

pub struct RpcBlockSubscriber {
    inner: Arc<Inner>,
}

impl BlockSubscriber for RpcBlockSubscriber {
    type Prepared = JoinHandle<Result<()>>;

    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        let handle = tokio::task::spawn({
            let inner = self.inner.clone();
            let mc_block_id = cx.mc_block_id;
            let block = cx.block.clone();
            async move { inner.update(&mc_block_id, &block).await }
        });

        futures_util::future::ready(Ok(handle))
    }

    fn handle_block<'a>(
        &'a self,
        ctx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(async move {
            prepared.await??;
            if ctx.block.id().is_masterchain() {
                self.inner.update_mc_info(&ctx.block)?;

                // NOTE: Update snapshot only for masterchain because it is handled last.
                // It is updated only after processing all shards and mc block.
                if let Some(rpc_storage) = self.inner.storage.rpc_storage() {
                    rpc_storage.update_snapshot();
                }
            }
            Ok(())
        })
    }
}

pub enum RunGetMethodPermit {
    Acquired(tokio::sync::OwnedSemaphorePermit),
    Timeout,
    Disabled,
}

struct Inner {
    config: RpcConfig,
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,
    mc_info: RwLock<LatestMcInfo>,
    mc_accounts: RwLock<Option<CachedAccounts>>,
    sc_accounts: RwLock<FastHashMap<ShardIdent, CachedAccounts>>,
    download_block_semaphore: Semaphore,
    run_get_method_semaphore: Arc<Semaphore>,
    is_ready: AtomicBool,
    timings: ArcSwap<StateTimings>,
    blockchain_config: ArcSwap<LatestBlockchainConfig>,
    jrpc_cache: JrpcEndpointCache,
    proto_cache: ProtoEndpointCache,
    // GC
    gc_notify: Arc<Notify>,
    gc_handle: Option<JoinHandle<()>>,
    // RPC blacklist
    blacklisted_accounts: Option<BlacklistedAccounts>,
    blacklist_watcher_handle: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct LatestMcInfo {
    block_id: Arc<BlockId>,
    timings: GenTimings,
}

pub struct LatestBlockchainConfig {
    pub raw: BlockchainConfigParams,
    pub unpacked: tycho_vm::UnpackedConfig,
    pub modifiers: tycho_vm::BehaviourModifiers,
}

impl Default for LatestBlockchainConfig {
    fn default() -> Self {
        Self {
            raw: BlockchainConfigParams::from_raw(Cell::empty_cell()),
            unpacked: tycho_vm::UnpackedConfig {
                latest_storage_prices: None,
                global_id: None,
                mc_gas_prices: None,
                gas_prices: None,
                mc_fwd_prices: None,
                fwd_prices: None,
                size_limits_config: None,
            },
            modifiers: Default::default(),
        }
    }
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

            if handle.has_data() {
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

        let mut mc_state = shard_states.load_state(mc_block_id).await?;
        self.update_timings(mc_state.as_ref().gen_utime, mc_state.as_ref().seqno);

        if let Some(rpc_storage) = self.storage.rpc_storage() {
            let node_instance_id = self.storage.node_state().load_instance_id();
            let rpc_instance_id = rpc_storage.load_instance_id();

            let make_cached_accounts = |state: &ShardStateStuff| -> Result<CachedAccounts> {
                let state_info = state.as_ref();
                Ok(CachedAccounts {
                    libraries: Default::default(),
                    accounts: state_info.load_accounts()?.dict().clone(),
                    mc_ref_hanlde: state.ref_mc_state_handle().clone(),
                    timings: GenTimings {
                        gen_lt: state_info.gen_lt,
                        gen_utime: state_info.gen_utime,
                    },
                })
            };

            let shards = mc_state.shards()?.clone();

            if node_instance_id != rpc_instance_id || self.config.storage.is_force_reindex() {
                // Reset masterchain accounts.
                // NOTE: Consume shard state to prevent if from being fully loaded.
                rpc_storage
                    .reset_accounts(mc_state, self.config.shard_split_depth)
                    .await?;

                for item in shards.latest_blocks() {
                    let block_id = item?;

                    let state = shard_states.load_state(&block_id).await?;

                    // Reset shard accounts.
                    // NOTE: Consume shard state to prevent if from being fully loaded.
                    rpc_storage
                        .reset_accounts(state, self.config.shard_split_depth)
                        .await?;
                }

                // Rewrite RPC instance id
                rpc_storage.store_instance_id(node_instance_id);

                // Reload mc state.
                mc_state = shard_states.load_state(mc_block_id).await?;
            }

            // Fill config.
            if let Some(config) = load_blockchain_config(&mc_state) {
                self.blockchain_config.store(config);
            }

            // Fill masterchain cache
            *self.mc_accounts.write() = Some(make_cached_accounts(&mc_state)?);

            for item in shards.latest_blocks() {
                let block_id = item?;
                let state = shard_states.load_state(&block_id).await?;

                // Fill accounts cache.
                self.sc_accounts
                    .write()
                    .insert(block_id.shard, make_cached_accounts(&state)?);
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
                Some(cache) => {
                    let mc_info = self.mc_info.read().clone();
                    cache.get(mc_info, &address.address)
                }
            }
        } else {
            let cache = self.sc_accounts.read();
            let mc_info = self.mc_info.read().clone();

            let mut state = Err(RpcStateError::NotReady);

            // Search in all shard caches
            let mut gen_utime = 0;
            let mut found = false;
            for (shard, cache) in &*cache {
                if !shard.contains_account(&address.address) || cache.timings.gen_utime < gen_utime
                {
                    continue;
                }

                gen_utime = cache.timings.gen_utime;
                state = cache.get(mc_info.clone(), &address.address);
                found = true;
            }

            // Handle case when account is not found in any shard
            if !found && gen_utime > 0 {
                state = Ok(LoadedAccountState::NotFound {
                    mc_block_id: mc_info.block_id,
                    timings: mc_info.timings,
                });
            }

            // Done
            state
        }
    }

    async fn update(&self, mc_block_id: &BlockId, block: &BlockStuff) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_rpc_state_update_time");

        let is_masterchain = block.id().is_masterchain();
        if is_masterchain {
            self.update_mc_block_cache(block)?;
        }

        if let Some(rpc_storage) = self.storage.rpc_storage() {
            rpc_storage
                .update(
                    mc_block_id,
                    block.clone(),
                    self.blacklisted_accounts.as_ref(),
                )
                .await?;
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
        if self.config.storage.gc_is_enabled() {
            self.gc_notify.notify_waiters();
        }

        let custom = block.load_custom()?;

        // Try to update cached config:
        if let Some(ref config) = custom.config {
            self.update_config(block.as_ref().global_id, block.id().seqno, config);
        } else {
            tracing::error!("key block without config");
        }

        self.jrpc_cache.handle_key_block(block.as_ref());
        self.proto_cache.handle_key_block(block.as_ref());
        Ok(())
    }

    fn update_mc_info(&self, block: &BlockStuff) -> Result<()> {
        let info = block.load_info()?;
        let block_id = Arc::new(*block.id());
        *self.mc_info.write() = LatestMcInfo {
            block_id,
            timings: GenTimings {
                gen_lt: info.end_lt,
                gen_utime: info.gen_utime,
            },
        };
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
        self.jrpc_cache.handle_config(global_id, seqno, config);
        self.proto_cache.handle_config(global_id, seqno, config);
    }

    fn update_accounts_cache(&self, block: &BlockStuff, state: &ShardStateStuff) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_rpc_state_update_accounts_cache_time");

        let shard = block.id().shard;

        // TODO: Get `gen_utime` from somewhere else.
        let block_info = block.load_info()?;

        let accounts = state.state().load_accounts()?.dict().clone();
        let libraries = state.state().libraries.clone();

        let cached = CachedAccounts {
            libraries,
            accounts,
            mc_ref_hanlde: state.ref_mc_state_handle().clone(),
            timings: GenTimings {
                gen_lt: block_info.end_lt,
                gen_utime: block_info.gen_utime,
            },
        };

        if shard.is_masterchain() {
            // Fill config.
            if let Some(config) = load_blockchain_config(state) {
                self.blockchain_config.store(config);
            }

            // Update accounts cache.
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

        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self.gc_handle.take() {
            handle.abort();
        }

        if let Some(handle) = self.blacklist_watcher_handle.take() {
            handle.abort();
        }
    }
}

fn load_blockchain_config(mc_state: &ShardStateStuff) -> Option<Arc<LatestBlockchainConfig>> {
    let extra = mc_state.state_extra().ok()?;

    // Fill config.
    let now = mc_state.as_ref().gen_utime;
    match tycho_vm::SmcInfoTonV6::unpack_config_partial(&extra.config.params, now) {
        Ok(unpacked) => {
            let mut modifiers = tycho_vm::BehaviourModifiers::default();
            if let Ok(global_id) = extra.config.params.get_global_id() {
                if let Ok(global) = extra.config.params.get_global_version() {
                    modifiers.signature_with_id = global
                        .capabilities
                        .contains(GlobalCapability::CapSignatureWithId)
                        .then_some(global_id);
                }
            }

            Some(Arc::new(LatestBlockchainConfig {
                raw: extra.config.params.clone(),
                unpacked,
                modifiers,
            }))
        }
        Err(e) => {
            tracing::error!(
                block_id = %mc_state.block_id(),
                "failed to unpack blockchain config: {e:?}",
            );
            None
        }
    }
}

pub enum LoadedAccountState {
    NotFound {
        mc_block_id: Arc<BlockId>,
        timings: GenTimings,
    },
    Found {
        mc_block_id: Arc<BlockId>,
        state: ShardAccount,
        mc_ref_handle: RefMcStateHandle,
        timings: GenTimings,
    },
}

struct CachedAccounts {
    libraries: Dict<HashBytes, LibDescr>,
    accounts: ShardAccountsDict,
    mc_ref_hanlde: RefMcStateHandle,
    timings: GenTimings,
}

impl CachedAccounts {
    fn get(
        &self,
        mc_info: LatestMcInfo,
        account: &HashBytes,
    ) -> Result<LoadedAccountState, RpcStateError> {
        match self.accounts.get(account) {
            Ok(Some((_, state))) => Ok(LoadedAccountState::Found {
                mc_block_id: mc_info.block_id,
                state,
                mc_ref_handle: self.mc_ref_hanlde.clone(),
                timings: self.timings.max_by_lt(mc_info.timings),
            }),
            Ok(None) => Ok(LoadedAccountState::NotFound {
                mc_block_id: mc_info.block_id,
                timings: self.timings.max_by_lt(mc_info.timings),
            }),
            Err(e) => Err(RpcStateError::Internal(e.into())),
        }
    }
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;

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
        let gc_range = match find_closest_key_block_lt(&storage, target_utime).await {
            Ok(lt) => lt,
            Err(e) => {
                tracing::error!(target_utime, "failed to find the closest key block lt: {e}");
                continue;
            }
        };

        if let Err(e) = persistent_storage
            .remove_old_transactions(gc_range.mc_seqno, gc_range.lt, config.keep_tx_per_account)
            .await
        {
            tracing::error!(
                target_utime,
                mc_seqno = gc_range.mc_seqno,
                min_lt = gc_range.lt,
                "failed to remove old transactions: {e:?}"
            );
        }
    }
}

pub async fn watch_blacklisted_accounts(config_path: PathBuf, accounts: BlacklistedAccounts) {
    tracing::info!(
        config_path = %config_path.display(),
        "started watching for changes in rpc blacklist config"
    );

    let get_metadata = || {
        std::fs::metadata(&config_path)
            .ok()
            .and_then(|m| m.modified().ok())
    };

    let mut last_modified = None;

    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;

        let modified = get_metadata();
        if last_modified == modified {
            continue;
        }
        last_modified = modified;

        // Handle
        match BlackListConfig::load_from(&config_path) {
            Ok(config) => accounts.update(config.accounts),
            Err(e) => {
                tracing::error!("failed to load blacklist config: {e}");
            }
        }
    }
}

async fn find_closest_key_block_lt(storage: &Storage, utime: u32) -> Result<GcRange> {
    let block_handle_storage = storage.block_handle_storage();

    // Find the key block with max seqno which was preduced not later than `utime`
    let handle = 'last_key_block: {
        let iter = block_handle_storage.key_blocks_iterator(KeyBlocksDirection::Backward);
        for key_block_id in iter {
            let handle = block_handle_storage
                .load_handle(&key_block_id)
                .with_context(|| format!("key block not found: {key_block_id}"))?;

            if handle.gen_utime() <= utime {
                break 'last_key_block handle;
            }
        }

        return Ok(GcRange::default());
    };

    // Load block proof
    let block_proof = storage.block_storage().load_block_proof(&handle).await?;

    // Read `start_lt` from virtual block info
    let (virt_block, _) = block_proof.virtualize_block()?;
    let info = virt_block.info.load()?;
    Ok(GcRange {
        mc_seqno: info.seqno,
        lt: info.start_lt,
    })
}

#[derive(Default)]
struct GcRange {
    mc_seqno: u32,
    lt: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RpcStateError {
    #[error("not ready")]
    NotReady,
    #[error("not supported")]
    NotSupported,
    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
    #[error(transparent)]
    BadRequest(#[from] BadRequestError),
}

impl RpcStateError {
    pub fn bad_request<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::BadRequest(BadRequestError(error.into()))
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct BadRequestError(anyhow::Error);

impl From<anyhow::Error> for BadRequestError {
    #[inline]
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

impl From<axum::extract::rejection::QueryRejection> for BadRequestError {
    #[inline]
    fn from(value: axum::extract::rejection::QueryRejection) -> Self {
        Self(anyhow::Error::msg(value.body_text()))
    }
}

impl From<axum::extract::rejection::JsonRejection> for BadRequestError {
    #[inline]
    fn from(value: axum::extract::rejection::JsonRejection) -> Self {
        Self(anyhow::Error::msg(value.body_text()))
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use anyhow::Result;
    use everscale_types::boc::Boc;
    use everscale_types::models::{Block, BlockId};
    use everscale_types::prelude::HashBytes;
    use tycho_block_util::block::{BlockStuff, BlockStuffAug};
    use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext, DelayedTasks};
    use tycho_core::blockchain_rpc::{BlockchainRpcClient, BlockchainRpcService};
    use tycho_core::overlay_client::{PublicOverlayClient, PublicOverlayClientConfig};
    use tycho_network::{
        service_query_fn, BoxCloneService, Network, NetworkConfig, OverlayId, PublicOverlay,
        Response, ServiceExt, ServiceRequest,
    };
    use tycho_storage::{Storage, StorageConfig};

    use crate::{RpcConfig, RpcState};

    fn echo_service() -> BoxCloneService<ServiceRequest, Response> {
        let handle = |request: ServiceRequest| async move {
            tracing::trace!("received: {}", request.body.escape_ascii());
            let response = Response {
                version: Default::default(),
                body: request.body,
            };
            Some(response)
        };
        service_query_fn(handle).boxed_clone()
    }

    fn make_network() -> Result<Network> {
        Network::builder()
            .with_config(NetworkConfig::default())
            .with_random_private_key()
            .build("127.0.0.1:0", echo_service())
    }

    #[allow(dead_code)]
    fn get_block() -> BlockStuffAug {
        let block_data = include_bytes!("../../../core/tests/data/block.bin");

        let root = Boc::decode(block_data).unwrap();
        let block = root.parse::<Block>().unwrap();

        let block_id = BlockId {
            root_hash: *root.repr_hash(),
            ..Default::default()
        };

        BlockStuff::from_block_and_root(&block_id, block, root, block_data.len())
            .with_archive_data(block_data.as_slice())
    }

    fn get_empty_block() -> BlockStuffAug {
        let block_data = include_bytes!("../../../core/tests/data/empty_block.bin");

        let root = Boc::decode(block_data).unwrap();
        let block = root.parse::<Block>().unwrap();

        let block_id = BlockId {
            root_hash: *root.repr_hash(),
            ..Default::default()
        };

        BlockStuff::from_block_and_root(&block_id, block, root, block_data.len())
            .with_archive_data(block_data.as_slice())
    }

    // TODO: needs to change contract code for some account in devnet and dump block that included this
    //#[tokio::test]
    async fn _rpc_state_handle_block() -> Result<()> {
        tycho_util::test::init_logger("rpc_state_handle_block", "debug");

        let tmp_dir = tempfile::tempdir()?;
        let storage = Storage::builder()
            .with_config(StorageConfig::new_potato(tmp_dir.path()))
            .with_rpc_storage(true)
            .build()
            .await?;

        let config = RpcConfig::default();

        let network = make_network()?;

        let public_overlay = PublicOverlay::builder(PUBLIC_OVERLAY_ID).build(
            BlockchainRpcService::builder()
                .with_storage(storage.clone())
                .without_broadcast_listener()
                .build(),
        );

        let blockchain_rpc_client = BlockchainRpcClient::builder()
            .with_public_overlay_client(PublicOverlayClient::new(
                network,
                public_overlay,
                PublicOverlayClientConfig::default(),
            ))
            .build();

        let rpc_state = RpcState::builder()
            .with_config(config)
            .with_storage(storage)
            .with_blockchain_rpc_client(blockchain_rpc_client)
            .build();

        let block = get_block();

        let (delayed_handle, delayed) = DelayedTasks::new();
        let ctx = BlockSubscriberContext {
            mc_block_id: BlockId::default(),
            mc_is_key_block: false,
            is_key_block: false,
            block: block.data,
            archive_data: block.archive_data,
            delayed,
        };

        let (block_subscriber, _) = rpc_state.clone().split();
        let delayed_handle = delayed_handle.spawn();
        let prepared = block_subscriber.prepare_block(&ctx).await?;

        block_subscriber.handle_block(&ctx, prepared).await?;
        delayed_handle.join().await?;

        let account = HashBytes::from_str(
            "d7ce76fcf11423e3eb332e72c5f10e4b2cd45a8f356161c930e391e4023784d3",
        )?;

        let new_code_hash = HashBytes::from_str(
            "d66d198766abdbe1253f3415826c946c371f5112552408625aeb0b31e0ef2df3",
        )?;

        let account_by_code_hash = rpc_state
            .get_accounts_by_code_hash(&new_code_hash, None)?
            .last()
            .unwrap();

        assert_eq!(account, account_by_code_hash.address);

        Ok(())
    }

    #[tokio::test]
    async fn rpc_state_handle_empty_block() -> Result<()> {
        tycho_util::test::init_logger("rpc_state_handle_empty_block", "debug");

        let config = RpcConfig::default();

        let tmp_dir = tempfile::tempdir()?;
        let storage = Storage::builder()
            .with_config(StorageConfig::new_potato(tmp_dir.path()))
            .with_rpc_storage(true)
            .build()
            .await?;

        let network = make_network()?;

        let public_overlay = PublicOverlay::builder(PUBLIC_OVERLAY_ID).build(
            BlockchainRpcService::builder()
                .with_storage(storage.clone())
                .without_broadcast_listener()
                .build(),
        );

        let blockchain_rpc_client = BlockchainRpcClient::builder()
            .with_public_overlay_client(PublicOverlayClient::new(
                network,
                public_overlay,
                PublicOverlayClientConfig::default(),
            ))
            .build();

        let rpc_state = RpcState::builder()
            .with_config(config)
            .with_storage(storage)
            .with_blockchain_rpc_client(blockchain_rpc_client)
            .build();

        let block = get_empty_block();

        let (delayed_handle, delayed) = DelayedTasks::new();
        let ctx = BlockSubscriberContext {
            mc_block_id: BlockId::default(),
            mc_is_key_block: false,
            is_key_block: false,
            block: block.data,
            archive_data: block.archive_data,
            delayed,
        };

        let (block_subscriber, _) = rpc_state.clone().split();
        let delayed_handle = delayed_handle.spawn();
        let prepared = block_subscriber.prepare_block(&ctx).await?;

        block_subscriber.handle_block(&ctx, prepared).await?;
        delayed_handle.join().await?;

        Ok(())
    }

    static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
}
