use std::collections::{BTreeMap, VecDeque, hash_map};
use std::sync::Arc;
use ahash::HashMapExt;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tycho_block_util::block::{ValidatorSubsetInfo, calc_next_block_id_short};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_core::storage::ShardStateStorageError;
use tycho_crypto::ed25519::KeyPair;
use tycho_types::models::{
    BlockId, BlockIdShort, CollationConfig, GlobalCapabilities, ProcessedUptoInfo,
    ShardIdent, ValidatorDescription,
};
use tycho_util::futures::AwaitBlocking;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap, FastHashSet};
use types::{
    ActiveCollator, ActiveSync, BlockCacheEntry, BlockCacheEntryData,
    BlockCacheStoreResult, CollatedBlockInfo, CollationState, CollationStatus,
    CollatorState, HandledBlockFromBcCtx, ImportedAnchorEvent, McBlockSubgraph,
    NextCollationStep,
};
use self::blocks_cache::BlocksCache;
use self::types::{
    BlockCacheKey, CandidateStatus, CollationSyncState, McBlockSubgraphExtract,
};
use self::utils::find_us_in_collators_set;
use crate::collator::{
    CollationCancelReason, Collator, CollatorContext, CollatorEventListener,
    CollatorFactory, ForceMasterCollation,
};
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, EnqueuedMessage, QueueDiffWithMessages,
};
use crate::mempool::{
    MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolAnchorId,
    MempoolEventListener, StateUpdateContext,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{
    StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener,
};
use crate::types::processed_upto::{
    BlockSeqno, ProcessedUptoInfoExtension, ProcessedUptoInfoStuff,
    find_min_processed_to_by_shards,
};
use crate::types::{
    BlockCollationResult, BlockIdExt, CollationSessionId, CollationSessionInfo,
    CollatorConfig, DebugIter, DisplayAsShortId, DisplayBlockIdsIntoIter, McData,
    ProcessedToByPartitions, ShardDescriptionShort, ShardDescriptionShortExt,
    ShardHashesExt,
};
use crate::utils::async_dispatcher::{
    AsyncDispatcher, STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE,
};
use crate::utils::block::detect_top_processed_to_anchor;
use crate::utils::shard::calc_split_merge_actions;
use crate::utils::vset_cache::ValidatorSetCache;
use crate::validator::{AddSession, ValidationStatus, Validator};
use crate::{method_to_async_closure, tracing_targets};
mod blocks_cache;
mod types;
mod utils;
#[cfg(test)]
#[path = "tests/manager_tests.rs"]
pub(super) mod tests;
const BLOCKS_FROM_BC_QUEUE_LIMIT: usize = 1000;
pub struct RunningCollationManager<CF, V>
where
    CF: CollatorFactory,
{
    cancel_async_tasks: CancellationToken,
    dispatcher: AsyncDispatcher<CollationManager<CF, V>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
}
impl<CF: CollatorFactory, V> RunningCollationManager<CF, V> {
    pub fn dispatcher(&self) -> &AsyncDispatcher<CollationManager<CF, V>> {
        &self.dispatcher
    }
    pub fn state_node_adapter(&self) -> &Arc<dyn StateNodeAdapter> {
        &self.state_node_adapter
    }
    pub fn mpool_adapter(&self) -> &Arc<dyn MempoolAdapter> {
        &self.mpool_adapter
    }
    pub fn mq_adapter(&self) -> &Arc<dyn MessageQueueAdapter<EnqueuedMessage>> {
        &self.mq_adapter
    }
}
impl<CF: CollatorFactory, V> Drop for RunningCollationManager<CF, V> {
    fn drop(&mut self) {
        self.cancel_async_tasks.cancel();
    }
}
pub struct CollationManager<CF, V>
where
    CF: CollatorFactory,
{
    keypair: Arc<KeyPair>,
    config: Arc<CollatorConfig>,
    dispatcher: Arc<AsyncDispatcher<Self>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    collator_factory: CF,
    validator: Arc<V>,
    active_collation_sessions: RwLock<
        FastHashMap<ShardIdent, Arc<CollationSessionInfo>>,
    >,
    collation_sessions_to_finish: FastDashMap<
        CollationSessionId,
        Arc<CollationSessionInfo>,
    >,
    active_collators: FastDashMap<ShardIdent, ActiveCollator<Arc<CF::Collator>>>,
    collators_to_stop: FastDashMap<
        CollationSessionId,
        ActiveCollator<Arc<CF::Collator>>,
    >,
    blocks_cache: BlocksCache,
    /// Queue of received blocks from bc
    blocks_from_bc_queue_sender: tokio::sync::mpsc::Sender<HandledBlockFromBcCtx>,
    /// Used to sync tasks that may cause `sync_to_applied_mc_block`
    ready_to_sync: Arc<Notify>,
    /// block id of last processed master state (after collation or on sync)
    last_processed_mc_block_id: Arc<Mutex<Option<BlockId>>>,
    /// state to sync collation between master and shard chains
    collation_sync_state: Arc<Mutex<CollationSyncState>>,
    /// Cache for validator sets from config
    validator_set_cache: ValidatorSetCache,
    /// Mempool config override for a new genesis
    mempool_config_override: Option<MempoolGlobalConfig>,
    /// `McData` which processing was delayed until block is validated.
    delayed_mc_state_update: Arc<Mutex<Option<Arc<McData>>>>,
}
#[async_trait]
impl<CF, V> MempoolEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_new_anchor)),
            file!(),
            155u32,
        );
        let anchor = anchor;
        {
            __guard.end_section(160u32);
            let __result = self
                .spawn_task(
                    method_to_async_closure!(process_new_anchor_from_mempool, anchor),
                )
                .await;
            __guard.start_section(160u32);
            __result
        }
    }
}
#[async_trait]
impl<CF, V> StateNodeEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_block_accepted(&self, state: &ShardStateStuff) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_block_accepted)),
            file!(),
            170u32,
        );
        let state = state;
        let processed_upto = state.state().processed_upto.load()?;
        metrics_report_last_applied_block_and_anchor(state, &processed_upto)?;
        let state_cloned = state.clone();
        {
            __guard.end_section(182u32);
            let __result = self
                .spawn_task(
                    method_to_async_closure!(
                        detect_top_processed_to_anchor_and_notify_mempool, state_cloned,
                        processed_upto.get_min_externals_processed_to() ?.0
                    ),
                )
                .await;
            __guard.start_section(182u32);
            __result
        }?;
        let state_cloned = state.clone();
        {
            __guard.end_section(188u32);
            let __result = self
                .spawn_task(move |worker| {
                    Box::pin(async move {
                        let mut __guard = crate::__async_profile_guard__::Guard::new(
                            concat!(module_path!(), "::async_block"),
                            file!(),
                            186u32,
                        );
                        worker.cancel_validation_sessions_until_block(state_cloned)
                    })
                })
                .await;
            __guard.start_section(188u32);
            __result
        }?;
        Ok(())
    }
    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_block_accepted_external)),
            file!(),
            193u32,
        );
        let state = state;
        let processed_upto = state.state().processed_upto.load()?;
        metrics_report_last_applied_block_and_anchor(state, &processed_upto)?;
        let state = state.clone();
        let ctx = HandledBlockFromBcCtx {
            state,
            processed_upto,
        };
        {
            __guard.end_section(205u32);
            let __result = self
                .enqueue_task(
                    method_to_async_closure!(enqueue_handle_block_from_bc, ctx),
                )
                .await;
            __guard.start_section(205u32);
            __result
        }?;
        Ok(())
    }
}
fn metrics_report_last_applied_block_and_anchor(
    state: &ShardStateStuff,
    processed_upto: &ProcessedUptoInfo,
) -> Result<()> {
    let block_id = state.block_id();
    let labels = [("workchain", block_id.shard.workchain().to_string())];
    let block_ct = state.get_gen_chain_time();
    let processed_to_anchor_id = processed_upto.get_min_externals_processed_to()?.0;
    metrics::gauge!("tycho_last_applied_block_seqno", & labels).set(block_id.seqno);
    metrics::gauge!("tycho_last_processed_to_anchor_id", & labels)
        .set(processed_to_anchor_id);
    tracing::info!(
        target : tracing_targets::COLLATION_MANAGER, block_id = %
        DisplayAsShortId(block_id), block_ct, processed_to_anchor_id =
        processed_to_anchor_id, "last applied block",
    );
    Ok(())
}
#[async_trait]
impl<CF, V> CollatorEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_skipped(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_skipped)),
            file!(),
            247u32,
        );
        let prev_mc_block_id = prev_mc_block_id;
        let next_block_id_short = next_block_id_short;
        let anchor_chain_time = anchor_chain_time;
        let force_mc_block = force_mc_block;
        let collation_config = collation_config;
        {
            __guard.end_section(256u32);
            let __result = self
                .spawn_task(
                    method_to_async_closure!(
                        handle_collation_skipped, prev_mc_block_id, next_block_id_short,
                        anchor_chain_time, force_mc_block, collation_config
                    ),
                )
                .await;
            __guard.start_section(256u32);
            __result
        }
    }
    async fn on_cancelled(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        cancel_reason: CollationCancelReason,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_cancelled)),
            file!(),
            264u32,
        );
        let prev_mc_block_id = prev_mc_block_id;
        let next_block_id_short = next_block_id_short;
        let cancel_reason = cancel_reason;
        {
            __guard.end_section(271u32);
            let __result = self
                .spawn_task(
                    method_to_async_closure!(
                        handle_collation_cancelled, prev_mc_block_id,
                        next_block_id_short, cancel_reason
                    ),
                )
                .await;
            __guard.start_section(271u32);
            __result
        }
    }
    async fn on_block_candidate(
        &self,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_block_candidate)),
            file!(),
            274u32,
        );
        let collation_result = collation_result;
        {
            __guard.end_section(279u32);
            let __result = self
                .spawn_task(
                    method_to_async_closure!(
                        handle_collated_block_candidate, collation_result
                    ),
                )
                .await;
            __guard.start_section(279u32);
            __result
        }
    }
    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(on_collator_stopped)),
            file!(),
            282u32,
        );
        let stop_key = stop_key;
        {
            __guard.end_section(286u32);
            let __result = self
                .spawn_task(move |worker| {
                    Box::pin(async move {
                        let mut __guard = crate::__async_profile_guard__::Guard::new(
                            concat!(module_path!(), "::async_block"),
                            file!(),
                            284u32,
                        );
                        worker.handle_collator_stopped(stop_key)
                    })
                })
                .await;
            __guard.start_section(286u32);
            __result
        }
    }
}
impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    #[allow(clippy::too_many_arguments)]
    pub fn start<STF, MPF>(
        keypair: Arc<KeyPair>,
        config: CollatorConfig,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        state_node_adapter_factory: STF,
        mpool_adapter_factory: MPF,
        validator: V,
        collator_factory: CF,
        mempool_config_override: Option<MempoolGlobalConfig>,
    ) -> RunningCollationManager<CF, V>
    where
        STF: StateNodeAdapterFactory,
        MPF: MempoolAdapterFactory,
    {
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, "Creating collation manager..."
        );
        let (dispatcher, dispatcher_ctx) = AsyncDispatcher::new(
            "collation_manager_async_dispatcher",
            STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE,
        );
        let arc_dispatcher = Arc::new(dispatcher.clone());
        let state_node_adapter = Arc::new(
            state_node_adapter_factory.create(arc_dispatcher.clone()),
        );
        let mpool_adapter = mpool_adapter_factory.create(arc_dispatcher.clone());
        let validator = Arc::new(validator);
        let (blocks_from_bc_queue_sender, blocks_from_bc_queue_receiver) = tokio::sync::mpsc::channel::<
            HandledBlockFromBcCtx,
        >(BLOCKS_FROM_BC_QUEUE_LIMIT);
        let ready_to_sync = Arc::new(Notify::new());
        ready_to_sync.notify_one();
        let collation_manager = Self {
            keypair,
            config: Arc::new(config),
            dispatcher: arc_dispatcher.clone(),
            state_node_adapter: state_node_adapter.clone(),
            mpool_adapter: mpool_adapter.clone(),
            mq_adapter: mq_adapter.clone(),
            collator_factory,
            validator,
            active_collation_sessions: Default::default(),
            collation_sessions_to_finish: Default::default(),
            active_collators: Default::default(),
            collators_to_stop: Default::default(),
            blocks_cache: BlocksCache::new(),
            blocks_from_bc_queue_sender,
            ready_to_sync,
            last_processed_mc_block_id: Default::default(),
            collation_sync_state: Default::default(),
            validator_set_cache: Default::default(),
            mempool_config_override,
            delayed_mc_state_update: Arc::new(Mutex::new(None)),
        };
        let collation_manager = Arc::new(collation_manager);
        let cancel_async_tasks = CancellationToken::new();
        tokio::spawn(
            collation_manager
                .clone()
                .process_handle_block_from_bc_queue(
                    blocks_from_bc_queue_receiver,
                    cancel_async_tasks.clone(),
                ),
        );
        arc_dispatcher.run(collation_manager, dispatcher_ctx);
        tracing::trace!(
            target : tracing_targets::COLLATION_MANAGER, "Tasks dispatchers started"
        );
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, "Collation manager created"
        );
        RunningCollationManager {
            cancel_async_tasks,
            dispatcher,
            state_node_adapter,
            mpool_adapter,
            mq_adapter,
        }
    }
    /// (TODO) Check sync status between mempool and blockchain state
    /// and pause collation when we are far behind other nodesб
    /// jusct sync blcoks from blockchain
    pub async fn process_new_anchor_from_mempool(
        &self,
        _anchor: Arc<MempoolAnchor>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(process_new_anchor_from_mempool)),
            file!(),
            397u32,
        );
        let _anchor = _anchor;
        Ok(())
    }
    /// Tries to determine top anchor that was processed to
    /// by info from received state and notify mempool
    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    async fn detect_top_processed_to_anchor_and_notify_mempool(
        &self,
        state: ShardStateStuff,
        mc_processed_to_anchor_id: MempoolAnchorId,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(detect_top_processed_to_anchor_and_notify_mempool)
            ),
            file!(),
            409u32,
        );
        let state = state;
        let mc_processed_to_anchor_id = mc_processed_to_anchor_id;
        if !state.block_id().is_masterchain() {
            {
                __guard.end_section(412u32);
                return Ok(());
            };
        }
        {
            __guard.end_section(424u32);
            let __result = self
                .detect_top_processed_to_anchor_and_notify_mempool_impl(
                    state.block_id().seqno,
                    state.shards()?.as_vec()?.into_iter().map(|(_, descr)| descr),
                    mc_processed_to_anchor_id,
                )
                .await;
            __guard.start_section(424u32);
            __result
        }
    }
    async fn detect_top_processed_to_anchor_and_notify_mempool_impl<I>(
        &self,
        mc_block_seqno: BlockSeqno,
        mc_top_shards: I,
        mc_processed_to_anchor_id: MempoolAnchorId,
    ) -> Result<()>
    where
        I: Iterator<Item = ShardDescriptionShort>,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(detect_top_processed_to_anchor_and_notify_mempool_impl)
            ),
            file!(),
            435u32,
        );
        let mc_block_seqno = mc_block_seqno;
        let mc_top_shards = mc_top_shards;
        let mc_processed_to_anchor_id = mc_processed_to_anchor_id;
        let top_processed_to_anchor = detect_top_processed_to_anchor(
            mc_top_shards,
            mc_processed_to_anchor_id,
        );
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, top_processed_to_anchor,
            mc_processed_to_anchor_id,
            "detected minimal top_processed_to_anchor, will notify mempool",
        );
        {
            __guard.end_section(447u32);
            let __result = self
                .notify_top_processed_to_anchor_to_mempool(
                    mc_block_seqno,
                    top_processed_to_anchor,
                )
                .await;
            __guard.start_section(447u32);
            __result
        }?;
        Ok(())
    }
    async fn notify_top_processed_to_anchor_to_mempool(
        &self,
        mc_block_seqno: BlockSeqno,
        top_processed_to_anchor: MempoolAnchorId,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(notify_top_processed_to_anchor_to_mempool)
            ),
            file!(),
            456u32,
        );
        let mc_block_seqno = mc_block_seqno;
        let top_processed_to_anchor = top_processed_to_anchor;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, top_processed_to_anchor,
            "will notify top_processed_to_anchor to mempool",
        );
        {
            __guard.end_section(464u32);
            let __result = self
                .mpool_adapter
                .handle_signed_mc_block(mc_block_seqno)
                .await;
            __guard.start_section(464u32);
            __result
        }?;
        self.mpool_adapter.clear_anchors_cache(top_processed_to_anchor)?;
        Ok(())
    }
    #[tracing::instrument(skip_all, fields(block_id = %state.block_id().as_short_id()))]
    fn cancel_validation_sessions_until_block(
        &self,
        state: ShardStateStuff,
    ) -> Result<()> {
        let block_id = state.block_id().as_short_id();
        let session_id = self
            .active_collation_sessions
            .read()
            .get(&block_id.shard)
            .map(|session_info| session_info.get_validation_session_id());
        self.validator.cancel_validation(&block_id, session_id)?;
        Ok(())
    }
    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    fn commit_block_queue_diff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_id: &BlockId,
        top_shard_blocks_info: &[(BlockId, bool)],
        partitions: &FastHashSet<QueuePartitionIdx>,
    ) -> Result<()> {
        if !block_id.is_masterchain() {
            return Ok(());
        }
        let _histogram = HistogramGuard::begin("tycho_collator_commit_queue_diffs_time");
        let mut top_blocks: Vec<_> = top_shard_blocks_info
            .iter()
            .map(|(id, updated)| (*id, *updated))
            .collect();
        top_blocks.push((*block_id, true));
        if let Err(err) = mq_adapter.commit_diff(top_blocks, partitions) {
            bail!(
                "Error committing message queue diff of block ({}): {:?}", block_id, err,
            )
        }
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER,
            "message queue diff was committed",
        );
        Ok(())
    }
    /// Returns `BlockId` if diff was applied.
    /// * `first_required_diffs` - contains ids of known first required diffs for queue for each shard
    fn apply_block_queue_diff_from_entry_stuff(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        block_entry: &BlockCacheEntry,
        min_processed_to: Option<&QueueKey>,
        first_required_diffs: &mut FastHashMap<ShardIdent, BlockId>,
    ) -> Result<Option<BlockId>> {
        let block_id = block_entry.block_id;
        if block_id.seqno == 0 {
            return Ok(None);
        }
        let queue_diff = match &block_entry.data {
            BlockCacheEntryData::Collated { candidate_stuff, .. } => {
                &candidate_stuff.candidate.queue_diff_aug.data
            }
            BlockCacheEntryData::Received { queue_diff, .. } => queue_diff,
        };
        if let Some(min_pt) = min_processed_to
            && queue_diff.as_ref().max_message <= *min_pt
        {
            tracing::debug!(
                target : tracing_targets::COLLATION_MANAGER,
                "Skipping diff for block {}: max_message {} <= min_processed_to {}",
                block_id.as_short_id(), queue_diff.as_ref().max_message, min_pt,
            );
            return Ok(None);
        }
        if mq_adapter.is_diff_exists(&block_id.as_short_id())? {
            tracing::trace!(
                target : tracing_targets::COLLATION_MANAGER, queue_diff_block_id = %
                block_id.as_short_id(),
                "queue diff apply skipped because it is already applied",
            );
            first_required_diffs.insert(block_id.shard, BlockId::default());
            return Ok(None);
        }
        let out_msgs = match &block_entry.data {
            BlockCacheEntryData::Collated { candidate_stuff, .. } => {
                &candidate_stuff
                    .candidate
                    .block
                    .data
                    .load_extra()?
                    .out_msg_description
                    .load()?
            }
            BlockCacheEntryData::Received { out_msgs, .. } => &out_msgs.load()?,
        };
        let queue_diff_with_msgs = QueueDiffWithMessages::from_queue_diff(
            queue_diff,
            out_msgs,
        )?;
        let statistics = DiffStatistics::from_diff(
            &queue_diff_with_msgs,
            queue_diff.block_id().shard,
            queue_diff.as_ref().min_message,
            queue_diff.as_ref().max_message,
        );
        let check_sequence = match first_required_diffs.get(&block_id.shard).copied() {
            None => {
                first_required_diffs.insert(block_id.shard, block_id);
                None
            }
            Some(id) if id == block_id => None,
            _ => Some(DiffZone::Both),
        };
        mq_adapter
            .apply_diff(
                queue_diff_with_msgs,
                queue_diff.block_id().as_short_id(),
                queue_diff.diff_hash(),
                statistics,
                check_sequence,
            )
            .context("apply_block_queue_diff_from_entry_stuff")?;
        Ok(Some(block_id))
    }
    #[tracing::instrument(skip_all, fields(next_block_id = %next_block_id_short))]
    async fn handle_collation_cancelled(
        self: &Arc<Self>,
        _prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        cancel_reason: CollationCancelReason,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_collation_cancelled)),
            file!(),
            619u32,
        );
        let _prev_mc_block_id = _prev_mc_block_id;
        let next_block_id_short = next_block_id_short;
        let cancel_reason = cancel_reason;
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, ? cancel_reason,
            "start handle collation cancelled",
        );
        match cancel_reason {
            CollationCancelReason::AnchorNotFound(_)
            | CollationCancelReason::NextAnchorNotFound(_)
            | CollationCancelReason::ExternalCancel
            | CollationCancelReason::DiffNotFoundInQueue(_) => {
                {
                    __guard.end_section(633u32);
                    let __result = self.ready_to_sync.notified().await;
                    __guard.start_section(633u32);
                    __result
                };
                scopeguard::defer!(self.ready_to_sync.notify_one());
                self.set_collator_state(
                    &next_block_id_short.shard,
                    |ac| {
                        ac.state = CollatorState::Cancelled;
                    },
                );
                let has_active = self
                    .active_collators
                    .iter()
                    .any(|ac| {
                        ac.state == CollatorState::Active
                            || ac.state == CollatorState::CancelPending
                    });
                if !has_active {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        "no active collators in shards and masterchain, \
                        will run sync to last applied mc block",
                    );
                    let (last_collated_mc_block_id, applied_range) = self
                        .blocks_cache
                        .get_last_collated_block_and_applied_mc_queue_range();
                    {
                        __guard.end_section(661u32);
                        let __result = self
                            .sync_to_applied_mc_block_if_exist(
                                last_collated_mc_block_id,
                                applied_range,
                            )
                            .await;
                        __guard.start_section(661u32);
                        __result
                    }?;
                }
            }
        }
        Ok(())
    }
    #[tracing::instrument(
        skip_all,
        fields(
            next_block_id = %next_block_id_short,
            ct = anchor_chain_time,
            ?force_mc_block
        )
    )]
    async fn handle_collation_skipped(
        &self,
        prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_collation_skipped)),
            file!(),
            676u32,
        );
        let prev_mc_block_id = prev_mc_block_id;
        let next_block_id_short = next_block_id_short;
        let anchor_chain_time = anchor_chain_time;
        let force_mc_block = force_mc_block;
        let collation_config = collation_config;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, "will run next collation step",
        );
        {
            __guard.end_section(682u32);
            let __result = self.ready_to_sync.notified().await;
            __guard.start_section(682u32);
            __result
        };
        scopeguard::defer!(self.ready_to_sync.notify_one());
        let updated_collator_state = self
            .set_collator_state(
                &next_block_id_short.shard,
                |ac| {
                    if ac.state == CollatorState::CancelPending {
                        ac.state = CollatorState::Cancelled;
                    }
                },
            );
        if updated_collator_state == Some(CollatorState::Cancelled) {
            tracing::debug!(
                target : tracing_targets::COLLATION_MANAGER, shard_id = %
                next_block_id_short.shard, "collator was cancelled before",
            );
            {
                __guard.end_section(696u32);
                return Ok(());
            };
        }
        {
            __guard.end_section(711u32);
            let __result = self
                .run_next_collation_step(
                    &prev_mc_block_id,
                    next_block_id_short.shard,
                    None,
                    DetectNextCollationStepContext::new(
                        anchor_chain_time,
                        force_mc_block,
                        collation_config.mc_block_min_interval_ms as _,
                        collation_config.mc_block_max_interval_ms as _,
                        None,
                    ),
                )
                .await;
            __guard.start_section(711u32);
            __result
        }
    }
    /// 1. Check if should collate master
    /// 2. And schedule master block collation
    /// 3. Or schedule next collation attempt in current shard
    async fn run_next_collation_step(
        &self,
        prev_mc_block_id: &BlockId,
        shard_id: ShardIdent,
        trigger_shard_block_id_opt: Option<BlockId>,
        ctx: DetectNextCollationStepContext,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run_next_collation_step)),
            file!(),
            723u32,
        );
        let prev_mc_block_id = prev_mc_block_id;
        let shard_id = shard_id;
        let trigger_shard_block_id_opt = trigger_shard_block_id_opt;
        let ctx = ctx;
        let next_step = Self::detect_next_collation_step(
            &mut self.collation_sync_state.lock(),
            self.active_collation_sessions.read().keys().cloned().collect(),
            shard_id,
            ctx,
        );
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, next_step = ? next_step,
            "detected next collation step",
        );
        match next_step {
            NextCollationStep::CollateMaster(next_mc_block_chain_time) => {
                if !shard_id.is_masterchain() {
                    self.set_collator_state(
                        &shard_id,
                        |ac| ac.state = CollatorState::Waiting,
                    );
                }
                self.set_collator_state(
                    &ShardIdent::MASTERCHAIN,
                    |ac| {
                        ac.state = CollatorState::Active;
                    },
                );
                {
                    __guard.end_section(756u32);
                    let __result = self
                        .enqueue_mc_block_collation(
                            prev_mc_block_id.get_next_id_short(),
                            next_mc_block_chain_time,
                            trigger_shard_block_id_opt,
                        )
                        .await;
                    __guard.start_section(756u32);
                    __result
                }?;
            }
            NextCollationStep::WaitForMasterStatus
            | NextCollationStep::WaitForShardStatus => {
                self.set_collator_state(
                    &shard_id,
                    |ac| ac.state = CollatorState::Waiting,
                );
            }
            NextCollationStep::ResumeAttemptsIn(shards_to_resume_attempts) => {
                let mut current_shard_should_wait = true;
                for shard_ident in shards_to_resume_attempts {
                    __guard.checkpoint(768u32);
                    if shard_ident == shard_id {
                        current_shard_should_wait = false;
                    }
                    self.set_collator_state(
                        &shard_ident,
                        |ac| ac.state = CollatorState::Active,
                    );
                    {
                        __guard.end_section(773u32);
                        let __result = self.enqueue_try_collate(&shard_ident).await;
                        __guard.start_section(773u32);
                        __result
                    }?;
                }
                if current_shard_should_wait {
                    self.set_collator_state(
                        &shard_id,
                        |ac| ac.state = CollatorState::Waiting,
                    );
                }
            }
        }
        Ok(())
    }
    /// Process collated block candidate
    /// 1. Save block to cache
    /// 2. Spawn block validation
    /// 3. Check if the master block interval elapsed (according to chain time) and schedule collation
    /// 4. If master block then update last master block chain time
    /// 5. Notify mempool about new master block (it may perform gc or nodes rotation)
    /// 6. Execute master block processing routines
    #[tracing::instrument(
        skip_all,
        fields(block_id = %collation_result.candidate.block.id().as_short_id()),
    )]
    pub async fn handle_collated_block_candidate(
        self: &Arc<Self>,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_collated_block_candidate)),
            file!(),
            798u32,
        );
        let collation_result = collation_result;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, ct = collation_result.candidate
            .chain_time, processed_to_anchor_id = collation_result.candidate
            .processed_to_anchor_id, "start processing block candidate",
        );
        let _histogram = HistogramGuard::begin(
            "tycho_collator_handle_collated_block_candidate_time",
        );
        let block_id = *collation_result.candidate.block.id();
        let candidate_chain_time = collation_result.candidate.chain_time;
        let consensus_config_changed = collation_result
            .candidate
            .consensus_config_changed;
        debug_assert_eq!(block_id.is_masterchain(), collation_result.mc_data.is_some(),);
        {
            __guard.end_section(818u32);
            let __result = self.ready_to_sync.notified().await;
            __guard.start_section(818u32);
            __result
        };
        scopeguard::defer!(self.ready_to_sync.notify_one());
        let session_info = match self
            .active_collation_sessions
            .read()
            .get(&block_id.shard)
            .cloned()
        {
            Some(
                session_info,
            ) if session_info.id() == collation_result.collation_session_id => {
                session_info
            }
            _ => {
                tracing::warn!(
                    target : tracing_targets::COLLATION_MANAGER,
                    "there is no active session related to collated block. Skipped",
                );
                self.set_collator_state(
                    &block_id.shard,
                    |ac| ac.state = CollatorState::Waiting,
                );
                {
                    __guard.end_section(841u32);
                    return Ok(());
                };
            }
        };
        let updated_collator_state = self
            .set_collator_state(
                &block_id.shard,
                |ac| {
                    if ac.state == CollatorState::CancelPending {
                        ac.state = CollatorState::Cancelled;
                    }
                },
            );
        let collator_cancelled = updated_collator_state
            == Some(CollatorState::Cancelled);
        let store_res = if collator_cancelled {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER, shard_id = % block_id.shard,
                "collator was cancelled before",
            );
            let top_shards = self.blocks_cache.get_last_top_shards();
            self.mq_adapter.clear_uncommitted_state(&top_shards)?;
            let (last_collated_mc_block_id, applied_mc_queue_range) = self
                .blocks_cache
                .get_last_collated_block_and_applied_mc_queue_range();
            let store_res = BlockCacheStoreResult {
                received_and_collated: false,
                block_mismatch: false,
                last_collated_mc_block_id,
                applied_mc_queue_range,
            };
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER, ? store_res,
                "block candidate was not saved to cache",
            );
            store_res
        } else {
            let top_shard_blocks_info = collation_result
                .mc_data
                .as_ref()
                .map(|mc_data| {
                    mc_data
                        .shards
                        .iter()
                        .map(|(shard_id, shard_descr)| {
                            (
                                shard_descr.get_block_id(*shard_id),
                                shard_descr.top_sc_block_updated,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let top_processed_to_anchor = collation_result
                .mc_data
                .as_ref()
                .map(|mc_data| mc_data.top_processed_to_anchor);
            let store_res = self
                .blocks_cache
                .store_collated(
                    collation_result.candidate,
                    top_shard_blocks_info,
                    top_processed_to_anchor,
                )?;
            if store_res.block_mismatch {
                let labels = [("workchain", block_id.shard.workchain().to_string())];
                metrics::counter!("tycho_collator_block_mismatch_count", & labels)
                    .increment(1);
                self.set_collator_state(
                    &block_id.shard,
                    |ac| ac.state = CollatorState::Cancelled,
                );
                if block_id.is_masterchain() {
                    for mut ac in self
                        .active_collators
                        .iter_mut()
                        .filter(|ac| ac.key() != &block_id.shard)
                    {
                        __guard.checkpoint(922u32);
                        ac.state = match ac.state {
                            CollatorState::Waiting | CollatorState::Cancelled => {
                                CollatorState::Cancelled
                            }
                            _ => CollatorState::CancelPending,
                        };
                    }
                }
                let top_shards = self.blocks_cache.get_last_top_shards();
                self.mq_adapter.clear_uncommitted_state(&top_shards)?;
                tracing::info!(
                    target : tracing_targets::COLLATION_MANAGER, ? store_res,
                    "saved block candidate to cache",
                );
            } else {
                tracing::debug!(
                    target : tracing_targets::COLLATION_MANAGER, ? store_res,
                    "saved block candidate to cache",
                );
            }
            store_res
        };
        let collation_cancelled = collator_cancelled || store_res.block_mismatch;
        let should_sync_to_last_applied_mc_block = 'check_should_sync: {
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                let last_received_mc_block_seqno = self
                    .get_last_received_mc_block_seqno();
                if matches!(
                    last_received_mc_block_seqno, Some(last_received_seqno) if
                    last_received_seqno.saturating_sub(applied_range_end) > 1
                ) {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        last_received_mc_block_seqno, applied_range_end,
                        "check_should_sync: should not sync to last applied mc block \
                        because a newer one already received",
                    );
                    {
                        __guard.end_section(976u32);
                        __guard.start_section(976u32);
                        break 'check_should_sync false;
                    };
                }
            }
            if collation_cancelled {
                true
            } else if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range
            {
                if let Some(last_collated_mc_block_id) = store_res
                    .last_collated_mc_block_id
                {
                    let applied_range_end_delta = applied_range_end
                        .saturating_sub(last_collated_mc_block_id.seqno);
                    if applied_range_end_delta
                        < self.config.min_mc_block_delta_from_bc_to_sync
                    {
                        tracing::debug!(
                            target : tracing_targets::COLLATION_MANAGER,
                            "check_should_sync: should collate next own mc block: \
                            last applied ({}) ahead last collated ({}) on {} < {}",
                            applied_range_end, last_collated_mc_block_id.seqno,
                            applied_range_end_delta, self.config
                            .min_mc_block_delta_from_bc_to_sync,
                        );
                        false
                    } else {
                        tracing::info!(
                            target : tracing_targets::COLLATION_MANAGER,
                            "check_should_sync: should sync to last applied mc block from bc: \
                            last applied ({}) ahead last collated ({}) on {} >= {}",
                            applied_range_end, last_collated_mc_block_id.seqno,
                            applied_range_end_delta, self.config
                            .min_mc_block_delta_from_bc_to_sync,
                        );
                        true
                    }
                } else {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        "check_should_sync: should sync to last applied mc block from bc: \
                        last applied ({}) and last collated not exist",
                        applied_range_end,
                    );
                    true
                }
            } else {
                tracing::debug!(
                    target : tracing_targets::COLLATION_MANAGER,
                    "check_should_sync: should collate next own mc block after because nothing applied ahead",
                );
                false
            }
        };
        if should_sync_to_last_applied_mc_block {
            if block_id.is_masterchain() {
                {
                    __guard.end_section(1036u32);
                    let __result = self
                        .sync_to_applied_mc_block_if_exist(
                            store_res.last_collated_mc_block_id,
                            store_res.applied_mc_queue_range,
                        )
                        .await;
                    __guard.start_section(1036u32);
                    __result
                }?;
            } else {
                tracing::info!(
                    target : tracing_targets::COLLATION_MANAGER,
                    "sync_to_applied_mc_block: mark shard collator Cancelled for shard",
                );
                self.set_collator_state(
                    &block_id.shard,
                    |ac| ac.state = CollatorState::Cancelled,
                );
                let has_active = self
                    .active_collators
                    .iter()
                    .any(|ac| {
                        ac.state == CollatorState::Active
                            || ac.state == CollatorState::CancelPending
                    });
                if !has_active {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        "sync_to_applied_mc_block: no active collators in shards and master, \
                        will run sync to last applied mc block",
                    );
                    {
                        __guard.end_section(1061u32);
                        let __result = self
                            .sync_to_applied_mc_block_if_exist(
                                store_res.last_collated_mc_block_id,
                                store_res.applied_mc_queue_range,
                            )
                            .await;
                        __guard.start_section(1061u32);
                        __result
                    }?;
                }
            }
        } else if block_id.is_masterchain() {
            if consensus_config_changed == Some(true) {
                let mut delayed_mc_state_update = self.delayed_mc_state_update.lock();
                *delayed_mc_state_update = collation_result.mc_data.clone();
            } else {
                {
                    __guard.end_section(1074u32);
                    let __result = self
                        .notify_mc_state_update_to_mempool(
                            collation_result.mc_data.clone().unwrap(),
                        )
                        .await;
                    __guard.start_section(1074u32);
                    __result
                }?;
            }
            if store_res.received_and_collated {
                {
                    __guard.end_section(1082u32);
                    let __result = self.commit_valid_master_block(&block_id).await;
                    __guard.start_section(1082u32);
                    __result
                }?;
            } else {
                let validator = self.validator.clone();
                let validation_session_id = session_info.get_validation_session_id();
                let dispatcher = self.dispatcher.clone();
                tokio::spawn(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        1087u32,
                    );
                    let validation_result = {
                        __guard.end_section(1089u32);
                        let __result = validator
                            .validate(validation_session_id, &block_id)
                            .await;
                        __guard.start_section(1089u32);
                        __result
                    };
                    match validation_result {
                        Ok(status) => {
                            _ = {
                                __guard.end_section(1099u32);
                                let __result = dispatcher
                                    .spawn_task(
                                        method_to_async_closure!(
                                            handle_validated_master_block, block_id, status
                                        ),
                                    )
                                    .await;
                                __guard.start_section(1099u32);
                                __result
                            };
                        }
                        Err(e) => {
                            tracing::error!(
                                target : tracing_targets::COLLATION_MANAGER,
                                "block candidate validation failed: {e:?}",
                            );
                        }
                    }
                });
            }
            if consensus_config_changed != Some(true) {
                {
                    __guard.end_section(1118u32);
                    let __result = self
                        .process_mc_state_update(
                            collation_result.mc_data.unwrap(),
                            ProcessMcStateUpdateMode::StartCollation {
                                reset_collators: false,
                            },
                        )
                        .await;
                    __guard.start_section(1118u32);
                    __result
                }?;
            }
        } else {
            tracing::debug!(
                target : tracing_targets::COLLATION_MANAGER,
                "will run next collation step",
            );
            {
                __guard.end_section(1143u32);
                let __result = self
                    .run_next_collation_step(
                        &collation_result.prev_mc_block_id,
                        block_id.shard,
                        Some(block_id),
                        DetectNextCollationStepContext::new(
                            candidate_chain_time,
                            collation_result.force_next_mc_block,
                            collation_result.collation_config.mc_block_min_interval_ms
                                as _,
                            collation_result.collation_config.mc_block_max_interval_ms
                                as _,
                            Some(
                                CollatedBlockInfo::new(
                                    collation_result.prev_mc_block_id.seqno,
                                    collation_result.has_processed_externals,
                                ),
                            ),
                        ),
                    )
                    .await;
                __guard.start_section(1143u32);
                __result
            }?;
        }
        Ok(())
    }
    /// Finish active sync if it is not finished yet
    /// and enqueue received block
    #[tracing::instrument(
        skip_all,
        fields(block_id = %ctx.state.block_id().as_short_id())
    )]
    pub async fn enqueue_handle_block_from_bc(
        &self,
        ctx: HandledBlockFromBcCtx,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_handle_block_from_bc)),
            file!(),
            1152u32,
        );
        let ctx = ctx;
        tracing::trace!(
            target : tracing_targets::COLLATION_MANAGER,
            "cancel sync: enqueue_handle_block_from_bc: started",
        );
        let labels = [
            ("workchain", ctx.state.block_id().shard.workchain().to_string()),
            ("src", "01_received".to_string()),
        ];
        metrics::gauge!("tycho_last_block_seqno", & labels)
            .set(ctx.state.block_id().seqno);
        self.update_last_received_mc_block_seqno(ctx.state.block_id());
        self.finish_active_sync_to_applied(ctx.state.block_id());
        {
            __guard.end_section(1176u32);
            let __result = self.blocks_from_bc_queue_sender.send(ctx).await;
            __guard.start_section(1176u32);
            __result
        }?;
        Ok(())
    }
    /// Process the queue of received blocks from bc
    #[tracing::instrument(skip_all)]
    async fn process_handle_block_from_bc_queue(
        self: Arc<Self>,
        mut blocks_from_bc_queue_receiver: tokio::sync::mpsc::Receiver<
            HandledBlockFromBcCtx,
        >,
        cancel_task: CancellationToken,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::", stringify!(process_handle_block_from_bc_queue)
            ),
            file!(),
            1187u32,
        );
        let mut blocks_from_bc_queue_receiver = blocks_from_bc_queue_receiver;
        let cancel_task = cancel_task;
        const BATCH_SIZE: usize = 300;
        tracing::info!(target : tracing_targets::COLLATION_MANAGER, "started",);
        loop {
            __guard.checkpoint(1194u32);
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            {
                __guard.end_section(1196u32);
                let __result = tokio::select! {
                    received_count = blocks_from_bc_queue_receiver.recv_many(& mut batch,
                    BATCH_SIZE) => { if received_count == 0 { tracing::info!(target :
                    tracing_targets::COLLATION_MANAGER, "channel closed",); break; } let
                    mut last_mc_block_id_opt = None; for HandledBlockFromBcCtx { state,
                    .. } in batch.iter().rev() { if state.block_id().is_masterchain() {
                    last_mc_block_id_opt = Some(* state.block_id()); } } for ctx in batch
                    { let is_last_mc_block_in_batch = matches!(last_mc_block_id_opt,
                    Some(last_mc_block_id) if ctx.state.block_id() == &
                    last_mc_block_id); self.handle_block_from_bc(ctx,
                    is_last_mc_block_in_batch). await .map_err(| err | {
                    tracing::error!(target : tracing_targets::COLLATION_MANAGER,
                    "error handling block from bc: {err:?}",); err }) ?; } }, _ =
                    cancel_task.cancelled() => { tracing::info!(target :
                    tracing_targets::COLLATION_MANAGER, "cancelled",); break; }
                };
                __guard.start_section(1196u32);
                __result
            }
        }
        tracing::info!(target : tracing_targets::COLLATION_MANAGER, "finished",);
        Ok(())
    }
    /// Process new block from blockchain:
    /// 1. Save block to cache
    /// 2. Stop block validation if needed
    /// 3. Commit block if it was collated first
    /// 4. Notify mempool about new master block
    /// 5. Sync to received block if it is far ahead last collated and last `synced_to`
    #[tracing::instrument(
        skip_all,
        fields(block_id = %ctx.state.block_id().as_short_id(), is_last_mc_block_in_batch)
    )]
    async fn handle_block_from_bc(
        self: &Arc<Self>,
        ctx: HandledBlockFromBcCtx,
        is_last_mc_block_in_batch: bool,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_block_from_bc)),
            file!(),
            1255u32,
        );
        let ctx = ctx;
        let is_last_mc_block_in_batch = is_last_mc_block_in_batch;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER,
            "start processing block from bc",
        );
        let block_id = *ctx.state.block_id();
        let _histogram = HistogramGuard::begin(
            "tycho_collator_handle_block_from_bc_time",
        );
        let state = ctx.state;
        {
            __guard.end_section(1267u32);
            let __result = self.ready_to_sync.notified().await;
            __guard.start_section(1267u32);
            __result
        };
        scopeguard::defer!(self.ready_to_sync.notify_one());
        let processed_upto = ProcessedUptoInfoStuff::try_from(ctx.processed_upto)?;
        let Some(store_res) = {
            __guard.end_section(1279u32);
            let __result = self
                .blocks_cache
                .store_received(
                    self.state_node_adapter.clone(),
                    state.clone(),
                    processed_upto,
                )
                .await;
            __guard.start_section(1279u32);
            __result
        }? else {
            {
                __guard.end_section(1281u32);
                return Ok(());
            };
        };
        if store_res.block_mismatch {
            let labels = [("workchain", block_id.shard.workchain().to_string())];
            metrics::counter!("tycho_collator_block_mismatch_count", & labels)
                .increment(1);
            self.set_collator_state(
                &block_id.shard,
                |ac| {
                    ac.state = match ac.state {
                        CollatorState::Waiting | CollatorState::Cancelled => {
                            CollatorState::Cancelled
                        }
                        _ => CollatorState::CancelPending,
                    };
                },
            );
            if block_id.is_masterchain() {
                for mut ac in self
                    .active_collators
                    .iter_mut()
                    .filter(|ac| ac.key() != &block_id.shard)
                {
                    __guard.checkpoint(1299u32);
                    ac.state = match ac.state {
                        CollatorState::Waiting | CollatorState::Cancelled => {
                            CollatorState::Cancelled
                        }
                        _ => CollatorState::CancelPending,
                    };
                }
            }
            let top_shards = self.blocks_cache.get_last_top_shards();
            self.mq_adapter.clear_uncommitted_state(&top_shards)?;
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER, ? store_res,
                "saved block from bc to cache",
            );
        } else {
            tracing::debug!(
                target : tracing_targets::COLLATION_MANAGER, ? store_res,
                "saved block from bc to cache",
            );
        }
        if !block_id.is_masterchain() {
            {
                __guard.end_section(1332u32);
                return Ok(());
            };
        }
        let is_key_block = state.state_extra()?.after_key_block;
        let session_id = self
            .active_collation_sessions
            .read()
            .get(&block_id.shard)
            .map(|session_info| session_info.get_validation_session_id());
        self.validator.cancel_validation(&block_id.as_short_id(), session_id)?;
        let should_sync_to_last_applied_mc_block = 'check_should_sync: {
            if !is_last_mc_block_in_batch && !is_key_block {
                {
                    __guard.end_section(1353u32);
                    __guard.start_section(1353u32);
                    break 'check_should_sync false;
                };
            }
            if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                let last_received_mc_block_seqno = self
                    .get_last_received_mc_block_seqno();
                if matches!(
                    last_received_mc_block_seqno, Some(last_received_seqno) if
                    last_received_seqno.saturating_sub(applied_range_end) > 1
                ) {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        last_received_mc_block_seqno, received_is_key_block =
                        is_key_block,
                        "check_should_sync: should not sync to last applied mc block \
                        because a newer one already received",
                    );
                    {
                        __guard.end_section(1369u32);
                        __guard.start_section(1369u32);
                        break 'check_should_sync false;
                    };
                }
            }
            if let Some(top_mc_block_id_for_next_collation) = self
                .get_top_mc_block_id_for_next_collation(
                    store_res.last_collated_mc_block_id,
                )
            {
                if let Some((_, applied_range_end)) = store_res.applied_mc_queue_range {
                    let should_sync = {
                        let applied_range_end_delta = applied_range_end
                            .saturating_sub(top_mc_block_id_for_next_collation.seqno);
                        let required_min_mc_block_delta = if is_key_block
                            && !self
                                .active_collators
                                .contains_key(&ShardIdent::MASTERCHAIN)
                        {
                            1
                        } else {
                            self.config.min_mc_block_delta_from_bc_to_sync
                        };
                        if applied_range_end_delta < required_min_mc_block_delta {
                            tracing::debug!(
                                target : tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ? self
                                .get_last_synced_to_mc_block_id().map(| id | id
                                .as_short_id().to_string()), last_collated_mc_block_id = ?
                                store_res.last_collated_mc_block_id.map(| id | id
                                .as_short_id().to_string()), last_processed_mc_block_id = ?
                                self.get_last_processed_mc_block_id().map(| id | id
                                .as_short_id().to_string()), received_is_key_block =
                                is_key_block,
                                "check_should_sync: should wait for next collated own mc block: \
                                last applied ({}) ahead of top for collation ({}) on {} < {}",
                                applied_range_end, top_mc_block_id_for_next_collation.seqno,
                                applied_range_end_delta, required_min_mc_block_delta,
                            );
                            false
                        } else {
                            tracing::info!(
                                target : tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ? self
                                .get_last_synced_to_mc_block_id().map(| id | id
                                .as_short_id().to_string()), last_collated_mc_block_id = ?
                                store_res.last_collated_mc_block_id.map(| id | id
                                .as_short_id().to_string()), last_processed_mc_block_id = ?
                                self.get_last_processed_mc_block_id().map(| id | id
                                .as_short_id().to_string()), received_is_key_block =
                                is_key_block,
                                "check_should_sync: should sync to last applied mc block from bc: \
                                last applied ({}) ahead of top for collation ({}) on {} >= {}",
                                applied_range_end, top_mc_block_id_for_next_collation.seqno,
                                applied_range_end_delta, required_min_mc_block_delta,
                            );
                            true
                        }
                    };
                    if should_sync {
                        let mut has_active = false;
                        for active_collator in self
                            .active_collators
                            .iter()
                            .filter(|ac| {
                                ac.state == CollatorState::Active
                                    || ac.state == CollatorState::CancelPending
                            })
                        {
                            __guard.checkpoint(1422u32);
                            active_collator.cancel_collation.notify_one();
                            has_active = true;
                        }
                        if has_active {
                            tracing::info!(
                                target : tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ? self
                                .get_last_synced_to_mc_block_id().map(| id | id
                                .as_short_id().to_string()), last_collated_mc_block_id = ?
                                store_res.last_collated_mc_block_id.map(| id | id
                                .as_short_id().to_string()), last_processed_mc_block_id = ?
                                self.get_last_processed_mc_block_id().map(| id | id
                                .as_short_id().to_string()), received_is_key_block =
                                is_key_block,
                                "check_should_sync: cannot sync when there are active collations, \
                                try to gracefully cancel them",
                            );
                            false
                        } else {
                            tracing::info!(
                                target : tracing_targets::COLLATION_MANAGER,
                                last_synced_to_mc_block_id = ? self
                                .get_last_synced_to_mc_block_id().map(| id | id
                                .as_short_id().to_string()), last_collated_mc_block_id = ?
                                store_res.last_collated_mc_block_id.map(| id | id
                                .as_short_id().to_string()), last_processed_mc_block_id = ?
                                self.get_last_processed_mc_block_id().map(| id | id
                                .as_short_id().to_string()), received_is_key_block =
                                is_key_block,
                                "check_should_sync: can sync to last applied mc block \
                                when all collators were cancelled, or waiting, or there are no collators (node not in set)",
                            );
                            true
                        }
                    } else {
                        false
                    }
                } else {
                    tracing::debug!(
                        target : tracing_targets::COLLATION_MANAGER,
                        last_synced_to_mc_block_id = ? self
                        .get_last_synced_to_mc_block_id().map(| id | id.as_short_id()
                        .to_string()), last_collated_mc_block_id = ? store_res
                        .last_collated_mc_block_id.map(| id | id.as_short_id()
                        .to_string()), last_processed_mc_block_id = ? self
                        .get_last_processed_mc_block_id().map(| id | id.as_short_id()
                        .to_string()),
                        "check_should_sync: should collate next own mc block after because nothing applied ahead",
                    );
                    false
                }
            } else {
                tracing::info!(
                    target : tracing_targets::COLLATION_MANAGER, received_is_key_block =
                    is_key_block,
                    "should sync to last applied mc block when no last collated or prev sync to",
                );
                true
            }
        };
        if should_sync_to_last_applied_mc_block {
            {
                __guard.end_section(1480u32);
                let __result = self
                    .sync_to_applied_mc_block_if_exist(
                        store_res.last_collated_mc_block_id,
                        store_res.applied_mc_queue_range,
                    )
                    .await;
                __guard.start_section(1480u32);
                __result
            }?;
        } else {
            if store_res.received_and_collated {
                {
                    __guard.end_section(1491u32);
                    let __result = self.commit_valid_master_block(&block_id).await;
                    __guard.start_section(1491u32);
                    __result
                }?;
            }
        }
        Ok(())
    }
    async fn sync_to_applied_mc_block_if_exist(
        self: &Arc<Self>,
        last_collated_mc_block_id: Option<BlockId>,
        applied_range: Option<(BlockSeqno, BlockSeqno)>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(sync_to_applied_mc_block_if_exist)),
            file!(),
            1502u32,
        );
        let last_collated_mc_block_id = last_collated_mc_block_id;
        let applied_range = applied_range;
        if let Some(applied_range) = applied_range {
            let this = self.clone();
            let res = {
                __guard.end_section(1509u32);
                let __result = tokio::task::spawn_blocking(move || {
                        this.sync_to_applied_mc_block(
                            applied_range,
                            last_collated_mc_block_id,
                        )
                    })
                    .await;
                __guard.start_section(1509u32);
                __result
            }??;
            if !res {
                tracing::info!(
                    target : tracing_targets::COLLATION_MANAGER, last_applied_mc_block_id
                    = % BlockIdShort { shard : ShardIdent::MASTERCHAIN, seqno :
                    applied_range.1, },
                    "sync_to_applied_mc_block: unable to sync to last applied mc block, \
                    need to receive next blocks from bc",
                );
            }
        } else {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER,
                "sync_to_applied_mc_block: there is no received applied mc blocks in cache, \
                will wait for next blocks from bc",
            );
        }
        Ok(())
    }
    /// Restores internals queue state,
    /// processes last applied mc state
    /// to run next blocks collation.
    ///
    /// Returns `false` if unable to
    /// load diff to restore queue state
    #[tracing::instrument(skip_all, fields(applied_range = ?applied_range))]
    fn sync_to_applied_mc_block(
        &self,
        applied_range: (BlockSeqno, BlockSeqno),
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Result<bool> {
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER,
            "start sync to applied mc block",
        );
        let _histogram = HistogramGuard::begin(
            "tycho_collator_sync_to_applied_mc_block_time",
        );
        metrics::counter!("tycho_collator_sync_to_applied_mc_block_count").increment(1);
        let first_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.0,
        };
        let last_applied_mc_block_key = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno: applied_range.1,
        };
        let cancelled = self.set_active_sync_info(last_applied_mc_block_key.seqno)?;
        tracing::trace!(
            target : tracing_targets::COLLATION_MANAGER,
            "cancel sync: sync_to_applied_mc_block: set_active_sync_info for seqno={}",
            last_applied_mc_block_key.seqno,
        );
        scopeguard::defer!(
            { self.clean_active_sync_info(); tracing::trace!(target :
            tracing_targets::COLLATION_MANAGER,
            "cancel sync: sync_to_applied_mc_block: active_sync_info cleaned",); }
        );
        scopeguard::defer!(self.blocks_cache.gc_prev_blocks());
        if let Some(mp_cfg_override) = &self.mempool_config_override {
            let last_consesus_info = self
                .blocks_cache
                .get_consensus_info_for_mc_block(&last_applied_mc_block_key)?;
            if mp_cfg_override.genesis_info.overrides(&last_consesus_info.genesis_info) {
                tracing::info!(
                    target : tracing_targets::COLLATION_MANAGER, prev_genesis_start_round
                    = last_consesus_info.genesis_info.start_round,
                    prev_genesis_time_millis = last_consesus_info.genesis_info
                    .genesis_millis, new_genesis_start_round = mp_cfg_override
                    .genesis_info.start_round, new_genesis_time_millis = mp_cfg_override
                    .genesis_info.genesis_millis,
                    "will drop uncommitted internal messages from queue on new genesis",
                );
                let top_shards = self.blocks_cache.get_last_top_shards();
                self.mq_adapter.clear_uncommitted_state(&top_shards)?;
            }
        }
        let all_shards_processed_to_by_partitions = Self::get_all_shards_processed_to_by_partitions_for_mc_block(
                &last_applied_mc_block_key,
                &self.blocks_cache,
                self.state_node_adapter.clone(),
            )
            .await_blocking()?;
        let min_processed_to_by_shards = find_min_processed_to_by_shards(
            &all_shards_processed_to_by_partitions,
        );
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, ? min_processed_to_by_shards,
        );
        let before_tail_block_ids = self
            .blocks_cache
            .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)?;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, tail_block_ids = ?
            before_tail_block_ids.iter().map(| (shard_id, (id, prev_ids)) | {
            format!("({}, id={:?}, prev_ids={})", shard_id, id.as_ref()
            .map(DisplayAsShortId), DisplayBlockIdsIntoIter(prev_ids),) }).collect::< Vec
            < _ >> ().as_slice(),
        );
        for shard in before_tail_block_ids.keys() {
            let labels = [("workchain", shard.workchain().to_string())];
            metrics::gauge!("tycho_collator_sync_is_running", & labels).set(1);
        }
        let queue_diffs_applied_to_top_blocks = if self.config.fast_sync {
            if let Some(applied_to_mc_block_id) = self
                .get_queue_diffs_applied_to_mc_block_id(last_collated_mc_block_id)?
            {
                Self::get_top_blocks_seqno(
                        &applied_to_mc_block_id,
                        &self.blocks_cache,
                        self.state_node_adapter.clone(),
                    )
                    .await_blocking()?
            } else {
                FastHashMap::default()
            }
        } else {
            FastHashMap::default()
        };
        let queue_restore_res = Self::restore_queue(
                &self.blocks_cache,
                self.state_node_adapter.clone(),
                self.mq_adapter.clone(),
                applied_range.0,
                min_processed_to_by_shards,
                before_tail_block_ids,
                queue_diffs_applied_to_top_blocks,
            )
            .await_blocking()?;
        let Some(last_mc_state) = queue_restore_res.last_mc_state else {
            return Ok(false);
        };
        if last_mc_state.block_id().seqno != 0 {
            Self::renew_mc_block_latest_chain_time(
                &mut self.collation_sync_state.lock(),
                last_mc_state.get_gen_chain_time(),
            );
        }
        Self::reset_collation_sync_status(&mut self.collation_sync_state.lock());
        self.update_last_synced_to_mc_block_id(*last_mc_state.block_id());
        self.blocks_cache.reset_top_shard_blocks_additional_info();
        let mc_data = McData::load_from_state(
            &last_mc_state,
            all_shards_processed_to_by_partitions,
        )?;
        self.blocks_cache
            .remove_next_collated_blocks_from_cache(
                &queue_restore_res.synced_to_blocks_keys,
            );
        self.notify_mc_state_update_to_mempool(mc_data.clone()).await_blocking()?;
        let process_state_update_mode = match cancelled.is_cancelled() {
            true => ProcessMcStateUpdateMode::SkipCollation,
            false => {
                ProcessMcStateUpdateMode::StartCollation {
                    reset_collators: true,
                }
            }
        };
        self.process_mc_state_update(mc_data.clone(), process_state_update_mode)
            .await_blocking()?;
        self.notify_top_processed_to_anchor_to_mempool(
                mc_data.block_id.seqno,
                mc_data.top_processed_to_anchor,
            )
            .await_blocking()?;
        for synced_to_block_id in queue_restore_res.synced_to_blocks_keys {
            let labels = [
                ("workchain", synced_to_block_id.shard.workchain().to_string()),
                ("src", "02_synced_to".to_string()),
            ];
            metrics::gauge!("tycho_last_block_seqno", & labels)
                .set(synced_to_block_id.seqno);
        }
        Ok(true)
    }
    async fn get_all_shards_processed_to_by_partitions_for_mc_block(
        mc_block_key: &BlockCacheKey,
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(get_all_shards_processed_to_by_partitions_for_mc_block)
            ),
            file!(),
            1749u32,
        );
        let mc_block_key = mc_block_key;
        let blocks_cache = blocks_cache;
        let state_node_adapter = state_node_adapter;
        let mut result = FastHashMap::default();
        if mc_block_key.seqno == 0 {
            {
                __guard.end_section(1753u32);
                return Ok(result);
            };
        }
        let from_cache = blocks_cache
            .get_top_blocks_processed_to_by_partitions(mc_block_key)?;
        for (top_block_id, (updated, processed_to_opt)) in from_cache {
            __guard.checkpoint(1758u32);
            let processed_to = match processed_to_opt {
                Some(processed_to) => processed_to,
                None => {
                    if top_block_id.seqno == 0 {
                        FastHashMap::default()
                    } else {
                        let state = {
                            __guard.end_section(1768u32);
                            let __result = state_node_adapter
                                .load_state(mc_block_key.seqno, &top_block_id)
                                .await;
                            __guard.start_section(1768u32);
                            __result
                        }?;
                        let processed_upto = state.state().processed_upto.load()?;
                        let processed_upto = ProcessedUptoInfoStuff::try_from(
                            processed_upto,
                        )?;
                        processed_upto.get_internals_processed_to_by_partitions()
                    }
                }
            };
            result.insert(top_block_id.shard, (updated, processed_to));
        }
        Ok(result)
    }
    fn get_queue_diffs_applied_to_mc_block_id(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Result<Option<BlockId>> {
        let last_queue_comitted_on = self.mq_adapter.get_last_commited_mc_block_id()?;
        let last_collated_or_synced_to = self
            .get_top_mc_block_id_for_next_collation(last_collated_mc_block_id);
        let mc_block_id = match (last_queue_comitted_on, last_collated_or_synced_to) {
            (Some(last_queue_comitted_on), Some(last_collated_or_synced_to)) => {
                if last_collated_or_synced_to.seqno >= last_queue_comitted_on.seqno {
                    Some(last_collated_or_synced_to)
                } else {
                    Some(last_queue_comitted_on)
                }
            }
            (Some(mc_block_id), _) | (_, Some(mc_block_id)) => Some(mc_block_id),
            _ => None,
        };
        Ok(mc_block_id)
    }
    /// Return last collated if it is ahead of last received and "synced to".
    /// Or last received and "synced to" if it is ahead of last correct collated.
    /// Last collated may be incorrect but we don not know this until we receive block from bc.
    /// We use this to detect the lag from last received to check if we need to run next sync.
    fn get_top_mc_block_id_for_next_collation(
        &self,
        last_collated_mc_block_id: Option<BlockId>,
    ) -> Option<BlockId> {
        let last_synced_to_mc_block_id = self.get_last_synced_to_mc_block_id();
        match (last_synced_to_mc_block_id, last_collated_mc_block_id) {
            (Some(last_synced_to), Some(last_collated)) => {
                if last_synced_to.seqno > last_collated.seqno {
                    Some(last_synced_to)
                } else {
                    Some(last_collated)
                }
            }
            (Some(mc_block_id), _) | (_, Some(mc_block_id)) => Some(mc_block_id),
            _ => None,
        }
    }
    #[tracing::instrument(skip_all, fields(from_mc_block_seqno))]
    async fn restore_queue(
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        from_mc_block_seqno: u32,
        min_processed_to_by_shards: BTreeMap<ShardIdent, QueueKey>,
        before_tail_block_ids: BTreeMap<ShardIdent, (Option<BlockId>, Vec<BlockId>)>,
        queue_diffs_applied_to_top_blocks: FastHashMap<ShardIdent, u32>,
    ) -> Result<RestoreQueueResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(restore_queue)),
            file!(),
            1839u32,
        );
        let blocks_cache = blocks_cache;
        let state_node_adapter = state_node_adapter;
        let mq_adapter = mq_adapter;
        let from_mc_block_seqno = from_mc_block_seqno;
        let min_processed_to_by_shards = min_processed_to_by_shards;
        let before_tail_block_ids = before_tail_block_ids;
        let queue_diffs_applied_to_top_blocks = queue_diffs_applied_to_top_blocks;
        let mut res = RestoreQueueResult::default();
        let init_mc_block_id = state_node_adapter.load_init_block_id();
        let mut init_mc_block_reached_on = FastHashMap::new();
        let mut first_required_diffs = FastHashMap::new();
        for (shard_id, min_processed_to) in &min_processed_to_by_shards {
            __guard.checkpoint(1848u32);
            let mut prev_queue_diffs = vec![];
            let Some((_, prev_block_ids)) = before_tail_block_ids.get(shard_id) else {
                {
                    __guard.end_section(1851u32);
                    __guard.start_section(1851u32);
                    continue;
                };
            };
            let mut prev_block_ids: VecDeque<_> = prev_block_ids
                .iter()
                .cloned()
                .collect();
            while let Some(prev_block_id) = prev_block_ids.pop_front() {
                __guard.checkpoint(1855u32);
                if prev_block_id.seqno == 0 {
                    {
                        __guard.end_section(1857u32);
                        __guard.start_section(1857u32);
                        continue;
                    };
                }
                if let Some(border) = queue_diffs_applied_to_top_blocks.get(shard_id)
                    && prev_block_id.seqno <= *border
                {
                    tracing::debug!(
                        target : tracing_targets::COLLATION_MANAGER, prev_block_id = %
                        prev_block_id.as_short_id(), top_applied_seqno = border,
                        "previous queue diff skipped because it below top applied",
                    );
                    {
                        __guard.end_section(1869u32);
                        __guard.start_section(1869u32);
                        continue;
                    };
                }
                if let Some(init_mc_block_id) = init_mc_block_id {
                    let mut skip_diff = false;
                    if prev_block_id.is_masterchain() {
                        if prev_block_id.seqno <= init_mc_block_id.seqno {
                            tracing::debug!(
                                target : tracing_targets::COLLATION_MANAGER, prev_block_id =
                                % prev_block_id.as_short_id(), init_mc_block_id = %
                                init_mc_block_id.as_short_id(),
                                "master block queue diff apply skipped because it is below init block from persistent state",
                            );
                            skip_diff = true;
                        }
                    } else {
                        let mut init_mc_block_reached = matches!(
                            init_mc_block_reached_on.get(& prev_block_id.shard),
                            Some(reached_seqno) if prev_block_id.seqno <= *
                            reached_seqno,
                        );
                        if !init_mc_block_reached {
                            let prev_ref_by_mc_seqno = {
                                __guard.end_section(1895u32);
                                let __result = state_node_adapter
                                    .get_ref_by_mc_seqno(&prev_block_id)
                                    .await;
                                __guard.start_section(1895u32);
                                __result
                            }?
                                .unwrap();
                            init_mc_block_reached = prev_ref_by_mc_seqno
                                <= init_mc_block_id.seqno;
                            if init_mc_block_reached {
                                init_mc_block_reached_on
                                    .insert(prev_block_id.shard, prev_block_id.seqno);
                            }
                        }
                        if init_mc_block_reached {
                            tracing::debug!(
                                target : tracing_targets::COLLATION_MANAGER, prev_block_id =
                                % prev_block_id.as_short_id(), init_mc_block_id = %
                                init_mc_block_id.as_short_id(),
                                "shard block queue diff apply skipped because it is below init block from persistent state",
                            );
                            skip_diff = true;
                        }
                    }
                    if skip_diff {
                        first_required_diffs
                            .insert(prev_block_id.shard, BlockId::default());
                        {
                            __guard.end_section(1916u32);
                            __guard.start_section(1916u32);
                            continue;
                        };
                    }
                }
                let Some(queue_diff_stuff) = {
                    __guard.end_section(1921u32);
                    let __result = state_node_adapter.load_diff(&prev_block_id).await;
                    __guard.start_section(1921u32);
                    __result
                }? else {
                    tracing::debug!(
                        target : tracing_targets::COLLATION_MANAGER, prev_block_id = %
                        prev_block_id,
                        "unable to load prev diff to sync queue state, cancel sync",
                    );
                    for shard in before_tail_block_ids.keys() {
                        __guard.checkpoint(1929u32);
                        let labels = [("workchain", shard.workchain().to_string())];
                        metrics::gauge!("tycho_collator_sync_is_running", & labels)
                            .set(0);
                    }
                    {
                        __guard.end_section(1934u32);
                        return Ok(res);
                    };
                };
                let diff_required = &queue_diff_stuff.as_ref().max_message
                    > min_processed_to;
                tracing::debug!(
                    target : tracing_targets::COLLATION_MANAGER, diff_block_id = %
                    prev_block_id.as_short_id(), diff_required, max_message = %
                    queue_diff_stuff.as_ref().max_message, min_processed_to = %
                    min_processed_to,
                    "check if diff required to restore queue working state on sync:",
                );
                if diff_required {
                    first_required_diffs.insert(prev_block_id.shard, prev_block_id);
                    let block_stuff = {
                        __guard.end_section(1951u32);
                        let __result = state_node_adapter
                            .load_block(&prev_block_id)
                            .await;
                        __guard.start_section(1951u32);
                        __result
                    }?
                        .unwrap();
                    let out_msgs = block_stuff.load_extra()?.out_msg_description.load()?;
                    let queue_diff_with_messages = QueueDiffWithMessages::from_queue_diff(
                        &queue_diff_stuff,
                        &out_msgs,
                    )?;
                    prev_queue_diffs
                        .push((
                            queue_diff_with_messages,
                            *queue_diff_stuff.diff_hash(),
                            prev_block_id,
                            queue_diff_stuff.as_ref().min_message,
                            queue_diff_stuff.as_ref().max_message,
                        ));
                    let prev_ids_info = block_stuff.construct_prev_id()?;
                    prev_block_ids.push_back(prev_ids_info.0);
                    if let Some(id) = prev_ids_info.1 {
                        prev_block_ids.push_back(id);
                    }
                }
            }
            while let Some((diff, diff_hash, block_id, min_message, max_message)) = prev_queue_diffs
                .pop()
            {
                __guard.checkpoint(1975u32);
                let statistics = DiffStatistics::from_diff(
                    &diff,
                    block_id.shard,
                    min_message,
                    max_message,
                );
                let check_sequence = match first_required_diffs.get(&block_id.shard) {
                    Some(id) if *id == block_id => None,
                    _ => Some(DiffZone::Both),
                };
                mq_adapter
                    .apply_diff(
                        diff,
                        block_id.as_short_id(),
                        &diff_hash,
                        statistics,
                        check_sequence,
                    )
                    .context("sync_to_applied_mc_block")?;
                res.applied_diffs_ids.insert(block_id);
            }
        }
        loop {
            __guard.checkpoint(2003u32);
            let (mc_block_subgraph_extract, is_last) = blocks_cache
                .pop_front_applied_mc_block_subgraph(from_mc_block_seqno)?;
            let subgraph = match mc_block_subgraph_extract {
                McBlockSubgraphExtract::Extracted(subgraph) => subgraph,
                McBlockSubgraphExtract::AlreadyExtracted => {
                    bail!("mc block subgraph extract result cannot be AlreadyExtracted")
                }
            };
            let mc_block_entry = &subgraph.master_block;
            if subgraph.master_block.block_id.seqno != 0 {
                for block_entry in [mc_block_entry]
                    .into_iter()
                    .chain(subgraph.shard_blocks.iter())
                {
                    __guard.checkpoint(2022u32);
                    if let Some(border) = queue_diffs_applied_to_top_blocks
                        .get(&block_entry.block_id.shard)
                        && block_entry.block_id.seqno <= *border
                    {
                        tracing::debug!(
                            target : tracing_targets::COLLATION_MANAGER,
                            received_block_id = % block_entry.block_id.as_short_id(),
                            top_applied_seqno = border,
                            "queue diff apply skipped because it is below top applied",
                        );
                        {
                            __guard.end_section(2036u32);
                            __guard.start_section(2036u32);
                            continue;
                        };
                    }
                    let min_processed_to = min_processed_to_by_shards
                        .get(&block_entry.block_id.shard);
                    if let Some(applied_diff_block_id) = Self::apply_block_queue_diff_from_entry_stuff(
                        mq_adapter.clone(),
                        block_entry,
                        min_processed_to,
                        &mut first_required_diffs,
                    )? {
                        res.applied_diffs_ids.insert(applied_diff_block_id);
                    }
                }
            }
            let to_blocks_keys = mc_block_entry.get_top_blocks_keys()?;
            blocks_cache.set_gc_to_boundary(&to_blocks_keys);
            if is_last {
                let partitions = subgraph.get_partitions();
                Self::commit_block_queue_diff(
                    mq_adapter.clone(),
                    &mc_block_entry.block_id,
                    &mc_block_entry.top_shard_blocks_info,
                    &partitions,
                )?;
                let top_shards = blocks_cache.get_last_top_shards();
                mq_adapter.clear_uncommitted_state(&top_shards)?;
                let state = mc_block_entry.cached_state()?.clone();
                res.last_mc_state = Some(state);
                res.synced_to_blocks_keys.extend(to_blocks_keys.into_iter());
                {
                    __guard.end_section(2080u32);
                    return Ok(res);
                };
            }
        }
    }
    fn get_last_processed_mc_block_id(&self) -> Option<BlockId> {
        *self.last_processed_mc_block_id.lock()
    }
    fn check_should_process_and_update_last_processed_mc_block(
        &self,
        block_id: &BlockId,
    ) -> bool {
        let mut guard = self.last_processed_mc_block_id.lock();
        let last_processed_mc_block_id_opt = guard.as_ref();
        let (seqno_delta, is_equal) = Self::compare_mc_block_with(
            block_id,
            last_processed_mc_block_id_opt,
        );
        if seqno_delta < 0 || is_equal {
            false
        } else {
            guard.replace(*block_id);
            true
        }
    }
    /// Returns: (seqno delta from other, true - if equal).
    /// If `other_mc_block_id_opt` is none, returns : (0, false)
    fn compare_mc_block_with(
        mc_block_id: &BlockId,
        other_mc_block_id_opt: Option<&BlockId>,
    ) -> (i32, bool) {
        let (seqno_delta, is_equal) = match other_mc_block_id_opt {
            None => {
                tracing::info!(
                    target : tracing_targets::COLLATION_MANAGER,
                    "other mc block is None: current {} other ({:?}): is_equal = false, seqno_delta = 0",
                    mc_block_id.as_short_id(), other_mc_block_id_opt.map(| b | b
                    .as_short_id()),
                );
                (0, false)
            }
            Some(other_mc_block_id) => {
                (
                    mc_block_id.seqno as i32 - other_mc_block_id.seqno as i32,
                    mc_block_id == other_mc_block_id,
                )
            }
        };
        if seqno_delta < 0 || is_equal {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER,
                "mc block is NOT AHEAD of other: current {} other ({:?}): is_equal = {}, seqno_delta = {}",
                mc_block_id.as_short_id(), other_mc_block_id_opt.map(| b | b
                .as_short_id()), is_equal, seqno_delta,
            );
        }
        (seqno_delta, is_equal)
    }
    async fn process_mc_state_update(
        &self,
        mc_data: Arc<McData>,
        mode: ProcessMcStateUpdateMode,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(process_mc_state_update)),
            file!(),
            2139u32,
        );
        let mc_data = mc_data;
        let mode = mode;
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, ? mode,
            "will process master state update",
        );
        if let ProcessMcStateUpdateMode::StartCollation { reset_collators } = mode {
            let block_global = mc_data.config.get_global_version()?;
            if self.config.supported_block_version >= block_global.version
                && block_global
                    .capabilities
                    .is_subset_of(self.config.supported_capabilities)
            {
                {
                    __guard.end_section(2153u32);
                    let __result = self
                        .refresh_collation_sessions(mc_data, reset_collators)
                        .await;
                    __guard.start_section(2153u32);
                    __result
                }?;
            } else {
                tracing::warn!(
                    target : tracing_targets::COLLATION_MANAGER,
                    collator_supported_block_version = self.config
                    .supported_block_version, mc_block_version = block_global.version,
                    collator_supported_capabilities = ? self.config
                    .supported_capabilities, mc_block_capabilities = ? block_global
                    .capabilities,
                    "Refresh collation sessions is skipped: collator does not support mc block version or capabilities",
                );
            }
        }
        Ok(())
    }
    /// Returns top processed to anchor id if delayed state was processed
    async fn notify_to_mempool_and_process_delayed_mc_state_update(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<MempoolAnchorId>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(notify_to_mempool_and_process_delayed_mc_state_update)
            ),
            file!(),
            2172u32,
        );
        let block_id = block_id;
        let mut delayed_mc_data = None;
        {
            let mut guard = self.delayed_mc_state_update.lock();
            if let Some(mc_data) = guard.clone() && mc_data.block_id <= *block_id {
                if mc_data.block_id == *block_id {
                    delayed_mc_data = Some(mc_data);
                }
                *guard = None;
            }
        }
        if let Some(mc_data) = delayed_mc_data {
            {
                __guard.end_section(2190u32);
                let __result = self
                    .notify_mc_state_update_to_mempool(mc_data.clone())
                    .await;
                __guard.start_section(2190u32);
                __result
            }?;
            {
                __guard.end_section(2197u32);
                let __result = self
                    .process_mc_state_update(
                        mc_data.clone(),
                        ProcessMcStateUpdateMode::StartCollation {
                            reset_collators: false,
                        },
                    )
                    .await;
                __guard.start_section(2197u32);
                __result
            }?;
            Ok(Some(mc_data.top_processed_to_anchor))
        } else {
            Ok(None)
        }
    }
    async fn notify_mc_state_update_to_mempool(
        &self,
        mc_data: Arc<McData>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(notify_mc_state_update_to_mempool)),
            file!(),
            2205u32,
        );
        let mc_data = mc_data;
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, block_id = % mc_data.block_id
            .as_short_id(), "will notify master state update to mempool",
        );
        let prev_validator_set = self
            .validator_set_cache
            .get_prev_validator_set(&mc_data.config)?;
        let current_validator_set = self
            .validator_set_cache
            .get_current_validator_set(&mc_data.config)?;
        let next_validator_set = self
            .validator_set_cache
            .get_next_validator_set(&mc_data.config)?;
        let cx = StateUpdateContext {
            mc_block_id: mc_data.block_id,
            mc_block_chain_time: mc_data.gen_chain_time,
            top_processed_to_anchor_id: mc_data.top_processed_to_anchor,
            consensus_info: mc_data.consensus_info,
            shuffle_validators: mc_data
                .config
                .get_collation_config()?
                .shuffle_mc_validators,
            consensus_config: mc_data.config.get_consensus_config()?,
            prev_validator_set,
            current_validator_set,
            next_validator_set,
        };
        {
            __guard.end_section(2233u32);
            let __result = self.mpool_adapter.handle_mc_state_update(cx).await;
            __guard.start_section(2233u32);
            __result
        }
    }
    /// Get shards and validator set info from the master state,
    /// then start missing collation sessions, finish outdated, resume actual.
    /// Start/stop/resume collators and validators.
    #[tracing::instrument(skip_all)]
    async fn refresh_collation_sessions(
        &self,
        mc_data: Arc<McData>,
        reset_collators: bool,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(refresh_collation_sessions)),
            file!(),
            2244u32,
        );
        let mc_data = mc_data;
        let reset_collators = reset_collators;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER,
            "Start refresh collation sessions by mc state ({})...", mc_data.block_id
            .as_short_id(),
        );
        let _histogram = HistogramGuard::begin(
            "tycho_collator_refresh_collation_sessions_time",
        );
        if !self
            .check_should_process_and_update_last_processed_mc_block(&mc_data.block_id)
        {
            {
                __guard.end_section(2256u32);
                return Ok(());
            };
        }
        tracing::trace!(
            target : tracing_targets::COLLATION_MANAGER, "mc_data: {:?}", mc_data
        );
        let mut new_shards_info = FastHashMap::default();
        new_shards_info.insert(ShardIdent::MASTERCHAIN, vec![mc_data.block_id]);
        for (shard_id, descr) in mc_data.shards.iter() {
            __guard.checkpoint(2264u32);
            let top_block_id = descr.get_block_id(*shard_id);
            new_shards_info.insert(*shard_id, vec![top_block_id]);
        }
        let active_shards_ids: Vec<_> = self
            .active_collation_sessions
            .read()
            .keys()
            .cloned()
            .collect();
        let new_shards_ids: Vec<&ShardIdent> = new_shards_info.keys().collect();
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER,
            "Detecting split/merge actions to move from current shards {:?} to new shards {:?}...",
            active_shards_ids.as_slice(), new_shards_ids
        );
        let split_merge_actions = calc_split_merge_actions(
            &active_shards_ids,
            new_shards_ids,
        )?;
        if !split_merge_actions.is_empty() {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER,
                "Detected split/merge actions: {:?}", split_merge_actions,
            );
        }
        let current_session_seqno = mc_data.validator_info.catchain_seqno;
        let full_validators_set = mc_data.config.get_current_validator_set()?;
        tracing::trace!(
            target : tracing_targets::COLLATION_MANAGER,
            "full_validators_set: since={}, until={}, main={}, total_weight={}, list={:?}",
            full_validators_set.utime_since, full_validators_set.utime_until,
            full_validators_set.main, full_validators_set.total_weight,
            DebugIter(full_validators_set.list.iter().map(| i | i.public_key)),
        );
        let collation_config = mc_data.config.get_collation_config()?;
        let mut subset_cache = FastHashMap::new();
        let mut get_validator_subset = |shard_id| match subset_cache.entry(shard_id) {
            hash_map::Entry::Occupied(entry) => {
                let (
                    subset,
                    hash_short,
                ): &(Arc<FastHashMap<[u8; 32], ValidatorDescription>>, u32) = entry
                    .get();
                Result::<_>::Ok((subset.clone(), *hash_short))
            }
            hash_map::Entry::Vacant(entry) => {
                let (subset, hash_short) = full_validators_set
                    .compute_mc_subset(
                        current_session_seqno,
                        collation_config.shuffle_mc_validators,
                    )
                    .ok_or_else(|| {
                        anyhow!(
                            "Error calculating subset of validators for session (shard_id = {}, seqno = {})",
                            ShardIdent::MASTERCHAIN, current_session_seqno,
                        )
                    })?;
                let subset: FastHashMap<_, _> = subset
                    .into_iter()
                    .map(|vldr| (vldr.public_key.into(), vldr))
                    .collect();
                let subset = Arc::new(subset);
                entry.insert((subset.clone(), hash_short));
                Ok((subset, hash_short))
            }
        };
        let mut sessions_to_keep = Vec::new();
        let mut sessions_to_start = Vec::new();
        let mut to_finish_sessions = Vec::new();
        let mut to_stop_collators = Vec::new();
        {
            let mut active_collation_sessions_guard = self
                .active_collation_sessions
                .write();
            let mut missed_shards_ids: FastHashSet<_> = active_shards_ids
                .into_iter()
                .collect();
            for (shard_id, block_ids) in new_shards_info {
                __guard.checkpoint(2342u32);
                missed_shards_ids.remove(&shard_id);
                let (subset, hash_short) = get_validator_subset(shard_id)?;
                let local_pubkey = find_us_in_collators_set(&self.keypair, &subset);
                if local_pubkey.is_none() {
                    tracing::debug!(
                        target : tracing_targets::COLLATION_MANAGER, public_key = % self
                        .keypair.public_key,
                        "Current node was not authorized to collate shard {}", shard_id,
                    );
                    metrics::gauge!("tycho_node_in_current_vset").set(0);
                } else {
                    metrics::gauge!("tycho_node_in_current_vset").set(1);
                }
                match active_collation_sessions_guard.entry(shard_id) {
                    hash_map::Entry::Occupied(entry) => {
                        let existing_session_info = entry.get().clone();
                        if local_pubkey.is_some() {
                            if existing_session_info.collators().short_hash == hash_short
                                && existing_session_info.seqno() == current_session_seqno
                            {
                                sessions_to_keep
                                    .push((shard_id, existing_session_info, block_ids));
                            } else {
                                to_finish_sessions.push(entry.remove());
                                sessions_to_start.push((shard_id, block_ids));
                            }
                        } else {
                            to_finish_sessions.push(entry.remove());
                            if let Some((_, collator)) = self
                                .active_collators
                                .remove(&shard_id)
                            {
                                to_stop_collators.push((existing_session_info, collator));
                            }
                        }
                    }
                    hash_map::Entry::Vacant(_) => {
                        if local_pubkey.is_some() {
                            sessions_to_start.push((shard_id, block_ids));
                        }
                    }
                }
            }
            for shard_id in missed_shards_ids {
                __guard.checkpoint(2391u32);
                if let Some(existing_session_info) = active_collation_sessions_guard
                    .remove(&shard_id)
                {
                    to_finish_sessions.push(existing_session_info.clone());
                    if let Some((_, collator)) = self.active_collators.remove(&shard_id)
                    {
                        to_stop_collators.push((existing_session_info, collator));
                    }
                }
            }
        }
        if !sessions_to_start.is_empty() {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER,
                "Will start new collation sessions: {:?}", DebugIter(sessions_to_start
                .iter().map(| (s, _) | (s, current_session_seqno))),
            );
        }
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            __guard.checkpoint(2413u32);
            let (subset, hash_short) = get_validator_subset(shard_id)?;
            let new_session_info = Arc::new(
                CollationSessionInfo::new(
                    shard_id,
                    current_session_seqno,
                    ValidatorSubsetInfo {
                        validators: subset.values().cloned().collect(),
                        short_hash: hash_short,
                    },
                    Some(self.keypair.clone()),
                ),
            );
            tracing::debug!(
                target : tracing_targets::COLLATION_MANAGER, "new_session_info: {:?}",
                new_session_info,
            );
            let next_block_id_short = calc_next_block_id_short(&prev_blocks_ids);
            match self.active_collators.entry(shard_id) {
                DashMapEntry::Occupied(_) => {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        "Active collator exists for collation session {:?}. Will resume it",
                        new_session_info.id(),
                    );
                    sessions_to_keep
                        .push((shard_id, new_session_info.clone(), prev_blocks_ids));
                }
                DashMapEntry::Vacant(entry) => {
                    tracing::info!(
                        target : tracing_targets::COLLATION_MANAGER,
                        "There is no active collator for collation session {:?}. Will start it",
                        new_session_info.id(),
                    );
                    let cancel_collation_notify = Arc::new(Notify::new());
                    match {
                        __guard.end_section(2466u32);
                        let __result = self
                            .collator_factory
                            .start(CollatorContext {
                                mq_adapter: self.mq_adapter.clone(),
                                mpool_adapter: self.mpool_adapter.clone(),
                                state_node_adapter: self.state_node_adapter.clone(),
                                config: self.config.clone(),
                                collation_session: new_session_info.clone(),
                                listener: self.dispatcher.clone(),
                                shard_id,
                                prev_blocks_ids,
                                mc_data: mc_data.clone(),
                                mempool_config_override: self
                                    .mempool_config_override
                                    .clone(),
                                cancel_collation: cancel_collation_notify.clone(),
                            })
                            .await;
                        __guard.start_section(2466u32);
                        __result
                    } {
                        Err(err) => {
                            tracing::error!(
                                target : tracing_targets::COLLATION_MANAGER, session_id = ?
                                new_session_info.id(), ? err, "error starting collator"
                            );
                        }
                        Ok(collator) => {
                            entry
                                .insert(ActiveCollator {
                                    collator: Arc::new(collator),
                                    state: CollatorState::Active,
                                    cancel_collation: cancel_collation_notify,
                                });
                        }
                    }
                }
            }
            if shard_id.is_masterchain() {
                self.validator
                    .add_session(AddSession {
                        shard_ident: shard_id,
                        session_id: new_session_info.get_validation_session_id(),
                        start_block_seqno: next_block_id_short.seqno,
                        validators: &new_session_info.collators().validators,
                    })?;
            }
            self.active_collation_sessions.write().insert(shard_id, new_session_info);
        }
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}", DebugIter(sessions_to_keep
            .iter().map(| (_, s, _) | s.id())),
        );
        for (shard_id, new_session_info, prev_blocks_ids) in sessions_to_keep {
            __guard.checkpoint(2508u32);
            let collator = {
                let Some(mut active_collator) = self.active_collators.get_mut(&shard_id)
                else {
                    bail!(
                        "Collator for shard should exist for active session {:?}",
                        new_session_info.id(),
                    )
                };
                active_collator.state = CollatorState::Active;
                active_collator.collator.clone()
            };
            tracing::debug!(
                target : tracing_targets::COLLATION_MANAGER,
                "Resuming collation attempts in shard session {:?}", new_session_info
                .id(),
            );
            {
                __guard.end_section(2532u32);
                let __result = collator
                    .enqueue_resume_collation(
                        mc_data.clone(),
                        reset_collators,
                        new_session_info,
                        prev_blocks_ids,
                    )
                    .await;
                __guard.start_section(2532u32);
                __result
            }?;
        }
        if !to_finish_sessions.is_empty() {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER,
                "Will finish outdated collation sessions: {:?}",
                DebugIter(to_finish_sessions.iter().map(| s | s.id())),
            );
        }
        for session_info in to_finish_sessions {
            __guard.checkpoint(2544u32);
            self.collation_sessions_to_finish
                .insert(session_info.id(), session_info.clone());
            self.finish_collation_session(session_info)?;
        }
        if !to_stop_collators.is_empty() {
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                DebugIter(to_stop_collators.iter().map(| (s, _) | s.id())),
            );
        }
        for (session_info, active_collator) in to_stop_collators {
            __guard.checkpoint(2559u32);
            let collator = active_collator.collator.clone();
            self.collators_to_stop.insert(session_info.id(), active_collator);
            {
                __guard.end_section(2563u32);
                let __result = collator.enqueue_stop().await;
                __guard.start_section(2563u32);
                __result
            }?;
        }
        Ok(())
    }
    /// Execute collation session finalization routines
    pub fn finish_collation_session(
        &self,
        collation_session: Arc<CollationSessionInfo>,
    ) -> Result<()> {
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER,
            "finish_collation_session: {:?}", collation_session.id(),
        );
        self.collation_sessions_to_finish.remove(&collation_session.id());
        Ok(())
    }
    /// Remove stopped collator from cache
    pub fn handle_collator_stopped(
        &self,
        collation_session_id: CollationSessionId,
    ) -> Result<()> {
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, "handle_collator_stopped: {:?}",
            collation_session_id,
        );
        self.collators_to_stop.remove(&collation_session_id);
        Ok(())
    }
    fn set_collator_state<F>(&self, shard_id: &ShardIdent, f: F) -> Option<CollatorState>
    where
        F: Fn(&mut ActiveCollator<Arc<CF::Collator>>),
    {
        match self.active_collators.get_mut(shard_id) {
            Some(mut active_collator) => {
                f(&mut active_collator);
                Some(active_collator.state)
            }
            None => None,
        }
    }
    fn set_active_sync_info(
        &self,
        target_mc_block_seqno: BlockSeqno,
    ) -> Result<CancellationToken> {
        let mut guard = self.collation_sync_state.lock();
        if let Some(active_sync) = &guard.active_sync_to_applied {
            bail!(
                "previous sync_to_applied_mc_block should be finished \
                before: previous seqno={}, target seqno={}",
                active_sync.target_mc_block_seqno, target_mc_block_seqno,
            )
        }
        let cancelled = CancellationToken::new();
        guard.active_sync_to_applied = Some(ActiveSync {
            target_mc_block_seqno,
            cancelled: cancelled.clone(),
        });
        Ok(cancelled)
    }
    fn clean_active_sync_info(&self) {
        let mut guard = self.collation_sync_state.lock();
        guard.active_sync_to_applied = None;
    }
    fn update_last_received_mc_block_seqno(&self, received_block_id: &BlockId) {
        if !received_block_id.is_masterchain() {
            return;
        }
        let mut guard = self.collation_sync_state.lock();
        guard.last_received_mc_block_seqno = Some(received_block_id.seqno);
    }
    fn get_last_received_mc_block_seqno(&self) -> Option<BlockSeqno> {
        let guard = self.collation_sync_state.lock();
        guard.last_received_mc_block_seqno
    }
    fn update_last_synced_to_mc_block_id(&self, mc_block_id: BlockId) {
        let mut guard = self.collation_sync_state.lock();
        guard.last_synced_to_mc_block_id = Some(mc_block_id);
    }
    fn get_last_synced_to_mc_block_id(&self) -> Option<BlockId> {
        let guard = self.collation_sync_state.lock();
        guard.last_synced_to_mc_block_id
    }
    /// Returns `true` if active sync was cancelled
    fn finish_active_sync_to_applied(&self, received_block_id: &BlockId) -> bool {
        if !received_block_id.is_masterchain() {
            return false;
        }
        let guard = self.collation_sync_state.lock();
        if let Some(active_sync_info) = &guard.active_sync_to_applied {
            if received_block_id
                .seqno
                .saturating_sub(active_sync_info.target_mc_block_seqno)
                >= self.config.min_mc_block_delta_from_bc_to_sync
            {
                tracing::debug!(
                    target : tracing_targets::COLLATION_MANAGER, prev_target_block_id = %
                    BlockIdShort { shard : ShardIdent::MASTERCHAIN, seqno :
                    active_sync_info.target_mc_block_seqno, },
                    "cancel sync: will force finish previous sync \
                    to applied master block if not started to process state update",
                );
                active_sync_info.cancelled.cancel();
                return true;
            } else {
                tracing::trace!(
                    target : tracing_targets::COLLATION_MANAGER, prev_target_block_id = %
                    BlockIdShort { shard : ShardIdent::MASTERCHAIN, seqno :
                    active_sync_info.target_mc_block_seqno, },
                    "cancel sync: will not force finish previous sync \
                    to applied master block, because new block is not far ahead",
                );
            }
        } else {
            tracing::trace!(
                target : tracing_targets::COLLATION_MANAGER,
                "cancel sync: route_handle_block_from_bc: no active sync to cancel",
            );
        }
        false
    }
    /// Set master block latest chain time to calc next interval for master block collation.
    /// Prune all cached chain times for all shards upto current
    fn renew_mc_block_latest_chain_time(
        guard: &mut CollationSyncState,
        chain_time: u64,
    ) {
        if guard.mc_block_latest_chain_time < chain_time {
            guard.mc_block_latest_chain_time = chain_time;
        }
        for (_, collation_state) in guard.states.iter_mut() {
            collation_state.last_imported_anchor_events.retain(|it| it.ct > chain_time);
        }
    }
    /// Reset collation status from `WaitForMasterStatus` to `AttemptsInProgress` for every shard.
    ///
    /// Use this method before resuming collation after sync to avoid ambiguous situations.
    /// If any shard has collation status `WaitForMasterStatus` and sync was executed,
    /// when master collation check was finished first then it will enqueue one more resume for shard,
    /// so we will have two parallel collations for shard that will cause panic further.
    fn reset_collation_sync_status(guard: &mut CollationSyncState) {
        for (_, collation_state) in guard.states.iter_mut() {
            if collation_state.status == CollationStatus::WaitForMasterStatus {
                collation_state.status = CollationStatus::AttemptsInProgress;
            }
        }
    }
    /// 1. Store collation status for current shard
    /// 2. Detect the next step
    #[tracing::instrument(skip_all)]
    fn detect_next_collation_step(
        guard: &mut CollationSyncState,
        active_shards: Vec<ShardIdent>,
        shard_id: ShardIdent,
        ctx: DetectNextCollationStepContext,
    ) -> NextCollationStep {
        let DetectNextCollationStepContext {
            last_imported_anchor_ct,
            force_mc_block,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            collated_block_info,
        } = ctx;
        let mc_block_max_interval_ms = if mc_block_max_interval_ms == 0 {
            mc_block_min_interval_ms
        } else {
            mc_block_max_interval_ms
        };
        let _histogram = HistogramGuard::begin("detect_next_collation_step_time");
        assert!(
            active_shards.contains(& shard_id),
            "active_shards must include current shard"
        );
        let mc_block_latest_chain_time = guard.mc_block_latest_chain_time;
        let mc_collation_state_exist = shard_id.is_masterchain()
            || guard
                .states
                .get(&ShardIdent::MASTERCHAIN)
                .is_some_and(|state| !state.last_imported_anchor_events.is_empty());
        if shard_id.is_masterchain()
            && matches!(force_mc_block, ForceMasterCollation::ByUnprocessedMessages)
        {
            guard.mc_collation_forced_for_all = true;
        }
        if matches!(
            force_mc_block, ForceMasterCollation::NoPendingMessagesAfterShardBlocks
        ) {
            guard.mc_forced_by_no_pending_msgs_on_ct = Some(last_imported_anchor_ct);
        }
        let hard_forced_for_all = guard.mc_collation_forced_for_all;
        let mc_forced_by_no_pending_msgs_on_ct = guard
            .mc_forced_by_no_pending_msgs_on_ct;
        let forced_in_current_shard = force_mc_block.is_forced();
        tracing::trace!(
            target : tracing_targets::COLLATION_MANAGER, shard_id =? shard_id,
            last_imported_anchor_ct, forced_in_current_shard, ? collated_block_info,
            "anchor event appended"
        );
        let current_collation_state = guard.states.entry(shard_id).or_default();
        current_collation_state
            .last_imported_anchor_events
            .push(ImportedAnchorEvent {
                ct: last_imported_anchor_ct,
                mc_forced: forced_in_current_shard,
                collated_block_info,
            });
        #[derive(Debug, Clone)]
        struct ShardFact {
            shard_id: ShardIdent,
            status: CollationStatus,
            first_ct: Option<u64>,
            mc_forced_ct: Option<u64>,
            min_ct: Option<u64>,
            max_ct: Option<u64>,
            has_shard_block_with_externals: bool,
        }
        impl ShardFact {
            fn with_status(shard_id: ShardIdent, status: CollationStatus) -> Self {
                Self {
                    shard_id,
                    status,
                    first_ct: None,
                    mc_forced_ct: None,
                    min_ct: None,
                    max_ct: None,
                    has_shard_block_with_externals: false,
                }
            }
            fn calc(
                shard_id: ShardIdent,
                state: &CollationState,
                mc_ct: u64,
                min_interval_ms: u64,
                max_interval_ms: u64,
            ) -> Self {
                let mut fact = Self::with_status(shard_id, state.status);
                let mut last_known_collated_block_info: Option<CollatedBlockInfo> = None;
                for s in &state.last_imported_anchor_events {
                    let mut is_first_shard_block_with_externals = false;
                    if let Some(curr_b_info) = s.collated_block_info {
                        let is_first_after_prev_master = match &last_known_collated_block_info {
                            Some(
                                last_b_info,
                            ) if last_b_info.prev_mc_block_seqno
                                < curr_b_info.prev_mc_block_seqno => true,
                            None => true,
                            _ => false,
                        };
                        if !shard_id.is_masterchain() {
                            if is_first_after_prev_master {
                                fact.has_shard_block_with_externals = false;
                            }
                            is_first_shard_block_with_externals = curr_b_info
                                .has_processed_externals
                                && !fact.has_shard_block_with_externals;
                            if is_first_shard_block_with_externals {
                                fact.has_shard_block_with_externals = true;
                            }
                        }
                        last_known_collated_block_info = Some(curr_b_info);
                    }
                    if fact.first_ct.is_none() {
                        fact.first_ct = Some(s.ct);
                    }
                    if s.mc_forced && fact.mc_forced_ct.is_none() {
                        fact.mc_forced_ct = Some(s.ct);
                    }
                    if (fact.min_ct.is_none() || is_first_shard_block_with_externals)
                        && s.ct.saturating_sub(mc_ct) >= min_interval_ms
                    {
                        fact.min_ct = Some(s.ct);
                    }
                    if fact.max_ct.is_none()
                        && s.ct.saturating_sub(mc_ct) >= max_interval_ms
                    {
                        fact.max_ct = Some(s.ct);
                    }
                }
                fact
            }
        }
        let mut facts = Vec::with_capacity(active_shards.len());
        for sid in &active_shards {
            if let Some(st) = guard.states.get(sid) {
                let f = ShardFact::calc(
                    *sid,
                    st,
                    mc_block_latest_chain_time,
                    mc_block_min_interval_ms,
                    mc_block_max_interval_ms,
                );
                facts.push(f);
            } else {
                facts
                    .push(
                        ShardFact::with_status(*sid, CollationStatus::AttemptsInProgress),
                    );
            }
        }
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, mc_block_latest_chain_time,
            mc_block_min_interval_ms, mc_block_max_interval_ms, hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct, ? facts, "calculated shard facts"
        );
        let any_has_shard_block_with_externals = facts
            .iter()
            .any(|f| f.has_shard_block_with_externals);
        fn choose_candidate(
            curr_sid: &ShardIdent,
            f: &ShardFact,
            mc_forced_by_shard_on_ct: Option<u64>,
            any_has_shard_block_with_externals: bool,
            hard_forced_for_all: bool,
        ) -> Option<u64> {
            let ready_to_detect_next_step = f.shard_id == *curr_sid
                || matches!(
                    f.status, CollationStatus::ReadyToCollateMaster |
                    CollationStatus::WaitForMasterStatus |
                    CollationStatus::WaitForShardStatus
                );
            f.mc_forced_ct
                .or(if hard_forced_for_all { f.first_ct } else { None })
                .or(
                    if any_has_shard_block_with_externals
                        || mc_forced_by_shard_on_ct.is_some()
                    {
                        if ready_to_detect_next_step {
                            f.min_ct
                                .map(|min_ct| {
                                    mc_forced_by_shard_on_ct
                                        .map_or(min_ct, |forced_ct| min_ct.max(forced_ct))
                                })
                        } else {
                            None
                        }
                    } else {
                        f.max_ct
                    },
                )
        }
        let mut should_collate_by_current_shard = false;
        let candidates: Vec<_> = facts
            .iter()
            .map(|f| {
                let ct = choose_candidate(
                    &shard_id,
                    f,
                    mc_forced_by_no_pending_msgs_on_ct,
                    any_has_shard_block_with_externals,
                    hard_forced_for_all,
                );
                if f.shard_id == shard_id && ct.is_some() {
                    should_collate_by_current_shard = true;
                }
                ct
            })
            .collect();
        let should_collate_by_every_shard = !candidates.is_empty()
            && candidates.iter().all(|c| c.is_some());
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, shard_id =? shard_id, ?
            candidates, should_collate_by_current_shard, should_collate_by_every_shard,
            any_has_shard_block_with_externals, hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct, mc_block_min_interval_ms,
            mc_block_max_interval_ms, "candidates collected"
        );
        if !should_collate_by_every_shard {
            let mut shards_to_resume_attempts = vec![];
            let all_other_shards_ready_to_collate = guard
                .states
                .iter()
                .all(|(sid, s)| {
                    *sid == shard_id || s.status == CollationStatus::ReadyToCollateMaster
                });
            let current_collation_state = guard.states.entry(shard_id).or_default();
            if !should_collate_by_current_shard {
                if !mc_collation_state_exist {
                    current_collation_state.status = CollationStatus::WaitForMasterStatus;
                } else if shard_id.is_masterchain() && !all_other_shards_ready_to_collate
                {
                    current_collation_state.status = CollationStatus::WaitForShardStatus;
                } else {
                    current_collation_state.status = CollationStatus::AttemptsInProgress;
                    shards_to_resume_attempts.push(shard_id);
                }
            } else {
                current_collation_state.status = CollationStatus::ReadyToCollateMaster;
            };
            for (sid, collation_state) in guard.states.iter_mut() {
                if (shard_id.is_masterchain()
                    && collation_state.status == CollationStatus::WaitForMasterStatus)
                    || (!shard_id.is_masterchain()
                        && collation_state.status == CollationStatus::WaitForShardStatus)
                {
                    collation_state.status = CollationStatus::AttemptsInProgress;
                    shards_to_resume_attempts.push(*sid);
                }
            }
            let res = if !shards_to_resume_attempts.is_empty() {
                NextCollationStep::ResumeAttemptsIn(shards_to_resume_attempts)
            } else if shard_id.is_masterchain() {
                NextCollationStep::WaitForShardStatus
            } else {
                NextCollationStep::WaitForMasterStatus
            };
            tracing::info!(
                target : tracing_targets::COLLATION_MANAGER, shard_id =? shard_id, ?
                candidates, should_collate_by_current_shard,
                should_collate_by_every_shard, any_has_shard_block_with_externals,
                hard_forced_for_all, mc_forced_by_no_pending_msgs_on_ct,
                mc_collation_state_exist, mc_block_min_interval_ms,
                mc_block_max_interval_ms, decision =? res, "step decision"
            );
            return res;
        }
        let next_mc_block_chain_time = candidates.into_iter().flatten().max().unwrap();
        for (sid, st) in guard.states.iter_mut() {
            st.status = CollationStatus::AttemptsInProgress;
            if sid.is_masterchain() {
                st.last_imported_anchor_events
                    .retain(|s| s.ct <= next_mc_block_chain_time);
            }
        }
        Self::renew_mc_block_latest_chain_time(guard, next_mc_block_chain_time);
        guard.mc_collation_forced_for_all = false;
        guard.mc_forced_by_no_pending_msgs_on_ct = None;
        let res = NextCollationStep::CollateMaster(next_mc_block_chain_time);
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER, shard_id =? shard_id,
            should_collate_by_current_shard, should_collate_by_every_shard,
            any_has_shard_block_with_externals, hard_forced_for_all,
            mc_forced_by_no_pending_msgs_on_ct, mc_collation_state_exist,
            mc_block_min_interval_ms, mc_block_max_interval_ms, decision =? res,
            "step decision"
        );
        res
    }
    /// Enqueue master block collation task. Will determine top shard blocks for this collation
    async fn enqueue_mc_block_collation(
        &self,
        next_mc_block_id_short: BlockIdShort,
        next_mc_block_chain_time: u64,
        _trigger_block_id_opt: Option<BlockId>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_mc_block_collation)),
            file!(),
            3143u32,
        );
        let next_mc_block_id_short = next_mc_block_id_short;
        let next_mc_block_chain_time = next_mc_block_chain_time;
        let _trigger_block_id_opt = _trigger_block_id_opt;
        let _histogram = HistogramGuard::begin(
            "tycho_collator_enqueue_mc_block_collation_time",
        );
        let Some(mc_collator) = self
            .active_collators
            .get(&ShardIdent::MASTERCHAIN)
            .map(|r| r.collator.clone()) else {
            bail!("Masterchain collator is not started yet!");
        };
        let top_shard_blocks_info = self
            .blocks_cache
            .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)?;
        {
            __guard.end_section(3167u32);
            let __result = mc_collator
                .enqueue_do_collate(top_shard_blocks_info, next_mc_block_chain_time)
                .await;
            __guard.start_section(3167u32);
            __result
        }?;
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER,
            "Master block collation enqueued: (block_id={} ct={})",
            next_mc_block_id_short, next_mc_block_chain_time,
        );
        Ok(())
    }
    async fn enqueue_try_collate(&self, shard_id: &ShardIdent) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_try_collate)),
            file!(),
            3178u32,
        );
        let shard_id = shard_id;
        let Some(collator) = self
            .active_collators
            .get(shard_id)
            .map(|r| r.collator.clone()) else {
            tracing::warn!(
                target : tracing_targets::COLLATION_MANAGER,
                "Node does not collate blocks for shard {}", shard_id,
            );
            {
                __guard.end_section(3190u32);
                return Ok(());
            };
        };
        {
            __guard.end_section(3193u32);
            let __result = collator.enqueue_try_collate().await;
            __guard.start_section(3193u32);
            __result
        }?;
        tracing::info!(
            target : tracing_targets::COLLATION_MANAGER,
            "Enqueued next attempt to collate block for shard {}", shard_id,
        );
        Ok(())
    }
    /// Process validated block
    /// 1. Process invalid block (currently, just panic)
    /// 2. Update block in cache with validation info
    /// 2. Execute processing for master or shard block
    #[tracing::instrument(skip_all, fields(block_id = %block_id.as_short_id()))]
    pub async fn handle_validated_master_block(
        &self,
        block_id: BlockId,
        status: ValidationStatus,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_validated_master_block)),
            file!(),
            3213u32,
        );
        let block_id = block_id;
        let status = status;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, is_complete = matches!(& status,
            ValidationStatus::Complete(_)),
            "Start processing block validation result...",
        );
        let _histogram = HistogramGuard::begin(
            "tycho_collator_handle_validated_master_block_time",
        );
        let updated = self
            .blocks_cache
            .store_master_block_validation_result(&block_id, status);
        if !updated {
            {
                __guard.end_section(3227u32);
                return Ok(());
            };
        }
        {
            __guard.end_section(3230u32);
            let __result = self.ready_to_sync.notified().await;
            __guard.start_section(3230u32);
            __result
        };
        {
            __guard.end_section(3233u32);
            let __result = self.commit_valid_master_block(&block_id).await;
            __guard.start_section(3233u32);
            __result
        }?;
        self.ready_to_sync.notify_one();
        Ok(())
    }
    /// Try to commit validated and valid master block
    /// if it was not already committed before
    /// 1. Check if master block is valid
    /// 2. Extract master block subgraph with shard blocks
    /// 3. Send to sync
    /// 4. Commit queue diff
    /// 5. Clean up from cache
    /// 6. Process delayed master state update if exists
    /// 7. Notify top processed anchor to mempool if block commited by received from bc
    async fn commit_valid_master_block(&self, mc_block_id: &BlockId) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(commit_valid_master_block)),
            file!(),
            3249u32,
        );
        let mc_block_id = mc_block_id;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER,
            "Start to commit validated and valid master block ({})...", mc_block_id
            .as_short_id(),
        );
        scopeguard::defer!(self.blocks_cache.gc_prev_blocks());
        let mut top_processed_to_anchor_to_notify = {
            __guard.end_section(3262u32);
            let __result = self
                .notify_to_mempool_and_process_delayed_mc_state_update(mc_block_id)
                .await;
            __guard.start_section(3262u32);
            __result
        }?;
        let histogram = HistogramGuard::begin(
            "tycho_collator_commit_valid_master_block_time",
        );
        let histogram_extract = HistogramGuard::begin(
            "tycho_collator_extract_master_block_subgraph_time",
        );
        let mut extract_elapsed = Default::default();
        let mut sync_elapsed = Default::default();
        match self.blocks_cache.extract_mc_block_subgraph_for_sync(mc_block_id) {
            McBlockSubgraphExtract::Extracted(subgraph) => {
                extract_elapsed = histogram_extract.finish();
                let partitions = subgraph.get_partitions();
                let McBlockSubgraph { master_block, shard_blocks } = subgraph;
                let to_blocks_keys = master_block.get_top_blocks_keys()?;
                self.blocks_cache.set_gc_to_boundary(&to_blocks_keys);
                if matches!(
                    & master_block.data, BlockCacheEntryData::Collated {
                    received_after_collation : false, .. }
                ) {
                    let histogram = HistogramGuard::begin(
                        "tycho_collator_send_blocks_to_sync_time",
                    );
                    self.send_block_to_sync(master_block.data)?;
                    for shard_block in shard_blocks {
                        __guard.checkpoint(3302u32);
                        self.send_block_to_sync(shard_block.data)?;
                    }
                    sync_elapsed = histogram.finish();
                    tracing::debug!(
                        target : tracing_targets::COLLATION_MANAGER, total = sync_elapsed
                        .as_millis(), "send_blocks_to_sync timings",
                    );
                    top_processed_to_anchor_to_notify = None;
                } else {
                    top_processed_to_anchor_to_notify = master_block
                        .top_processed_to_anchor;
                }
                let _histogram = HistogramGuard::begin(
                    "tycho_collator_send_blocks_to_sync_commit_diffs_time",
                );
                Self::commit_block_queue_diff(
                    self.mq_adapter.clone(),
                    &master_block.block_id,
                    &master_block.top_shard_blocks_info,
                    &partitions,
                )?;
            }
            McBlockSubgraphExtract::AlreadyExtracted => {
                tracing::debug!(
                    target : tracing_targets::COLLATION_MANAGER,
                    "Master block subgraph is already extracted and cleaned up from cache ({}). Do nothing",
                    mc_block_id.as_short_id(),
                );
            }
        }
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER, total = histogram.finish()
            .as_millis(), extract_subgraph = extract_elapsed.as_millis(), sync =
            sync_elapsed.as_millis(), "commit_valid_master_block timings",
        );
        if let Some(top_processed_to_anchor) = top_processed_to_anchor_to_notify {
            {
                __guard.end_section(3356u32);
                let __result = self
                    .notify_top_processed_to_anchor_to_mempool(
                        mc_block_id.seqno,
                        top_processed_to_anchor,
                    )
                    .await;
                __guard.start_section(3356u32);
                __result
            }?;
        }
        Ok(())
    }
    fn send_block_to_sync(&self, data: BlockCacheEntryData) -> Result<()> {
        let candidate_stuff = match data {
            BlockCacheEntryData::Collated {
                candidate_stuff,
                status,
                received_after_collation: false,
                ..
            } if status != CandidateStatus::Synced => candidate_stuff,
            _ => return Ok(()),
        };
        let block_id = *candidate_stuff.candidate.block.id();
        self.state_node_adapter.accept_block(candidate_stuff.into_block_for_sync())?;
        tracing::debug!(
            target : tracing_targets::COLLATION_MANAGER,
            "Block was successfully sent to sync ({})", block_id,
        );
        Ok(())
    }
    /// Collect top blocks seqno from all shards by master block id.
    /// Master block seqno included
    async fn get_top_blocks_seqno(
        mc_block_id: &BlockId,
        blocks_cache: &BlocksCache,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
    ) -> Result<FastHashMap<ShardIdent, BlockSeqno>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_top_blocks_seqno)),
            file!(),
            3390u32,
        );
        let mc_block_id = mc_block_id;
        let blocks_cache = blocks_cache;
        let state_node_adapter = state_node_adapter;
        let mut result = FastHashMap::default();
        result.insert(mc_block_id.shard, mc_block_id.seqno);
        let top_shard_blocks = blocks_cache
            .get_top_shard_blocks(mc_block_id.as_short_id());
        match top_shard_blocks {
            None => {
                let state = match {
                    __guard.end_section(3401u32);
                    let __result = state_node_adapter
                        .load_state(mc_block_id.seqno, mc_block_id)
                        .await;
                    __guard.start_section(3401u32);
                    __result
                } {
                    Err(err) => {
                        match err.downcast_ref::<ShardStateStorageError>() {
                            Some(ShardStateStorageError::NotFound(_)) => {
                                tracing::warn!(
                                    target : tracing_targets::COLLATION_MANAGER, % mc_block_id,
                                    "master state not found in get_top_blocks_seqno",
                                );
                                {
                                    __guard.end_section(3409u32);
                                    return Ok(FastHashMap::default());
                                };
                            }
                            _ => Err(err),
                        }
                    }
                    state => state,
                }?;
                for item in state.shards()?.iter() {
                    __guard.checkpoint(3415u32);
                    let (shard_id, shard_descr) = item?;
                    result.insert(shard_id, shard_descr.get_block_id(shard_id).seqno);
                }
            }
            Some(top_shard_blocks) => {
                result.extend(top_shard_blocks);
            }
        }
        Ok(result)
    }
}
#[derive(Debug)]
enum ProcessMcStateUpdateMode {
    StartCollation { reset_collators: bool },
    SkipCollation,
}
#[derive(Default)]
struct RestoreQueueResult {
    last_mc_state: Option<ShardStateStuff>,
    synced_to_blocks_keys: Vec<BlockCacheKey>,
    applied_diffs_ids: FastHashSet<BlockId>,
}
trait GlobalCapabilitiesExt {
    /// Checks whether this capabilities set is fully
    /// included into `other` (comparing raw bits to include unnamed variants).
    fn is_subset_of(&self, other: GlobalCapabilities) -> bool;
}
impl GlobalCapabilitiesExt for GlobalCapabilities {
    fn is_subset_of(&self, other: GlobalCapabilities) -> bool {
        let this = self.into_inner();
        let other = other.into_inner();
        this & !other == 0
    }
}
#[derive(Debug)]
struct DetectNextCollationStepContext {
    pub last_imported_anchor_ct: u64,
    pub force_mc_block: ForceMasterCollation,
    pub mc_block_min_interval_ms: u64,
    pub mc_block_max_interval_ms: u64,
    pub collated_block_info: Option<CollatedBlockInfo>,
}
impl DetectNextCollationStepContext {
    pub fn new(
        last_imported_anchor_ct: u64,
        force_mc_block: ForceMasterCollation,
        mc_block_min_interval_ms: u64,
        mc_block_max_interval_ms: u64,
        collated_block_info: Option<CollatedBlockInfo>,
    ) -> Self {
        Self {
            last_imported_anchor_ct,
            force_mc_block,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            collated_block_info,
        }
    }
}
