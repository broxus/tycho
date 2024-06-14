use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_util::FastHashMap;

use self::types::{
    BlockCacheKey, BlockCandidateContainer, BlockCandidateToSend, BlocksCache,
    McBlockSubgraphToSend, SendSyncStatus,
};
use self::utils::{build_block_stuff_for_sync, find_us_in_collators_set};
use crate::collator::{Collator, CollatorContext, CollatorEventListener, CollatorFactory};
use crate::mempool::{MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolEventListener};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener};
use crate::types::{
    BlockCandidate, BlockCollationResult, CollationConfig, CollationSessionId,
    CollationSessionInfo, OnValidatedBlockEvent, ProofFunds, TopBlockDescription,
};
use crate::utils::async_queued_dispatcher::{
    AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
};
use crate::utils::schedule_async_action;
use crate::utils::shard::calc_split_merge_actions;
use crate::validator::{Validator, ValidatorContext, ValidatorEventListener, ValidatorFactory};
use crate::{method_to_async_task_closure, tracing_targets};

mod types;
mod utils;

pub struct RunningCollationManager<CF, V>
where
    CF: CollatorFactory,
{
    dispatcher: AsyncQueuedDispatcher<CollationManager<CF, V>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter>,
}

impl<CF: CollatorFactory, V> RunningCollationManager<CF, V> {
    pub fn dispatcher(&self) -> &AsyncQueuedDispatcher<CollationManager<CF, V>> {
        &self.dispatcher
    }

    pub fn state_node_adapter(&self) -> &Arc<dyn StateNodeAdapter> {
        &self.state_node_adapter
    }

    pub fn mpool_adapter(&self) -> &Arc<dyn MempoolAdapter> {
        &self.mpool_adapter
    }

    pub fn mq_adapter(&self) -> &Arc<dyn MessageQueueAdapter> {
        &self.mq_adapter
    }
}

pub struct CollationManager<CF, V>
where
    CF: CollatorFactory,
{
    keypair: Arc<KeyPair>,
    config: Arc<CollationConfig>,

    dispatcher: Arc<AsyncQueuedDispatcher<Self>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter>,

    collator_factory: CF,
    validator: Arc<V>,

    active_collation_sessions: FastHashMap<ShardIdent, Arc<CollationSessionInfo>>,
    collation_sessions_to_finish: FastHashMap<CollationSessionId, Arc<CollationSessionInfo>>,
    active_collators: FastHashMap<ShardIdent, CF::Collator>,
    collators_to_stop: FastHashMap<CollationSessionId, CF::Collator>,

    state_tracker: MinRefMcStateTracker,

    blocks_cache: BlocksCache,

    last_processed_mc_block_id: Option<BlockId>,
    /// id of last master block collated by ourselves
    last_collated_mc_block_id: Option<BlockId>,
    /// chain time of last collated master block or received from bc
    last_collated_mc_block_chain_time: u64,
    /// chain time for next master block to be collated
    next_mc_block_chain_time: u64,

    last_collated_chain_times_by_shards: FastHashMap<ShardIdent, Vec<(u64, bool)>>,

    #[cfg(any(test, feature = "test"))]
    test_validators_keypairs: Vec<Arc<KeyPair>>,
}

#[async_trait]
impl<CF, V> MempoolEventListener for AsyncQueuedDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_new_anchor_from_mempool,
            anchor
        ))
        .await
    }
}

#[async_trait]
impl<CF, V> StateNodeEventListener for AsyncQueuedDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_block_accepted(&self, _block_id: &BlockId) -> Result<()> {
        // TODO: remove accepted block from cache
        // STUB: do nothing, currently we remove block from cache when it sent to state node
        Ok(())
    }

    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()> {
        // TODO: should store block info from blockchain if it was not already collated
        //      and validated by ourself. Will use this info for faster validation further:
        //      will consider that just collated block is already validated if it have the
        //      same root hash and file hash
        if state.block_id().is_masterchain() {
            let mc_block_id = *state.block_id();
            self.enqueue_task(method_to_async_task_closure!(
                process_mc_block_from_bc,
                mc_block_id
            ))
            .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<CF, V> CollatorEventListener for AsyncQueuedDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_skipped_anchor(
        &self,
        next_block_id_short: BlockIdShort,
        anchor: Arc<MempoolAnchor>,
        force_mc_block: bool,
    ) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_skipped_anchor,
            next_block_id_short,
            anchor,
            force_mc_block
        ))
        .await
    }

    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_collated_block_candidate,
            collation_result
        ))
        .await
    }

    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_collator_stopped,
            stop_key
        ))
        .await
    }
}

#[async_trait]
impl<CF, V> ValidatorEventListener for AsyncQueuedDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_block_validated(
        &self,
        block_id: BlockId,
        event: OnValidatedBlockEvent,
    ) -> Result<()> {
        self.enqueue_task(method_to_async_task_closure!(
            process_validated_block,
            block_id,
            event
        ))
        .await
    }
}

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    #[allow(clippy::too_many_arguments)]
    pub fn start<STF, MPF, VF>(
        keypair: Arc<KeyPair>,
        config: CollationConfig,
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        state_node_adapter_factory: STF,
        mpool_adapter_factory: MPF,
        validator_factory: VF,
        collator_factory: CF,
        #[cfg(any(test, feature = "test"))] test_validators_keypairs: Vec<Arc<KeyPair>>,
    ) -> RunningCollationManager<CF, V>
    where
        STF: StateNodeAdapterFactory,
        MPF: MempoolAdapterFactory,
        VF: ValidatorFactory<Validator = V>,
    {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let arc_dispatcher = Arc::new(dispatcher.clone());

        // create state node adapter
        let state_node_adapter =
            Arc::new(state_node_adapter_factory.create(arc_dispatcher.clone()));

        // create mempool adapter
        let mpool_adapter = mpool_adapter_factory.create(arc_dispatcher.clone());

        // create validator and start its tasks queue
        let validator = validator_factory.create(ValidatorContext {
            listeners: vec![arc_dispatcher.clone()],
            state_node_adapter: state_node_adapter.clone(),
            keypair: keypair.clone(),
        });

        let validator = Arc::new(validator);

        let processor = Self {
            keypair,
            config: Arc::new(config),
            dispatcher: arc_dispatcher.clone(),
            state_node_adapter: state_node_adapter.clone(),
            mpool_adapter: mpool_adapter.clone(),
            mq_adapter: mq_adapter.clone(),
            collator_factory,
            validator,
            state_tracker: MinRefMcStateTracker::default(),
            active_collation_sessions: FastHashMap::default(),
            collation_sessions_to_finish: FastHashMap::default(),
            active_collators: FastHashMap::default(),
            collators_to_stop: FastHashMap::default(),

            blocks_cache: BlocksCache::default(),

            last_processed_mc_block_id: None,
            last_collated_mc_block_id: None,
            last_collated_mc_block_chain_time: 0,
            next_mc_block_chain_time: 0,

            last_collated_chain_times_by_shards: FastHashMap::default(),

            #[cfg(any(test, feature = "test"))]
            test_validators_keypairs,
        };
        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks queue dispatcher started");

        // start other async processes

        // TODO: Move outside of the start method?

        // schedule to check collation sessions and force refresh
        // if not initialized (when started from zerostate)
        schedule_async_action(
            tokio::time::Duration::from_secs(10),
            || async move {
                arc_dispatcher
                    .enqueue_task(method_to_async_task_closure!(
                        check_refresh_collation_sessions,
                    ))
                    .await
            },
            "CollationProcessor::check_refresh_collation_sessions()".into(),
        );

        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Action scheduled in 10s: CollationProcessor::check_refresh_collation_sessions()");
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation manager created");

        RunningCollationManager {
            dispatcher,
            state_node_adapter,
            mpool_adapter,
            mq_adapter,
        }
    }

    /// Return last collated master block id
    fn last_collated_mc_block_id(&self) -> Option<&BlockId> {
        self.last_collated_mc_block_id.as_ref()
    }
    fn set_last_collated_mc_block_id(&mut self, block_id: BlockId) {
        self.last_collated_mc_block_id = Some(block_id);
    }

    fn last_processed_mc_block_id(&self) -> Option<&BlockId> {
        self.last_processed_mc_block_id.as_ref()
    }
    fn set_last_processed_mc_block_id(&mut self, block_id: BlockId) {
        self.last_processed_mc_block_id = Some(block_id);
    }

    /// Prunes the cache of last collated chain times
    fn process_last_collated_mc_block_chain_time(&mut self, chain_time: u64) {
        for (_, last_collated_chain_times) in self.last_collated_chain_times_by_shards.iter_mut() {
            last_collated_chain_times.retain(|(ct, _)| ct > &chain_time);
        }
    }

    /// (TODO) Check sync status between mempool and blockchain state
    /// and pause collation when we are far behind other nodesб
    /// jusct sync blcoks from blockchain
    pub async fn process_new_anchor_from_mempool(
        &mut self,
        _anchor: Arc<MempoolAnchor>,
    ) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        Ok(())
    }

    /// Process new master block from blockchain:
    /// 1. Load block state
    /// 2. Notify mempool about new master block
    /// 3. Enqueue collation sessions refresh task
    pub async fn process_mc_block_from_bc(&self, mc_block_id: BlockId) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Processing master block ({})", mc_block_id.as_short_id(),
        );

        // check if we should skip this master block from the blockchain
        // because it is not far ahead of last collated by ourselves
        if !self.check_should_process_mc_block_from_bc(&mc_block_id) {
            return Ok(());
        }

        // request mc state for this master block
        // TODO: should await state and schedule processing in async task
        let mc_state = self.state_node_adapter.load_state(&mc_block_id).await?;

        // when state received execute master block processing routines
        let mpool_adapter = self.mpool_adapter.clone();
        let dispatcher = self.dispatcher.clone();

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Processing requested mc state for block ({})...",
            mc_state.block_id().as_short_id()
        );
        Self::notify_mempool_about_mc_block(mpool_adapter, mc_state.clone()).await?;

        dispatcher
            .enqueue_task(method_to_async_task_closure!(
                refresh_collation_sessions,
                mc_state
            ))
            .await?;

        Ok(())
    }

    /// 1. Skip if it is equal or not far ahead from last collated by ourselves
    /// 2. Skip if it was already processed before
    /// 3. Skip if waiting for the first own master block collation less then `max_mc_block_delta_from_bc_to_await_own`
    fn check_should_process_mc_block_from_bc(&self, mc_block_id: &BlockId) -> bool {
        let last_collated_mc_block_id_opt = self.last_collated_mc_block_id();
        let last_processed_mc_block_id_opt = self.last_processed_mc_block_id();
        if last_collated_mc_block_id_opt.is_some() {
            // when we have last own collated master block then skip if incoming one is equal
            // or not far ahead from last own collated
            // then will wait for next own collated master block
            let (seqno_delta, is_equal) =
                Self::compare_mc_block_with(mc_block_id, self.last_collated_mc_block_id());
            if is_equal || seqno_delta <= self.config.max_mc_block_delta_from_bc_to_await_own {
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    r#"Should NOT process mc block ({}) from bc: should wait for next own collated:
                    is_equal = {}, seqno_delta = {}, max_mc_block_delta_from_bc_to_await_own = {}"#,
                    mc_block_id.as_short_id(), is_equal, seqno_delta,
                    self.config.max_mc_block_delta_from_bc_to_await_own,
                );

                return false;
            } else if !is_equal {
                // STUB: skip processing master block from bc even if it is far away from own last collated
                //      because the logic for updating collators in this case is not implemented yet
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "STUB: skip processing mc block ({}) from bc anyway if we are collating by ourselves",
                    mc_block_id.as_short_id(),
                );
                return false;
            }
        } else {
            // When we do not have last own collated master block then check last processed master block
            // If None then we should process incoming master block anyway to init collation process
            // If we have already processed some previous incoming master block and colaltions were started
            // then we should wait for the first own collated master block
            // but not more then `max_mc_block_delta_from_bc_to_await_own`
            if last_processed_mc_block_id_opt.is_some() {
                let (seqno_delta, is_equal) =
                    Self::compare_mc_block_with(mc_block_id, last_processed_mc_block_id_opt);
                let already_processed_before = is_equal || seqno_delta < 0;
                if already_processed_before {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Should NOT process mc block ({}) from bc: it was already processed before",
                        mc_block_id.as_short_id(),
                    );

                    return false;
                }
                let should_wait_for_next_own_collated = seqno_delta
                    <= self.config.max_mc_block_delta_from_bc_to_await_own
                    && self.active_collators.contains_key(&ShardIdent::MASTERCHAIN);
                if should_wait_for_next_own_collated {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        r#"Should NOT process mc block ({}) from bc: should wait for first own collated:
                        seqno_delta = {}, max_mc_block_delta_from_bc_to_await_own = {}"#,
                        mc_block_id.as_short_id(), seqno_delta,
                        self.config.max_mc_block_delta_from_bc_to_await_own,
                    );
                    return false;
                }
            }
        }
        true
    }

    /// Returns: (seqno delta from other, true - if equal)
    fn compare_mc_block_with(
        mc_block_id: &BlockId,
        other_mc_block_id_opt: Option<&BlockId>,
    ) -> (i32, bool) {
        // TODO: consider block shard?
        let (seqno_delta, is_equal) = match other_mc_block_id_opt {
            None => (0, false),
            Some(other_mc_block_id) => (
                mc_block_id.seqno as i32 - other_mc_block_id.seqno as i32,
                mc_block_id == other_mc_block_id,
            ),
        };
        if seqno_delta < 0 || is_equal {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "mc block ({}) is NOT AHEAD of other ({:?}): is_equal = {}, seqno_delta = {}",
                mc_block_id.as_short_id(),
                other_mc_block_id_opt.map(|b| b.as_short_id()),
                is_equal, seqno_delta,
            );
        }
        (seqno_delta, is_equal)
    }

    /// * TRUE - provided `mc_block_id` is before or equal to last processed
    /// * FALSE - provided `mc_block_id` is ahead of last processed
    fn _check_if_mc_block_not_ahead_last_processed(&self, mc_block_id: &BlockId) -> bool {
        // TODO: consider block shard?
        let last_processed_mc_block_id_opt = self.last_processed_mc_block_id();
        let is_not_ahead = matches!(last_processed_mc_block_id_opt, Some(last_processed_mc_block_id)
            if mc_block_id.seqno < last_processed_mc_block_id.seqno
                || mc_block_id == last_processed_mc_block_id);
        if is_not_ahead {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "mc block ({}) is NOT AHEAD of last processed ({:?})",
                mc_block_id.as_short_id(),
                self.last_processed_mc_block_id().map(|b| b.as_short_id()),
            );
        }
        is_not_ahead
    }

    /// Check if collation sessions initialized and try to force refresh them if they not.
    /// This needed when start from zerostate. State node adapter will be initialized after
    /// zerostate load and won't fire `[StateNodeListener::on_mc_block_event()]` for the 1 block.
    /// Also when whole network was restarted then nobody will produce next master block and we need
    /// to start collation sessions based on the actual state
    pub async fn check_refresh_collation_sessions(&self) -> Result<()> {
        // the sessions list is not enpty so the collation process was already started from
        // actual state or incoming master block from blockchain
        if !self.active_collation_sessions.is_empty() {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Collation sessions already activated");
            return Ok(());
        }

        // here we will wait for last applied master block then process it
        // TODO: otherwise we can just request to resend last applied master block via `[StateNodeListener::on_mc_block_event()]`
        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Requesting last applied mc block to activate collation sessions...",
        );
        let last_mc_block_id = self
            .state_node_adapter
            .load_last_applied_mc_block_id()
            .await?;
        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Running processing last mc block ({}) to activate collation sessions...",
            last_mc_block_id.as_short_id(),
        );

        self.process_mc_block_from_bc(last_mc_block_id).await
    }

    /// Get shards info from the master state,
    /// then start missing sessions for these shards, or refresh existing.
    /// For each shard run collation process if current node is included in collators subset.
    #[tracing::instrument(skip_all, fields(mc_block_id = %mc_state.block_id().as_short_id()))]
    pub async fn refresh_collation_sessions(&mut self, mc_state: ShardStateStuff) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Trying to refresh collation sessions by mc state for block ({})...",
            mc_state.block_id().as_short_id(),
        );

        // TODO: Possibly we have already updated collation sessions for this master block,
        //      because we may have collated it by ourselves before receiving it from the blockchain
        //      or because we have received it from the blockchain before we collated it
        //
        //      It may be a situation when we have received new master block from the blockchain
        //      before we have collated it by ourselves, we can stop current block collations,
        //      update working state in active collators and then continue to collate.
        //      But this can produce a significant overhead for the little bit slower node
        //      because some 2/3f+1 nodes will always be little bit faster.
        //      So we should reset active collators only when master block from the blockchain is
        //      notably ahead of last collated by ourselves
        //
        //      So we will:
        //      1. Check if we should process master block from the blockchain in `process_mc_block_from_bc`
        //      2. Skip refreshing sessions if this master was processed by any chance

        // do not re-process this master block if it is lower then last processed or equal to it
        // but process a new version of block with the same seqno
        let processing_mc_block_id = *mc_state.block_id();
        let (seqno_delta, is_equal) =
            Self::compare_mc_block_with(&processing_mc_block_id, self.last_processed_mc_block_id());
        if seqno_delta < 0 || is_equal {
            return Ok(());
        }

        let mc_state_extra = mc_state.state_extra()?;
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER, "mc_state_extra: {:?}", mc_state_extra);

        // get new shards info from updated master state
        let mut new_shards_info = HashMap::new();
        new_shards_info.insert(ShardIdent::MASTERCHAIN, vec![processing_mc_block_id]);
        for shard in mc_state_extra.shards.iter() {
            let (shard_id, descr) = shard?;
            let top_block = BlockId {
                shard: shard_id,
                seqno: descr.seqno,
                root_hash: descr.root_hash,
                file_hash: descr.file_hash,
            };
            // TODO: consider split and merge
            new_shards_info.insert(shard_id, vec![top_block]);
        }

        // update shards in msgs queue
        let current_shards_ids = self.active_collation_sessions.keys().collect();
        let new_shards_ids: Vec<&ShardIdent> = new_shards_info.keys().collect();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Detecting split/merge actions to move from current shards {:?} to new shards {:?}...",
            current_shards_ids,
            new_shards_ids
        );

        let split_merge_actions = calc_split_merge_actions(current_shards_ids, new_shards_ids)?;
        if !split_merge_actions.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Detected split/merge actions: {:?}",
                split_merge_actions,
            );
            self.mq_adapter.update_shards(split_merge_actions).await?;
        }

        // find out the actual collation session seqno from master state
        let new_session_seqno = mc_state_extra.validator_info.catchain_seqno;

        // we need full validators set to define the subset for each session and to check if current node should collate
        let full_validators_set = mc_state.config_params()?.get_current_validator_set()?;
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "full_validators_set {:?}", full_validators_set);

        // compare with active sessions and detect new sessions to start and outdated sessions to finish
        let mut sessions_to_keep = HashMap::new();
        let mut sessions_to_start = vec![];
        let mut to_finish_sessions = HashMap::new();
        let mut to_stop_collators = HashMap::new();
        for shard_info in new_shards_info {
            if let Some(existing_session) =
                self.active_collation_sessions.remove_entry(&shard_info.0)
            {
                if existing_session.1.seqno() >= new_session_seqno {
                    sessions_to_keep.insert(shard_info.0, existing_session.1);
                } else {
                    sessions_to_start.push(shard_info);
                    to_finish_sessions
                        .insert((existing_session.0, new_session_seqno), existing_session.1);
                }
            } else {
                sessions_to_start.push(shard_info);
            }
        }

        // if we still have some active sessions that do not match with new shards
        // then we need to finish them and stop their collators
        for current_active_session in self.active_collation_sessions.drain() {
            to_finish_sessions.insert(
                (current_active_session.0, new_session_seqno),
                current_active_session.1,
            );
            if let Some(collator) = self.active_collators.remove(&current_active_session.0) {
                to_stop_collators.insert((current_active_session.0, new_session_seqno), collator);
            }
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}",
            sessions_to_keep.keys(),
        );
        if !sessions_to_start.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will start new collation sessions: {:?}",
                sessions_to_start.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            );
        }

        let cc_config = mc_state_extra.config.get_catchain_config()?;

        // update master state in existing collators and resume collation
        for (shard_id, session_info) in sessions_to_keep {
            self.active_collation_sessions
                .insert(shard_id, session_info);

            // if there is no active collator then current node does not collate this shard
            // so we do not need to do anything
            let Some(collator) = self.active_collators.get(&shard_id) else {
                continue;
            };

            if shard_id.is_masterchain() {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Resuming collation attempts in master chain {}",
                    shard_id,
                );
                collator.equeue_try_collate().await?;
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Updating McData in active collator for shard {} and resuming collation in it...",
                    shard_id,
                );
                collator
                    .equeue_update_mc_data_and_try_collate(mc_state.clone())
                    .await?;
            }
        }

        // we may have sessions to finish, collators to stop, and sessions to start
        // additionally we may have some active collators
        // for each new session we should check if current node should collate,
        // then stop collators if should not, otherwise start missing collators
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            let (subset, hash_short) = full_validators_set
                .compute_subset(shard_id, &cc_config, new_session_seqno)
                .ok_or(anyhow!(
                    "Error calculating subset of collators for the session (shard_id = {}, seqno = {})",
                    shard_id,
                    new_session_seqno,
                ))?;

            // TEST: override with test subset with test keypairs defined on test run
            #[cfg(feature = "test")]
            let subset = if self.test_validators_keypairs.is_empty() {
                subset
            } else {
                let mut test_subset = vec![];
                for (i, keypair) in self.test_validators_keypairs.iter().enumerate() {
                    let val_descr = &subset[i];
                    test_subset.push(everscale_types::models::ValidatorDescription {
                        public_key: keypair.public_key.to_bytes().into(),
                        adnl_addr: val_descr.adnl_addr,
                        weight: val_descr.weight,
                        mc_seqno_since: val_descr.mc_seqno_since,
                        prev_total_weight: val_descr.prev_total_weight,
                    });
                }
                test_subset
            };
            #[cfg(feature = "test")]
            tracing::warn!(
                target: tracing_targets::COLLATION_MANAGER,
                "FOR TEST: overrided subset of validators to collate shard {}: {:?}",
                shard_id,
                subset,
            );

            let local_pubkey_opt = find_us_in_collators_set(&self.keypair, &subset);

            let new_session_info = Arc::new(CollationSessionInfo::new(
                shard_id.workchain(),
                new_session_seqno,
                ValidatorSubsetInfo {
                    validators: subset,
                    short_hash: hash_short,
                },
                Some(self.keypair.clone()),
            ));

            if let Some(_local_pubkey) = local_pubkey_opt {
                if let Entry::Vacant(entry) = self.active_collators.entry(shard_id) {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "There is no active collator for collation session {}. Will start it",
                        shard_id,
                    );
                    let collator = self
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
                            mc_state: mc_state.clone(),
                            state_tracker: self.state_tracker.clone(),
                        })
                        .await;
                    entry.insert(collator);
                }

                // notify validator, it will start overlay initialization

                let session_seqno = new_session_info.seqno();

                self.validator
                    .add_session(
                        shard_id,
                        session_seqno,
                        new_session_info.collators().validators.as_slice(),
                    )
                    .await?;
            } else {
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Node was not athorized to collate shard {}",
                    shard_id,
                );
                if let Some(collator) = self.active_collators.remove(&shard_id) {
                    to_stop_collators.insert((shard_id, new_session_seqno), collator);
                }
            }

            // TODO: possibly do not need to store collation sessions if we do not collate in them
            self.active_collation_sessions
                .insert(shard_id, new_session_info);
        }

        if !to_finish_sessions.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will finish outdated collation sessions: {:?}",
                to_finish_sessions.keys(),
            );
        }

        // enqueue outdated sessions finish tasks
        for (finish_key, session_info) in to_finish_sessions {
            self.collation_sessions_to_finish
                .insert(finish_key, session_info.clone());
            self.dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    finish_collation_session,
                    session_info,
                    finish_key
                ))
                .await?;
        }

        if !to_stop_collators.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                to_stop_collators.keys(),
            );
        }

        // equeue dangling collators stop tasks
        for (stop_key, collator) in to_stop_collators {
            collator.equeue_stop(stop_key).await?;
            self.collators_to_stop.insert(stop_key, collator);
        }

        // store last processed master block id to avoid processing it again
        self.set_last_processed_mc_block_id(processing_mc_block_id);

        Ok(())

        // finally we will have initialized `active_collation_sessions` and `active_collators`
        // which run async block collations processes
    }

    /// Execute collation session finalization routines
    pub async fn finish_collation_session(
        &mut self,
        _session_info: Arc<CollationSessionInfo>,
        finish_key: CollationSessionId,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "finish_collation_session: {:?}", finish_key,
        );
        self.collation_sessions_to_finish.remove(&finish_key);
        Ok(())
    }

    /// Remove stopped collator from cache
    pub async fn process_collator_stopped(&mut self, stop_key: CollationSessionId) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "process_collator_stopped: {:?}", stop_key,
        );
        self.collators_to_stop.remove(&stop_key);
        Ok(())
    }

    /// Process collated block candidate
    /// 1. Store block in a structure that allow to append signatures
    /// 2. Schedule block validation
    /// 3. Check if the master block interval elapsed (according to chain time) and schedule collation
    /// 4. If master block then update last master block chain time
    /// 5. Notify mempool about new master block (it may perform gc or nodes rotation)
    /// 6. Execute master block processing routines like for the block from bc
    #[tracing::instrument(
        skip_all,
        fields(
            block_id = %collation_result.candidate.block_id.as_short_id(),
            ct = collation_result.candidate.chain_time,
        ),
    )]
    pub async fn process_collated_block_candidate(
        &mut self,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start processing block candidate",
        );

        // find session related to this block by shard
        let session_info = self
            .active_collation_sessions
            .get(&collation_result.candidate.block_id.shard)
            .ok_or(anyhow!(
                "There is no active collation session for the shard that block belongs to"
            ))?
            .clone();

        let candidate_chain_time = collation_result.candidate.chain_time;
        let candidate_id = collation_result.candidate.block_id;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Saving block candidate to cache...",
        );
        let new_state_stuff = collation_result.new_state_stuff;
        let new_mc_state = new_state_stuff.clone();
        self.store_candidate(collation_result.candidate)?;

        // send validation task to validator
        // we need to send session info with the collators list to the validator
        // to understand whom we must ask for signatures
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Enqueueing block candidate validation...",
        );

        let _handle = self
            .validator
            .clone()
            .spawn_validate(candidate_id, session_info.seqno())
            .await;

        // when candidate is master
        if candidate_id.shard.is_masterchain() {
            // store last collated master block id and chain time
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Candidate is a master block",
            );
            self.process_last_collated_mc_block_chain_time(candidate_chain_time);
            self.set_last_collated_mc_block_id(candidate_id);

            // collate next master block right now if there are pending internals
            if collation_result.has_pending_internals {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "there are pending internals in master queue",
                );
                self.enqueue_mc_block_collation(candidate_chain_time, Some(candidate_id))
                    .await?;
            } else {
                // otherwise execute master block processing routines
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Candidate (id: {}, chain_time: {}): will notify mempool and equeue collation sessions refresh",
                    candidate_id.as_short_id(),
                    candidate_chain_time,
                );

                Self::notify_mempool_about_mc_block(
                    self.mpool_adapter.clone(),
                    new_mc_state.clone(),
                )
                .await?;

                self.dispatcher
                    .enqueue_task(method_to_async_task_closure!(
                        refresh_collation_sessions,
                        new_mc_state
                    ))
                    .await?;
            }
        } else {
            // when candidate is shard
            // chek if master block interval elapsed and it needs to collate new master block
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will check if master block interval elapsed by chain time from candidate (id: {}, chain_time: {})",
                candidate_id.as_short_id(),
                candidate_chain_time,
            );
            self.collate_mc_block_by_interval_or_continue_shard_collation(
                candidate_id.shard,
                candidate_chain_time,
                false,
                Some(candidate_id),
            )
            .await?;
        }

        Ok(())
    }

    /// Send master state related to master block to mempool (it may perform gc or nodes rotation)
    async fn notify_mempool_about_mc_block(
        mpool_adapter: Arc<dyn MempoolAdapter>,
        mc_state: ShardStateStuff,
    ) -> Result<()> {
        // TODO: in current implementation CollationProcessor should not notify mempool
        //      about one master block more than once, but better to handle repeated request here or at mempool
        mpool_adapter
            .enqueue_process_new_mc_block_state(mc_state)
            .await
    }

    #[tracing::instrument(skip_all, fields(block_id = %next_block_id_short, ct = anchor.chain_time(), force_mc_block))]
    async fn process_skipped_anchor(
        &mut self,
        next_block_id_short: BlockIdShort,
        anchor: Arc<MempoolAnchor>,
        force_mc_block: bool,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Will check if should collate next master block",
        );
        self.collate_mc_block_by_interval_or_continue_shard_collation(
            next_block_id_short.shard,
            anchor.chain_time(),
            force_mc_block,
            None,
        )
        .await
    }

    /// 1. Check if should collate master
    /// 2. If true, schedule master block collation
    /// 3. If no, schedule next collation attempt in current shard
    async fn collate_mc_block_by_interval_or_continue_shard_collation(
        &mut self,
        shard_id: ShardIdent,
        chain_time: u64,
        force_mc_block: bool,
        trigger_shard_block_id_opt: Option<BlockId>,
    ) -> Result<()> {
        // if should collate master block due to current shard chain state
        // then stop current shard collation
        let (should_collate_mc_block, next_mc_block_chain_time_opt) = self
            .update_last_collated_chain_time_and_check_should_collate_mc_block(
                shard_id,
                chain_time,
                force_mc_block,
            );
        if should_collate_mc_block {
            // and if chain time elapsed master block interval in every shard
            // then run master block collation
            if let Some(next_mc_block_chain_time) = next_mc_block_chain_time_opt {
                self.enqueue_mc_block_collation(
                    next_mc_block_chain_time,
                    trigger_shard_block_id_opt,
                )
                .await?;
            }
        } else {
            // if should not collate master block
            // then continue shard collation by running `try_collate` that will
            // - in masterchain: just import next anchor
            // - in workchain: run next attempt to collate shard block
            self.enqueue_try_collate(&shard_id).await?;
        }
        Ok(())
    }

    /// 1. Store last collated chain time by shards
    /// 2. Check if should collate master block in current shard (by interval or "force" flag)
    /// 3. And if should in every shard, then return chain time for the next master block collation
    ///
    /// Returns: (`should_collate_mc_block`, `next_mc_block_chain_time`)
    /// * `next_mc_block_chain_time.is_some()` when master collation condition met in every shard
    fn update_last_collated_chain_time_and_check_should_collate_mc_block(
        &mut self,
        shard_id: ShardIdent,
        chain_time: u64,
        force_mc_block: bool,
    ) -> (bool, Option<u64>) {
        // Idea is to store collated chain times and "force" flags for each shard.
        // Then we can collate master block if interval elapsed or have "force" flag in every shards.
        // We should take the max of first chain times that meet master block collation condition from each shard.

        let last_collated_chain_times = self
            .last_collated_chain_times_by_shards
            .entry(shard_id)
            .or_default();
        last_collated_chain_times.push((chain_time, force_mc_block));

        // check if should collate master in current shard
        let should_collate_mc_block = if force_mc_block {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Master collation forced in current shard {}",
                shard_id,
            );
            true
        } else {
            // check if master block interval elapsed in current shard
            let chain_time_elapsed = chain_time
                .checked_sub(self.next_mc_block_chain_time)
                .unwrap_or_default();
            let mc_block_interval_elapsed =
                chain_time_elapsed > self.config.mc_block_min_interval_ms;
            if mc_block_interval_elapsed {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block interval is {}ms, elapsed chain time {}ms exceeded the interval in current shard {}",
                    self.config.mc_block_min_interval_ms, chain_time_elapsed, shard_id,
                );
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Elapsed chain time {}ms has not elapsed master block interval {}ms in current shard \
                    - do not need to collate next master block",
                    chain_time_elapsed, self.config.mc_block_min_interval_ms,
                );
            }
            mc_block_interval_elapsed
        };

        if should_collate_mc_block {
            // if master block should be collated in every shard
            let mut first_elapsed_chain_times = vec![];
            let mut should_collate_in_every_shard = true;
            for active_shard in self.active_collation_sessions.keys() {
                if let Some(last_collated_chain_times) =
                    self.last_collated_chain_times_by_shards.get(active_shard)
                {
                    if let Some((chain_time_that_elapsed, _)) = last_collated_chain_times
                        .iter()
                        .find(|(chain_time, force)| {
                            *force
                                || (*chain_time)
                                    .checked_sub(self.next_mc_block_chain_time)
                                    .unwrap_or_default()
                                    > self.config.mc_block_min_interval_ms
                        })
                    {
                        first_elapsed_chain_times.push(*chain_time_that_elapsed);
                    } else {
                        // we have collated chain times in active shard
                        // but master block should not be collated according to them
                        should_collate_in_every_shard = false;
                        break;
                    }
                } else {
                    // we do not have collated chain times in active shard
                    // master block should not be collated
                    should_collate_in_every_shard = false;
                    break;
                }
            }
            if should_collate_in_every_shard {
                let max_first_chain_time_that_elapsed = first_elapsed_chain_times
                    .into_iter()
                    .max()
                    .expect("Here `first_elapsed_chain_times` vec should not be empty");
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block collation forced or interval {}ms elapsed in every shard - \
                    will collate next master block with chain time {}ms",
                    self.config.mc_block_min_interval_ms, max_first_chain_time_that_elapsed,
                );
                return (
                    should_collate_mc_block,
                    Some(max_first_chain_time_that_elapsed),
                );
            }
        }

        (should_collate_mc_block, None)
    }

    /// Find top shard blocks in cacche for the next master block collation
    fn detect_top_shard_blocks_info_for_mc_block(
        &self,
        _next_mc_block_chain_time: u64,
        _trigger_shard_block_id_opt: Option<BlockId>,
    ) -> Result<Vec<TopBlockDescription>> {
        // TODO: make real implementation (see comments in `enqueue_mc_block_collation``)

        let mut prev_shard_blocks_keys = self
            .blocks_cache
            .shards
            .iter()
            .filter_map(|(_, shard_cache)| shard_cache.last_key_value().map(|(_, v)| v.key()))
            .cloned()
            .collect::<VecDeque<BlockCacheKey>>();

        let mut result = HashMap::new();
        while let Some(prev_shard_block_key) = prev_shard_blocks_keys.pop_front() {
            let shard_cache = self
                .blocks_cache
                .shards
                .get(&prev_shard_block_key.shard)
                .ok_or_else(|| {
                    anyhow!("Shard block ({}) not found in cache!", prev_shard_block_key)
                })?;
            if let Some(shard_block_container) = shard_cache.get(&prev_shard_block_key.seqno) {
                // if shard block is not included in any master block
                if shard_block_container.containing_mc_block.is_none() {
                    let block = shard_block_container.get_block()?;
                    let value_flow = block.load_value_flow()?;
                    let block_extra = block.load_extra()?;
                    let creator = block_extra.created_by;
                    let fees_collected = value_flow.fees_collected.clone();
                    let funds_created = value_flow.created.clone();

                    let top_block = result.entry(shard_block_container.key().shard).or_insert(
                        TopBlockDescription {
                            block_id: *shard_block_container.block_id(),
                            block_info: block.load_info()?,
                            value_flow,
                            proof_funds: ProofFunds::default(),
                            creators: vec![],
                        },
                    );
                    top_block
                        .proof_funds
                        .fees_collected
                        .checked_add(&fees_collected)?;
                    top_block
                        .proof_funds
                        .funds_created
                        .checked_add(&funds_created)?;
                    top_block.creators.push(creator);

                    shard_block_container
                        .prev_blocks_keys()
                        .iter()
                        .cloned()
                        .for_each(|sub_prev| prev_shard_blocks_keys.push_back(sub_prev));
                }
            }
        }

        // STUB: when we work with only one shard we can just get the last shard block
        //      because collator manager will try run master block collation before
        //      before processing any next candidate from the shard collator
        //      because of dispatcher tasks queue

        Ok(result.into_values().collect())
    }

    /// (TODO) Enqueue master block collation task. Will determine top shard blocks for this collation
    async fn enqueue_mc_block_collation(
        &mut self,
        next_mc_block_chain_time: u64,
        trigger_block_id_opt: Option<BlockId>,
    ) -> Result<()> {
        // TODO: make real implementation

        // get masterchain collator if exists
        let Some(mc_collator) = self.active_collators.get(&ShardIdent::MASTERCHAIN) else {
            bail!("Masterchain collator is not started yet!");
        };

        // TODO: How to choose top shard blocks for master block collation when they are collated async and in parallel?
        //      We know the last anchor (An) used in shard (ShA) block that causes master block collation,
        //      so we search for block from other shard (ShB) that includes the same anchor (An).
        //      Or the first from previouses (An-x) that includes externals for that shard (ShB)
        //      if all next including required one ([An-x+1, An]) do not contain externals for shard (ShB).

        let top_shard_blocks_info = self.detect_top_shard_blocks_info_for_mc_block(
            next_mc_block_chain_time,
            trigger_block_id_opt,
        )?;

        // TODO: We should somehow collect externals for masterchain during the shard blocks collation
        //      or pull them directly when collating master

        self.next_mc_block_chain_time = next_mc_block_chain_time;

        mc_collator
            .equeue_do_collate(next_mc_block_chain_time, top_shard_blocks_info)
            .await?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Master block collation enqueued: (block_id={} ct={})",
            self.last_collated_mc_block_id()
                .map(|id| BlockIdShort { shard: id.shard, seqno: id.seqno + 1 }.to_string())
                .unwrap_or_default(),
            next_mc_block_chain_time,
        );

        Ok(())
    }

    async fn enqueue_try_collate(&self, shard_id: &ShardIdent) -> Result<()> {
        // get collator if exists
        let Some(collator) = self.active_collators.get(shard_id) else {
            tracing::warn!(
                target: tracing_targets::COLLATION_MANAGER,
                "Node does not collate blocks for shard {}",
                shard_id,
            );
            return Ok(());
        };

        collator.equeue_try_collate().await?;

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Equeued next attempt to collate block for shard {}",
            shard_id,
        );

        Ok(())
    }

    /// Process validated block
    /// 1. Process invalid block (currently, just panic)
    /// 2. Update block in cache with validation info
    /// 2. Execute processing for master or shard block
    pub async fn process_validated_block(
        &mut self,
        block_id: BlockId,
        validation_result: OnValidatedBlockEvent,
    ) -> Result<()> {
        let short_id = block_id.as_short_id();

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start processing block validation result (id: {}, is_valid: {})...",
            short_id,
            validation_result.is_valid(),
        );

        // execute required actions if block invalid
        if !validation_result.is_valid() {
            // TODO: implement more graceful reaction on invalid block
            panic!("Block has collected more than 1/3 invalid signatures! Unable to continue collation process!")
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Saving block validation result to cache (id: {})...",
            block_id.as_short_id(),
        );
        // update block in cache with signatures info
        self.store_block_validation_result(block_id, validation_result)?;

        // process valid block
        if block_id.shard.is_masterchain() {
            self.process_valid_master_block(&block_id).await?;
        } else {
            self.process_valid_shard_block(&block_id).await?;
        }

        Ok(())
    }

    /// Store block in a cache structure that allow to append signatures
    fn store_candidate(&mut self, candidate: BlockCandidate) -> Result<()> {
        // TODO: in future we may store to cache a block received from blockchain before,
        //      then it will exist in cache when we try to store collated candidate
        //      but the `root_hash` may differ, so we have to handle such a case

        let candidate_id = candidate.block_id;
        let block_container = BlockCandidateContainer::new(candidate);
        if candidate_id.shard.is_masterchain() {
            // traverse through including shard blocks and update their link to the containing master block
            let mut prev_shard_blocks_keys = block_container
                .top_shard_blocks_keys()
                .iter()
                .cloned()
                .collect::<VecDeque<_>>();
            while let Some(prev_shard_block_key) = prev_shard_blocks_keys.pop_front() {
                if let Some(shard_cache) = self
                    .blocks_cache
                    .shards
                    .get_mut(&prev_shard_block_key.shard)
                {
                    if let Some(shard_block) = shard_cache.get_mut(&prev_shard_block_key.seqno) {
                        if shard_block.containing_mc_block.is_none() {
                            shard_block.containing_mc_block = Some(*block_container.key());
                            shard_block
                                .prev_blocks_keys()
                                .iter()
                                .cloned()
                                .for_each(|sub_prev| prev_shard_blocks_keys.push_back(sub_prev));
                        }
                    }
                }
            }

            // save block to cache
            if let Some(_existing) = self
                .blocks_cache
                .master
                .insert(*block_container.key(), block_container)
            {
                bail!(
                    "Should not collate the same master block ({}) again!",
                    candidate_id,
                );
            }
        } else {
            let shard_cache = self
                .blocks_cache
                .shards
                .entry(block_container.key().shard)
                .or_default();
            if let Some(_existing) =
                shard_cache.insert(block_container.key().seqno, block_container)
            {
                bail!(
                    "Should not collate the same shard block ({}) again!",
                    candidate_id,
                );
            }
        }

        Ok(())
    }

    /// Find block candidate in cache, append signatures info and return updated
    fn store_block_validation_result(
        &mut self,
        block_id: BlockId,
        validation_result: OnValidatedBlockEvent,
    ) -> Result<&BlockCandidateContainer> {
        if let Some(block_container) = if block_id.is_masterchain() {
            self.blocks_cache.master.get_mut(&block_id.as_short_id())
        } else {
            self.blocks_cache
                .shards
                .get_mut(&block_id.shard)
                .and_then(|shard_cache| shard_cache.get_mut(&block_id.seqno))
        } {
            let (is_valid, already_synced, signatures) = match validation_result {
                OnValidatedBlockEvent::ValidByState => (true, true, Default::default()),
                OnValidatedBlockEvent::Valid(bs) => (true, false, bs.signatures),
                OnValidatedBlockEvent::Invalid => (false, false, Default::default()),
            };
            block_container.set_validation_result(is_valid, already_synced, signatures);

            Ok(block_container)
        } else {
            bail!("Block ({}) does not exist in cache!", block_id)
        }
    }

    /// Find shard block in cache and then get containing master block if link exists
    fn find_containing_mc_block(
        &self,
        shard_block_id: &BlockId,
    ) -> Option<&BlockCandidateContainer> {
        // TODO: handle when master block link exist but there is not block itself
        if let Some(mc_block_key) = self
            .blocks_cache
            .shards
            .get(&shard_block_id.shard)
            .and_then(|shard_cache| shard_cache.get(&shard_block_id.seqno))
            .and_then(|sbc| sbc.containing_mc_block)
        {
            self.blocks_cache.master.get(&mc_block_key)
        } else {
            None
        }
    }

    /// Find all shard blocks that form master block subgraph.
    /// Then extract and return them if all are valid
    fn extract_mc_block_subgraph_if_valid(
        &mut self,
        block_id: &BlockId,
    ) -> Result<Option<McBlockSubgraphToSend>> {
        // 1. Find current master block
        let mc_block_container = self
            .blocks_cache
            .master
            .get_mut(&block_id.as_short_id())
            .ok_or_else(|| {
                anyhow!(
                    "Master block ({}) not found in cache!",
                    block_id.as_short_id()
                )
            })?;
        if !mc_block_container.is_valid() {
            return Ok(None);
        }
        let mut subgraph = McBlockSubgraphToSend {
            mc_block: BlockCandidateToSend {
                entry: mc_block_container.extract_entry_for_sending()?,
                send_sync_status: SendSyncStatus::Sending,
            },
            shard_blocks: vec![],
        };

        // 3. By the top shard blocks info find shard blocks of current master block
        // 4. Recursively find prev shard blocks until the end or top shard blocks of prev master reached
        let mut prev_shard_blocks_keys = mc_block_container
            .top_shard_blocks_keys()
            .iter()
            .cloned()
            .collect::<VecDeque<_>>();
        while let Some(prev_shard_block_key) = prev_shard_blocks_keys.pop_front() {
            let shard_cache = self
                .blocks_cache
                .shards
                .get_mut(&prev_shard_block_key.shard)
                .ok_or_else(|| {
                    anyhow!("Shard block ({}) not found in cache!", prev_shard_block_key)
                })?;
            if let Some(shard_block_container) = shard_cache.get_mut(&prev_shard_block_key.seqno) {
                // if shard block included in current master block subgraph
                if matches!(shard_block_container.containing_mc_block, Some(containing_mc_block_key) if &containing_mc_block_key == mc_block_container.key())
                {
                    // 5. If master block and all shard blocks valid the extract them from entries and return
                    if !shard_block_container.is_valid() {
                        tracing::debug!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Not all blocks are valid in master block ({}) subgraph",
                            block_id.as_short_id(),
                        );
                        let mut blocks_to_restore = vec![subgraph.mc_block];
                        blocks_to_restore.append(&mut subgraph.shard_blocks);
                        self.restore_blocks_in_cache(blocks_to_restore)?;
                        return Ok(None);
                    }
                    subgraph.shard_blocks.push(BlockCandidateToSend {
                        entry: shard_block_container.extract_entry_for_sending()?,
                        send_sync_status: SendSyncStatus::Sending,
                    });
                    shard_block_container
                        .prev_blocks_keys()
                        .iter()
                        .cloned()
                        .for_each(|sub_prev| prev_shard_blocks_keys.push_back(sub_prev));
                }
            }
        }

        let _tracing_shard_blocks_descr = subgraph
            .shard_blocks
            .iter()
            .map(|sb| sb.entry.key.to_string())
            .collect::<Vec<_>>();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Extracted valid master block ({}) subgraph for sending to sync: {:?}",
            block_id.as_short_id(),
            _tracing_shard_blocks_descr.as_slice(),
        );

        Ok(Some(subgraph))
    }

    /// Remove block entries from cache and compact cache
    async fn cleanup_blocks_from_cache(&mut self, blocks_keys: Vec<BlockCacheKey>) -> Result<()> {
        let _tracing_blocks_descr = blocks_keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<_>>();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Cleaning up blocks from cache: {:?}",
            _tracing_blocks_descr.as_slice(),
        );
        for block_key in blocks_keys {
            if block_key.shard.is_masterchain() {
                self.blocks_cache.master.remove(&block_key);
            } else if let Some(shard_cache) = self.blocks_cache.shards.get_mut(&block_key.shard) {
                shard_cache.remove(&block_key.seqno);
            }
        }
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Blocks cleaned up from cache: {:?}",
            _tracing_blocks_descr.as_slice(),
        );
        Ok(())
    }

    async fn restore_blocks_in_cache_async(
        &mut self,
        blocks_to_restore: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        self.restore_blocks_in_cache(blocks_to_restore)
    }

    /// Find and restore block entries in cache updating sync statuses
    fn restore_blocks_in_cache(
        &mut self,
        blocks_to_restore: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        let _tracing_blocks_descr = blocks_to_restore
            .iter()
            .map(|b| b.entry.key.to_string())
            .collect::<Vec<_>>();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Restoring blocks in cache: {:?}",
            _tracing_blocks_descr.as_slice(),
        );
        for block in blocks_to_restore {
            // find block in cache
            let block_container = if block.entry.key.shard.is_masterchain() {
                self.blocks_cache
                    .master
                    .get_mut(&block.entry.key)
                    .ok_or_else(|| {
                        anyhow!("Master block ({}) not found in cache!", block.entry.key)
                    })?
            } else {
                self.blocks_cache
                    .shards
                    .get_mut(&block.entry.key.shard)
                    .and_then(|shard_cache| shard_cache.get_mut(&block.entry.key.seqno))
                    .ok_or_else(|| {
                        anyhow!("Shard block ({}) not found in cache!", block.entry.key)
                    })?
            };
            // restore entry and update sync status
            block_container.restore_entry(block.entry, block.send_sync_status)?;
        }
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Blocks restored in cache: {:?}",
            _tracing_blocks_descr.as_slice(),
        );
        Ok(())
    }

    /// Process validated and valid master block
    /// 1. Check if all included shard blocks validated, return if not
    /// 2. Send master and shard blocks to state node to sync
    async fn process_valid_master_block(&mut self, block_id: &BlockId) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start processing validated and valid master block ({})...",
            block_id.as_short_id(),
        );
        // extract master block with all shard blocks if valid, and process them
        if let Some(mc_block_subgraph) = self.extract_mc_block_subgraph_if_valid(block_id)? {
            let mut blocks_to_send = mc_block_subgraph.shard_blocks;
            blocks_to_send.reverse();
            blocks_to_send.push(mc_block_subgraph.mc_block);

            // spawn async task to send all shard and master blocks
            let join_handle = tokio::spawn({
                let dispatcher = (*self.dispatcher).clone();
                let mq_adapter = self.mq_adapter.clone();
                let state_node_adapter = self.state_node_adapter.clone();
                async move {
                    Self::send_blocks_to_sync(
                        dispatcher,
                        mq_adapter,
                        state_node_adapter,
                        blocks_to_send,
                    )
                    .await
                }
            });
            // TODO: make proper panic and error processing without waiting for spawned task
            join_handle.await??;
        } else {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Master block ({}) subgraph is not full valid. Will wait until all included shard blocks been validated",
                block_id.as_short_id(),
            );
        }
        Ok(())
    }

    /// 1. Try find master block info and execute [`CollationProcessor::process_valid_master_block`]
    async fn process_valid_shard_block(&mut self, block_id: &BlockId) -> Result<()> {
        if let Some(mc_block_container) = self.find_containing_mc_block(block_id) {
            let mc_block_id = *mc_block_container.block_id();
            if mc_block_container.is_valid() {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Found containing master block ({}) for just validated shard block ({}) in cache",
                    mc_block_id.as_short_id(),
                    block_id.as_short_id(),
                );
                self.process_valid_master_block(&mc_block_id).await?;
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Containing master block ({}) for just validated shard block ({}) is not validated yet. Will wait for master block validation",
                    mc_block_id.as_short_id(),
                    block_id.as_short_id(),
                );
            }
        } else {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "There is no containing master block for just validated shard block ({}) in cache. Will wait for master block collation",
                block_id.as_short_id(),
            );
        }
        Ok(())
    }

    /// 1. Send shard blocks and master to sync to state node
    /// 2. Commit msg queue diffs related to these shard and master blocks
    /// 3. Clean up sent blocks entries from cache
    /// 4. Return all blocks to cache if got error (separate task will try to resend further)
    /// 5. Return `Error` if it seems to be unrecoverable
    async fn send_blocks_to_sync(
        dispatcher: AsyncQueuedDispatcher<Self>,
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mut blocks_to_send: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        // TODO: it is better to send each block separately, but it will be more tricky to handle the correct cleanup

        let _tracing_blocks_to_send_descr = blocks_to_send
            .iter()
            .map(|b| b.entry.key.to_string())
            .collect::<Vec<_>>();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start sending blocks to sync: {:?}",
            _tracing_blocks_to_send_descr.as_slice(),
        );

        // skip already synced blocks that were validated by existing blocks in the state
        // send other blocks to sync
        let mut should_restore_blocks_in_cache = false;
        let mut sent_blocks = vec![];
        for block_to_send in blocks_to_send.iter_mut() {
            match block_to_send.send_sync_status {
                SendSyncStatus::Sent | SendSyncStatus::Synced => sent_blocks.push(block_to_send),
                _ => {
                    let block_for_sync = build_block_stuff_for_sync(&block_to_send.entry)?;
                    // TODO: handle different errors types
                    if let Err(err) = state_node_adapter.accept_block(block_for_sync).await {
                        tracing::warn!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Block ({}) sync: was not accepted. err: {:?}",
                            block_to_send.entry.candidate.block_id.as_short_id(),
                            err,
                        );
                        should_restore_blocks_in_cache = true;
                        break;
                    } else {
                        tracing::debug!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Block ({}) sync: was successfully sent to sync",
                            block_to_send.entry.candidate.block_id.as_short_id(),
                        );
                        block_to_send.send_sync_status = SendSyncStatus::Sent;
                        sent_blocks.push(block_to_send);
                    }
                }
            }
        }

        if !should_restore_blocks_in_cache {
            // commit queue diffs for each block
            for sent_block in sent_blocks.iter() {
                // TODO: handle if diff does not exist

                if let Err(err) = mq_adapter
                    .commit_diff(&sent_block.entry.candidate.block_id.as_short_id())
                    .await
                {
                    tracing::warn!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block ({}) sync: error committing message queue diff: {:?}",
                        sent_block.entry.candidate.block_id.as_short_id(),
                        err,
                    );
                    should_restore_blocks_in_cache = true;
                    break;
                } else {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block ({}) sync: message queue diff was committed",
                        sent_block.entry.candidate.block_id.as_short_id(),
                    );
                }
            }

            // do not clenup blocks if msg queue diffs commit was unsuccessful
            if !should_restore_blocks_in_cache {
                let sent_blocks_keys = sent_blocks.iter().map(|b| b.entry.key).collect::<Vec<_>>();
                let _tracing_sent_blocks_descr = sent_blocks_keys
                    .iter()
                    .map(|key| key.to_string())
                    .collect::<Vec<_>>();
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "All blocks were successfully sent to sync. Will cleanup them from cache: {:?}",
                    _tracing_sent_blocks_descr.as_slice(),
                );
                dispatcher
                    .enqueue_task(method_to_async_task_closure!(
                        cleanup_blocks_from_cache,
                        sent_blocks_keys
                    ))
                    .await?;
            }
        }

        if should_restore_blocks_in_cache {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Not all blocks were sent to sync. Will restore all blocks in cache for one more sync attempt: {:?}",
                _tracing_blocks_to_send_descr.as_slice(),
            );
            // queue blocks restore task
            dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    restore_blocks_in_cache_async,
                    blocks_to_send
                ))
                .await?;
            // TODO: should implement resending for restored blocks
        }

        Ok(())
    }
}
