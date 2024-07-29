use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use parking_lot::{Mutex, RwLock};
use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use self::types::{
    BlockCacheKey, BlockCandidateContainer, BlockCandidateToSend, BlocksCache, ChainTimesSyncState,
    McBlockSubgraphToSend, SendSyncStatus,
};
use self::utils::find_us_in_collators_set;
use crate::collator::{Collator, CollatorContext, CollatorEventListener, CollatorFactory};
use crate::mempool::{MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolEventListener};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterFactory, StateNodeEventListener};
use crate::types::{
    BlockCandidate, BlockCollationResult, CollationConfig, CollationSessionId,
    CollationSessionInfo, McData, ProofFunds, TopBlockDescription,
};
use crate::utils::async_dispatcher::{AsyncDispatcher, STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE};
use crate::utils::schedule_async_action;
use crate::utils::shard::calc_split_merge_actions;
use crate::validator::{AddSession, ValidationStatus, Validator};
use crate::{method_to_async_closure, tracing_targets};

mod types;
mod utils;

pub struct RunningCollationManager<CF, V>
where
    CF: CollatorFactory,
{
    dispatcher: AsyncDispatcher<CollationManager<CF, V>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter>,
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

    dispatcher: Arc<AsyncDispatcher<Self>>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    mpool_adapter: Arc<dyn MempoolAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter>,

    collator_factory: CF,
    validator: Arc<V>,

    active_collation_sessions: RwLock<FastHashMap<ShardIdent, Arc<CollationSessionInfo>>>,
    collation_sessions_to_finish: FastDashMap<CollationSessionId, Arc<CollationSessionInfo>>,
    active_collators: FastDashMap<ShardIdent, Arc<CF::Collator>>,
    collators_to_stop: FastDashMap<CollationSessionId, Arc<CF::Collator>>,

    blocks_cache: BlocksCache,

    last_processed_mc_block_id: Mutex<Option<BlockId>>,
    /// id of last master block collated by ourselves
    last_collated_mc_block_id: Mutex<Option<BlockId>>,

    chain_times_sync_state: Mutex<ChainTimesSyncState>,

    #[cfg(any(test, feature = "test"))]
    test_validators_keypairs: Vec<Arc<KeyPair>>,
}

#[async_trait]
impl<CF, V> MempoolEventListener for AsyncDispatcher<CollationManager<CF, V>>
where
    CF: CollatorFactory,
    V: Validator,
{
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            process_new_anchor_from_mempool,
            anchor
        ))
        .await
    }
}

#[async_trait]
impl<CF, V> StateNodeEventListener for AsyncDispatcher<CollationManager<CF, V>>
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
            let mc_data = McData::load_from_state(state)?;
            self.spawn_task(method_to_async_closure!(process_mc_block_from_bc, mc_data))
                .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<CF, V> CollatorEventListener for AsyncDispatcher<CollationManager<CF, V>>
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
        self.spawn_task(method_to_async_closure!(
            process_skipped_anchor,
            next_block_id_short,
            anchor,
            force_mc_block
        ))
        .await
    }

    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()> {
        self.spawn_task(method_to_async_closure!(
            process_collated_block_candidate,
            collation_result
        ))
        .await
    }

    async fn on_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
        self.spawn_task(method_to_async_closure!(process_collator_stopped, stop_key))
            .await
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
        config: CollationConfig,
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        state_node_adapter_factory: STF,
        mpool_adapter_factory: MPF,
        validator: V,
        collator_factory: CF,
        #[cfg(any(test, feature = "test"))] test_validators_keypairs: Vec<Arc<KeyPair>>,
    ) -> RunningCollationManager<CF, V>
    where
        STF: StateNodeAdapterFactory,
        MPF: MempoolAdapterFactory,
    {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER, "Creating collation manager...");

        // create dispatcher for own tasks
        let (dispatcher, spawn_receiver) = AsyncDispatcher::new(
            "collation_manager_async_dispatcher",
            STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE,
        );
        let arc_dispatcher = Arc::new(dispatcher.clone());

        // create state node adapter
        let state_node_adapter =
            Arc::new(state_node_adapter_factory.create(arc_dispatcher.clone()));

        // create mempool adapter
        let mpool_adapter = mpool_adapter_factory.create(arc_dispatcher.clone());

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

            active_collation_sessions: Default::default(),
            collation_sessions_to_finish: Default::default(),
            active_collators: Default::default(),
            collators_to_stop: Default::default(),

            blocks_cache: BlocksCache::default(),

            last_processed_mc_block_id: Default::default(),
            last_collated_mc_block_id: Default::default(),
            chain_times_sync_state: Default::default(),

            #[cfg(any(test, feature = "test"))]
            test_validators_keypairs,
        };
        arc_dispatcher.run(Arc::new(processor), spawn_receiver);
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "Tasks dispatchers started");

        // start other async processes

        // TODO: Move outside of the start method?

        // schedule to check collation sessions and force refresh
        // if not initialized (when started from zerostate)
        schedule_async_action(
            tokio::time::Duration::from_secs(10),
            || async move {
                arc_dispatcher
                    .spawn_task(method_to_async_closure!(check_refresh_collation_sessions,))
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
    fn get_last_collated_mc_block_id(&self) -> Option<BlockId> {
        *self.last_collated_mc_block_id.lock()
    }
    fn set_last_collated_mc_block_id(&self, block_id: BlockId) {
        let mut guard = self.last_collated_mc_block_id.lock();
        guard.replace(block_id);
    }

    fn get_last_processed_mc_block_id(&self) -> Option<BlockId> {
        *self.last_processed_mc_block_id.lock()
    }

    /// Prunes the cache of last collated chain times
    fn process_last_collated_mc_block_chain_time(&self, chain_time: u64) {
        let mut chain_times_guard = self.chain_times_sync_state.lock();
        for (_, last_collated_chain_times) in chain_times_guard
            .last_collated_chain_times_by_shards
            .iter_mut()
        {
            last_collated_chain_times.retain(|(ct, _)| ct > &chain_time);
        }
    }

    /// (TODO) Check sync status between mempool and blockchain state
    /// and pause collation when we are far behind other nodesб
    /// jusct sync blcoks from blockchain
    pub async fn process_new_anchor_from_mempool(&self, _anchor: Arc<MempoolAnchor>) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        Ok(())
    }

    /// Process new master block from blockchain:
    /// 1. Load block state
    /// 2. Notify mempool about new master block
    /// 3. Enqueue collation sessions refresh task
    pub async fn process_mc_block_from_bc(&self, mc_data: Arc<McData>) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Processing master block ({})", mc_data.block_id.as_short_id(),
        );

        // check if we should skip this master block from the blockchain
        // because it is not far ahead of last collated by ourselves
        if !self.check_should_process_mc_block_from_bc(&mc_data.block_id) {
            return Ok(());
        }

        // when state received execute master block processing routines
        let mpool_adapter = self.mpool_adapter.clone();

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Processing requested mc state for block ({})...",
            mc_data.block_id.as_short_id()
        );

        Self::notify_mempool_about_mc_block(mpool_adapter, &mc_data.block_id).await?;

        self.refresh_collation_sessions(mc_data).await?;

        Ok(())
    }

    /// 1. Skip if it is equal or not far ahead from last collated by ourselves
    /// 2. Skip if it was already processed before
    /// 3. Skip if waiting for the first own master block collation less then `max_mc_block_delta_from_bc_to_await_own`
    fn check_should_process_mc_block_from_bc(&self, mc_block_id: &BlockId) -> bool {
        let last_collated_mc_block_id_opt = self.get_last_collated_mc_block_id();
        let last_processed_mc_block_id_opt = self.get_last_processed_mc_block_id();
        if last_collated_mc_block_id_opt.is_some() {
            // when we have last own collated master block then skip if incoming one is equal
            // or not far ahead from last own collated
            // then will wait for next own collated master block
            let (seqno_delta, is_equal) =
                Self::compare_mc_block_with(mc_block_id, last_collated_mc_block_id_opt.as_ref());
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
            // If we have already processed some previous incoming master block and collations were started
            // then we should wait for the first own collated master block
            // but not more then `max_mc_block_delta_from_bc_to_await_own`
            if last_processed_mc_block_id_opt.is_some() {
                let (seqno_delta, is_equal) = Self::compare_mc_block_with(
                    mc_block_id,
                    last_processed_mc_block_id_opt.as_ref(),
                );
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

    fn check_should_process_and_update_last_processed_mc_block(&self, block_id: &BlockId) -> bool {
        let mut guard = self.last_processed_mc_block_id.lock();
        let last_processed_mc_block_id_opt = guard.as_ref();
        let (seqno_delta, is_equal) =
            Self::compare_mc_block_with(block_id, last_processed_mc_block_id_opt);
        if seqno_delta < 0 || is_equal {
            false
        } else {
            guard.replace(*block_id);
            true
        }
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

    /// Check if collation sessions initialized and try to force refresh them if they not.
    /// This needed when start from zerostate. State node adapter will be initialized after
    /// zerostate load and won't fire `[StateNodeListener::on_mc_block_event()]` for the 1 block.
    /// Also when whole network was restarted then nobody will produce next master block and we need
    /// to start collation sessions based on the actual state
    pub async fn check_refresh_collation_sessions(&self) -> Result<()> {
        // the sessions list is not enpty so the collation process was already started from
        // actual state or incoming master block from blockchain
        if !self.active_collation_sessions.read().is_empty() {
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

        let state = self
            .state_node_adapter
            .load_state(&last_mc_block_id)
            .await?;
        let mc_data = McData::load_from_state(&state)?;

        self.process_mc_block_from_bc(mc_data).await
    }

    /// Get shards info from the master state,
    /// then start missing sessions for these shards, or refresh existing.
    /// For each shard run collation process if current node is included in collators subset.
    #[tracing::instrument(skip_all, fields(mc_block_id = %mc_data.block_id.as_short_id()))]
    pub async fn refresh_collation_sessions(&self, mc_data: Arc<McData>) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Trying to refresh collation sessions by mc state for block ({})...",
            mc_data.block_id.as_short_id(),
        );

        let _histogram = HistogramGuard::begin("tycho_collator_refresh_collation_sessions_time");

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
        if !self.check_should_process_and_update_last_processed_mc_block(&mc_data.block_id) {
            return Ok(());
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER, "mc_data: {:?}", mc_data);

        // get new shards info from updated master state
        let mut new_shards_info = HashMap::new();
        new_shards_info.insert(ShardIdent::MASTERCHAIN, vec![mc_data.block_id]);
        for shard in mc_data.shards.iter() {
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
        let active_shards_ids: Vec<_> = self
            .active_collation_sessions
            .read()
            .keys()
            .cloned()
            .collect();
        let new_shards_ids: Vec<&ShardIdent> = new_shards_info.keys().collect();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Detecting split/merge actions to move from current shards {:?} to new shards {:?}...",
            active_shards_ids.as_slice(),
            new_shards_ids
        );

        let split_merge_actions = calc_split_merge_actions(&active_shards_ids, new_shards_ids)?;
        if !split_merge_actions.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Detected split/merge actions: {:?}",
                split_merge_actions,
            );
            self.mq_adapter.update_shards(split_merge_actions).await?;
        }

        // find out the actual collation session seqno from master state
        let new_session_seqno = mc_data.validator_info.catchain_seqno;

        // we need full validators set to define the subset for each session and to check if current node should collate
        let full_validators_set = mc_data.config.get_current_validator_set()?;
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "full_validators_set {:?}", full_validators_set);

        // compare with active sessions and detect new sessions to start and outdated sessions to finish
        let mut sessions_to_keep = HashMap::new();
        let mut sessions_to_start = vec![];
        let mut to_finish_sessions = HashMap::new();
        let mut to_stop_collators = HashMap::new();
        {
            let mut active_collation_sessions_guard = self.active_collation_sessions.write();
            let mut missed_shards_ids: FastHashSet<_> = active_shards_ids.into_iter().collect();
            for shard_info in new_shards_info {
                missed_shards_ids.remove(&shard_info.0);
                match active_collation_sessions_guard.entry(shard_info.0) {
                    Entry::Occupied(entry) => {
                        let existing_session = entry.get().clone();
                        if existing_session.seqno() >= new_session_seqno {
                            sessions_to_keep.insert(shard_info.0, existing_session);
                        } else {
                            to_finish_sessions
                                .insert((shard_info.0, new_session_seqno), existing_session);
                            sessions_to_start.push(shard_info);
                            entry.remove();
                        }
                    }
                    Entry::Vacant(_) => {
                        sessions_to_start.push(shard_info);
                    }
                }
            }

            // if we still have some active sessions that do not match with new shards
            // then we need to finish them and stop their collators
            for shard_id in missed_shards_ids {
                // TODO: we should remove session from active and add to finished in one atomic operation
                //      to not to miss session on processing block candidate that could be from old session
                if let Some(current_active_session) =
                    active_collation_sessions_guard.remove(&shard_id)
                {
                    to_finish_sessions
                        .insert((shard_id, new_session_seqno), current_active_session);
                    if let Some(collator) = self.active_collators.remove(&shard_id) {
                        to_stop_collators.insert((shard_id, new_session_seqno), collator.1);
                    }
                }
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

        let cc_config = mc_data.config.get_catchain_config()?;

        // update master state in existing collators and resume collation
        for (shard_id, _) in sessions_to_keep {
            // if there is no active collator then current node does not collate this shard
            // so we do not need to do anything
            let Some(collator) = self.active_collators.get(&shard_id).map(|r| r.clone()) else {
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
                    .equeue_update_mc_data_and_try_collate(mc_data.clone())
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
                let prev_seqno = prev_blocks_ids.iter().map(|b| b.seqno).max().unwrap_or(0);

                if !self.active_collators.contains_key(&shard_id) {
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
                            mc_data: mc_data.clone(),
                        })
                        .await;

                    self.active_collators.insert(shard_id, Arc::new(collator));
                }

                // notify validator, it will start overlay initialization

                let session_id = new_session_info.seqno();

                self.validator.add_session(AddSession {
                    shard_ident: shard_id,
                    session_id,
                    start_block_seqno: prev_seqno + 1,
                    validators: &new_session_info.collators().validators,
                })?;
            } else {
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Node was not athorized to collate shard {}",
                    shard_id,
                );
                if let Some(collator) = self.active_collators.remove(&shard_id) {
                    to_stop_collators.insert((shard_id, new_session_seqno), collator.1);
                }
            }

            // TODO: possibly do not need to store collation sessions if we do not collate in them
            self.active_collation_sessions
                .write()
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
            self.finish_collation_session(session_info, finish_key)
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

        Ok(())

        // finally we will have initialized `active_collation_sessions` and `active_collators`
        // which run async block collations processes
    }

    /// Execute collation session finalization routines
    pub async fn finish_collation_session(
        &self,
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
    pub async fn process_collator_stopped(&self, stop_key: CollationSessionId) -> Result<()> {
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
            block_id = %collation_result.candidate.block.id().as_short_id(),
            ct = collation_result.candidate.chain_time,
        ),
    )]
    pub async fn process_collated_block_candidate(
        &self,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start processing block candidate",
        );

        let _histogram =
            HistogramGuard::begin("tycho_collator_process_collated_block_candidate_time");

        let block_id = *collation_result.candidate.block.id();

        // find session related to this block by shard
        let Some(session_info) = self
            .active_collation_sessions
            .read()
            .get(&block_id.shard)
            .cloned()
        else {
            anyhow::bail!(
                "There is no active collation session for the shard that block belongs to"
            );
        };

        let candidate_chain_time = collation_result.candidate.chain_time;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Saving block candidate to cache...",
        );

        self.store_candidate(collation_result.candidate)?;

        // send validation task to validator
        // we need to send session info with the collators list to the validator
        // to understand whom we must ask for signatures
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Enqueueing block candidate validation...",
        );

        let validator = self.validator.clone();
        let session_seqno = session_info.seqno();
        let dispatcher = self.dispatcher.clone();
        tokio::spawn(async move {
            // TODO: Fail collation instead of panicking?
            let status = validator.validate(session_seqno, &block_id).await.unwrap();

            _ = dispatcher
                .spawn_task(method_to_async_closure!(
                    process_validated_block,
                    block_id,
                    status
                ))
                .await;
        });

        debug_assert_eq!(
            block_id.is_masterchain(),
            collation_result.mc_data.is_some(),
        );

        // when candidate is master
        if let Some(mc_data) = collation_result.mc_data {
            // store last collated master block id and chain time
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Candidate is a master block",
            );
            self.process_last_collated_mc_block_chain_time(candidate_chain_time);
            self.set_last_collated_mc_block_id(block_id);

            // collate next master block right now if there are pending internals
            if collation_result.has_pending_internals {
                tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                    "there are pending internals in master queue",
                );
                self.enqueue_mc_block_collation(candidate_chain_time, Some(block_id))
                    .await?;
            } else {
                // otherwise execute master block processing routines
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Candidate (id: {}, chain_time: {}): will notify mempool and equeue collation sessions refresh",
                    block_id.as_short_id(),
                    candidate_chain_time,
                );

                Self::notify_mempool_about_mc_block(self.mpool_adapter.clone(), &block_id).await?;

                self.refresh_collation_sessions(mc_data).await?;
            }
        } else {
            // when candidate is shard
            // chek if master block interval elapsed and it needs to collate new master block
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will check if master block interval elapsed by chain time from candidate (id: {}, chain_time: {})",
                block_id.as_short_id(),
                candidate_chain_time,
            );
            self.collate_mc_block_by_interval_or_continue_shard_collation(
                block_id.shard,
                candidate_chain_time,
                false,
                Some(block_id),
            )
            .await?;
        }

        Ok(())
    }

    /// Send master state related to master block to mempool (it may perform gc or nodes rotation)
    async fn notify_mempool_about_mc_block(
        mpool_adapter: Arc<dyn MempoolAdapter>,
        mc_block_id: &BlockId,
    ) -> Result<()> {
        // TODO: in current implementation CollationProcessor should not notify mempool
        //      about one master block more than once, but better to handle repeated request here or at mempool
        mpool_adapter.on_new_mc_state(mc_block_id).await
    }

    #[tracing::instrument(skip_all, fields(block_id = %next_block_id_short, ct = anchor.chain_time, force_mc_block))]
    async fn process_skipped_anchor(
        &self,
        next_block_id_short: BlockIdShort,
        anchor: Arc<MempoolAnchor>,
        force_mc_block: bool,
    ) -> Result<()> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Will check if should collate next master block",
        );
        self.collate_mc_block_by_interval_or_continue_shard_collation(
            next_block_id_short.shard,
            anchor.chain_time,
            force_mc_block,
            None,
        )
        .await
    }

    /// 1. Check if should collate master
    /// 2. If true, schedule master block collation
    /// 3. If no, schedule next collation attempt in current shard
    async fn collate_mc_block_by_interval_or_continue_shard_collation(
        &self,
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
        &self,
        shard_id: ShardIdent,
        chain_time: u64,
        force_mc_block: bool,
    ) -> (bool, Option<u64>) {
        // Idea is to store collated chain times and "force" flags for each shard.
        // Then we can collate master block if interval elapsed or have "force" flag in every shards.
        // We should take the max of first chain times that meet master block collation condition from each shard.

        let _histogram = HistogramGuard::begin(
            "tycho_collator_update_last_collated_chain_time_and_check_should_collate_mc_block_time",
        );

        let mut chain_times_guard = self.chain_times_sync_state.lock();
        let last_collated_chain_times = chain_times_guard
            .last_collated_chain_times_by_shards
            .entry(shard_id)
            .or_default();
        last_collated_chain_times.push((chain_time, force_mc_block));

        let mc_block_min_interval_ms = self.config.mc_block_min_interval.as_millis() as u64;

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
                .checked_sub(chain_times_guard.mc_block_latest_chain_time)
                .unwrap_or_default();
            let mc_block_interval_elapsed = chain_time_elapsed > mc_block_min_interval_ms;
            if mc_block_interval_elapsed {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block interval is {}ms, elapsed chain time {}ms exceeded the interval in current shard {}",
                    mc_block_min_interval_ms, chain_time_elapsed, shard_id,
                );
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Elapsed chain time {}ms has not elapsed master block interval {}ms in current shard \
                    - do not need to collate next master block",
                    chain_time_elapsed, mc_block_min_interval_ms,
                );
            }
            mc_block_interval_elapsed
        };

        if should_collate_mc_block {
            // if master block should be collated in every shard
            let mut first_elapsed_chain_times = vec![];
            let mut should_collate_in_every_shard = true;
            let active_shards = self
                .active_collation_sessions
                .read()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for active_shard in active_shards {
                if let Some(last_collated_chain_times) = chain_times_guard
                    .last_collated_chain_times_by_shards
                    .get(&active_shard)
                {
                    if let Some((chain_time_that_elapsed, _)) = last_collated_chain_times
                        .iter()
                        .find(|(chain_time, force)| {
                            *force
                                || (*chain_time)
                                    .checked_sub(chain_times_guard.mc_block_latest_chain_time)
                                    .unwrap_or_default()
                                    > mc_block_min_interval_ms
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
                    mc_block_min_interval_ms, max_first_chain_time_that_elapsed,
                );
                chain_times_guard.mc_block_latest_chain_time = max_first_chain_time_that_elapsed;
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
            .filter_map(|shard_cache| shard_cache.value().last_key_value().map(|(_, v)| *v.key()))
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
        &self,
        next_mc_block_chain_time: u64,
        trigger_block_id_opt: Option<BlockId>,
    ) -> Result<()> {
        // TODO: make real implementation

        let _histogram = HistogramGuard::begin("tycho_collator_enqueue_mc_block_collation_time");

        // get masterchain collator if exists
        let Some(mc_collator) = self
            .active_collators
            .get(&ShardIdent::MASTERCHAIN)
            .map(|r| r.clone())
        else {
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

        mc_collator
            .equeue_do_collate(next_mc_block_chain_time, top_shard_blocks_info)
            .await?;

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Master block collation enqueued: (block_id={} ct={})",
            self.get_last_collated_mc_block_id()
                .map(|id| BlockIdShort { shard: id.shard, seqno: id.seqno + 1 }.to_string())
                .unwrap_or_default(),
            next_mc_block_chain_time,
        );

        Ok(())
    }

    async fn enqueue_try_collate(&self, shard_id: &ShardIdent) -> Result<()> {
        // get collator if exists
        let Some(collator) = self.active_collators.get(shard_id).map(|r| r.clone()) else {
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
    #[tracing::instrument(skip_all, fields(block_id = %block_id.as_short_id()))]
    pub async fn process_validated_block(
        &self,
        block_id: BlockId,
        status: ValidationStatus,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            is_complete = matches!(&status, ValidationStatus::Complete(_)),
            "Start processing block validation result...",
        );

        let _histogram = HistogramGuard::begin("tycho_collator_process_validated_block_time");

        // execute required actions if block invalid

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Saving block validation result to cache...",
        );
        // update block in cache with signatures info
        let updated = self.store_block_validation_result(block_id, status);
        if !updated {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Block does not exist in cache - skip validation result",
            );
            return Ok(());
        }

        // process valid block
        if block_id.shard.is_masterchain() {
            self.process_valid_master_block(&block_id).await?;
        } else {
            self.process_valid_shard_block(&block_id).await?;
        }

        Ok(())
    }

    /// Store block in a cache structure that allow to append signatures
    fn store_candidate(&self, candidate: Box<BlockCandidate>) -> Result<()> {
        // TODO: in future we may store to cache a block received from blockchain before,
        //      then it will exist in cache when we try to store collated candidate
        //      but the `root_hash` may differ, so we have to handle such a case

        let block_id = *candidate.block.id();
        let block_container = BlockCandidateContainer::new(candidate);
        if block_id.shard.is_masterchain() {
            // traverse through including shard blocks and update their link to the containing master block
            let mut prev_shard_blocks_keys = block_container
                .top_shard_blocks_keys()
                .iter()
                .cloned()
                .collect::<VecDeque<_>>();
            while let Some(prev_shard_block_key) = prev_shard_blocks_keys.pop_front() {
                if let Some(mut shard_cache) = self
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
                    block_id,
                );
            }
        } else {
            let mut shard_cache = self
                .blocks_cache
                .shards
                .entry(block_container.key().shard)
                .or_default();
            if let Some(_existing) =
                shard_cache.insert(block_container.key().seqno, block_container)
            {
                bail!(
                    "Should not collate the same shard block ({}) again!",
                    block_id,
                );
            }
        }

        Ok(())
    }

    /// Find block candidate in cache, append signatures info and return updated
    fn store_block_validation_result(
        &self,
        block_id: BlockId,
        validation_result: ValidationStatus,
    ) -> bool {
        let (is_valid, already_synced, signatures) = match validation_result {
            ValidationStatus::Skipped => (true, true, Default::default()),
            ValidationStatus::Complete(signatures) => (true, false, signatures),
        };

        if block_id.is_masterchain() {
            if let Some(mut block_container) =
                self.blocks_cache.master.get_mut(&block_id.as_short_id())
            {
                block_container.set_validation_result(is_valid, already_synced, signatures);
                return true;
            }
        } else if let Some(mut shard_cache) = self.blocks_cache.shards.get_mut(&block_id.shard) {
            if let Some(block_container) = shard_cache.get_mut(&block_id.seqno) {
                block_container.set_validation_result(is_valid, already_synced, signatures);
                return true;
            }
        }
        false
    }

    /// Find shard block in cache and then get containing master block id if link exists
    fn find_containing_mc_block(&self, shard_block_id: &BlockId) -> Option<(BlockId, bool)> {
        // TODO: handle when master block link exist but there is not block itself
        if let Some(shard_cache) = self.blocks_cache.shards.get(&shard_block_id.shard) {
            if let Some(mc_block_key) = shard_cache
                .value()
                .get(&shard_block_id.seqno)
                .and_then(|sbc| sbc.containing_mc_block)
            {
                let res = self
                    .blocks_cache
                    .master
                    .get(&mc_block_key)
                    .map(|block_container| {
                        (*block_container.block_id(), block_container.is_valid())
                    });
                return res;
            }
        }
        None
    }

    /// Find all shard blocks that form master block subgraph.
    /// Then extract and return them if all are valid
    fn extract_mc_block_subgraph_if_valid(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<McBlockSubgraphToSend>> {
        // 1. Find current master block
        let (mc_block_container_key, mc_block_candidate_to_send, mut prev_shard_blocks_keys) = {
            let mut mc_block_container = self
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

            let mc_block_candidate_to_send = mc_block_container.extract_entry_for_sending()?;

            let prev_shard_blocks_keys = mc_block_container
                .top_shard_blocks_keys()
                .iter()
                .cloned()
                .collect::<VecDeque<_>>();

            (
                *mc_block_container.key(),
                mc_block_candidate_to_send,
                prev_shard_blocks_keys,
            )
        };

        let mut subgraph = McBlockSubgraphToSend {
            mc_block: BlockCandidateToSend {
                entry: mc_block_candidate_to_send,
                send_sync_status: SendSyncStatus::Sending,
            },
            shard_blocks: vec![],
        };

        // 3. By the top shard blocks info find shard blocks of current master block
        // 4. Recursively find prev shard blocks until the end or top shard blocks of prev master reached
        let mut not_all_blocks_valid = false;
        while let Some(prev_shard_block_key) = prev_shard_blocks_keys.pop_front() {
            let mut shard_cache = self
                .blocks_cache
                .shards
                .get_mut(&prev_shard_block_key.shard)
                .ok_or_else(|| {
                    anyhow!("Shard block ({}) not found in cache!", prev_shard_block_key)
                })?;
            if let Some(shard_block_container) = shard_cache.get_mut(&prev_shard_block_key.seqno) {
                // if shard block included in current master block subgraph
                if matches!(shard_block_container.containing_mc_block, Some(containing_mc_block_key) if containing_mc_block_key == mc_block_container_key)
                {
                    // 5. If master block and all shard blocks valid then extract them from entries and return
                    if !shard_block_container.is_valid() {
                        not_all_blocks_valid = true;
                        break;
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

        if not_all_blocks_valid {
            let mut blocks_to_restore = vec![subgraph.mc_block];
            blocks_to_restore.append(&mut subgraph.shard_blocks);
            self.restore_blocks_in_cache(blocks_to_restore)?;
            return Ok(None);
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Extracted valid master block ({}) subgraph for sending to sync: {:?}",
            block_id.as_short_id(),
            subgraph
            .shard_blocks
            .iter()
            .map(|sb| sb.entry.key.to_string())
            .collect::<Vec<_>>().as_slice(),
        );

        Ok(Some(subgraph))
    }

    /// Remove block entries from cache and compact cache
    fn cleanup_blocks_from_cache(&self, blocks_keys: Vec<BlockCacheKey>) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Cleaning up blocks from cache: {:?}",
            blocks_keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<_>>()
            .as_slice(),
        );
        for block_key in blocks_keys.iter() {
            if block_key.shard.is_masterchain() {
                self.blocks_cache.master.remove(block_key);
            } else if let Some(mut shard_cache) = self.blocks_cache.shards.get_mut(&block_key.shard)
            {
                shard_cache.remove(&block_key.seqno);
            }
        }
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Blocks cleaned up from cache: {:?}",
            blocks_keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<_>>()
            .as_slice(),
        );
        Ok(())
    }

    /// Find and restore block entries in cache updating sync statuses
    fn restore_blocks_in_cache(&self, blocks_to_restore: Vec<BlockCandidateToSend>) -> Result<()> {
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
            if block.entry.key.shard.is_masterchain() {
                let mut block_container = self
                    .blocks_cache
                    .master
                    .get_mut(&block.entry.key)
                    .ok_or_else(|| {
                        anyhow!("Master block ({}) not found in cache!", block.entry.key)
                    })?;
                block_container.restore_entry(block.entry, block.send_sync_status)?;
            } else {
                let mut shard_cache = self
                    .blocks_cache
                    .shards
                    .get_mut(&block.entry.key.shard)
                    .ok_or_else(|| {
                        anyhow!(
                            "Shard blocks map ({}) not found in cache!",
                            block.entry.key.shard
                        )
                    })?;
                let block_container =
                    shard_cache.get_mut(&block.entry.key.seqno).ok_or_else(|| {
                        anyhow!("Shard block ({}) not found in cache!", block.entry.key)
                    })?;
                block_container.restore_entry(block.entry, block.send_sync_status)?;
            };
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
    async fn process_valid_master_block(&self, block_id: &BlockId) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start processing validated and valid master block ({})...",
            block_id.as_short_id(),
        );

        let histogram = HistogramGuard::begin("tycho_collator_process_valid_master_block_time");
        let histogram_extract =
            HistogramGuard::begin("tycho_collator_extract_master_block_subgraph_time");
        let mut extract_elapsed = Default::default();
        let mut sync_elapsed = Default::default();

        // extract master block with all shard blocks if valid, and process them
        if let Some(mc_block_subgraph) = self.extract_mc_block_subgraph_if_valid(block_id)? {
            extract_elapsed = histogram_extract.finish();
            let timer = std::time::Instant::now();

            let mut blocks_to_send = mc_block_subgraph.shard_blocks;
            blocks_to_send.reverse();
            blocks_to_send.push(mc_block_subgraph.mc_block);

            // send all shard and master blocks
            self.send_blocks_to_sync(
                self.mq_adapter.clone(),
                self.state_node_adapter.clone(),
                blocks_to_send,
            )
            .await?;

            sync_elapsed = timer.elapsed();
        } else {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Master block ({}) subgraph is not full valid. Will wait until all included shard blocks been validated",
                block_id.as_short_id(),
            );
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            total = histogram.finish().as_millis(),
            extract_subgraph = extract_elapsed.as_millis(),
            sync = sync_elapsed.as_millis(),
            "process_valid_master_block timings",
        );

        Ok(())
    }

    /// 1. Try find master block info and execute [`CollationProcessor::process_valid_master_block`]
    async fn process_valid_shard_block(&self, block_id: &BlockId) -> Result<()> {
        if let Some((mc_block_id, is_valid)) = self.find_containing_mc_block(block_id) {
            if is_valid {
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
        &self,
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        state_node_adapter: Arc<dyn StateNodeAdapter>,
        mut blocks_to_send: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        // TODO: it is better to send each block separately, but it will be more tricky to handle the correct cleanup
        let histogram = HistogramGuard::begin("tycho_collator_send_blocks_to_sync_time");

        let _tracing_blocks_to_send_descr = blocks_to_send
            .iter()
            .map(|b| b.entry.key.to_string())
            .collect::<Vec<_>>();
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start sending blocks to sync: {:?}",
            _tracing_blocks_to_send_descr.as_slice(),
        );

        let mut build_stuff_for_sync_elapsed = Duration::ZERO;
        let mut sync_stuff_elapsed = Duration::ZERO;

        // skip already synced blocks that were validated by existing blocks in the state
        // send other blocks to sync
        let mut should_restore_blocks_in_cache = false;
        let mut sent_blocks = vec![];
        for block_to_send in blocks_to_send.iter_mut() {
            match block_to_send.send_sync_status {
                SendSyncStatus::Sent | SendSyncStatus::Synced => sent_blocks.push(block_to_send),
                _ => {
                    let timer = std::time::Instant::now();
                    let block_for_sync = block_to_send.entry.as_block_for_sync();
                    build_stuff_for_sync_elapsed += timer.elapsed();

                    let timer = std::time::Instant::now();
                    // TODO: handle different errors types
                    if let Err(err) = state_node_adapter.accept_block(block_for_sync).await {
                        tracing::warn!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Block ({}) sync: was not accepted. err: {:?}",
                            block_to_send.entry.candidate.block.id().as_short_id(),
                            err,
                        );
                        should_restore_blocks_in_cache = true;
                        break;
                    } else {
                        tracing::debug!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Block ({}) sync: was successfully sent to sync",
                            block_to_send.entry.candidate.block.id().as_short_id(),
                        );
                        block_to_send.send_sync_status = SendSyncStatus::Sent;
                        sent_blocks.push(block_to_send);
                    }

                    sync_stuff_elapsed += timer.elapsed();
                }
            }
        }

        let mut commit_diffs_elapsed = Default::default();
        if !should_restore_blocks_in_cache {
            let histogram =
                HistogramGuard::begin("tycho_collator_send_blocks_to_sync_commit_diffs_time");
            // commit queue diffs for each block
            for sent_block in sent_blocks.iter() {
                // TODO: handle if diff does not exist

                let block_id_short = sent_block.entry.candidate.block.id().as_short_id();

                if let Err(err) = mq_adapter.commit_diff(&block_id_short).await {
                    tracing::warn!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block ({}) sync: error committing message queue diff: {:?}",
                        block_id_short,
                        err,
                    );
                    should_restore_blocks_in_cache = true;
                    break;
                } else {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block ({}) sync: message queue diff was committed",
                        block_id_short,
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
                self.cleanup_blocks_from_cache(sent_blocks_keys)?;
            }

            commit_diffs_elapsed = histogram.finish();
        }

        if should_restore_blocks_in_cache {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Not all blocks were sent to sync. Will restore all blocks in cache for one more sync attempt: {:?}",
                _tracing_blocks_to_send_descr.as_slice(),
            );
            // queue blocks restore task
            self.restore_blocks_in_cache(blocks_to_send)?;
            // TODO: should implement resending for restored blocks
        }

        metrics::histogram!("tycho_collator_build_block_stuff_for_sync_time")
            .record(build_stuff_for_sync_elapsed);
        metrics::histogram!("tycho_collator_sync_block_stuff_time").record(sync_stuff_elapsed);

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            total = histogram.finish().as_millis(),
            build_stuff_for_sync = build_stuff_for_sync_elapsed.as_millis(),
            sync = sync_stuff_elapsed.as_millis(),
            commit_diffs = commit_diffs_elapsed.as_millis(),
            "send_blocks_to_sync timings",
        );

        Ok(())
    }
}
