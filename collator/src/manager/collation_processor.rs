use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::Arc,
};

use everscale_crypto::ed25519::KeyPair;

use anyhow::{anyhow, bail, Result};

use everscale_types::models::{BlockId, ShardIdent, ValidatorDescription, ValidatorSet};
use tycho_block_util::{
    block::ValidatorSubsetInfo,
    state::{MinRefMcStateTracker, ShardStateStuff},
};

use crate::{
    collator::Collator,
    mempool::{MempoolAdapter, MempoolAnchor},
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{
        BlockCandidate, BlockCollationResult, CollationConfig, CollationSessionId,
        CollationSessionInfo, OnValidatedBlockEvent,
    },
    utils::{async_queued_dispatcher::AsyncQueuedDispatcher, shard::calc_split_merge_actions},
    validator::Validator,
};

use super::{
    types::{
        BlockCacheKey, BlockCandidateContainer, BlockCandidateToSend, BlocksCache,
        McBlockSubgraphToSend, SendSyncStatus, ShardStateStuffExt,
    },
    utils::{build_block_stuff_for_sync, find_us_in_collators_set},
};

pub(super) struct CollationProcessor<C, V, MQ, MP, ST>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    config: Arc<CollationConfig>,

    dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
    mpool_adapter: Arc<MP>,
    state_node_adapter: Arc<ST>,
    mq_adapter: Arc<MQ>,

    //TODO: possibly use V because manager may not need a ref to validator
    validator: Arc<V>,

    active_collation_sessions: HashMap<ShardIdent, Arc<CollationSessionInfo>>,
    collation_sessions_to_finish: HashMap<CollationSessionId, Arc<CollationSessionInfo>>,
    active_collators: HashMap<ShardIdent, Arc<C>>,
    collators_to_stop: HashMap<CollationSessionId, Arc<C>>,

    state_tracker: MinRefMcStateTracker,

    blocks_cache: BlocksCache,

    last_processed_mc_block_id: Option<BlockId>,
    /// id of last master block collated by ourselves
    last_collated_mc_block_id: Option<BlockId>,
    /// chain time of last collated master block or received from bc
    last_mc_block_chain_time: u64,
    /// chain time for next master block to be collated
    next_mc_block_chain_time: u64,
}

impl<C, V, MQ, MP, ST> CollationProcessor<C, V, MQ, MP, ST>
where
    C: Collator<MQ, MP, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    pub fn new(
        config: Arc<CollationConfig>,
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        mpool_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        validator: Arc<V>,
    ) -> Self {
        Self {
            config,
            dispatcher,
            mpool_adapter,
            state_node_adapter,
            mq_adapter: Arc::new(MQ::new()),
            validator,
            state_tracker: MinRefMcStateTracker::default(),
            active_collation_sessions: HashMap::new(),
            collation_sessions_to_finish: HashMap::new(),
            active_collators: HashMap::new(),
            collators_to_stop: HashMap::new(),

            blocks_cache: BlocksCache::default(),

            last_processed_mc_block_id: None,
            last_collated_mc_block_id: None,
            last_mc_block_chain_time: 0,
            next_mc_block_chain_time: 0,
        }
    }

    /// Return last master block chain time
    fn last_mc_block_chain_time(&self) -> u64 {
        self.last_mc_block_chain_time
    }
    fn set_last_mc_block_chain_time(&mut self, chain_time: u64) {
        self.last_mc_block_chain_time = chain_time;
    }

    fn next_mc_block_chain_time(&self) -> u64 {
        self.next_mc_block_chain_time
    }
    fn set_next_mc_block_chain_time(&mut self, chain_time: u64) {
        self.next_mc_block_chain_time = chain_time;
    }

    fn last_processed_mc_block_id(&self) -> Option<&BlockId> {
        self.last_processed_mc_block_id.as_ref()
    }
    fn set_last_processed_mc_block_id(&mut self, block_id: BlockId) {
        self.last_processed_mc_block_id = Some(block_id);
    }

    fn last_collated_mc_block_id(&self) -> Option<&BlockId> {
        self.last_collated_mc_block_id.as_ref()
    }
    fn set_last_collated_mc_block_id(&mut self, block_id: BlockId) {
        self.last_collated_mc_block_id = Some(block_id);
    }

    /// (TODO) Check sync status between mempool and blockchain state
    /// and pause collation when we are far behind other nodesб
    /// jusct sync blcoks from blockchain
    pub async fn process_new_anchor_from_mempool(
        &mut self,
        _anchor: Arc<MempoolAnchor>,
    ) -> Result<()> {
        //TODO: make real implementation, currently does nothing
        Ok(())
    }

    /// Process new master block from blockchain:
    /// 1. Load block state
    /// 2. Notify mempool about new master block
    /// 3. Enqueue collation sessions refresh task
    pub async fn process_mc_block_from_bc(&self, mc_block_id: BlockId) -> Result<()> {
        // check if we should skip this master block from the blockchain
        // because it is not far ahead of last collated by ourselves
        if !self.should_process_mc_block_from_bc(&mc_block_id) {
            return Ok(());
        }

        // request mc state for this master block
        //TODO: should await state and schedule processing in async task
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

    /// 1. Skip if it was already processed before
    /// 2. Skip if it is not far ahead of last collated by ourselves
    fn should_process_mc_block_from_bc(&self, mc_block_id: &BlockId) -> bool {
        let (seqno_delta, is_equal) =
            Self::compare_mc_block_with(mc_block_id, self.last_processed_mc_block_id());
        // check if already processed before
        let already_processed_before = is_equal || seqno_delta < 0;
        if already_processed_before {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Should NOT process mc block ({}) from bc: it was already processed before",
                mc_block_id.as_short_id(),
            );

            return false;
        } else {
            let last_collated_mc_block_id_opt = self.last_collated_mc_block_id();
            if last_collated_mc_block_id_opt.is_some() {
                let (seqno_delta, _) =
                    Self::compare_mc_block_with(mc_block_id, self.last_collated_mc_block_id());
                // check if need await own collated block
                if seqno_delta <= self.config.max_mc_block_delta_from_bc_to_await_own {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        r#"Should NOT process mc block ({}) from bc: seqno_delta = {}",
                        max_mc_block_delta_from_bc_to_await_own = {}"#,
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
        //TODO: consider block shard?
        let (seqno_delta, is_equal) = match other_mc_block_id_opt {
            None => (0, false),
            Some(other_mc_block_id) => (
                mc_block_id.seqno as i32 - other_mc_block_id.seqno as i32,
                mc_block_id != other_mc_block_id,
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
    fn check_if_mc_block_not_ahead_last_processed(&self, mc_block_id: &BlockId) -> bool {
        //TODO: consider block shard?
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
    pub async fn refresh_collation_sessions(
        &mut self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Trying to refresh collation sessions by mc state for block ({})...",
            mc_state.block_id().as_short_id()
        );

        //TODO: Possibly we have already updated collation sessions for this master block,
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
            //TODO: consider split and merge
            new_shards_info.insert(shard_id, vec![top_block]);
        }

        // update shards in msgs queue
        let current_shards_ids = self.active_collation_sessions.keys().collect();
        let new_shards_ids = new_shards_info.keys().collect();
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
        //let full_validators_set = mc_state.config_params()?.get_current_validator_set()?;
        //STUB: return dummy validator set
        let full_validators_set = ValidatorSet {
            utime_since: 0,
            utime_until: 0,
            main: std::num::NonZeroU16::MIN,
            total_weight: 0,
            list: vec![],
        };

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

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}",
            sessions_to_keep.keys(),
        );
        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will start new collation sessions: {:?}",
            sessions_to_start.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        );

        // store existing sessions that we should keep
        self.active_collation_sessions = sessions_to_keep;

        // we may have sessions to finish, collators to stop, and sessions to start
        // additionally we may have some active collators
        // for each new session we should check if current node should collate,
        // then stop collators if should not, otherwise start missing collators
        let cc_config = mc_state_extra.config.get_catchain_config()?;
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            let (subset, hash_short) = full_validators_set
                .compute_subset(shard_id, &cc_config, new_session_seqno)
                .ok_or(anyhow!(
                    "Error calculating subset of collators for the session (shard_id = {}, seqno = {})",
                    shard_id,
                    new_session_seqno,
                ))?;

            //STUB: create subset with only us
            let subset = vec![ValidatorDescription {
                public_key: self.config.key_pair.public_key.to_bytes().into(),
                adnl_addr: Some(self.config.key_pair.public_key.to_bytes().into()),
                weight: 90,
                prev_total_weight: 90,
                mc_seqno_since: 0,
            }];

            let local_pubkey_opt = find_us_in_collators_set(&self.config, &subset);

            let new_session_info = Arc::new(CollationSessionInfo::new(
                new_session_seqno,
                ValidatorSubsetInfo {
                    validators: subset,
                    short_hash: hash_short,
                },
                Some(self.config.key_pair),
            ));

            if let Some(_local_pubkey) = local_pubkey_opt {
                if let Entry::Vacant(entry) = self.active_collators.entry(shard_id) {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "There is no active collator for collation session {}. Will start it",
                        shard_id,
                    );
                    let collator = C::start(
                        self.dispatcher.clone(),
                        self.mq_adapter.clone(),
                        self.mpool_adapter.clone(),
                        self.state_node_adapter.clone(),
                        shard_id,
                        prev_blocks_ids,
                        mc_state.clone(),
                    )
                    .await;
                    entry.insert(Arc::new(collator));
                }

                // notify validator, it will start overlay initialization
                self.validator
                    .enqueue_add_session(Arc::new(new_session_info.clone().try_into()?))
                    .await?;
            } else if let Some(collator) = self.active_collators.remove(&shard_id) {
                to_stop_collators.insert((shard_id, new_session_seqno), collator);
            }

            //TODO: possibly do not need to store collation sessions if we do not collate in them
            self.active_collation_sessions
                .insert(shard_id, new_session_info);
        }

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will finish outdated collation sessions: {:?}",
            to_finish_sessions.keys(),
        );

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

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will stop collators for sessions that we do not serve: {:?}",
            to_stop_collators.keys(),
        );

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
        self.collation_sessions_to_finish.remove(&finish_key);
        Ok(())
    }

    /// Remove stopped collator from cache
    pub async fn process_collator_stopped(&mut self, stop_key: CollationSessionId) -> Result<()> {
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
    pub async fn process_block_candidate(
        &mut self,
        collation_result: BlockCollationResult,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start processing block candidate (id: {}, chain_time: {})...",
            collation_result.candidate.block_id().as_short_id(),
            collation_result.candidate.chain_time(),
        );

        // find session related to this block by shard
        let session_info = self
            .active_collation_sessions
            .get(collation_result.candidate.shard_id())
            .ok_or(anyhow!(
                "There is no active collation session for the shard that block belongs to"
            ))?
            .clone();

        let candidate_chain_time = collation_result.candidate.chain_time();
        let candidate_id = *collation_result.candidate.block_id();

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Saving block candidate to cache (id: {}, chain_time: {})...",
            candidate_id.as_short_id(),
            candidate_chain_time,
        );
        self.store_candidate(collation_result.candidate)?;

        // send validation task to validator
        // we need to send session info with the collators list to the validator
        // to understand whom we must ask for signatures
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Enqueueing block candidate validation (id: {}, chain_time: {})...",
            candidate_id.as_short_id(),
            candidate_chain_time,
        );
        let current_collator_keypair = self.config.key_pair;
        self.validator
            .enqueue_candidate_validation(
                candidate_id,
                session_info.seqno(),
                current_collator_keypair,
            )
            .await?;

        // chek if master block min interval elapsed and it needs to collate new master block
        if !candidate_id.shard.is_masterchain() {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will check if master block interval elapsed by chain time from candidate (id: {}, chain_time: {})",
                candidate_id.as_short_id(),
                candidate_chain_time,
            );
            if let Some(next_mc_block_chain_time) = self
                .update_last_collated_chain_time_and_check_mc_block_interval(
                    candidate_id.shard,
                    candidate_chain_time,
                )
            {
                self.enqueue_mc_block_collation(next_mc_block_chain_time, Some(candidate_id))
                    .await?;
            }
        } else {
            // store last master block chain time
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Candidate (id: {}, chain_time: {}) is a master block, will update `last_mc_block_chain_time`",
                candidate_id.as_short_id(),
                candidate_chain_time,
            );
            self.set_last_mc_block_chain_time(candidate_chain_time);
        }

        // execute master block processing routines
        if candidate_id.shard.is_masterchain() {
            tracing::debug!(
                target: tracing_targets::COLLATION_MANAGER,
                "Candidate (id: {}, chain_time: {}) is a master block, will notify mempool and equeue collation sessions refresh",
                candidate_id.as_short_id(),
                candidate_chain_time,
            );

            self.set_last_collated_mc_block_id(candidate_id);

            let new_mc_state = ShardStateStuff::from_state(
                candidate_id,
                collation_result.new_state,
                &self.state_tracker,
            )?;

            Self::notify_mempool_about_mc_block(self.mpool_adapter.clone(), new_mc_state.clone())
                .await?;

            self.dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    refresh_collation_sessions,
                    new_mc_state
                ))
                .await?;
        }

        Ok(())
    }

    /// Send master state related to master block to mempool (it may perform gc or nodes rotation)
    async fn notify_mempool_about_mc_block(
        mpool_adapter: Arc<MP>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        //TODO: in current implementation CollationProcessor should not notify mempool
        //      about one master block more than once, but better to handle repeated request here or at mempool
        mpool_adapter
            .enqueue_process_new_mc_block_state(mc_state)
            .await
    }

    /// 1. Store last collated chain time from anchor and check if master block interval elapsed in each shard
    /// 2. If true, schedule master block collation
    pub async fn process_empty_skipped_anchor(
        &mut self,
        shard_id: ShardIdent,
        anchor: Arc<MempoolAnchor>,
    ) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will check if master block interval elapsed by chain time {} from empty anchor {}",
            anchor.chain_time(),
            anchor.id(),
        );
        if let Some(next_mc_block_chain_time) = self
            .update_last_collated_chain_time_and_check_mc_block_interval(
                shard_id,
                anchor.chain_time(),
            )
        {
            self.enqueue_mc_block_collation(next_mc_block_chain_time, None)
                .await?;
        }
        Ok(())
    }

    /// 1. (TODO) Store last collated chain time from anchor
    /// 2. (TODO) Check if master block interval expired in each shard
    /// 3. Return chain time for master block collation if interval expired
    fn update_last_collated_chain_time_and_check_mc_block_interval(
        &mut self,
        _shard_id: ShardIdent,
        chain_time: u64,
    ) -> Option<u64> {
        //TODO: make real implementation

        //TODO: idea is to store for each shard each chain time and related shard block
        //      that expired master block interval. So we will have a list of such chain times.
        //      Then we can collate master block if interval expired in all shards.
        //      We should take the max chain time among first that expired the masterblock interval in each shard
        //      then we take shard blocks which chain time less then determined max

        //STUB: when we work with only one shard we can check for master block interval easier
        let elapsed = chain_time - self.last_mc_block_chain_time();
        let check = elapsed > self.config.mc_block_min_interval_ms;

        if check {
            // additionally check `next_mc_block_chain_time`
            // probably the master block collation was already enqueued
            let elapsed = chain_time - self.next_mc_block_chain_time();
            let check = elapsed > self.config.mc_block_min_interval_ms;
            if check {
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block interval is {}ms, chain time elapsed {}ms from last one - will collate next",
                    self.config.mc_block_min_interval_ms, elapsed,
                );
                return Some(chain_time);
            } else {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Elapsed chain time {}ms has not expired master block interval {}ms - do not need to collate next master block",
                    elapsed, self.config.mc_block_min_interval_ms,
                );
            }
        }

        None
    }

    /// Find top shard blocks in cacche for the next master block collation
    fn detect_top_shard_blocks_ids_for_mc_block(
        &self,
        _next_mc_block_chain_time: u64,
        _trigger_shard_block_id: Option<BlockId>,
    ) -> Vec<BlockId> {
        //TODO: make real implementation (see comments in `enqueue_mc_block_collation``)

        //STUB: when we work with only one shard we can just get the last shard block
        //      because collator manager will try run master block collation before
        //      before processing any next candidate from the shard collator
        //      because of dispatcher tasks queue
        let res = self
            .blocks_cache
            .shards
            .iter()
            .filter_map(|(_, shard_cache)| shard_cache.last_key_value().map(|(_, v)| *v.block_id()))
            .collect::<Vec<_>>();

        res
    }

    /// (TODO) Enqueue master block collation task. Will determine top shard blocks for this collation
    async fn enqueue_mc_block_collation(
        &mut self,
        next_mc_block_chain_time: u64,
        trigger_shard_block_id: Option<BlockId>,
    ) -> Result<()> {
        //TODO: make real implementation

        // get masterchain collator if exists
        let Some(mc_collator) = self.active_collators.get(&ShardIdent::MASTERCHAIN).cloned() else {
            bail!("Masterchain collator is not started yet!");
        };

        //TODO: How to choose top shard blocks for master block collation when they are collated async and in parallel?
        //      We know the last anchor (An) used in shard (ShA) block that causes master block collation,
        //      so we search for block from other shard (ShB) that includes the same anchor (An).
        //      Or the first from previouses (An-x) that includes externals for that shard (ShB)
        //      if all next including required one ([An-x+1, An]) do not contain externals for shard (ShB).

        let top_shard_blocks_ids = self.detect_top_shard_blocks_ids_for_mc_block(
            next_mc_block_chain_time,
            trigger_shard_block_id,
        );

        //TODO: We should somehow collect externals for masterchain during the shard blocks collation
        //      or pull them directly when collating master

        self.set_next_mc_block_chain_time(next_mc_block_chain_time);

        let _tracing_top_shard_blocks_descr = top_shard_blocks_ids
            .iter()
            .map(|id| id.as_short_id().to_string())
            .collect::<Vec<_>>();

        mc_collator
            .equeue_do_collate(next_mc_block_chain_time, top_shard_blocks_ids)
            .await?;

        tracing::info!(
            target: tracing_targets::COLLATION_MANAGER,
            "Master block collation enqueued (next_chain_time: {}, top_shard_blocks_ids: {:?})",
            next_mc_block_chain_time,
            _tracing_top_shard_blocks_descr.as_slice(),
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
            //TODO: implement more graceful reaction on invalid block
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
        //TODO: in future we may store to cache a block received from blockchain before,
        //      then it will exist in cache when we try to store collated candidate
        //      but the `root_hash` may differ, so we have to handle such a case

        let candidate_id = *candidate.block_id();
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
        //TODO: handle when master block link exist but there is not block itself
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
        tracing::info!(
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

    /// Find and restore block entries in cache updating sync statuses
    async fn restore_blocks_in_cache(
        &mut self,
        blocks_to_restore: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        let _tracing_blocks_descr = blocks_to_restore
            .iter()
            .map(|b| b.entry.key.to_string())
            .collect::<Vec<_>>();
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
                let dispatcher = self.dispatcher.clone();
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
            //TODO: make proper panic and error processing without waiting for spawned task
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
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ()>>,
        mq_adapter: Arc<MQ>,
        state_node_adapter: Arc<ST>,
        mut blocks_to_send: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        //TODO: it is better to send each block separately, but it will be more tricky to handle the correct cleanup

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
                    //TODO: handle different errors types
                    if let Err(err) = state_node_adapter.accept_block(block_for_sync).await {
                        tracing::warn!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Block ({}) sync: was not accepted. err: {:?}",
                            block_to_send.entry.candidate.block_id().as_short_id(),
                            err,
                        );
                        should_restore_blocks_in_cache = true;
                        break;
                    } else {
                        tracing::debug!(
                            target: tracing_targets::COLLATION_MANAGER,
                            "Block ({}) sync: was successfully sent to sync",
                            block_to_send.entry.candidate.block_id().as_short_id(),
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
                //TODO: handle if diff does not exist
                if let Err(err) = mq_adapter
                    .commit_diff(&sent_block.entry.candidate.block_id().as_short_id())
                    .await
                {
                    tracing::warn!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block ({}) sync: error committing message queue diff: {:?}",
                        sent_block.entry.candidate.block_id().as_short_id(),
                        err,
                    );
                    should_restore_blocks_in_cache = true;
                    break;
                } else {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Block ({}) sync: message queue diff was committed",
                        sent_block.entry.candidate.block_id().as_short_id(),
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
                    restore_blocks_in_cache,
                    blocks_to_send
                ))
                .await?;
            //TODO: should implement resending for restored blocks
        }

        Ok(())
    }
}
