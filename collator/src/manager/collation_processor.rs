use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};

use everscale_types::{
    cell::HashBytes,
    models::{BlockId, ShardIdent},
};
use tycho_block_util::{block::ValidatorSubsetInfo, state::ShardStateStuff};

use crate::{
    collator::Collator,
    mempool::MempoolAdapter,
    method_to_async_task_closure,
    msg_queue::MessageQueueAdapter,
    state_node::StateNodeAdapter,
    types::{
        BlockCandidate, BlockCollationResult, CollationConfig, CollationSessionId,
        CollationSessionInfo, ValidatedBlock,
    },
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
    validator::Validator,
};

use super::{
    types::{
        BlockCandidateContainer, BlockCandidateToSend, McBlockSubgraphToSend, SendSyncStatus,
        ShardStateStuffExt,
    },
    utils::{build_block_stuff_for_sync, find_us_in_collators_set},
};

pub enum CollationProcessorTaskResult {
    Void,
}
pub(super) struct CollationProcessor<C, V, MQ, MP, ST>
where
    C: Collator<MQ, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    config: Arc<CollationConfig>,

    dispatcher: Arc<AsyncQueuedDispatcher<Self, CollationProcessorTaskResult>>,
    mp_adapter: Arc<MP>,
    state_node_adapter: Arc<ST>,
    mq_adapter: Arc<MQ>,

    //TODO: possibly use V because manager may not need a ref to validator
    validator: Arc<V>,

    active_collation_sessions: HashMap<ShardIdent, Arc<CollationSessionInfo>>,
    collation_sessions_to_finish: HashMap<CollationSessionId, Arc<CollationSessionInfo>>,
    active_collators: HashMap<ShardIdent, C>,
    collators_to_stop: HashMap<CollationSessionId, C>,
}

impl<C, V, MQ, MP, ST> CollationProcessor<C, V, MQ, MP, ST>
where
    C: Collator<MQ, ST>,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    pub fn new(
        config: Arc<CollationConfig>,
        dispatcher: Arc<AsyncQueuedDispatcher<Self, CollationProcessorTaskResult>>,
        mp_adapter: Arc<MP>,
        state_node_adapter: Arc<ST>,
        validator: Arc<V>,
    ) -> Self {
        Self {
            config,
            dispatcher,
            mp_adapter,
            state_node_adapter,
            mq_adapter: Arc::new(MQ::new()),
            validator,
            active_collation_sessions: HashMap::new(),
            collation_sessions_to_finish: HashMap::new(),
            active_collators: HashMap::new(),
            collators_to_stop: HashMap::new(),
        }
    }

    /// Return last master block chain time
    fn last_mc_block_chain_time(&self) -> u64 {
        todo!()
    }

    /// Update last master block chain time
    fn update_last_mc_block_chain_time(&mut self, last_mc_block_chain_time: u64) {
        todo!()
    }

    /// Process new master block from blockchain:
    /// 1. Load block state
    /// 2. Notify mempool about new master block
    /// 3. Enqueue collation sessions refresh task
    pub async fn process_mc_block_from_bc(
        &self,
        mc_block_id: BlockId,
    ) -> Result<CollationProcessorTaskResult> {
        // request mc state for this master block
        let receiver = self.state_node_adapter.request_state(mc_block_id).await?;

        // when state received execute master block processing routines
        let mp_adapter = self.mp_adapter.clone();
        let dispatcher = self.dispatcher.clone();
        receiver.process_on_recv(|mc_state| async move {
            Self::notify_mempool_about_mc_block(mp_adapter, mc_state.clone()).await?;

            dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    refresh_collation_sessions,
                    mc_state
                ))
                .await
        });

        Ok(CollationProcessorTaskResult::Void)
    }

    /// Check if collation sessions initialized and try to force refresh them if they not.
    /// This needed when start from zerostate. State node adapter will be initialized after
    /// zerostate load and won't fire `[StateNodeListener::on_mc_block_event()]` for the 1 block.
    /// Also when whole network was restarted then nobody will produce next master block and we need
    /// to start collation sessions based on the actual state
    pub async fn check_refresh_collation_sessions(&self) -> Result<CollationProcessorTaskResult> {
        // the sessions list is not enpty so the collation process was already started from
        // actual state or incoming master block from blockchain
        if !self.active_collation_sessions.is_empty() {
            return Ok(CollationProcessorTaskResult::Void);
        }

        // here we will wait for last applied master block then process it
        // TODO: otherwise we can just request to resend last applied master block via `[StateNodeListener::on_mc_block_event()]`
        let last_mc_block_id = self
            .state_node_adapter
            .get_last_applied_mc_block_id()
            .await?;

        self.process_mc_block_from_bc(last_mc_block_id).await
    }

    /// Get shards info from the master state,
    /// then start missing sessions for these shards, or refresh existing.
    /// For each shard run collation process if current node is included in collators subset.
    pub async fn refresh_collation_sessions(
        &mut self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<CollationProcessorTaskResult> {
        let mc_extra = mc_state.state_extra()?;

        // get new shards info from updated master state
        let mut new_shards = HashMap::new();
        new_shards.insert(ShardIdent::MASTERCHAIN, vec![*mc_state.block_id()]);
        for shard in mc_extra.shards.iter() {
            let (shard_id, descr) = shard?;
            let top_block = BlockId {
                shard: shard_id,
                seqno: descr.seqno,
                root_hash: descr.root_hash,
                file_hash: descr.file_hash,
            };
            //TODO: consider split and merge
            new_shards.insert(shard_id, vec![top_block]);
        }

        // find out the actual collation session seqno from master state
        let new_session_seqno = mc_extra.validator_info.catchain_seqno;

        // we need full validators set to define the subset for each session and to check if current node should collate
        let full_validators_set = mc_state.config_params()?.get_current_validator_set()?;

        // compare with active sessions and detect new sessions to start and outdated sessions to finish
        let mut sessions_to_keep = HashMap::new();
        let mut sessions_to_start = vec![];
        let mut to_finish_sessions = HashMap::new();
        let mut to_stop_collators = HashMap::new();
        for shard_info in new_shards {
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

        // store existing sessions that we should keep
        self.active_collation_sessions = sessions_to_keep;

        // we may have sessions to finish, collators to stop, and sessions to start
        // additionally we may have some active collators
        // for each new session we should check if current node should collate,
        // then stop collators if should not, otherwise start missing collators
        let cc_config = mc_extra.config.get_catchain_config()?;
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            let (subset, hash_short) = full_validators_set
                .compute_subset(shard_id, &cc_config, new_session_seqno)
                .ok_or(anyhow!(
                    "Error calculating subset of collators for the session (shard_id = {}, seqno = {})",
                    shard_id,
                    new_session_seqno,
                ))?;

            if let Some(_local_pubkey) = find_us_in_collators_set(&self.config, &subset) {
                self.active_collators.entry(shard_id).or_insert_with(|| {
                    C::start(
                        self.dispatcher.clone(),
                        self.mq_adapter.clone(),
                        self.state_node_adapter.clone(),
                        shard_id,
                        prev_blocks_ids,
                    )
                });
            } else if let Some(collator) = self.active_collators.remove(&shard_id) {
                to_stop_collators.insert((shard_id, new_session_seqno), collator);
            }

            self.active_collation_sessions.insert(
                shard_id,
                Arc::new(CollationSessionInfo::new(
                    new_session_seqno,
                    ValidatorSubsetInfo {
                        validators: subset,
                        short_hash: hash_short,
                    },
                )),
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

        // equeue dangling collators stop tasks
        for (stop_key, collator) in to_stop_collators {
            collator.equeue_stop(stop_key).await?;
            self.collators_to_stop.insert(stop_key, collator);
        }

        Ok(CollationProcessorTaskResult::Void)

        // finally we will have initialized `active_collation_sessions` and `active_collators`
        // which run async block collations processes
    }

    /// Execute collation session finalization routines
    pub async fn finish_collation_session(
        &mut self,
        _session_info: Arc<CollationSessionInfo>,
        finish_key: CollationSessionId,
    ) -> Result<CollationProcessorTaskResult> {
        self.collation_sessions_to_finish.remove(&finish_key);
        Ok(CollationProcessorTaskResult::Void)
    }

    /// Remove stopped collator from cache
    pub async fn process_collator_stopped(
        &mut self,
        stop_key: CollationSessionId,
    ) -> Result<CollationProcessorTaskResult> {
        self.collators_to_stop.remove(&stop_key);
        Ok(CollationProcessorTaskResult::Void)
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
    ) -> Result<CollationProcessorTaskResult> {
        // find session related to this block by shard
        let session_info = self
            .active_collation_sessions
            .get(collation_result.candidate.shard_id())
            .ok_or(anyhow!(
                "There is no active collation session for the shard that block belongs to"
            ))?
            .clone();

        let candidate_chain_time = collation_result.candidate.chain_time();
        let candidate_id = collation_result.candidate.block_id().clone();

        //TODO: remove this when the Validator interface is changed - get candidate to pass then to validator
        let candidate = collation_result.candidate.clone();

        self.store_candidate(collation_result.candidate)?;

        // send validation task to validator
        // we need to send session info with the collators list to the validator
        // to understand whom we must ask for signatures
        self.validator
            .enqueue_candidate_validation(*candidate.block_id(), session_info)
            .await?;

        // chek if master block min interval elapsed and it needs to collate new master block
        if !candidate_id.shard.is_masterchain() {
            if candidate_chain_time - self.last_mc_block_chain_time()
                > self.config.mc_block_min_interval_ms
            {
                self.enqueue_mc_block_collation(Some(candidate_id)).await?;
            }
        } else {
            // store last master block chain time
            self.update_last_mc_block_chain_time(candidate_chain_time);
        }

        // execute master block processing routines
        if candidate_id.shard.is_masterchain() {
            let new_mc_state =
                ShardStateStuff::from_state(candidate_id, collation_result.new_state)?;

            Self::notify_mempool_about_mc_block(self.mp_adapter.clone(), new_mc_state.clone())
                .await?;

            self.dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    refresh_collation_sessions,
                    new_mc_state
                ))
                .await?;
        }

        Ok(CollationProcessorTaskResult::Void)
    }

    /// Send master state related to master block to mempool (it may perform gc or nodes rotation)
    async fn notify_mempool_about_mc_block(
        mp_adapter: Arc<MP>,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        mp_adapter
            .enqueue_process_new_mc_block_state(mc_state)
            .await
    }

    /// (TODO) Enqueue master block collation task. Will determine top shard blocks for this collation
    async fn enqueue_mc_block_collation(
        &self,
        trigger_shard_block_id: Option<BlockId>,
    ) -> Result<()> {
        //TODO: How to choose top shard blocks for master block collation when they are collated async and in parallel?
        //      We know the last anchor (An) used in shard (ShA) block that causes master block collation,
        //      so we search for block from other shard (ShB) that includes the same anchor (An).
        //      Or the first from previouses (An-x) that includes externals for that shard (ShB)
        //      if all next including required ([An-x+1, An]) do not contain externals for shard (ShB).
        todo!()
    }

    /// Process validated block
    /// 1. Process invalid block (currently, just panic)
    /// 2. Update block in cache with validation info
    /// 2. Execute processing for master or shard block
    pub async fn process_validated_block(
        &mut self,
        validated_block: ValidatedBlock,
    ) -> Result<CollationProcessorTaskResult> {
        // execute required actions if block invalid
        if !validated_block.is_valid() {
            //TODO: implement more graceful reaction on invalid block
            panic!("Block has collected more than 1/3 invalid signatures! Unable to continue collation process!")
        }

        let block_id = *validated_block.id();

        // update block in cache with signatures info
        self.store_block_validation_result(validated_block)?;

        // process valid block
        if block_id.shard.is_masterchain() {
            self.process_valid_master_block(&block_id).await?;
        } else {
            self.process_valid_shard_block(&block_id).await?;
        }

        Ok(CollationProcessorTaskResult::Void)
    }

    /// (TODO) Store block in a structure that allow to append signatures
    fn store_candidate(&mut self, candidate: BlockCandidate) -> Result<()> {
        todo!()
    }

    /// (TODO) Find block candidate in cache, append signatures info and return updated
    fn store_block_validation_result(
        &mut self,
        validated_block: ValidatedBlock,
    ) -> Result<&BlockCandidateContainer> {
        todo!()
    }

    /// (TODO) Remove block entries from cache and compact cache
    async fn cleanup_blocks_from_cache(
        &mut self,
        blocks_keys: Vec<HashBytes>,
    ) -> Result<CollationProcessorTaskResult> {
        todo!()
    }

    /// (TODO) Find and restore block entries in cache
    async fn restore_blocks_in_cache(
        &mut self,
        blocks_to_restore: Vec<BlockCandidateToSend>,
    ) -> Result<CollationProcessorTaskResult> {
        todo!()
    }

    /// Process validated and valid master block
    /// 1. Check if all included shard blocks validated, return if not
    /// 2. Send master and shard blocks to state node to sync
    async fn process_valid_master_block(&mut self, block_id: &BlockId) -> Result<()> {
        // extract master block with all shard blocks if valid, and process them
        if let Some(mc_block_subgraph_set) = self.extract_mc_block_subgraph_if_valid(block_id) {
            let mut blocks_to_send = mc_block_subgraph_set.shard_blocks;
            blocks_to_send.reverse();
            blocks_to_send.push(mc_block_subgraph_set.mc_block);

            // spawn async task to send all shard and master blocks
            tokio::spawn({
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
        }
        Ok(())
    }

    /// Process validated and valid shard block
    /// 1. (TODO) Try find master block info and execute [`CollationProcessor::process_valid_master_block`]
    async fn process_valid_shard_block(&mut self, block_id: &BlockId) -> Result<()> {
        todo!()
        // if let Some(mc_block_container) = self.travers_to_containing_mc_block_if_exists(block_id) {
        //     todo!()
        // }
        // Ok(())
    }

    /// (TODO) Find all shard blocks that form master block subgraph.
    /// Then extract and return them if all are valid
    fn extract_mc_block_subgraph_if_valid(
        &mut self,
        block_id: &BlockId,
    ) -> Option<McBlockSubgraphToSend> {
        // 1. Find current master block
        // 2. Find prev master block
        // 3. By the top shard blocks info find shard blocks of current master block
        // 4. Recursively find prev shard blocks until the end or top shard blocks of prev master reached
        // 5. If master block and all shard blocks valid the extrac them from entries and return
        todo!()
    }

    /// 1. Send shard blocks and master to sync to state node
    /// 2. Commit msg queue diffs related to these shard and master blocks
    /// 3. Clean up sent blocks entries from cache
    /// 4. Return all blocks to cache if got error (separate task will try to resend further)
    /// 5. Return `Error` if it seems to be unrecoverable
    async fn send_blocks_to_sync(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, CollationProcessorTaskResult>>,
        mq_adapter: Arc<MQ>,
        state_node_adapter: Arc<ST>,
        blocks_to_send: Vec<BlockCandidateToSend>,
    ) -> Result<()> {
        //TODO: it is better to send each block separately, but it will be more tricky to handle the correct cleanup

        // extract already synced blocks that were validated by existing blocks in the state
        // send other blocks to sync
        let mut should_restore_blocks_in_cache = false;
        let mut sent_blocks = vec![];
        for block_to_send in blocks_to_send.iter() {
            match block_to_send.send_sync_status {
                SendSyncStatus::Sent | SendSyncStatus::Synced => sent_blocks.push(block_to_send),
                _ => {
                    let block_for_sync = build_block_stuff_for_sync(&block_to_send.entry)?;
                    //TODO: handle and log error
                    if let Err(err) = state_node_adapter.accept_block(block_for_sync).await {
                        should_restore_blocks_in_cache = true;
                        break;
                    } else {
                        sent_blocks.push(block_to_send);
                    }
                }
            }
        }

        if !should_restore_blocks_in_cache {
            // commit queue diffs for each block
            for &sent_block in sent_blocks.iter() {
                //TODO: handle and log error
                if let Err(err) = mq_adapter
                    .commit_diff(sent_block.entry.candidate.block_id().clone())
                    .await
                {
                    should_restore_blocks_in_cache = true;
                    break;
                }
            }

            // do not clenup blocks if msg queue diffs commit was unsuccessful
            if !should_restore_blocks_in_cache {
                let sent_blocks_keys = sent_blocks
                    .iter()
                    .map(|b| b.entry.key.clone())
                    .collect::<Vec<_>>();
                dispatcher
                    .enqueue_task(method_to_async_task_closure!(
                        cleanup_blocks_from_cache,
                        sent_blocks_keys
                    ))
                    .await?;
            }
        }

        if should_restore_blocks_in_cache {
            // queue blocks restore task
            dispatcher
                .enqueue_task(method_to_async_task_closure!(
                    restore_blocks_in_cache,
                    blocks_to_send
                ))
                .await?;
        }

        Ok(())
    }
}
