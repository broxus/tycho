use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};

use crate::{
    collator::Collator,
    method_to_async_task_closure,
    msg_queue::{MessageQueueAdapter, QueueIterator},
    state_node::StateNodeAdapter,
    types::{
        ext_types::{BlockIdExt, ShardIdent, ValidatorSet},
        BlockCandidate, CollationSessionInfo, CollatorSubset, ShardStateStuff, ValidatedBlock,
    },
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
    validator::Validator,
};

pub enum CollationProcessorTaskResult {
    Void,
}
pub(super) struct CollationProcessor<C, V, MQ, ST>
where
    C: Collator,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    ST: StateNodeAdapter,
{
    dispatcher: Arc<AsyncQueuedDispatcher<Self, CollationProcessorTaskResult>>,
    state_node_adapter: Arc<ST>,
    //TODO: possibly use V because manager may not need a ref to validator
    validator: Arc<V>,

    mq_adapter: MQ,
    active_collation_sessions: HashMap<ShardIdent, Arc<CollationSessionInfo>>,
    collation_sessions_to_finish: Vec<Arc<CollationSessionInfo>>,
    active_collators: HashMap<ShardIdent, C>,
    collators_to_stop: Vec<C>,
}

impl<C, V, MQ, ST> CollationProcessor<C, V, MQ, ST>
where
    C: Collator,
    V: Validator<ST>,
    MQ: MessageQueueAdapter,
    ST: StateNodeAdapter,
{
    pub fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, CollationProcessorTaskResult>>,
        state_node_adapter: Arc<ST>,
        validator: Arc<V>,
    ) -> Self {
        Self {
            dispatcher,
            state_node_adapter,
            validator,
            mq_adapter: MQ::new(),
            active_collation_sessions: HashMap::new(),
            collation_sessions_to_finish: vec![],
            active_collators: HashMap::new(),
            collators_to_stop: vec![],
        }
    }

    /// Process new master block from blockchain:
    /// 1. Update shards list and start collation sessions for each shard
    /// 2. (TODO) Notify mempool about new master block
    pub async fn process_mc_block_from_bc(
        &self,
        mc_block_id: BlockIdExt,
    ) -> Result<CollationProcessorTaskResult> {
        // request mc state for this master block
        let receiver = self.state_node_adapter.request_state(mc_block_id).await?;

        // queue collation sessions refresh task when state received
        let dispatcher = self.dispatcher.clone();
        receiver.process_on_recv(|mc_state| async move {
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

    /// Get shards info from the state, then start new or update existing
    /// collation sessions for these shards.
    /// Every collation session runs internal async collation process.
    #[deprecated(note = "should replace stub")]
    pub async fn refresh_collation_sessions(
        &mut self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<CollationProcessorTaskResult> {
        // get shards info

        // for each shard start a new session if it does not exist,
        // run collator for each new active session if we are on the list of session collators,
        // queue to finish outdated sessions,
        // queue to stop collators of merged shards
        let session_next_seq_no = 1;
        let full_shard_id = ShardIdent::new_full(0);
        let session_info = CollationSessionInfo::new(
            session_next_seq_no,
            CollatorSubset::create(ValidatorSet {}, &full_shard_id, session_next_seq_no),
        );
        let session_info = Arc::new(session_info);

        if let Some(prev_session_info) = self
            .active_collation_sessions
            .insert(full_shard_id, session_info.clone())
        {
            self.collation_sessions_to_finish.push(prev_session_info);
        }

        todo!()

        // finally we will have initialized `active_collation_sessions` and `active_collators`
        // which run async block collations processes
    }

    /// Process collated block candidate
    /// 1. Schedule block validation
    /// 2. (TODO) Store block info
    /// 3. (TODO) Check if the master block interval elapsed (according to chain time) and schedule collation
    /// 4. (TODO) If master block then update last master block chain time
    /// 5. (TODO) Notify mempool about new master block (it may perform gc or node rotation)
    /// 6. (TODO) Execute master block processing routines like for the block from bc
    pub async fn process_block_candidate(
        &self,
        candidate: BlockCandidate,
    ) -> Result<CollationProcessorTaskResult> {
        // find session related to this block by shard
        let session_info = self
            .active_collation_sessions
            .get(candidate.shard_id())
            .ok_or(anyhow!(
                "There is no active collation session for the shard that block belongs to"
            ))?;

        // we need to send session info with the collators list to the validator
        // to understand whom we must ask for signatures

        // send validation task to validator
        self.validator
            .enqueue_candidate_validation(candidate, session_info.clone())
            .await?;

        Ok(CollationProcessorTaskResult::Void)
    }

    /// Process validated block
    /// 1. Process invalid block (currently, just panic)
    /// 2. (TODO) Update block info that it validated and valid
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

        // process valid block
        if validated_block.is_master() {
            self.process_valid_master_block(validated_block).await?;
        } else {
            self.process_valid_shard_block(validated_block).await?;
        }

        Ok(CollationProcessorTaskResult::Void)
    }

    /// Process validated and valid master block
    /// 1. (TODO) Check if all including shard blocks validated, return if not
    /// 2. (TODO) Request these shard blocks from validator, send master and shard blocks to state node
    /// 3. (TODO) Commit msg queue diffs related to these shard and master blocks
    async fn process_valid_master_block(&mut self, validated_block: ValidatedBlock) -> Result<()> {
        todo!()
    }

    /// Process validated and valid shard block
    /// 1. (TODO) Try find master block info and execute steps 1-3 from [`CollationProcessor::process_valid_master_block`]
    async fn process_valid_shard_block(&mut self, validated_block: ValidatedBlock) -> Result<()> {
        todo!()
    }
}
