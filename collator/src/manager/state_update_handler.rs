use std::collections::hash_map;
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result, anyhow};
use tokio::sync::Notify;
use tycho_block_util::block::{ValidatorSubsetInfo, calc_next_block_id_short};
use tycho_block_util::config::BlockchainConfigExt;
use tycho_types::models::{
    BlockId, GlobalCapabilities, IndexedValidatorDescription, ShardIdent, ValidatorSet,
};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{DashMapEntry, FastHashMap, FastHashSet};

use super::CollationManager;
use super::types::{ActiveCollator, CollatorJoinTask, CollatorState};
use super::utils::find_us_in_collators_set;
use crate::collator::{Collator, CollatorContext, CollatorFactory};
use crate::mempool::{MempoolAnchorId, StateUpdateContext};
use crate::tracing_targets;
use crate::types::{CollationSessionInfo, DebugIter, McData, ShardDescriptionShortExt};
use crate::utils::shard::calc_split_merge_actions;
use crate::validator::{AddSession, Validator};

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    /// Returns top processed to anchor id if delayed state was processed
    pub(super) async fn notify_to_mempool_and_process_delayed_mc_state_update(
        &self,
        block_id: &BlockId,
        skip_process_delayed_mc_state_update: bool,
    ) -> Result<Option<(MempoolAnchorId, Vec<CollatorJoinTask<CF::Collator>>)>> {
        let mut delayed_mc_data = None;
        {
            let mut guard = self.delayed_mc_state_update.lock();
            if let Some(mc_data) = guard.clone()
                && mc_data.block_id <= *block_id
            {
                // process delayed mc state only if committed block is equal
                if mc_data.block_id == *block_id {
                    delayed_mc_data = Some(mc_data);
                }

                // remove delayed mc state even if committed block is ahead
                *guard = None;
            }
        }
        if let Some(mc_data) = delayed_mc_data {
            self.notify_mc_state_update_to_mempool(mc_data.clone())
                .await?;

            // validation may finish when collation cancel task is running,
            // so we should not run new collation tasks
            let mut collator_tasks = vec![];
            if !skip_process_delayed_mc_state_update {
                collator_tasks = self
                    .process_mc_state_update(
                        mc_data.clone(),
                        ProcessMcStateUpdateMode::StartCollation {
                            reset_collators: false,
                        },
                    )
                    .await?;
            }

            Ok(Some((mc_data.top_processed_to_anchor, collator_tasks)))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn notify_mc_state_update_to_mempool(
        &self,
        mc_data: Arc<McData>,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            block_id = %mc_data.block_id.as_short_id(),
            "will notify master state update to mempool",
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

        let cx = Box::new(StateUpdateContext {
            mc_block_id: mc_data.block_id,
            mc_block_chain_time: mc_data.gen_chain_time,
            top_processed_to_anchor_id: mc_data.top_processed_to_anchor,
            consensus_info: mc_data.consensus_info,
            shuffle_validators: mc_data.config.get_collation_config()?.shuffle_mc_validators,
            consensus_config: mc_data.config.get_consensus_config()?,
            prev_validator_set,
            current_validator_set,
            next_validator_set,
        });

        self.mpool_adapter.handle_mc_state_update(cx).await
    }

    pub(super) async fn process_mc_state_update(
        &self,
        mc_data: Arc<McData>,
        mode: ProcessMcStateUpdateMode,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::info!(target: tracing_targets::COLLATION_MANAGER,
            ?mode,
            "will process master state update",
        );

        let mut collator_tasks = vec![];

        if !matches!(mode, ProcessMcStateUpdateMode::SkipProcess) {
            let global = mc_data.config.get_global_version()?;
            if self.config.supported_block_version >= global.version
                && global
                    .capabilities
                    .is_subset_of(self.config.supported_capabilities)
            {
                collator_tasks = self.refresh_collation_sessions(mc_data, mode).await?;
            } else {
                tracing::warn!(target: tracing_targets::COLLATION_MANAGER,
                    collator_supported_block_version = self.config.supported_block_version,
                    mc_block_version = global.version,
                    collator_supported_capabilities = ?self.config.supported_capabilities,
                    mc_block_capabilities = ?global.capabilities,
                    "Refresh collation sessions is skipped: collator does not support mc block version or capabilities",
                );
            }
        }

        Ok(collator_tasks)
    }

    /// Get shards and validator set info from the master state,
    /// then start missing collation sessions, finish outdated, resume actual.
    /// Start/stop/resume collators and validators.
    #[tracing::instrument(skip_all)]
    async fn refresh_collation_sessions(
        &self,
        mc_data: Arc<McData>,
        mode: ProcessMcStateUpdateMode,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Start refresh collation sessions by mc state ({})...",
            mc_data.block_id.as_short_id(),
        );

        let _histogram = HistogramGuard::begin("tycho_collator_refresh_collation_sessions_time");

        let mut collator_tasks = vec![];

        // NOTE: in case of processing delayed state update
        //      after the validation of key block with consensus config changed
        //      we may try to process the state that is older then already processed from bc

        // do not re-process this master block if it is lower then last processed or equal to it
        // but process a new version of block with the same seqno
        if !self.check_should_process_and_update_last_processed_mc_block(&mc_data.block_id) {
            return Ok(collator_tasks);
        }

        tracing::trace!(target: tracing_targets::COLLATION_MANAGER, "mc_data: {:?}", mc_data);

        // get new shards info from updated master state
        let mut new_shards_info = FastHashMap::default();
        new_shards_info.insert(ShardIdent::MASTERCHAIN, vec![mc_data.block_id]);
        for (shard_id, descr) in mc_data.shards.iter() {
            let top_block_id = descr.get_block_id(*shard_id);
            // TODO: consider split and merge
            new_shards_info.insert(*shard_id, vec![top_block_id]);
        }

        // apply split/merge actions
        self.apply_split_merge_actions(&new_shards_info)?;

        // find out the actual collation session start round from master state
        let catchain_seqno = mc_data.validator_info.catchain_seqno;
        let vset_switch_round = mc_data.consensus_info.vset_switch_round;
        let validation_session_id = (catchain_seqno, vset_switch_round);

        // we need full validators set to define the subset for each session and to check if current node should collate
        let raw_validators_set = mc_data.config.get_current_validator_set_raw()?;
        let full_validators_set = raw_validators_set.parse::<ValidatorSet>()?;
        tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
            "full_validators_set: since={}, until={}, main={}, total_weight={}, list={:?}",
            full_validators_set.utime_since, full_validators_set.utime_until,
            full_validators_set.main, full_validators_set.total_weight,
            DebugIter(full_validators_set.list.iter().map(|i| i.public_key)),
        );
        let collation_config = mc_data.config.get_collation_config()?;
        let mut subset_cache = FastHashMap::new();
        let mut get_validator_subset = |shard_id| match subset_cache.entry(shard_id) {
            hash_map::Entry::Vacant(entry) => {
                let (subset, hash_short) = full_validators_set
                    .compute_mc_subset_indexed(
                        vset_switch_round,
                        collation_config.shuffle_mc_validators,
                    )
                    .ok_or_else(|| {
                        anyhow!(
                            "Error calculating subset of validators for catchain session \
                            (shard_id = {}, vset_switch_round = {vset_switch_round})",
                            ShardIdent::MASTERCHAIN,
                        )
                    })?;

                let subset: FastHashMap<[u8; 32], IndexedValidatorDescription> = subset
                    .into_iter()
                    .map(|vldr| (vldr.desc.public_key.into(), vldr))
                    .collect();
                let subset = Arc::new(subset);

                entry.insert((subset.clone(), hash_short));
                Ok((subset, hash_short))
            }
            hash_map::Entry::Occupied(entry) => {
                let (subset, hash_short) = entry.get();
                Result::<_>::Ok((subset.clone(), *hash_short))
            }
        };

        // detect sessions and collators to start and to finish
        let mut sessions_to_keep = Vec::new();
        let mut sessions_to_start = Vec::new();
        let mut to_finish_sessions = Vec::new();
        let mut to_stop_collators = Vec::new();
        {
            let mut active_collation_sessions_guard = self.active_collation_sessions.write();
            let mut missed_shards_ids: FastHashSet<_> =
                active_collation_sessions_guard.keys().cloned().collect();
            for (shard_id, block_ids) in new_shards_info {
                missed_shards_ids.remove(&shard_id);

                // check if current node is in subset
                let (subset, hash_short) = get_validator_subset(shard_id)?;
                let local_pubkey = find_us_in_collators_set(&self.keypair, &subset);

                if local_pubkey.is_none() {
                    tracing::debug!(
                        target: tracing_targets::COLLATION_MANAGER,
                        public_key = %self.keypair.public_key,
                        hash_short,
                        "Current node was not authorized to collate shard {}. Use TRACE to see subset",
                        shard_id,
                    );
                    tracing::trace!(target: tracing_targets::COLLATION_MANAGER,
                        subset = ?DebugIter(subset.values()),
                        "Current node was not authorized to collate shard {}",
                        shard_id,
                    );
                    metrics::gauge!("tycho_node_in_current_vset").set(0);
                } else {
                    metrics::gauge!("tycho_node_in_current_vset").set(1);
                }

                match active_collation_sessions_guard.entry(shard_id) {
                    hash_map::Entry::Occupied(entry) => {
                        let existing_session_info = entry.get().clone();
                        if local_pubkey.is_some() {
                            // start new session when seqno changed or subset changed for the same seqno
                            if existing_session_info.collators().short_hash == hash_short
                                && existing_session_info.get_validation_session_id()
                                    == validation_session_id
                            {
                                sessions_to_keep.push((shard_id, existing_session_info, block_ids));
                            } else {
                                to_finish_sessions.push(entry.remove());
                                sessions_to_start.push((shard_id, block_ids));
                            }
                        } else {
                            to_finish_sessions.push(entry.remove());
                            if let Some((_, collator)) = self.active_collators.remove(&shard_id) {
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

            // if we still have some active sessions that do not match with new shards and validator subset
            // then we need to finish them and stop their collators
            for shard_id in missed_shards_ids {
                if let Some(existing_session_info) =
                    active_collation_sessions_guard.remove(&shard_id)
                {
                    to_finish_sessions.push(existing_session_info.clone());
                    if let Some((_, collator)) = self.active_collators.remove(&shard_id) {
                        to_stop_collators.push((existing_session_info, collator));
                    }
                }
            }
        }

        if !sessions_to_start.is_empty() {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "Will start new collation sessions: {:?}",
                DebugIter(sessions_to_start.iter().map(|(s, _)| (s, validation_session_id))),
            );
        }

        // we may have sessions to finish, collators to stop, and sessions to start,
        // and we start missing collators for new sessions
        for (shard_id, prev_blocks_ids) in sessions_to_start {
            let (subset, hash_short) = get_validator_subset(shard_id)?;

            let new_session_info = Arc::new(CollationSessionInfo::new(
                shard_id,
                catchain_seqno,
                vset_switch_round,
                ValidatorSubsetInfo {
                    validators: subset.values().cloned().collect(),
                    short_hash: hash_short,
                },
                Some(self.keypair.clone()),
            ));

            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "new_session_info: {:?}",
                new_session_info,
            );

            let next_block_id_short = calc_next_block_id_short(&prev_blocks_ids);
            let add_validator_session = || {
                // need to add validation session only for masterchain blocks,
                // shard blocks are not being validated
                if shard_id.is_masterchain() {
                    self.validator.add_session(AddSession {
                        shard_ident: shard_id,
                        session_id: new_session_info.get_validation_session_id(),
                        start_block_seqno: next_block_id_short.seqno,
                        vset_hash: raw_validators_set.repr_hash(),
                        validators: &new_session_info.collators().validators,
                    })?;
                }
                anyhow::Ok(())
            };

            match self.active_collators.entry(shard_id) {
                DashMapEntry::Occupied(_) => {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "Active collator exists for collation session {:?}. Will resume it",
                        new_session_info.id(),
                    );
                    add_validator_session()?;
                    sessions_to_keep.push((shard_id, new_session_info.clone(), prev_blocks_ids));
                }
                DashMapEntry::Vacant(entry) => {
                    tracing::info!(
                        target: tracing_targets::COLLATION_MANAGER,
                        "There is no active collator for collation session {:?}. Will start it",
                        new_session_info.id(),
                    );

                    let cancel_collation_notify = Arc::new(Notify::new());

                    match self
                        .collator_factory
                        .create(CollatorContext {
                            mq_adapter: self.mq_adapter.clone(),
                            mpool_adapter: self.mpool_adapter.clone(),
                            state_node_adapter: self.state_node_adapter.clone(),
                            config: self.config.clone(),
                            collation_session: new_session_info.clone(),
                            zerostate_id: *self.state_node_adapter.zerostate_id(),
                            shard_id,
                            prev_blocks_ids: prev_blocks_ids.clone(),
                            mempool_config_override: self.mempool_config_override.clone(),
                            cancel_collation: cancel_collation_notify.clone(),
                        })
                        .await
                    {
                        Err(err) => {
                            tracing::error!(target: tracing_targets::COLLATION_MANAGER,
                                session_id = ?new_session_info.id(),
                                ?err,
                                "error creating collator"
                            );
                            // return error to stop node
                            return Err(err);
                        }
                        Ok(collator) => {
                            let collator = Box::new(collator);
                            add_validator_session()?;

                            // init collator
                            let collator =
                                if let ProcessMcStateUpdateMode::StartCollation { .. } = mode {
                                    let init_collator_task = JoinTask::new(
                                        collator.init(prev_blocks_ids, mc_data.clone()),
                                    );
                                    collator_tasks.push(init_collator_task);
                                    None
                                } else {
                                    Some(collator)
                                };

                            entry.insert(ActiveCollator {
                                state: if collator.is_none() {
                                    CollatorState::Active
                                } else {
                                    CollatorState::Waiting
                                },
                                collator,
                                cancel_collation: cancel_collation_notify,
                            });
                        }
                    }
                }
            }

            self.active_collation_sessions
                .write()
                .insert(shard_id, new_session_info);
        }

        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Will keep existing collation sessions: {:?}",
            DebugIter(sessions_to_keep.iter().map(|(_, s, _)| s.id())),
        );

        // update master state in existing collators and resume collation
        for (shard_id, new_session_info, prev_blocks_ids) in sessions_to_keep {
            if let ProcessMcStateUpdateMode::StartCollation { reset_collators } = mode {
                let collator = self
                    .take_collator_and_set_state_if(
                        &shard_id,
                        |_| true,
                        |ac| ac.state = CollatorState::Active,
                    )
                    .with_context(|| {
                        format!(
                            "Collator for shard should exist for active session {:?}",
                            new_session_info.id()
                        )
                    })?
                    .context("an empty check should pass")?;
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Resuming collation attempts in shard session {:?}",
                    new_session_info.id(),
                );
                // resume collation
                let resume_collation_task = JoinTask::new(collator.resume_collation(
                    mc_data.clone(),
                    reset_collators,
                    new_session_info,
                    prev_blocks_ids,
                ));
                collator_tasks.push(resume_collation_task);
            } else {
                // just reset collator state
                self.set_collator_state(&shard_id, |ac| ac.state = CollatorState::Waiting);
            }
        }

        // finish outdated collation sessions
        if !to_finish_sessions.is_empty() {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                "Will finish outdated collation sessions: {:?}",
                DebugIter(to_finish_sessions.iter().map(|s| s.id())),
            );
        }

        // stop dangling collators
        if !to_stop_collators.is_empty() {
            tracing::info!(target: tracing_targets::COLLATION_MANAGER,
                "Will stop collators for sessions that we do not serve: {:?}",
                DebugIter(to_stop_collators.iter().map(|(s, _)| s.id())),
            );
        }

        Ok(collator_tasks)

        // finally we will have initialized `active_collation_sessions`
        // and `active_collators` which run async block collations processes
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

    /// Returns: (seqno delta from other, true - if equal).
    /// If `other_mc_block_id_opt` is none, returns : (0, false)
    fn compare_mc_block_with(
        mc_block_id: &BlockId,
        other_mc_block_id_opt: Option<&BlockId>,
    ) -> (i32, bool) {
        let (seqno_delta, is_equal) = match other_mc_block_id_opt {
            None => {
                tracing::info!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "other mc block is None: current {} other ({:?}): is_equal = false, seqno_delta = 0",
                    mc_block_id.as_short_id(),
                    other_mc_block_id_opt.map(|b| b.as_short_id()),
                );
                (0, false)
            }
            Some(other_mc_block_id) => (
                mc_block_id.seqno as i32 - other_mc_block_id.seqno as i32,
                mc_block_id == other_mc_block_id,
            ),
        };
        if seqno_delta < 0 || is_equal {
            tracing::info!(
                target: tracing_targets::COLLATION_MANAGER,
                "mc block is NOT AHEAD of other: current {} other ({:?}): is_equal = {}, seqno_delta = {}",
                mc_block_id.as_short_id(),
                other_mc_block_id_opt.map(|b| b.as_short_id()),
                is_equal, seqno_delta,
            );
        }
        (seqno_delta, is_equal)
    }

    fn apply_split_merge_actions(
        &self,
        new_shards_info: &FastHashMap<ShardIdent, Vec<BlockId>>,
    ) -> Result<()> {
        let active_shards_ids: Vec<_> = self
            .active_collation_sessions
            .read()
            .keys()
            .cloned()
            .collect();
        let new_shards_ids: Vec<&ShardIdent> = new_shards_info.keys().collect();

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            "Detecting split/merge actions to move from current shards {:?} to new shards {:?}...",
            active_shards_ids.as_slice(),
            new_shards_ids.as_slice(),
        );

        let split_merge_actions = calc_split_merge_actions(&active_shards_ids, new_shards_ids)?;
        if !split_merge_actions.is_empty() {
            tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                "Detected split/merge actions: {:?}",
                split_merge_actions,
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) enum ProcessMcStateUpdateMode {
    StartCollation { reset_collators: bool },
    RefreshSessionsOnly,
    SkipProcess,
}

// TODO: Move into `tycho_types`.
pub trait GlobalCapabilitiesExt {
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
