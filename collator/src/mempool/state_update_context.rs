use std::sync::Arc;

use anyhow::Result;
use tycho_network::PeerId;
use tycho_types::cell::HashBytes;
use tycho_types::models::{
    BlockId, ConsensusConfig, ConsensusInfo, ValidatorDescription, ValidatorSet,
};

use super::MempoolAnchorId;

#[derive(Debug)]
pub struct StateUpdateContext {
    pub mc_block_id: BlockId,
    pub mc_block_chain_time: u64,
    pub top_processed_to_anchor_id: MempoolAnchorId,
    pub consensus_info: ConsensusInfo,
    pub consensus_config: ConsensusConfig,
    pub shuffle_validators: bool,
    pub prev_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
    pub current_validator_set: (HashBytes, Arc<ValidatorSet>),
    pub next_validator_set: Option<(HashBytes, Arc<ValidatorSet>)>,
}

/// Contains all data needed to deterministically derive all v(sub)sets
#[derive(PartialEq, Eq)]
pub struct VSetsId<'a> {
    prev_vset_switch_round: u32,
    prev_shuffle_mc_validators: bool,
    prev_validator_set: Option<&'a HashBytes>,

    vset_switch_round: u32,
    shuffle_validators: bool,
    current_validator_set: &'a HashBytes,

    next_validator_set: Option<&'a HashBytes>,
}

impl StateUpdateContext {
    /// equality by vid means all contained and derived v(sub)sets are the same
    pub fn vid(&self) -> VSetsId<'_> {
        VSetsId {
            prev_vset_switch_round: self.consensus_info.prev_vset_switch_round,
            prev_shuffle_mc_validators: self.consensus_info.prev_shuffle_mc_validators,
            prev_validator_set: self.prev_validator_set.as_ref().map(|(hash, _)| hash),

            vset_switch_round: self.consensus_info.vset_switch_round,
            shuffle_validators: self.shuffle_validators,
            current_validator_set: &self.current_validator_set.0,

            next_validator_set: self.next_validator_set.as_ref().map(|(hash, _)| hash),
        }
    }

    /// Preserves blockchain config peer order to pass into mempool
    pub fn prev_v_set(&self) -> Vec<PeerId> {
        Self::peer_ids((self.prev_validator_set.iter()).flat_map(|(_, v_set)| v_set.list.iter()))
    }

    /// Preserves blockchain config peer order to pass into mempool
    pub fn curr_v_set(&self) -> Vec<PeerId> {
        Self::peer_ids(self.current_validator_set.1.list.iter())
    }

    /// Preserves blockchain config peer order to pass into mempool
    pub fn next_v_set(&self) -> Vec<PeerId> {
        Self::peer_ids((self.next_validator_set.iter()).flat_map(|(_, v_set)| v_set.list.iter()))
    }

    /// Preserves shuffled peer order to pass into mempool;
    /// contains `validator_idx` peer position in original blockchain config
    pub fn prev_v_subset(&self) -> Result<Vec<(PeerId, u16)>> {
        let Some((_, prev_validator_set)) = self.prev_validator_set.as_ref() else {
            return Ok(Vec::new());
        };
        Self::compute_subset(
            prev_validator_set,
            self.consensus_info.prev_vset_switch_round,
            self.consensus_info.prev_shuffle_mc_validators,
        )
        .ok_or_else(|| anyhow::anyhow!("failed to compute previous validator subset"))
    }

    /// Preserves shuffled peer order to pass into mempool;
    /// contains `validator_idx` peer position in original blockchain config
    pub fn curr_v_subset(&self) -> Result<Vec<(PeerId, u16)>> {
        Self::compute_subset(
            &self.current_validator_set.1,
            self.consensus_info.vset_switch_round,
            self.shuffle_validators,
        )
        .ok_or_else(|| anyhow::anyhow!("failed to compute current validator subset"))
    }

    // NOTE: do not try to calculate subset from next set
    //  because it is impossible without known future `switch_round`

    fn compute_subset(
        validator_set: &ValidatorSet,
        switch_round: u32,
        shuffle_validators: bool,
    ) -> Option<Vec<(PeerId, u16)>> {
        let (v_subset, _) =
            validator_set.compute_mc_subset_indexed(switch_round, shuffle_validators)?;
        let peer_idxs = v_subset
            .into_iter()
            .map(|desc| (PeerId(desc.public_key.0), desc.validator_idx))
            .collect();
        Some(peer_idxs)
    }

    fn peer_ids<'a>(iter: impl Iterator<Item = &'a ValidatorDescription>) -> Vec<PeerId> {
        iter.map(|descr| PeerId(descr.public_key.0)).collect()
    }
}

pub struct DebugStateUpdateContext<'a>(pub &'a StateUpdateContext);
impl std::fmt::Debug for DebugStateUpdateContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateUpdateContext")
            .field("mc_block_id", &self.0.mc_block_id.as_short_id())
            .field("mc_block_chain_time", &self.0.mc_block_chain_time)
            .field(
                "top_processed_to_anchor_id",
                &self.0.top_processed_to_anchor_id,
            )
            .field("consensus_info", &self.0.consensus_info)
            .field("consensus_config", &self.0.consensus_config)
            .field("shuffle_validators", &self.0.shuffle_validators)
            .field(
                "prev_validator_set.hash",
                &self.0.prev_validator_set.as_ref().map(|s| s.0),
            )
            .field(
                "current_validator_set.hash",
                &self.0.current_validator_set.0,
            )
            .field(
                "next_validator_set.hash",
                &self.0.next_validator_set.as_ref().map(|s| s.0),
            )
            .finish()
    }
}
