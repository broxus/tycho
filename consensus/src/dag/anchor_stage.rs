use std::sync::atomic::AtomicBool;

use rand::{Rng, SeedableRng};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::{AnchorStageRole, Round};

/// Commit leader is changed every 4 rounds
const WAVE_ROUNDS: u32 = 4;

#[derive(Debug)]
pub struct AnchorStage {
    pub role: AnchorStageRole,
    pub leader: PeerId,
    /// if anchor is locally committed then it must be marked as used (and vice versa)
    ///
    /// trigger is not necessary used - proof may be included by the next anchor and its own trigger
    pub is_used: AtomicBool,
}

impl AnchorStage {
    pub fn of(round: Round, peer_schedule: &PeerSchedule, conf: &MempoolConfig) -> Option<Self> {
        // Genesis point appears as a Proof in anchor chain during commit,
        // so it has to be at a round with Proof role
        let anchor_candidate_round =
            ((round.0 / WAVE_ROUNDS) * WAVE_ROUNDS).max(conf.genesis_round.0);

        let (ordered_peers, current_peers) = {
            let guard = peer_schedule.atomic();
            let ordered_peers = guard.peers_ordered_for(Round(anchor_candidate_round));
            let current_peers = guard.peers_for(round);
            (ordered_peers.clone(), current_peers.clone())
        };
        assert!(!ordered_peers.is_empty(), "leader from empty validator set");
        // reproducible global coin
        let leader_index = rand_pcg::Pcg32::seed_from_u64(anchor_candidate_round as u64)
            .random_range(0..ordered_peers.len());
        let leader = ordered_peers[leader_index];
        // the leader cannot produce three points in a row, so we have an undefined leader,
        // rather than an intentional leaderless support round - all represented by `None`
        if !current_peers.contains(&leader) {
            return None;
        };

        let role = Self::role(round)?;
        Some(Self {
            role,
            leader,
            // genesis is a corner case, exclude it from commit chain with explicit "true"
            is_used: AtomicBool::new(round == conf.genesis_round),
        })
    }

    fn role(round: Round) -> Option<AnchorStageRole> {
        #[allow(clippy::match_same_arms, reason = "comments")]
        match round.0 % WAVE_ROUNDS {
            1 => None, // anchor candidate (surprisingly, nothing special about this point)
            2 => Some(AnchorStageRole::Proof),
            3 => Some(AnchorStageRole::Trigger),
            0 => None, // leaderless support round (that actually follows every leader point chain)
            _ => unreachable!(),
        }
    }

    pub fn align_genesis(start_round: u32) -> Round {
        Round(((start_round + 1) / WAVE_ROUNDS) * WAVE_ROUNDS + 2)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, ensure};

    use super::*;

    #[test]
    pub fn test_genesis_aligned() -> Result<()> {
        for start_round in 0..10 {
            let genesis_round = AnchorStage::align_genesis(start_round).0;
            ensure!(
                genesis_round >= start_round,
                "genesis round must not be less than start round after alignment, \
                 start_round={start_round}, genesis_round={genesis_round}",
            );
            ensure!(
                genesis_round < start_round + WAVE_ROUNDS,
                "aligned genesis increased too much, \
                start_round={start_round}, genesis_round={genesis_round}",
            );
            anyhow::ensure!(
                genesis_round > Round::BOTTOM.0,
                "aligned genesis {genesis_round:?} is too low and will make code panic"
            );
            anyhow::ensure!(
                genesis_round > Round::BOTTOM.0 + 1,
                "aligned genesis {genesis_round:?} is too low, first genesis cannot be used in \
                 Verifier::verify() and to set first working v_set in PeerSchedule"
            );

            let role = AnchorStage::role(Round(genesis_round));
            anyhow::ensure!(
                role == Some(AnchorStageRole::Proof),
                "genesis must be aligned to Proof anchor stage; round={genesis_round}",
            );
        }
        Ok(())
    }
}
