use std::sync::Arc;

use rand::{Rng, SeedableRng};
use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::engine::MempoolConfig;
use crate::intercom::PeerScheduleStateless;
use crate::models::{AnchorStageRole, Round};

/// How often the new leader is selected
pub const WAVE_ROUNDS: u32 = 3;

#[derive(Debug)]
pub struct ProofLeader {
    round: Round,
    anchor_candidate: Round,
    ordered_peers: Arc<Vec<PeerId>>,
    current_peers: Arc<FastHashSet<PeerId>>,
}

impl ProofLeader {
    pub fn new(round: Round, peer_schedule: &PeerScheduleStateless, conf: &MempoolConfig) -> Self {
        // Genesis point appears as a Proof in anchor chain during commit,
        // so it has to be at a round with Proof role
        let anchor_candidate =
            Round(((round.0 / WAVE_ROUNDS) * WAVE_ROUNDS).max(conf.genesis_round.0));
        Self {
            round,
            anchor_candidate,
            ordered_peers: peer_schedule.peers_ordered_for(anchor_candidate).clone(),
            current_peers: peer_schedule.peers_for(round).clone(),
        }
    }

    pub fn finish(self) -> Option<PeerId> {
        assert!(
            !self.ordered_peers.is_empty(),
            "leader from empty validator set"
        );
        // reproducible global coin
        let leader_index = rand_pcg::Pcg32::seed_from_u64(self.anchor_candidate.0 as u64)
            .random_range(0..self.ordered_peers.len());
        let leader = self.ordered_peers[leader_index];
        // the leader cannot produce three points in a row, so we have an undefined leader,
        // rather than an intentional leaderless support round - all represented by `None`
        if !self.current_peers.contains(&leader) {
            return None;
        };

        if Self::role(self.round)? != AnchorStageRole::Proof {
            return None;
        }
        Some(leader)
    }

    fn role(round: Round) -> Option<AnchorStageRole> {
        #[allow(clippy::match_same_arms, reason = "comments")]
        match round.0 % WAVE_ROUNDS {
            0 => None, // anchor candidate (surprisingly, nothing special about this point)
            1 => Some(AnchorStageRole::Proof),
            2 => Some(AnchorStageRole::Trigger),
            _ => unreachable!(),
        }
    }

    pub const fn align_genesis(start_round: u32) -> Round {
        let mut quotient = (start_round + 1) / WAVE_ROUNDS;
        if quotient == 0 {
            quotient = 1;
        };
        Round(quotient * WAVE_ROUNDS + 1)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, ensure};

    use super::*;

    #[test]
    pub fn test_genesis_aligned() -> Result<()> {
        for start_round in 0..10 {
            let genesis_round = ProofLeader::align_genesis(start_round).0;
            ensure!(
                genesis_round >= start_round,
                "genesis round must not be less than start round after alignment, \
                 start_round={start_round}, genesis_round={genesis_round}",
            );
            if start_round >= WAVE_ROUNDS {
                // skip check for near zero value - it's unimportant and impossible
                ensure!(
                    genesis_round < start_round + WAVE_ROUNDS,
                    "aligned genesis increased too much, \
                    start_round={start_round}, genesis_round={genesis_round}",
                );
            }
            anyhow::ensure!(
                genesis_round > Round::BOTTOM.0,
                "aligned genesis {genesis_round:?} is too low and will make code panic"
            );
            anyhow::ensure!(
                genesis_round > Round::BOTTOM.0 + 1,
                "aligned genesis {genesis_round:?} is too low, first genesis cannot be used in \
                 BasicVerifier::verify() and to set first working v_set in PeerSchedule"
            );

            let role = ProofLeader::role(Round(genesis_round));
            anyhow::ensure!(
                role == Some(AnchorStageRole::Proof),
                "genesis must be aligned to be Proof leader; round={genesis_round}",
            );
        }
        Ok(())
    }
}
