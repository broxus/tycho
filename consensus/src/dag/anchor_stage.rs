use std::sync::atomic::AtomicBool;

use rand::{Rng, SeedableRng};
use tycho_network::PeerId;

use crate::dag::WAVE_ROUNDS;
use crate::engine::Genesis;
use crate::intercom::PeerSchedule;
use crate::models::{AnchorStageRole, Round};

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
    pub fn of(round: Round, peer_schedule: &PeerSchedule) -> Option<Self> {
        // Genesis point appears as a Proof in anchor chain during commit,
        // so it has to be at a round with Proof role
        let anchor_candidate_round =
            ((round.0 / WAVE_ROUNDS) * WAVE_ROUNDS).max(Genesis::round().0);

        let (ordered_peers, current_peers) = {
            let guard = peer_schedule.atomic();
            let ordered_peers = guard.peers_ordered_for(Round(anchor_candidate_round));
            let current_peers = guard.peers_for(round);
            (ordered_peers.clone(), current_peers.clone())
        };
        assert!(!ordered_peers.is_empty(), "leader from empty validator set");
        // reproducible global coin
        let leader_index = rand_pcg::Pcg32::seed_from_u64(anchor_candidate_round as u64)
            .gen_range(0..ordered_peers.len());
        let leader = ordered_peers[leader_index];
        // the leader cannot produce three points in a row, so we have an undefined leader,
        // rather than an intentional leaderless support round - all represented by `None`
        if !current_peers.contains(&leader) {
            return None;
        };
        #[allow(clippy::match_same_arms)]
        let role = match round.0 % WAVE_ROUNDS {
            0 => None, // anchor candidate (surprisingly, nothing special about this point)
            1 => Some(AnchorStageRole::Proof),
            2 => Some(AnchorStageRole::Trigger),
            3 => None, // leaderless support round (that actually follows every leader point chain)
            _ => unreachable!(),
        };
        role.map(|role| Self {
            role,
            leader,
            // genesis is a corner case, exclude it from commit chain with explicit "true"
            is_used: AtomicBool::new(round == Genesis::round()),
        })
    }
}
