use std::sync::atomic::AtomicBool;

use rand::{Rng, SeedableRng};
use tycho_network::PeerId;

use crate::intercom::PeerSchedule;
use crate::models::{AnchorStageRole, Round};

pub const WAVE_ROUNDS: u32 = 4;

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
        let anchor_candidate_round = (round.0 / WAVE_ROUNDS) * WAVE_ROUNDS + 1;

        let [leader_peers, current_peers] = peer_schedule
            .atomic()
            .peers_for_array([Round(anchor_candidate_round), round]);
        // reproducible global coin
        let leader_index = rand_pcg::Pcg32::seed_from_u64(
            ((anchor_candidate_round as u64) << 32) + anchor_candidate_round as u64,
        )
        .gen_range(0..leader_peers.len());
        let leader = leader_peers
            .iter()
            .nth(leader_index)
            .expect("selecting a leader from an empty validator set");
        // the leader cannot produce three points in a row, so we have an undefined leader,
        // rather than an intentional leaderless support round - all represented by `None`
        if !current_peers.contains(leader) {
            return None;
        };
        let role = match round.0 % WAVE_ROUNDS {
            // 0 is a leaderless support round (that actually follows every leader point chain)
            // 1 is an anchor candidate (surprisingly, nothing special about this point)
            0 | 1 => None,
            2 => Some(AnchorStageRole::Proof),
            3 => Some(AnchorStageRole::Trigger),
            _ => unreachable!(),
        };
        role.map(|role| Self {
            role,
            leader: *leader,
            is_used: AtomicBool::new(false),
        })
    }
}
