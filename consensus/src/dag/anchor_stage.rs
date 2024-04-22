use std::sync::atomic::AtomicBool;

use rand::{Rng, SeedableRng};

use tycho_network::PeerId;

use crate::intercom::PeerSchedule;
use crate::models::Round;

#[derive(Debug)]
pub enum AnchorStage {
    Candidate(PeerId), // TODO nothing special, remove
    /// if anchor is locally committed then it must be marked as used (and vice versa)
    Proof {
        leader: PeerId,
        is_used: AtomicBool,
    },
    /// trigger is not necessary used - proof may be included by the next anchor and its own trigger
    Trigger {
        leader: PeerId,
        is_used: AtomicBool,
    },
}

impl AnchorStage {
    pub fn of(round: Round, peer_schedule: &PeerSchedule) -> Option<Self> {
        const WAVE_SIZE: u32 = 4;
        let anchor_candidate_round = (round.0 / WAVE_SIZE) * WAVE_SIZE + 1;

        let [leader_peers, current_peers] =
            peer_schedule.peers_for_array([Round(anchor_candidate_round), round]);
        // reproducible global coin
        let leader_index = rand_pcg::Pcg32::seed_from_u64(anchor_candidate_round as u64)
            .gen_range(0..leader_peers.len());
        let Some(leader) = leader_peers
            .iter()
            .nth(leader_index)
            .map(|(peer_id, _)| peer_id)
        else {
            panic!("selecting a leader from an empty validator set")
        };
        if !current_peers.contains_key(leader) {
            return None;
        };
        match round.0 % WAVE_SIZE {
            0 => None, // both genesis and trailing (proof inclusion) round
            1 => Some(AnchorStage::Candidate(leader.clone())),
            2 => Some(AnchorStage::Proof {
                leader: leader.clone(),
                is_used: AtomicBool::new(false),
            }),
            3 => Some(AnchorStage::Trigger {
                leader: leader.clone(),
                is_used: AtomicBool::new(false),
            }),
            _ => unreachable!(),
        }
    }
}
