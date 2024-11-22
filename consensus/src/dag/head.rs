use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;

use crate::dag::DagRound;
use crate::intercom::PeerSchedule;
use crate::models::Round;

#[derive(Clone)]
pub struct DagHead(Arc<DagHeadInner>);

struct DagHeadInner {
    key_group: KeyGroup,
    prev: DagRound,
    current: DagRound,
    next: DagRound,
    last_back_bottom: Round,
}

impl DagHead {
    pub(super) fn new(
        peer_schedule: &PeerSchedule,
        top_round: &DagRound,
        last_back_bottom: Round,
    ) -> Self {
        let current = top_round
            .prev()
            .upgrade()
            .expect("current engine round must be always in dag");
        let prev = current
            .prev()
            .upgrade()
            .expect("prev to engine round must be always in dag");

        Self(Arc::new(DagHeadInner {
            key_group: KeyGroup::new(current.round(), peer_schedule),
            prev,
            current,
            next: top_round.clone(),
            last_back_bottom,
        }))
    }

    pub fn keys(&self) -> &KeyGroup {
        &self.0.key_group
    }

    pub fn prev(&self) -> &DagRound {
        &self.0.prev
    }

    pub fn current(&self) -> &DagRound {
        &self.0.current
    }

    pub fn next(&self) -> &DagRound {
        &self.0.next
    }

    pub fn last_back_bottom(&self) -> Round {
        self.0.last_back_bottom
    }
}

/// if any `KeyPair` is not empty, then the node may use it at current round
pub struct KeyGroup {
    pub to_produce: Option<Arc<KeyPair>>,
    pub to_include: Option<Arc<KeyPair>>,
    pub to_witness: Option<Arc<KeyPair>>,
    /// used after round is advanced, to finish witness of previous round
    pub to_witness_prev: Option<Arc<KeyPair>>,
}

impl KeyGroup {
    pub fn new(current: Round, peer_schedule: &PeerSchedule) -> Self {
        let guard = peer_schedule.atomic();

        let to_include = guard.local_keys(current.next());
        let to_produce = guard.local_keys(current);
        let to_witness = to_include.as_ref().and_then(|_| to_produce.clone());
        let to_witness_prev = to_produce
            .as_ref()
            .and_then(|_| guard.local_keys(current.prev()));

        Self {
            to_produce,
            to_include,
            to_witness,
            to_witness_prev,
        }
    }
}
