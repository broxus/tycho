use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;

use crate::dag::DagRound;
use crate::intercom::PeerSchedule;
use crate::models::Round;

#[derive(Clone)]
pub struct DagHead(Arc<DagHeadInner>);

struct DagHeadInner {
    // if any `key_pair` is not empty, then the node may use it at current round
    produce_keys: Option<Arc<KeyPair>>,
    prev_produce_keys: Option<Arc<KeyPair>>,
    includes_keys: Option<Arc<KeyPair>>,
    witness_keys: Option<Arc<KeyPair>>,
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

        let guard = peer_schedule.atomic();

        let produce_keys = guard.local_keys(current.round());
        let prev_produce_keys = guard.local_keys(prev.round());

        let includes_keys = guard.local_keys(top_round.round());
        let witness_keys = includes_keys.as_ref().and_then(|_| produce_keys.clone());

        Self(Arc::new(DagHeadInner {
            produce_keys,
            prev_produce_keys,
            includes_keys,
            witness_keys,
            prev,
            current,
            next: top_round.clone(),
            last_back_bottom,
        }))
    }

    pub fn produce_keys(&self) -> Option<&KeyPair> {
        self.0.produce_keys.as_deref()
    }

    pub fn prev_produce_keys(&self) -> Option<&KeyPair> {
        self.0.prev_produce_keys.as_deref()
    }

    pub fn includes_keys(&self) -> Option<&KeyPair> {
        self.0.includes_keys.as_deref()
    }

    pub fn witness_keys(&self) -> Option<&KeyPair> {
        self.0.witness_keys.as_deref()
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
