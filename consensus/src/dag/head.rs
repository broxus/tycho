use std::sync::Arc;

use crate::dag::DagRound;
use crate::intercom::{KeyGroup, PeerSchedule};
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
    pub fn new(
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

        let key_group = peer_schedule.atomic().key_group(current.round());

        Self(Arc::new(DagHeadInner {
            key_group,
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
