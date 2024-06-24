use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::models::Round;

/// Allows a node to drive consensus by collected dependencies with
/// [`Collector`](crate::intercom::Collector)
/// or follow it from broadcasts received by
/// [`BroadcastFilter`](crate::intercom::BroadcastFilter)
///
/// `BroadcastFilter` sends reliably determined rounds (and their points) to `Collector` via channel,
/// but `Collector` doesn't know about the latest round, until it consumes the channel to the end.
/// Also, `BroadcastFilter` is left uninterrupted when [`Engine`](crate::Engine)
/// changes current dag round and takes some (little) time to respawn `Collector` and other tasks.

#[derive(Clone)]
pub struct ConsensusRound(Arc<AtomicU32>);

impl ConsensusRound {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU32::new(Round::BOTTOM.0)))
    }

    pub fn get(&self) -> Round {
        Round(self.0.load(Ordering::Relaxed))
    }

    pub fn set_max(&self, round: Round) {
        self.0.fetch_max(round.0, Ordering::Relaxed);
    }
}
