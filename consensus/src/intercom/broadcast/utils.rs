use std::ops::Range;
use std::time::Duration;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rand::{thread_rng, Rng};
use tokio_util::time::DelayQueue;
use tycho_network::PeerId;
use tycho_util::FastHashSet;

/// Encapsulates queue and its delay rule
pub struct PeerQueue {
    range: Range<Duration>,
    queue: DelayQueue<PeerId>,
}

impl PeerQueue {
    pub fn new(range: Range<Duration>) -> Self {
        Self {
            range,
            queue: DelayQueue::new(),
        }
    }

    pub fn push(&mut self, peer_id: &PeerId) {
        self.queue
            .insert(*peer_id, thread_rng().gen_range(self.range.clone()));
    }

    pub async fn next(&mut self) -> Option<PeerId> {
        self.queue.next().await.map(|expired| expired.into_inner())
    }
}

/// Encapsulates futures and corresponding peer ids for consistency
pub struct QueryResponses<T> {
    futures: FuturesUnordered<BoxFuture<'static, (PeerId, Result<T>)>>,
    peers: FastHashSet<PeerId>,
}

impl<T> Default for QueryResponses<T> {
    fn default() -> Self {
        Self {
            futures: Default::default(),
            peers: Default::default(),
        }
    }
}

impl<T> QueryResponses<T> {
    pub fn push(&mut self, peer: &PeerId, fut: BoxFuture<'static, (PeerId, Result<T>)>) {
        self.futures.push(fut);
        self.peers.insert(*peer);
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.peers.contains(peer_id)
    }

    pub async fn next(&mut self) -> Option<(PeerId, Result<T>)> {
        let next = self.futures.next().await;
        if let Some((peer, _)) = &next {
            self.peers.remove(peer);
        }
        next
    }
}
