use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tycho_network::PeerId;
use tycho_util::FastHashSet;

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
    pub fn push(&mut self, peer_id: &PeerId, fut: BoxFuture<'static, (PeerId, Result<T>)>) {
        assert!(self.peers.insert(*peer_id), "insert duplicated response");
        self.futures.push(fut);
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.peers.contains(peer_id)
    }

    pub async fn next(&mut self) -> Option<(PeerId, Result<T>)> {
        let (peer_id, result) = self.futures.next().await?;
        assert!(self.peers.remove(&peer_id), "detected duplicated response");
        Some((peer_id, result))
    }
}
