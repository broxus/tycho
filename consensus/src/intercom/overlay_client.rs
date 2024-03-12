use std::ops::DerefMut;
use std::sync::Arc;

use ahash::RandomState;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use tycho_network::{PeerId, PrivateOverlay, PrivateOverlayEntriesEvent};
use tycho_util::futures::JoinTask;
use tycho_util::{FastDashMap, FastDashSet};

#[derive(Clone)]
pub struct OverlayClient {
    pub peers: Arc<FastDashSet<PeerId>>,
    pub overlay: PrivateOverlay,
}

impl OverlayClient {
    pub fn new(node_count: usize, overlay: PrivateOverlay, local_id: PeerId) -> Self {
        let peers = Arc::new(FastDashSet::<PeerId>::with_capacity_and_hasher(
            node_count,
            RandomState::new(),
        ));
        tokio::spawn(Self::listen(
            peers.clone(),
            overlay.clone().read_entries().subscribe(),
            local_id,
        ));
        Self { peers, overlay }
    }

    pub async fn wait_for_peers(&self, node_count: usize) {
        if self.peers.len() >= node_count {
            return;
        } else {
            let mut rx = self.overlay.read_entries().subscribe();
            while self.peers.len() < node_count {
                match rx.recv().await {
                    Ok(PrivateOverlayEntriesEvent::Resolved(_)) => {}
                    _ => {}
                }
            }
        }
    }

    async fn listen(
        peers: Arc<FastDashSet<PeerId>>,
        mut rx: broadcast::Receiver<PrivateOverlayEntriesEvent>,
        local_id: PeerId,
    ) {
        loop {
            match rx.recv().await {
                Ok(PrivateOverlayEntriesEvent::Added(_)) => {}
                Ok(PrivateOverlayEntriesEvent::Resolved(node)) => {
                    if node != local_id {
                        peers.insert(node);
                    }
                }
                Ok(PrivateOverlayEntriesEvent::Removed(node)) => {
                    if node != local_id {
                        peers.remove(&node);
                    }
                }
                Err(RecvError::Closed) => {
                    let msg =
                        "Fatal: peer info updates channel closed, cannot maintain node connectivity";
                    tracing::error!(msg);
                    panic!("{msg}")
                }
                Err(RecvError::Lagged(qnt)) => {
                    tracing::warn!(
                        "Skipped {qnt} peer info updates, node connectivity may suffer. \
                         Consider increasing channel capacity."
                    )
                }
            }
        }
    }
}
