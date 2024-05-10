use std::sync::Arc;

use futures_util::StreamExt;
use parking_lot::Mutex;
use rand::prelude::IteratorRandom;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::AbortHandle;
use tycho_network::{PeerId, PrivateOverlay, PrivateOverlayEntriesEvent};

use crate::intercom::PeerSchedule;

#[derive(Clone)]
pub struct PeerScheduleUpdater {
    overlay: PrivateOverlay,
    peer_schedule: Arc<PeerSchedule>,
    abort_resolve_peers: Arc<Mutex<Option<AbortHandle>>>,
}

impl PeerScheduleUpdater {
    pub fn new(overlay: PrivateOverlay, peer_schedule: Arc<PeerSchedule>) -> Self {
        Self {
            overlay,
            peer_schedule,
            abort_resolve_peers: Default::default(),
        }
    }

    pub async fn run(self) -> ! {
        tracing::info!("started peer schedule updater");
        self.respawn_resolve_task();
        self.listen().await
    }

    pub fn set_next_peers(&self, peers: &[PeerId]) {
        self.peer_schedule.set_next_peers(&peers, &self.overlay)
    }

    fn respawn_resolve_task(&self) {
        let local_id = self.peer_schedule.local_id();
        tracing::info!("{local_id:.4?} respawn_resolve_task");
        let mut fut = futures_util::stream::FuturesUnordered::new();
        {
            // Note: set_next_peers() and respawn_resolve_task() will not deadlock
            //   although peer_schedule.inner is locked in two opposite orders
            //   because only read read lock on overlay entries is taken
            let entries = self.overlay.read_entries();
            for entry in entries
                .iter()
                .choose_multiple(&mut rand::thread_rng(), entries.len())
            {
                // skip updates on self
                if !(entry.peer_id == local_id || entry.resolver_handle.is_resolved()) {
                    let handle = entry.resolver_handle.clone();
                    fut.push(async move { handle.wait_resolved().await });
                }
            }
        };
        let new_abort_handle = if fut.is_empty() {
            None
        } else {
            let peer_schedule = self.peer_schedule.clone();
            let join = tokio::spawn(async move {
                while let Some(known_peer_handle) = fut.next().await {
                    _ = peer_schedule.set_resolved(&known_peer_handle.peer_info().id, true);
                }
            });
            Some(join.abort_handle())
        };
        let mut abort_resolve_handle = self.abort_resolve_peers.lock();
        if let Some(old) = abort_resolve_handle.as_ref() {
            old.abort();
        };
        *abort_resolve_handle = new_abort_handle;
    }

    async fn listen(self) -> ! {
        let local_id = self.peer_schedule.local_id();
        tracing::info!("{local_id:.4?} listen peer updates");
        let mut rx = self.overlay.read_entries().subscribe();
        loop {
            match rx.recv().await {
                Ok(ref event @ PrivateOverlayEntriesEvent::Removed(node)) if node != local_id => {
                    tracing::info!("{local_id:.4?} got {event:?}");
                    if self.peer_schedule.set_resolved(&node, false) {
                        // respawn resolve task with fewer peers to await
                        self.respawn_resolve_task();
                    } else {
                        tracing::info!("{local_id:.4?} Skipped {event:?}");
                    }
                }
                Err(RecvError::Closed) => {
                    panic!("peer info updates channel closed, cannot maintain node connectivity")
                }
                Err(RecvError::Lagged(qnt)) => {
                    tracing::warn!(
                        "Skipped {qnt} peer info updates, node connectivity may suffer. \
                         Consider increasing channel capacity."
                    )
                }
                Ok(a) => {
                    tracing::warn!("{local_id:.4?} peer schedule updater missed {a:?}");
                }
            }
        }
    }
}
