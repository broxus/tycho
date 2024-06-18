use std::future::Future;
use std::sync::Arc;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use parking_lot::Mutex;
use rand::thread_rng;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::AbortHandle;
use tycho_network::{
    KnownPeerHandle, PeerId, PrivateOverlay, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard, PrivateOverlayEntriesWriteGuard,
};

use crate::effects::{AltFmt, AltFormat};
use crate::intercom::dto::PeerState;
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
        self.restart_resolve_task(self.resolved_waiters(self.overlay.read_entries()));
        self.listen().await
    }

    pub fn set_next_peers(&self, peers: &[PeerId], update_overlay: bool) {
        if update_overlay {
            let mut entries: PrivateOverlayEntriesWriteGuard<'_> = self.overlay.write_entries();
            for peer_id in peers {
                entries.insert(peer_id);
            }
            self.restart_resolve_task(self.resolved_waiters(entries.downgrade()));
        }
        self.peer_schedule.set_next_peers(peers, &self.overlay);
    }

    fn resolved_waiters(
        &self,
        entries: PrivateOverlayEntriesReadGuard<'_>,
    ) -> FuturesUnordered<impl Future<Output = KnownPeerHandle> + Sized + Send + 'static> {
        let local_id = self.peer_schedule.local_id();
        let fut = FuturesUnordered::new();
        // Note: set_next_peers() and respawn_resolve_task() will not deadlock
        //   although peer_schedule.inner is locked in two opposite orders
        //   because only read read lock on overlay entries is taken
        for entry in entries.choose_multiple(&mut thread_rng(), entries.len()) {
            // skip updates on self
            if !(entry.peer_id == local_id || entry.resolver_handle.is_resolved()) {
                let handle = entry.resolver_handle.clone();
                fut.push(async move { handle.wait_resolved().await });
            }
        }
        fut
    }

    fn restart_resolve_task(
        &self,
        mut resolved_waiters: FuturesUnordered<
            impl Future<Output = KnownPeerHandle> + Sized + Send + 'static,
        >,
    ) {
        tracing::info!("restart resolve task");
        let new_abort_handle = if resolved_waiters.is_empty() {
            None
        } else {
            let peer_schedule = self.peer_schedule.clone();
            let join = tokio::spawn(async move {
                while let Some(known_peer_handle) = resolved_waiters.next().await {
                    _ = peer_schedule
                        .set_state(&known_peer_handle.peer_info().id, PeerState::Resolved);
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
        tracing::info!("listen peer updates");
        let mut rx = self.overlay.read_entries().subscribe();
        loop {
            match rx.recv().await {
                Ok(ref event @ PrivateOverlayEntriesEvent::Removed(peer)) if peer != local_id => {
                    let restart = self.peer_schedule.set_state(&peer, PeerState::Unknown);
                    if restart {
                        // respawn resolve task with fewer peers to await
                        self.restart_resolve_task(
                            self.resolved_waiters(self.overlay.read_entries()),
                        );
                    }
                    tracing::info!(
                        event = display(event.alt()),
                        peer = display(peer.alt()),
                        resolve_restarted = restart,
                        "peer schedule update"
                    );
                }
                Err(RecvError::Closed) => {
                    panic!("peer info updates channel closed, cannot maintain node connectivity")
                }
                Err(RecvError::Lagged(amount)) => {
                    tracing::error!(
                        amount = amount,
                        "Lagged peer info updates, node connectivity may suffer. \
                         Consider increasing channel capacity."
                    );
                }
                Ok(
                    ref event @ (PrivateOverlayEntriesEvent::Added(peer_id)
                    | PrivateOverlayEntriesEvent::Removed(peer_id)),
                ) => {
                    tracing::debug!(
                        event = display(event.alt()),
                        peer = display(peer_id.alt()),
                        "peer schedule update ignored"
                    );
                }
            }
        }
    }
}
impl AltFormat for PrivateOverlayEntriesEvent {}
impl std::fmt::Display for AltFmt<'_, PrivateOverlayEntriesEvent> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match AltFormat::unpack(self) {
            PrivateOverlayEntriesEvent::Added(_) => "Added",
            PrivateOverlayEntriesEvent::Removed(_) => "Removed",
        })
    }
}
