use std::future::Future;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rand::thread_rng;
use tokio::task::AbortHandle;
use tycho_network::{KnownPeerHandle, PeerId, PrivateOverlayEntriesReadGuard};

use crate::intercom::dto::PeerState;
use crate::intercom::PeerSchedule;

pub fn resolved_waiters(
    local_id: &PeerId,
    entries: &PrivateOverlayEntriesReadGuard<'_>,
) -> FuturesUnordered<impl Future<Output = KnownPeerHandle> + Sized + Send + 'static> {
    let fut = FuturesUnordered::new();
    for entry in entries.choose_multiple(&mut thread_rng(), entries.len()) {
        // skip updates on self
        if !(entry.peer_id == local_id || entry.resolver_handle.is_resolved()) {
            let handle = entry.resolver_handle.clone();
            fut.push(async move { handle.wait_resolved().await });
        }
    }
    fut
}

pub fn new_resolve_task(
    peer_schedule: PeerSchedule,
    mut resolved_waiters: FuturesUnordered<
        impl Future<Output = KnownPeerHandle> + Sized + Send + 'static,
    >,
) -> Option<AbortHandle> {
    tracing::info!("restart resolve task");
    if resolved_waiters.is_empty() {
        None
    } else {
        let join = tokio::spawn(async move {
            while let Some(known_peer_handle) = resolved_waiters.next().await {
                _ = peer_schedule
                    .write()
                    .set_state(&known_peer_handle.peer_info().id, PeerState::Resolved);
            }
        });
        Some(join.abort_handle())
    }
}
