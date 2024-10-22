use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use everscale_crypto::ed25519::KeyPair;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use rand::thread_rng;
use tokio::sync::broadcast;
use tokio::task::AbortHandle;
use tycho_network::{
    KnownPeerHandle, PeerId, PrivateOverlay, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard,
};
use tycho_util::FastHashSet;

use crate::effects::{AltFmt, AltFormat};
use crate::engine::Genesis;
use crate::intercom::dto::PeerState;
use crate::intercom::peer_schedule::locked::PeerScheduleLocked;
use crate::intercom::peer_schedule::stateless::PeerScheduleStateless;
use crate::models::Round;
// As validators are elected for wall-clock time range,
// the round of validator set switch is not known beforehand
// and will be determined by the time in anchor vertices:
// it must reach some predefined time range,
// when the new set is supposed to be online and start to request points,
// and a (relatively high) predefined number of support rounds must follow
// for the anchor chain to be committed by majority and for the new nodes to gather data.
// The switch will occur for validator sets as a whole.

#[derive(Clone)]
pub struct PeerSchedule(Arc<PeerScheduleInner>);

struct PeerScheduleInner {
    locked: RwLock<PeerScheduleLocked>,
    atomic: ArcSwap<PeerScheduleStateless>,
}

impl PeerSchedule {
    pub fn new(local_keys: Arc<KeyPair>, overlay: PrivateOverlay) -> Self {
        let local_id = PeerId::from(local_keys.public_key);
        let this = Self(Arc::new(PeerScheduleInner {
            locked: RwLock::new(PeerScheduleLocked::new(local_id, overlay)),
            atomic: ArcSwap::from_pointee(PeerScheduleStateless::new(local_keys)),
        }));

        this.set_next_peers(&[Genesis::id().author], false);
        this.set_next_start(Genesis::round());
        this.apply_scheduled(Genesis::round());

        this
    }

    pub fn read(&self) -> RwLockReadGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.read()
    }

    fn write(&self) -> RwLockWriteGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.write()
    }

    pub fn set_next_peers(&self, peers: &[PeerId], update_overlay: bool) {
        let mut locked = self.write();
        let all_resolved = {
            let local_id = locked.local_id;
            let entries = if update_overlay {
                let to_remove = self.atomic().next_to_remove(peers);
                locked.update_resolve(self, &to_remove, peers)
            } else {
                locked.overlay.read_entries()
            };
            entries
                .iter()
                .filter(|a| a.resolver_handle.is_resolved() && a.peer_id != local_id)
                .map(|a| a.peer_id)
                .collect::<FastHashSet<_>>()
        };

        locked.data.set_next_peers(peers, all_resolved);
        // atomic part is updated under lock too
        self.update_atomic(|stateless| stateless.set_next_peers(peers));
    }

    // `false` if next round is outdated
    pub fn set_next_start(&self, next_round: Round) -> bool {
        if next_round <= self.atomic().cur_epoch_start {
            return false; // ignore outdated
        }
        let mut locked = self.write();
        locked.data.next_epoch_start = Some(next_round);
        // atomic part is updated under lock too
        self.update_atomic(|stateless| stateless.next_epoch_start = Some(next_round));
        true
    }

    /// on peer set change
    pub fn apply_scheduled(&self, top_dag_round: Round) {
        if (self.atomic().next_epoch_start).map_or(true, |next| next < top_dag_round) {
            return;
        }
        let mut locked = self.write();
        tracing::debug!(
            "peer schedule before rotation at {top_dag_round:?}: {:?} {:?}",
            self.atomic(),
            locked.data,
        );
        let to_forget = locked.data.forget_previous();
        locked.data.rotate();
        locked.update_resolve(self, &to_forget, &[]);
        // atomic part is updated under lock too
        self.update_atomic(|stateless| {
            stateless.forget_previous();
            stateless.rotate();
        });
        tracing::info!(
            "peer schedule rotated at {top_dag_round:?}: {:?} {:?}",
            self.atomic(),
            locked.data,
        );
    }

    /// after successful sync to current epoch
    /// and validating all points from previous peer set
    /// free some memory and ignore overlay updates
    #[allow(dead_code)] // TODO use on change of validator set
    pub fn forget_previous(&self) {
        let mut locked = self.write();
        let to_forget = locked.data.forget_previous();
        locked.update_resolve(self, &to_forget, &[]);
        // atomic part is updated under lock too
        self.update_atomic(|stateless| stateless.forget_previous());
    }

    /// in-time snapshot if consistency with peer state is not needed;
    /// in case lock is taken - use this under lock too
    pub fn atomic(&self) -> Guard<Arc<PeerScheduleStateless>> {
        self.0.atomic.load()
    }

    /// atomic part is updated under write lock
    fn update_atomic<F>(&self, fun: F)
    where
        F: FnOnce(&mut PeerScheduleStateless),
    {
        let mut inner = self.atomic().deref().deref().clone();
        fun(&mut inner);
        self.0.atomic.store(Arc::new(inner));
    }

    pub async fn run_updater(self) -> ! {
        tracing::info!("starting peer schedule updates");
        let (local_id, mut rx) = {
            let mut guard = self.write();
            let local_id = guard.local_id;
            let (rx, resolved_waiters) = {
                let entries = guard.overlay.read_entries();
                let rx = entries.subscribe();
                (rx, Self::resolved_waiters(&local_id, &entries))
            };
            if let Some(handle) = &guard.abort_resolve_peers {
                handle.abort();
            }
            guard.abort_resolve_peers = self.clone().new_resolve_task(resolved_waiters);
            (local_id, rx)
        };

        loop {
            match rx.recv().await {
                Ok(ref event @ PrivateOverlayEntriesEvent::Removed(peer)) if peer != local_id => {
                    let mut guard = self.write();
                    let restart = guard.set_state(&peer, PeerState::Unknown);
                    if restart {
                        let resolved_waiters = {
                            let entries = guard.overlay.read_entries();
                            Self::resolved_waiters(&local_id, &entries)
                        };
                        // with fewer peers to await, because you cannot find and remove one task
                        if let Some(handle) = &guard.abort_resolve_peers {
                            handle.abort();
                        }
                        guard.abort_resolve_peers = self.clone().new_resolve_task(resolved_waiters);
                    }
                    drop(guard);
                    tracing::info!(
                        event = display(event.alt()),
                        peer = display(peer.alt()),
                        resolve_restarted = restart,
                        "peer schedule update"
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
                Err(broadcast::error::RecvError::Closed) => {
                    panic!("peer info updates channel closed, cannot maintain node connectivity")
                }
                Err(broadcast::error::RecvError::Lagged(amount)) => {
                    tracing::error!(
                        amount = amount,
                        "Lagged peer info updates, node connectivity may suffer. \
                         Consider increasing channel capacity."
                    );
                }
            }
        }
    }

    pub(super) fn resolved_waiters(
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

    pub(super) fn new_resolve_task(
        self,
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
                    _ = self
                        .write()
                        .set_state(&known_peer_handle.peer_info().id, PeerState::Resolved);
                }
            });
            Some(join.abort_handle())
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
