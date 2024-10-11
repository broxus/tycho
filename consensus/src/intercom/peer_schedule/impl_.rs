use std::ops::Deref;
use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use everscale_crypto::ed25519::KeyPair;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use tokio::sync::broadcast;
use tycho_network::{PeerId, PrivateOverlay, PrivateOverlayEntriesEvent};

use crate::effects::{AltFmt, AltFormat};
use crate::intercom::dto::PeerState;
use crate::intercom::peer_schedule::locked::PeerScheduleLocked;
use crate::intercom::peer_schedule::stateless::PeerScheduleStateless;
use crate::intercom::peer_schedule::utils;
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
        Self(Arc::new(PeerScheduleInner {
            locked: RwLock::new(PeerScheduleLocked::new(local_id, overlay)),
            atomic: ArcSwap::from_pointee(PeerScheduleStateless::new(local_keys)),
        }))
    }

    pub fn read(&self) -> RwLockReadGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.write()
    }

    pub fn set_epoch(&self, next_peers: &[PeerId], next_round: Round, update_overlay: bool) {
        let mut guard = self.write();
        let peer_schedule = self.clone();

        guard.set_next_peers(next_peers, &peer_schedule, update_overlay);
        guard.set_next_start(next_round, &peer_schedule);
        guard.apply_next_start(&peer_schedule);
    }

    /// in-time snapshot if consistency with peer state is not needed;
    /// in case lock is taken - use this under lock too
    pub fn atomic(&self) -> Guard<Arc<PeerScheduleStateless>> {
        self.0.atomic.load()
    }

    /// atomic part is updated under write lock
    pub fn update_atomic<F>(&self, fun: F)
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
                (rx, utils::resolved_waiters(&local_id, &entries))
            };
            if let Some(handle) = &guard.abort_resolve_peers {
                handle.abort();
            }
            guard.abort_resolve_peers = utils::new_resolve_task(self.clone(), resolved_waiters);
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
                            utils::resolved_waiters(&local_id, &entries)
                        };
                        // with fewer peers to await, because you cannot find and remove one task
                        if let Some(handle) = &guard.abort_resolve_peers {
                            handle.abort();
                        }
                        guard.abort_resolve_peers =
                            utils::new_resolve_task(self.clone(), resolved_waiters);
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
