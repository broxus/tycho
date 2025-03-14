use std::future::Future;
use std::ops::Deref;
use std::sync::{Arc, Weak};

use arc_swap::{ArcSwap, Guard};
use everscale_crypto::ed25519::KeyPair;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use rand::thread_rng;
use tokio::sync::broadcast;
use tycho_network::{
    KnownPeerHandle, PeerId, PrivateOverlay, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard,
};

use crate::effects::{AltFmt, AltFormat, Task, TaskTracker};
use crate::engine::MempoolMergedConfig;
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
pub(super) struct WeakPeerSchedule(Weak<PeerScheduleInner>);

struct PeerScheduleInner {
    locked: RwLock<PeerScheduleLocked>,
    atomic: ArcSwap<PeerScheduleStateless>,
    task_tracker: TaskTracker,
}

impl PeerSchedule {
    pub fn new(
        local_keys: Arc<KeyPair>,
        overlay: PrivateOverlay,
        merged_conf: &MempoolMergedConfig,
        task_tracker: &TaskTracker,
    ) -> Self {
        let local_id = PeerId::from(local_keys.public_key);
        let this = Self(Arc::new(PeerScheduleInner {
            locked: RwLock::new(PeerScheduleLocked::new(local_id, overlay)),
            atomic: ArcSwap::from_pointee(PeerScheduleStateless::new(local_keys)),
            task_tracker: task_tracker.clone(),
        }));
        // validator set is not defined for genesis
        let genesis_round = merged_conf.conf.genesis_round;
        this.set_next_subset(&[], genesis_round.prev(), &[merged_conf.genesis_author()]);
        this.apply_scheduled(genesis_round);

        this
    }

    pub fn read(&self) -> RwLockReadGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.read()
    }

    fn write(&self) -> RwLockWriteGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.write()
    }

    pub fn set_next_set(&self, validator_set: &[PeerId]) {
        let mut locked = self.write();
        locked.set_next_set(self.downgrade(), validator_set);
    }

    // `false` if next round is outdated
    pub fn set_next_subset(
        &self,
        validator_set: &[PeerId],
        next_round: Round,
        working_subset: &[PeerId],
    ) -> bool {
        if next_round <= self.atomic().cur_epoch_start {
            return false; // ignore outdated
        }
        let mut locked = self.write();

        if next_round <= locked.data.cur_epoch_start {
            return false; // double-check because arc-swap is racy with `self.apply_scheduled()`
        }

        locked.set_next_set(self.downgrade(), validator_set);

        locked.data.set_next_subset(working_subset);
        locked.data.next_epoch_start = Some(next_round);

        // atomic part is updated under lock too
        self.update_atomic(|stateless| {
            stateless.set_next_peers(working_subset);
            stateless.next_epoch_start = Some(next_round);
        });

        tracing::info!(
            "peer schedule next subset updated for {next_round:?} {:?}, trace: {:?}",
            self.atomic().alt(),
            tracing::enabled!(tracing::Level::TRACE).then_some(&locked.data),
        );

        true
    }

    /// on peer set change
    pub fn apply_scheduled(&self, current: Round) {
        if (self.atomic().next_epoch_start).is_none_or(|scheduled| scheduled > current) {
            return;
        }
        let mut locked = self.write();

        if (locked.data.next_epoch_start).is_none_or(|scheduled| scheduled > current) {
            return; // double-check because arc-swap is racy with `self.set_next_subset()`
        }

        tracing::debug!(
            "peer schedule before rotation for {current:?}: {:?}, trace: {:?}",
            self.atomic().alt(),
            tracing::enabled!(tracing::Level::TRACE).then_some(&locked.data),
        );

        // rotate only after previous data is cleaned
        locked.forget_previous(self.downgrade());
        locked.data.rotate();

        // atomic part is updated under lock too
        self.update_atomic(|stateless| {
            stateless.forget_previous();
            stateless.rotate();
        });
        tracing::info!(
            "peer schedule rotated for {current:?} {:?}, trace: {:?}",
            self.atomic().alt(),
            tracing::enabled!(tracing::Level::TRACE).then_some(&locked.data),
        );
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

    pub async fn run_updater(self) {
        tracing::info!("starting peer schedule updates");
        scopeguard::defer!(tracing::warn!("peer schedule updater stopped"));
        let (local_id, mut rx) = {
            let mut guard = self.write();

            guard.resolve_peers_task = None;
            let local_id = guard.local_id;
            let (rx, resolved_waiters) = {
                let entries = guard.overlay.read_entries();
                let rx = entries.subscribe();
                (rx, Self::resolved_waiters(&local_id, &entries))
            };
            guard.resolve_peers_task = self.downgrade().new_resolve_task(resolved_waiters);

            (local_id, rx)
        };

        loop {
            match rx.recv().await {
                Ok(ref event @ PrivateOverlayEntriesEvent::Removed(peer)) if peer != local_id => {
                    let mut guard = self.write();
                    let restart = guard.set_state(&peer, PeerState::Unknown);
                    if restart {
                        guard.resolve_peers_task = None;
                        let resolved_waiters = {
                            let entries = guard.overlay.read_entries();
                            Self::resolved_waiters(&local_id, &entries)
                        };
                        guard.resolve_peers_task =
                            self.downgrade().new_resolve_task(resolved_waiters);
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
                    tracing::error!(
                        "peer info updates channel closed, cannot maintain node connectivity"
                    );
                    break;
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

    fn downgrade(&self) -> WeakPeerSchedule {
        WeakPeerSchedule(Arc::downgrade(&self.0))
    }
}

impl WeakPeerSchedule {
    fn upgrade(&self) -> Option<PeerSchedule> {
        self.0.upgrade().map(PeerSchedule)
    }

    pub fn new_resolve_task(
        self,
        mut resolved_waiters: FuturesUnordered<
            impl Future<Output = KnownPeerHandle> + Sized + Send + 'static,
        >,
    ) -> Option<Task<()>> {
        let gauge = metrics::gauge!("tycho_mempool_peers_resolving");
        gauge.set(resolved_waiters.len() as u32);
        if resolved_waiters.is_empty() {
            tracing::info!("peer schedule resolve task not started: all peers resolved");
            return None;
        }
        let Some(peer_schedule) = self.upgrade() else {
            tracing::warn!("peer schedule is dropped, cannot spawn resolve task");
            return None;
        };
        let task_ctx = peer_schedule.0.task_tracker.ctx();
        Some(task_ctx.spawn(async move {
            tracing::info!("peer schedule resolve task started");
            scopeguard::defer!(tracing::info!("peer schedule resolve task stopped"));
            while let Some(known_peer_handle) = resolved_waiters.next().await {
                let Some(peer_schedule) = self.upgrade() else {
                    tracing::warn!("peer schedule is dropped, cannot apply resolve update");
                    break;
                };
                _ = peer_schedule
                    .write()
                    .set_state(&known_peer_handle.peer_info().id, PeerState::Resolved);
                gauge.decrement(1);
            }
        }))
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
