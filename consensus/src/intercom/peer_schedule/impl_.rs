use std::future::Future;
use std::ops::Deref;
use std::sync::{Arc, Weak};

use arc_swap::{ArcSwap, Guard};
use futures_util::StreamExt;
use futures_util::never::Never;
use futures_util::stream::FuturesUnordered;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use rand::rng;
use tokio::sync::broadcast;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{
    KnownPeerHandle, PeerId, PrivateOverlay, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard,
};

use crate::effects::{AltFmt, AltFormat, Cancelled, Task, TaskResult, TaskTracker};
use crate::engine::MempoolMergedConfig;
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
#[derive(Clone)]
pub struct WeakPeerSchedule(Weak<PeerScheduleInner>);

struct PeerScheduleInner {
    locked: RwLock<PeerScheduleLocked>,
    atomic: ArcSwap<PeerScheduleStateless>,
    task_tracker: TaskTracker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Not yet ready to connect or already disconnected; always includes local peer id.
    Unknown,
    /// remote peer ready to connect
    Resolved,
}

#[cfg_attr(any(feature = "test", test), derive(Clone))]
pub struct InitPeers {
    pub prev_start_round: u32,
    pub prev_v_set: Vec<PeerId>,
    pub prev_v_subset: Vec<PeerId>,
    pub curr_start_round: u32,
    pub curr_v_set: Vec<PeerId>,
    pub curr_v_subset: Vec<PeerId>,
    pub next_v_set: Vec<PeerId>,
}

impl InitPeers {
    #[cfg(any(feature = "test", test))]
    pub fn new(curr_v_subset: Vec<PeerId>) -> Self {
        Self {
            prev_start_round: 0,
            prev_v_set: vec![],
            prev_v_subset: vec![],
            curr_start_round: 0,
            curr_v_set: curr_v_subset.clone(),
            curr_v_subset,
            next_v_set: vec![],
        }
    }
}

impl PeerSchedule {
    pub fn new(
        local_keys: Arc<KeyPair>,
        overlay: PrivateOverlay,
        task_tracker: &TaskTracker,
    ) -> Self {
        let local_id = PeerId::from(local_keys.public_key);
        Self(Arc::new(PeerScheduleInner {
            locked: RwLock::new(PeerScheduleLocked::new(local_id, overlay)),
            atomic: ArcSwap::from_pointee(PeerScheduleStateless::new(local_keys)),
            task_tracker: task_tracker.clone(),
        }))
    }

    pub fn init(&self, merged_conf: &MempoolMergedConfig, init: &InitPeers) {
        let genesis_round = merged_conf.conf.genesis_round;
        let mut locked = self.write();
        self.set_next_subset(
            &mut locked,
            &[],
            genesis_round.prev(),
            &[merged_conf.genesis_author()],
            "Init genesis pseudo validator subset",
        );
        self.apply_scheduled_impl(&mut locked, genesis_round.prev());

        let after_genesis = merged_conf.conf.genesis_round.next();

        if init.curr_start_round > after_genesis.0 {
            let prev_start = Round(init.prev_start_round).max(after_genesis);
            self.set_next_subset(
                &mut locked,
                &init.prev_v_set,
                prev_start,
                &init.prev_v_subset,
                "Init prev validator subset",
            );
            self.apply_scheduled_impl(&mut locked, prev_start);
        }

        let curr_start = Round(init.curr_start_round).max(after_genesis);
        self.set_next_subset(
            &mut locked,
            &init.curr_v_set,
            curr_start,
            &init.curr_v_subset,
            "Init current validator subset",
        );

        if !init.next_v_set.is_empty() {
            self.apply_scheduled_impl(&mut locked, curr_start);
            tracing::info!(vset_len = init.next_v_set.len(), "Init next validator set");
            locked.set_next_set(self.downgrade(), &init.next_v_set);
        }
    }

    pub fn set_peers(&self, peers: &InitPeers) {
        let mut locked = self.write();

        self.set_next_subset(
            &mut locked,
            &peers.prev_v_set,
            Round(peers.prev_start_round),
            &peers.prev_v_subset,
            "Apply prev validator subset",
        );
        self.apply_scheduled_impl(&mut locked, Round(peers.prev_start_round));

        self.set_next_subset(
            &mut locked,
            &peers.curr_v_set,
            Round(peers.curr_start_round),
            &peers.curr_v_subset,
            "Apply current validator subset",
        );

        if !peers.next_v_set.is_empty() {
            // current v_set is applied when DAG is advanced unless there is a next one
            self.apply_scheduled_impl(&mut locked, Round(peers.curr_start_round));
            tracing::info!(
                vset_len = peers.next_v_set.len(),
                "Apply next validator set"
            );
            locked.set_next_set(self.downgrade(), &peers.next_v_set);
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.read()
    }

    fn write(&self) -> RwLockWriteGuard<'_, RawRwLock, PeerScheduleLocked> {
        self.0.locked.write()
    }

    pub fn to_forget(&self) -> Vec<PeerId> {
        std::mem::take(&mut self.write().data.to_forget)
    }

    fn set_next_subset(
        &self,
        locked: &mut PeerScheduleLocked,
        validator_set: &[PeerId],
        next_round: Round,
        working_subset: &[PeerId],
        message: &'static str,
    ) {
        if next_round <= locked.data.curr_epoch_start() || working_subset.is_empty() {
            tracing::trace!(
                message,
                set_round = next_round.0,
                curr_start = locked.data.curr_epoch_start().0,
                len = working_subset.len(),
                vset_len = validator_set.len(),
                note = "cannot schedule outdated round"
            );
            return;
        }

        tracing::info!(
            message, // field name "message" is used by macros to display a nameless value
            len = working_subset.len(),
            vset_len = validator_set.len(),
            start_round = next_round.0,
        );

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
    }

    pub fn apply_scheduled(&self, current: Round) {
        if (self.atomic().next_epoch_start).is_none_or(|scheduled| scheduled > current) {
            return; // will double-check because arc-swap is racy with `self.set_next_subset()`
        }
        let mut locked = self.write();
        self.apply_scheduled_impl(&mut locked, current);
    }

    /// on peer set change
    fn apply_scheduled_impl(&self, locked: &mut PeerScheduleLocked, current: Round) {
        if (locked.data.next_epoch_start).is_none_or(|scheduled| scheduled > current) {
            return;
        }

        tracing::debug!(
            "peer schedule before rotation for {current:?}: {:?}, trace: {:?}",
            self.atomic().alt(),
            tracing::enabled!(tracing::Level::TRACE).then_some(&locked.data),
        );

        // rotate only after previous data is cleaned
        locked.forget_oldest(self.downgrade());
        locked.data.rotate();

        // atomic part is updated under lock too
        self.update_atomic(|stateless| {
            stateless.forget_oldest();
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

    pub fn downgrade(&self) -> WeakPeerSchedule {
        WeakPeerSchedule(Arc::downgrade(&self.0))
    }
}

impl WeakPeerSchedule {
    pub fn upgrade(&self) -> Option<PeerSchedule> {
        self.0.upgrade().map(PeerSchedule)
    }

    pub async fn run_updater(self) -> TaskResult<Never> {
        tracing::info!("starting peer schedule updates");
        scopeguard::defer!(tracing::warn!("peer schedule updater stopped"));

        let (local_id, mut rx) = match self.upgrade() {
            Some(strong) => {
                let mut guard = strong.write();

                guard.resolve_peers_task = None;
                let local_id = guard.local_id;
                let (rx, resolved_waiters) = {
                    let entries = guard.overlay.read_entries();
                    let rx = entries.subscribe();
                    (rx, Self::resolved_waiters(&local_id, &entries))
                };
                guard.resolve_peers_task = self.clone().new_resolve_task(resolved_waiters);

                (local_id, rx)
            }
            None => {
                tracing::warn!("peer schedule dropped, cannot create updater");
                return Err(Cancelled());
            }
        };

        loop {
            match rx.recv().await {
                Ok(ref event @ PrivateOverlayEntriesEvent::Removed(peer)) if peer != local_id => {
                    let Some(strong) = self.upgrade() else {
                        tracing::warn!("peer schedule dropped, stopping updater");
                        break Err(Cancelled());
                    };
                    let mut guard = strong.write();
                    let restart = guard.set_state(&peer, PeerState::Unknown);
                    if restart {
                        guard.resolve_peers_task = None;
                        let resolved_waiters = {
                            let entries = guard.overlay.read_entries();
                            Self::resolved_waiters(&local_id, &entries)
                        };
                        guard.resolve_peers_task = self.clone().new_resolve_task(resolved_waiters);
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
                    break Err(Cancelled());
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
        for entry in entries.choose_multiple(&mut rng(), entries.len()) {
            // skip updates on self
            if !(entry.peer_id == local_id || entry.resolver_handle.is_resolved()) {
                let handle = entry.resolver_handle.clone();
                fut.push(async move { handle.wait_resolved().await });
            }
        }
        fut
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
                    return Err(Cancelled());
                };
                _ = peer_schedule
                    .write()
                    .set_state(&known_peer_handle.peer_info().id, PeerState::Resolved);
                gauge.decrement(1);
            }
            Ok(())
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
