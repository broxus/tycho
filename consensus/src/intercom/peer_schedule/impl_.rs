use std::future::Future;
use std::ops::Deref;
use std::sync::{Arc, Weak};

use arc_swap::{ArcSwap, Guard};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use rand::rng;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{
    KnownPeerHandle, PeerId, PrivateOverlay, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard,
};

use crate::effects::{AltFmt, AltFormat, Cancelled, Task, TaskTracker};
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

/// Full vsets preserve validator order in blockchain config,
/// and subsets preserve validator order after shuffle
#[cfg_attr(any(feature = "test", test), derive(Clone))]
pub struct InitPeers {
    pub prev_start_round: u32,
    pub prev_v_set: Vec<PeerId>,
    pub prev_v_subset: Vec<(PeerId, u16)>,

    pub curr_start_round: u32,
    pub curr_v_set: Vec<PeerId>,
    pub curr_v_subset: Vec<(PeerId, u16)>,

    pub next_v_set: Vec<PeerId>,
}

impl InitPeers {
    #[cfg(any(feature = "test", test))]
    pub fn new(curr_v_set: Vec<PeerId>) -> Self {
        Self {
            prev_start_round: 0,
            prev_v_set: vec![],
            prev_v_subset: vec![],

            curr_start_round: 0,
            curr_v_subset: (curr_v_set.iter().enumerate())
                .map(|(i, p)| (*p, i as u16))
                .collect(),
            curr_v_set,

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
            &[(merged_conf.genesis_author(), 0)],
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

        tracing::info!("starting peer schedule updates");

        let entries = locked.overlay.read_entries();
        let resolved_waiters = WeakPeerSchedule::resolved_waiters(entries, &locked.local_id);
        locked.resolve_peers_task = self.downgrade().new_resolve_task(resolved_waiters);
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

    pub fn set_banned(&self, peers: &[PeerId]) {
        if !peers.is_empty() {
            let mut guard = self.write();
            guard.set_banned(peers, self.downgrade());
            self.update_atomic(|stateless| stateless.insert_banned(peers));
        }
    }

    pub fn remove_bans(&self, peers: &[PeerId]) {
        if !peers.is_empty() {
            let mut guard = self.write();
            guard.remove_bans(peers, self.downgrade());
            self.update_atomic(|stateless| stateless.remove_banned(peers));
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
        working_subset: &[(PeerId, u16)],
        message: &'static str,
    ) {
        if next_round <= locked.data.epoch_starts.curr() || working_subset.is_empty() {
            tracing::trace!(
                message,
                set_round = next_round.0,
                curr_start = locked.data.epoch_starts.curr().0,
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
        locked.data.epoch_starts.next = Some(next_round);

        // atomic part is updated under lock too
        self.update_atomic(|stateless| {
            stateless.set_next_peers(next_round, working_subset);
        });

        tracing::info!(
            "peer schedule next subset updated for {next_round:?} {:?}, trace: {:?}",
            self.atomic().alt(),
            tracing::enabled!(tracing::Level::TRACE).then_some(&locked.data),
        );
    }

    pub fn apply_scheduled(&self, current: Round) {
        if (self.atomic().epoch_starts.next).is_none_or(|scheduled| scheduled > current) {
            return; // will double-check because arc-swap is racy with `self.set_next_subset()`
        }
        let mut locked = self.write();
        self.apply_scheduled_impl(&mut locked, current);
    }

    /// on peer set change
    fn apply_scheduled_impl(&self, locked: &mut PeerScheduleLocked, current: Round) {
        if (locked.data.epoch_starts.next).is_none_or(|scheduled| scheduled > current) {
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

    pub(super) fn resolved_waiters(
        entries: PrivateOverlayEntriesReadGuard<'_>,
        local_id: &PeerId,
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
                let peer_id = &known_peer_handle.peer_info().id;
                let mut guard = peer_schedule.write();
                if guard.banned.contains(peer_id) {
                    guard.overlay.write_entries().remove(peer_id); // undo
                } else {
                    guard.set_resolved(peer_id);
                }
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use tokio::time::timeout;
    use tycho_network::{Address, DhtService, Network, OverlayService, Router};

    use super::*;
    use crate::intercom::Responder;
    use crate::test_utils::{default_test_config, make_peer_info};

    #[tokio::test]
    async fn remove_bans_restores_already_resolved_peer() -> Result<()> {
        let local_keys = Arc::new(rand::random::<KeyPair>());
        let remote_keys = Arc::new(rand::random::<KeyPair>());

        let remote_peer = PeerId::from(remote_keys.public_key);

        let (peer_schedule, _handles) = make_peer_schedule(local_keys, &[&*remote_keys])?;
        assert_eq!(
            (peer_schedule.read().data).peer_state(&PeerId::from(remote_keys.public_key)),
            PeerState::Resolved
        );

        let mut updates = peer_schedule.read().updates();

        peer_schedule.set_banned(&[remote_peer]);
        assert!(peer_schedule.atomic().is_banned(&remote_peer));
        peer_schedule.remove_bans(&[remote_peer]);
        assert!(!peer_schedule.atomic().is_banned(&remote_peer));

        let mut seen = Vec::new();
        loop {
            match timeout(Duration::from_millis(200), updates.recv()).await {
                Ok(Ok(update)) => seen.push(update),
                Ok(Err(err)) => panic!("unexpected update channel state: {err:?}"),
                Err(_timeout) => break,
            }
        }
        assert_eq!(seen, vec![
            (remote_peer, PeerState::Unknown),
            (remote_peer, PeerState::Resolved),
        ]);
        assert_eq!(
            peer_schedule.read().data.peer_state(&remote_peer),
            PeerState::Resolved
        );

        Ok(())
    }

    fn make_peer_schedule(
        local_keys: Arc<KeyPair>,
        remote_keys: &[&KeyPair],
    ) -> Result<(PeerSchedule, (Network, Vec<KnownPeerHandle>))> {
        let local_id = PeerId::from(local_keys.public_key);

        let (dht_tasks, dht_service) = DhtService::builder(local_id).build();
        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_dht_service(dht_service.clone())
            .build();
        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();
        let network = Network::builder()
            .with_random_private_key()
            .build((Ipv4Addr::LOCALHOST, 0), router)?;

        dht_tasks.spawn_without_bootstrap(&network);
        overlay_tasks.spawn(&network);

        let peer_resolver = dht_service.make_peer_resolver().build(&network);
        let private_overlay = PrivateOverlay::builder(rand::random())
            .with_peer_resolver(peer_resolver)
            .build(Responder::default());
        overlay_service.add_private_overlay(&private_overlay);

        let task_tracker = TaskTracker::default();
        let peer_schedule = PeerSchedule::new(local_keys, private_overlay, &task_tracker);

        let mut handles = Vec::new();
        for (i, remote_key) in remote_keys.iter().enumerate() {
            let address_list = vec![Address::new_ip((Ipv4Addr::LOCALHOST, 10000 + i as u16))];
            let remote_info = Arc::new(make_peer_info(remote_key, address_list));
            handles.push(network.known_peers().insert(remote_info, false)?);
        }

        peer_schedule.init(
            &default_test_config(),
            &InitPeers::new((remote_keys.iter().map(|rk| PeerId::from(rk.public_key))).collect()),
        );

        Ok((peer_schedule, (network, handles)))
    }
}
