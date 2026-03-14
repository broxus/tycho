use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tycho_network::{PeerId, PrivateOverlay};
use tycho_util::FastHashSet;

use crate::effects::Task;
use crate::intercom::peer_schedule::stateful::PeerScheduleStateful;
use crate::intercom::peer_schedule::{PeerState, WeakPeerSchedule};
use crate::models::PeerCount;

pub struct PeerScheduleLocked {
    /// Keypair may be changed only with node restart, and is known before validator elections.
    /// Node should use its keypair only to produce own and sign others points.
    pub(super) local_id: PeerId,
    /// source of updates, remapped and filtered locally
    pub(super) overlay: PrivateOverlay,
    /// update task, aborts on drop
    /// Note: we don't expect peer removal events from Network as it'll be a second source of truth
    pub(super) resolve_peers_task: Option<Task<()>>,
    pub(super) banned: FastHashSet<PeerId>,
    // Connection to self is always "Added"
    // Updates are Resolved or Removed, sent single time
    // Must be kept under Mutex to provide consistent updates on retrieved data
    updates: broadcast::Sender<(PeerId, PeerState)>,
    pub data: PeerScheduleStateful,
}

impl PeerScheduleLocked {
    pub(super) fn new(local_id: PeerId, overlay: PrivateOverlay) -> Self {
        Self {
            local_id,
            overlay,
            resolve_peers_task: None,
            banned: FastHashSet::default(),
            updates: broadcast::Sender::new(PeerCount::MAX.full()),
            data: PeerScheduleStateful::default(),
        }
    }

    pub fn updates(&self) -> Receiver<(PeerId, PeerState)> {
        self.updates.subscribe()
    }

    /// Returns [true] if update was successfully applied.
    /// Always keeps local id as [`PeerState::Unknown`]
    pub(super) fn set_resolved(&mut self, peer_id: &PeerId) -> bool {
        let resolve_applied = self.data.set_state(peer_id, PeerState::Resolved);
        if resolve_applied {
            _ = self.updates.send((*peer_id, PeerState::Resolved));
        }
        resolve_applied
    }

    pub(super) fn set_banned(&mut self, peers: &[PeerId], parent: WeakPeerSchedule) {
        let mut entries = self.overlay.write_entries();
        let mut need_resolve = false;
        for peer_id in peers {
            self.banned.insert(*peer_id);

            entries.remove(peer_id);

            if self.data.is_in_any_vset(peer_id) {
                let remove_applied = self.data.set_state(peer_id, PeerState::Unknown);
                if remove_applied {
                    _ = self.updates.send((*peer_id, PeerState::Unknown));
                }
                need_resolve |= remove_applied;
            }
        }
        if need_resolve {
            drop(self.resolve_peers_task.take());
            let entries = entries.downgrade();
            let resolved_waiters = WeakPeerSchedule::resolved_waiters(entries, &self.local_id);
            self.resolve_peers_task = parent.new_resolve_task(resolved_waiters);
        }
    }

    pub(super) fn remove_bans(&mut self, peers: &[PeerId], parent: WeakPeerSchedule) {
        let mut entries = self.overlay.write_entries();
        let mut need_resolve = false;
        for peer_id in peers {
            self.banned.remove(peer_id);

            if self.data.is_in_any_vset(peer_id) {
                entries.insert(peer_id);

                let already_resolved =
                    (entries.get_handle(peer_id)).is_some_and(|handle| handle.is_resolved());
                if already_resolved {
                    let resolve_applied = self.data.set_state(peer_id, PeerState::Resolved);
                    if resolve_applied {
                        _ = self.updates.send((*peer_id, PeerState::Resolved));
                    }
                } else {
                    need_resolve = true;
                }
            }
        }
        if need_resolve {
            drop(self.resolve_peers_task.take());
            let entries = entries.downgrade();
            let resolved_waiters = WeakPeerSchedule::resolved_waiters(entries, &self.local_id);
            self.resolve_peers_task = parent.new_resolve_task(resolved_waiters);
        }
    }

    pub(super) fn forget_oldest(&mut self, parent: WeakPeerSchedule) {
        meter(false); // because used simultaneously with rotate()

        let to_forget = self.data.forget_oldest();

        let mut entries = self.overlay.write_entries();
        for peer_id in to_forget {
            entries.remove(&peer_id);
        }

        drop(self.resolve_peers_task.take());
        let entries = entries.downgrade();
        let resolved_waiters = WeakPeerSchedule::resolved_waiters(entries, &self.local_id);
        self.resolve_peers_task = parent.new_resolve_task(resolved_waiters);
    }

    pub(super) fn set_next_set(&mut self, parent: WeakPeerSchedule, validator_set: &[PeerId]) {
        meter(validator_set.contains(&self.local_id));

        let mut entries = self.overlay.write_entries();

        for peer_id in validator_set {
            if !self.banned.contains(peer_id) {
                entries.insert(peer_id);
            }
        }

        let all_resolved = entries
            .iter()
            .filter(|a| a.resolver_handle.is_resolved() && a.peer_id != self.local_id)
            .map(|a| a.peer_id)
            .collect::<FastHashSet<_>>();

        let to_forget = (self.data).set_next_validator_set(validator_set, all_resolved);

        for peer_id in to_forget {
            entries.remove(&peer_id);
        }

        drop(self.resolve_peers_task.take());
        let entries = entries.downgrade();
        let resolved_waiters = WeakPeerSchedule::resolved_waiters(entries, &self.local_id);
        self.resolve_peers_task = parent.new_resolve_task(resolved_waiters);
    }
}

fn meter(is_in_next_vset: bool) {
    metrics::gauge!("tycho_mempool_peer_in_next_vset").set(is_in_next_vset as u8);
}
