use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tycho_network::{PeerId, PrivateOverlay};
use tycho_util::FastHashSet;

use crate::effects::Task;
use crate::intercom::peer_schedule::stateful::PeerScheduleStateful;
use crate::intercom::peer_schedule::{PeerState, WeakPeerSchedule};
use crate::intercom::PeerSchedule;
use crate::models::PeerCount;

pub struct PeerScheduleLocked {
    /// Keypair may be changed only with node restart, and is known before validator elections.
    /// Node should use its keypair only to produce own and sign others points.
    pub(super) local_id: PeerId,
    /// source of updates, remapped and filtered locally
    pub(super) overlay: PrivateOverlay,
    /// update task, aborts on drop
    pub(super) resolve_peers_task: Option<Task<()>>,
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
            updates: broadcast::Sender::new(PeerCount::MAX.full()),
            data: PeerScheduleStateful::default(),
        }
    }

    pub fn updates(&self) -> Receiver<(PeerId, PeerState)> {
        self.updates.subscribe()
    }

    /// Returns [true] if update was successfully applied.
    /// Always keeps local id as [`PeerState::Unknown`]
    pub(super) fn set_state(&mut self, peer_id: &PeerId, state: PeerState) -> bool {
        let is_applied = self.data.set_state(peer_id, state);
        if is_applied {
            _ = self.updates.send((*peer_id, state));
        }
        is_applied
    }

    pub(super) fn forget_oldest(&mut self, parent: WeakPeerSchedule) {
        // because used simultaneously with rotate()
        meter(false);

        let to_forget = self.data.forget_oldest();
        self.resolve_peers_task = None;
        let resolved_waiters = {
            let mut entries = self.overlay.write_entries();
            for peer_id in to_forget {
                entries.remove(&peer_id);
            }
            PeerSchedule::resolved_waiters(&self.local_id, &entries.downgrade())
        };
        self.resolve_peers_task = parent.new_resolve_task(resolved_waiters);
    }

    pub(super) fn set_next_set(&mut self, parent: WeakPeerSchedule, validator_set: &[PeerId]) {
        self.resolve_peers_task = None;

        meter(validator_set.contains(&self.local_id));

        let resolved_waiters = {
            let mut write_entries = self.overlay.write_entries();

            for peer_id in validator_set {
                write_entries.insert(peer_id);
            }

            let all_resolved = write_entries
                .iter()
                .filter(|a| a.resolver_handle.is_resolved() && a.peer_id != self.local_id)
                .map(|a| a.peer_id)
                .collect::<FastHashSet<_>>();

            let to_forget = self
                .data
                .set_next_validator_set(validator_set, all_resolved);

            for peer_id in to_forget {
                write_entries.remove(&peer_id);
            }

            let read_entries = write_entries.downgrade();

            PeerSchedule::resolved_waiters(&self.local_id, &read_entries)
        };

        self.resolve_peers_task = parent.new_resolve_task(resolved_waiters);
    }
}

fn meter(is_in_next_vset: bool) {
    metrics::gauge!("tycho_mempool_peer_in_next_vset").set(is_in_next_vset as u8);
}
