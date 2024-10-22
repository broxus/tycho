use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::task::AbortHandle;
use tycho_network::{PeerId, PrivateOverlay, PrivateOverlayEntriesReadGuard};

use crate::intercom::dto::PeerState;
use crate::intercom::peer_schedule::stateful::PeerScheduleStateful;
use crate::intercom::PeerSchedule;
use crate::models::PeerCount;

pub struct PeerScheduleLocked {
    /// Keypair may be changed only with node restart, and is known before validator elections.
    /// Node should use its keypair only to produce own and sign others points.
    pub(super) local_id: PeerId,
    /// source of updates, remapped and filtered locally
    pub(super) overlay: PrivateOverlay,
    /// update task
    pub(super) abort_resolve_peers: Option<AbortHandle>,
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
            abort_resolve_peers: None,
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

    pub(super) fn update_resolve(
        &mut self,
        parent: &PeerSchedule,
        to_remove: &[PeerId],
        to_add: &[PeerId],
    ) -> PrivateOverlayEntriesReadGuard<'_> {
        let entries = {
            let mut entries = self.overlay.write_entries();
            for peer_id in to_remove {
                entries.remove(peer_id);
            }
            for peer_id in to_add {
                entries.insert(peer_id);
            }
            entries.downgrade()
        };
        let resolved_waiters = PeerSchedule::resolved_waiters(&self.local_id, &entries);
        if let Some(handle) = &self.abort_resolve_peers {
            handle.abort();
        }
        self.abort_resolve_peers = parent.clone().new_resolve_task(resolved_waiters);
        entries
    }
}
