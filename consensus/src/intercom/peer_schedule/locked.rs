use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::task::AbortHandle;
use tycho_network::{PeerId, PrivateOverlay};
use tycho_util::FastHashSet;

use crate::intercom::dto::PeerState;
use crate::intercom::peer_schedule::stateful::PeerScheduleStateful;
use crate::intercom::peer_schedule::utils;
use crate::intercom::PeerSchedule;
use crate::models::{PeerCount, Round};

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

    pub fn set_next_peers(
        &mut self,
        peers: &[PeerId],
        parent: &PeerSchedule,
        update_overlay: bool,
    ) {
        let all_resolved = {
            let entries = if update_overlay {
                let entries = {
                    let mut entries = self.overlay.write_entries();
                    for peer_id in peers {
                        entries.insert(peer_id);
                    }
                    entries.downgrade()
                };
                let resolved_waiters = utils::resolved_waiters(&self.local_id, &entries);
                if let Some(handle) = &self.abort_resolve_peers {
                    handle.abort();
                }
                self.abort_resolve_peers =
                    utils::new_resolve_task(parent.clone(), resolved_waiters);
                entries
            } else {
                self.overlay.read_entries()
            };
            entries
                .iter()
                .filter(|a| a.resolver_handle.is_resolved() && a.peer_id != self.local_id)
                .map(|a| a.peer_id)
                .collect::<FastHashSet<_>>()
        };

        self.data.set_next_peers(peers, all_resolved);
        // atomic part is updated under lock too
        parent.update_atomic(|stateless| stateless.set_next_peers(peers));
    }

    pub fn set_next_start(&mut self, round: Round, parent: &PeerSchedule) {
        self.data.set_next_start(round);
        // atomic part is updated under lock too
        parent.update_atomic(|stateless| stateless.set_next_start(round));
    }

    /// on epoch change
    pub fn rotate(&mut self, parent: &PeerSchedule) {
        self.data.rotate();
        // atomic part is updated under lock too
        parent.update_atomic(|stateless| stateless.rotate());
    }

    /// after successful sync to current epoch
    /// and validating all points from previous peer set
    /// free some memory and ignore overlay updates
    #[allow(dead_code)] // TODO use on change of validator set
    pub fn forget_previous(&mut self, parent: &PeerSchedule) {
        self.data.forget_previous();
        // atomic part is updated under lock too
        parent.update_atomic(|stateless| stateless.forget_previous());
    }
}
