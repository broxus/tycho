use tokio::sync::mpsc;
use tycho_network::PeerId;

use crate::intercom::{PeerSchedule, WeakPeerSchedule};
use crate::models::UnixTime;
use crate::moderator::ban::cache::BanCache;
use crate::moderator::ban::{CurrentBan, UpdaterQueueItem};
use crate::moderator::journal::item::JournalItemFull;
use crate::moderator::journal::record_key::RecordKeyFactory;
use crate::moderator::{BanConfig, EventTag, RecordKey, RecordValueShort};

pub struct BanCoreState {
    pub kf: RecordKeyFactory,
    cache: BanCache,
    peer_schedule: Option<WeakPeerSchedule>,
}

impl BanCoreState {
    pub fn new(kf: RecordKeyFactory, updates_tx: mpsc::UnboundedSender<UpdaterQueueItem>) -> Self {
        Self {
            kf,
            cache: BanCache::new(updates_tx),
            peer_schedule: None,
        }
    }

    /// returns newly created bans by current config, if no longer bans were stored
    pub fn restore(
        &mut self,
        now: UnixTime,
        shorts: &[(RecordKey, RecordValueShort)],
        ban_config: &BanConfig,
    ) -> Vec<JournalItemFull> {
        let new_ban_records = (self.cache).restore(&mut self.kf, now, shorts, ban_config);
        let banned = self.cache.exposed().banned_peers();
        if !banned.is_empty() {
            for peer_id in &banned {
                meter_banned(peer_id, true);
            }
            if let Some(peer_schedule) = self.peer_schedule.as_ref().and_then(|ps| ps.upgrade()) {
                peer_schedule.set_banned(&banned);
            }
        }
        new_ban_records
    }

    pub fn set_peer_schedule(&mut self, peer_schedule: &PeerSchedule) {
        let banned = self.cache.exposed().banned_peers();
        peer_schedule.set_banned(&banned);
        self.peer_schedule = Some(peer_schedule.downgrade());
    }

    pub fn maybe_ban(
        &mut self,
        peer_id: &PeerId,
        key: &RecordKey,
        tag: EventTag,
        ban_config: &BanConfig,
    ) -> Option<CurrentBan> {
        let (q_ban, is_first) =
            (self.cache).maybe_ban(&mut self.kf, peer_id, key, tag, ban_config)?;
        if is_first {
            meter_banned(peer_id, true);
            if let Some(peer_schedule) = self.peer_schedule.as_ref().and_then(|ps| ps.upgrade()) {
                peer_schedule.set_banned(&[*peer_id]);
            }
        }
        Some(q_ban)
    }

    /// returns `Err` in case a ban to remove neither exist nor matches expected
    pub fn auto_unban(&mut self, peer_id: &PeerId, expected_until: UnixTime) -> Result<(), ()> {
        self.cache.auto_unban(peer_id, expected_until)?;
        meter_banned(peer_id, false);
        if let Some(peer_schedule) = self.peer_schedule.as_ref().and_then(|ps| ps.upgrade()) {
            peer_schedule.remove_bans(&[*peer_id]);
        }
        Ok(())
    }
}

/// Meters when peer gets (un)banned on network layer, not when ban is written to DB
fn meter_banned(peer_id: &PeerId, is_banned: bool) {
    let labels = [("peer_id", format!("{:.4}", peer_id))];
    metrics::gauge!("tycho_mempool_moderator_banned", &labels).set(is_banned as u8);
}
