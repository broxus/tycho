mod cache;
pub mod core;
mod current_bans;
mod peer_events;
mod state;

#[derive(Debug, Copy, Clone, serde::Serialize)]
pub struct CurrentBan {
    pub until: crate::models::UnixTime,
    pub key: crate::moderator::RecordKey,
}

enum UpdaterQueueItem {
    Event(crate::moderator::JournalEvent),
    AutoUnban {
        peer_id: tycho_network::PeerId,
        q_ban: CurrentBan,
    },
}
