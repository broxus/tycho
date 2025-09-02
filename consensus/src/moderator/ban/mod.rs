use tycho_network::PeerId;

use crate::models::UnixTime;
use crate::moderator::RecordKey;
use crate::moderator::journal::item::ItemOrigin;

pub mod core;
mod events_cache;
mod peer_events;

struct QueuedBanItem {
    key: RecordKey,
    peer_id: PeerId,
    until: UnixTime,
    /// contains a value if should be stored or empty if already stored
    to_store: Option<ItemOrigin>,
}
