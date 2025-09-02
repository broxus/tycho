use std::collections::hash_map;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::time::{DelayQueue, delay_queue};
use tycho_network::{Network, PeerId};
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;

use crate::models::UnixTime;
use crate::moderator::ban::QueuedBanItem;
use crate::moderator::ban::events_cache::{EventsCache, EventsCacheBack};
use crate::moderator::journal::item::{
    BanItem, ItemOrigin, JournalItem, JournalItemFull, UnbanItem,
};
use crate::moderator::{BanConfig, JournalEvent, RecordKey, RecordValueShort};

pub struct BanCore {
    cache: EventsCache,
    ban_queue: mpsc::UnboundedSender<QueuedBanItem>,
    _updater: JoinTask<()>, // outlives mempool session(s) as a moderator part
}

impl BanCore {
    pub fn new(
        network: &Network,
        delayed_db_writes_tx: &mpsc::UnboundedSender<Vec<JournalItemFull>>,
    ) -> Self {
        let (ban_tx, ban_rx) = mpsc::unbounded_channel();
        let cache = EventsCache::default();
        let cache_back = cache.back();
        Self {
            cache,
            ban_queue: ban_tx,
            _updater: JoinTask::new(updater(
                network.clone(),
                cache_back,
                delayed_db_writes_tx.clone(),
                ban_rx,
            )),
        }
    }

    /// Internal [`ByPeer`] counts which ban is topmost.
    /// May return new ban items that should be stored.
    pub fn restore(
        &self,
        now: UnixTime,
        shorts: &[(RecordKey, RecordValueShort)],
        ban_config: &BanConfig,
    ) -> Vec<JournalItemFull> {
        let bans = self.cache.restore(now, shorts, ban_config);
        let mut new_items = Vec::with_capacity(
            bans.iter()
                .filter(|(_, item)| item.to_store.is_some())
                .count(),
        );
        for ban in bans.into_values() {
            if let Some(origin) = ban.to_store {
                let item = JournalItem::Banned(BanItem {
                    peer_id: ban.peer_id,
                    until: ban.until,
                    origin,
                });
                new_items.push(JournalItemFull { key: ban.key, item });
            }
            self.ban_queue.send(ban).ok();
        }
        new_items
    }

    pub fn maybe_ban_item(
        &self,
        key: RecordKey,
        event: &JournalEvent,
        ban_config: &BanConfig,
    ) -> Option<JournalItem> {
        if !event.action().is_ban_related() {
            return None;
        };
        let until = (self.cache).new_ban_until(event.peer_id(), key, event.tag(), ban_config)?;

        Some(JournalItem::Banned(BanItem {
            peer_id: *event.peer_id(),
            until,
            origin: ItemOrigin::Parent {
                key,
                tag: Some(event.tag()),
            },
        }))
    }

    pub fn send_ban(&self, key: RecordKey, item: &JournalItem) {
        let JournalItem::Banned(BanItem {
            peer_id,
            until,
            origin,
        }) = item
        else {
            return;
        };

        let new_ban_item = QueuedBanItem {
            key,
            peer_id: *peer_id,
            until: *until,
            to_store: Some(*origin),
        };
        self.ban_queue.send(new_ban_item).ok(); // consumer is closed only at shutdown
    }
}

async fn updater(
    network: Network,
    cache_back: EventsCacheBack,
    delayed_db_writes_tx: mpsc::UnboundedSender<Vec<JournalItemFull>>,
    mut ban_queue: mpsc::UnboundedReceiver<QueuedBanItem>,
) {
    let mut last_bans = FastHashMap::<PeerId, (delay_queue::Key, UnixTime)>::default();
    let mut unban_queue = DelayQueue::<UnbanItem>::new();
    loop {
        tokio::select! {
            Some(ban) = ban_queue.recv() => {
                // Note: restored ban doesn't contain tag so new unban item won't always inherit it
                let tag = match ban.to_store {
                    Some(ItemOrigin::Parent { tag, .. }) => tag ,
                    None => None,
                };
                let queued = UnbanItem {
                    peer_id: ban.peer_id,
                    origin: ItemOrigin::Parent { key: ban.key, tag },
                };
                let unban_wait = Duration::from_millis((ban.until - UnixTime::now()).millis());
                let is_new = match last_bans.entry(ban.peer_id) {
                    hash_map::Entry::Occupied(mut occupied) => {
                        let (dq_key, old_until) = *occupied.get();
                        if ban.until > old_until {
                            unban_queue.reset(&dq_key, unban_wait);
                            occupied.insert((dq_key, ban.until));
                        }
                        false
                    }
                    hash_map::Entry::Vacant(vacant) => {
                        let dq_key = unban_queue.insert(queued, unban_wait);
                        vacant.insert((dq_key, ban.until));
                        true
                    }
                };
                if is_new {
                    network.known_peers().ban(&ban.peer_id);
                }
            },
            Some(expired) = unban_queue.next() => {
                let (dq_key , unban_at) = last_bans
                    .remove(&expired.get_ref().peer_id)
                    .expect("peer unban was scheduled without last_bans entry");
                assert_eq!(expired.key(), dq_key, "last_bans entry has wrong delay queue key");

                let unban = expired.into_inner();
                if cache_back.remove_last_ban(&unban.peer_id, unban_at).is_err() {
                    continue; // newer ban is enqueued, don't toggle known peer
                }
                network.known_peers().remove(&unban.peer_id);

                let item_full = JournalItemFull {
                    key: RecordKey::new(),
                    item: JournalItem::Unbanned(unban),
                };
                delayed_db_writes_tx.send(vec![item_full]).ok();
            },
            else => break,
        }
    }
    tracing::warn!("Mempool ban core updater shut down");
}
