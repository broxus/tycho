use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::{FastDashMap, FastHashMap};

use crate::models::UnixTime;
use crate::moderator::ban::QueuedBanItem;
use crate::moderator::ban::peer_events::PeerEvents;
use crate::moderator::journal::item::ItemOrigin;
use crate::moderator::{BanConfig, EventTag, RecordKey, RecordKind, RecordValueShort};

/// Track per-peer events history to create bans
#[derive(Default)]
pub struct EventsCache(Arc<FastDashMap<PeerId, PeerEvents>>);

impl EventsCache {
    #[cfg(test)]
    pub fn dump(&self, peer_id: Option<&PeerId>) -> serde_json::Value {
        match peer_id {
            None => {
                let mut map = serde_json::Map::with_capacity(self.0.len());
                for item in self.0.as_ref() {
                    map.insert(item.key().to_string(), serde_json::json!(item.value()));
                }
                map.sort_keys();
                serde_json::Value::Object(map)
            }
            Some(peer_id) => {
                let k = peer_id.to_string();
                match self.0.get(peer_id) {
                    None => serde_json::json!({k: {}}),
                    Some(item) => serde_json::json!({k: item.value()}),
                }
            }
        }
    }

    pub fn new_ban_until(
        &self,
        peer_id: &PeerId,
        key: RecordKey,
        tag: EventTag,
        ban_config: &BanConfig,
    ) -> Option<UnixTime> {
        let mut by_peer = self.0.entry(*peer_id).or_default();
        by_peer.new_ban_until(key, tag, ban_config)
    }

    /// Internal [`PeerEvents`] counts which ban is topmost.
    /// May return new ban items that should be stored.
    pub fn restore(
        &self,
        now: UnixTime,
        shorts: &[(RecordKey, RecordValueShort)],
        ban_config: &BanConfig,
    ) -> FastHashMap<PeerId, QueuedBanItem> {
        assert!(
            shorts.is_sorted_by_key(|(k, _)| k),
            "keys needed in historical order"
        );
        let mut bans = FastHashMap::<PeerId, QueuedBanItem>::default();

        for (key, short) in shorts {
            assert!(short.is_ban_related, "restore only ban related records");
            let mut by_peer = self.0.entry(short.peer_id).or_default();
            match short.kind {
                RecordKind::NodeStarted => {}
                RecordKind::Event(tag) => {
                    if let Some(until) = by_peer.new_ban_until(*key, tag, ban_config)
                        && until > now
                        // do not overwrite already created bans if those exist, thus `old.until < until`
                        && (bans.get(&short.peer_id)).is_none_or(|old| old.until < until)
                    {
                        let new_ban_item = QueuedBanItem {
                            key: RecordKey::new(),
                            peer_id: short.peer_id,
                            until,
                            to_store: Some(ItemOrigin::Parent {
                                key: *key,
                                tag: Some(tag),
                            }),
                        };
                        bans.insert(short.peer_id, new_ban_item);
                    };
                }
                RecordKind::Banned(until) => {
                    if until > now
                    && by_peer.reset_ban_until(until)
                        // overwrite bans newly created in this loop, thus `old.until <= until`
                        && (bans.get(&short.peer_id)).is_none_or(|old| old.until <= until)
                    {
                        let new_ban_item = QueuedBanItem {
                            key: *key,
                            peer_id: short.peer_id,
                            until,
                            to_store: None, // already stored
                        };
                        bans.insert(short.peer_id, new_ban_item);
                    }
                }
                RecordKind::Unbanned => {
                    by_peer.set_no_ban(UnixTime::from_millis(u64::MAX));
                    bans.remove(&short.peer_id);
                }
            }
        }

        bans
    }

    pub fn back(&self) -> EventsCacheBack {
        EventsCacheBack(Arc::clone(&self.0))
    }
}

/// Track removal from bans for consistent snapshots (most of the time)
pub struct EventsCacheBack(Arc<FastDashMap<PeerId, PeerEvents>>);

impl EventsCacheBack {
    /// Ok if no newer ban is enqueued
    pub fn remove_last_ban(&self, peer_id: &PeerId, expected_last: UnixTime) -> Result<(), ()> {
        match self.0.get_mut(peer_id) {
            Some(mut cached) => {
                if cached.set_no_ban(expected_last) {
                    Ok(())
                } else {
                    Err(())
                }
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use super::*;
    use crate::effects::AltFormat;
    use crate::moderator::{BanConfigValue, BanToleration};
    use crate::test_utils::default_test_config;

    #[test]
    fn test() -> Result<()> {
        let _ = default_test_config();

        let config = BanConfig::filled_with(BanConfigValue {
            duration: serde_json::from_str("\"1m1s\"")?,
            toleration: Some(BanToleration {
                duration: serde_json::from_str("\"1M\"")?,
                count: 1.try_into()?,
            }),
        })?;

        let now = UnixTime::from_millis(1_000_000);
        let peer_id = PeerId([0; _]);

        println!("now {}", now.millis());

        let shorts = [
            (RecordKey::new_millis(0), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Event(EventTag::BadQuery),
                peer_id,
            }),
            (RecordKey::new_millis(700_001), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Unbanned,
                peer_id,
            }),
            (RecordKey::new_millis(700_002), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Event(EventTag::BadQuery),
                peer_id,
            }),
            (RecordKey::new_millis(700_003), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Banned(UnixTime::from_millis(100_500)),
                peer_id,
            }),
        ];

        let cache = EventsCache::default();

        let mut bans = cache
            .restore(now, &shorts, &config)
            .into_iter()
            .collect::<Vec<_>>();

        println!("\nqueued items count: {}", bans.len());

        bans.sort_unstable_by_key(|(peer_id, _)| *peer_id);

        for (peer_id, queued) in &bans {
            println!(
                "{} => {:?} {}",
                peer_id.alt(),
                queued.until.millis(),
                queued.to_store.map_or("stored", |_| "to store")
            );
        }

        println!("\ncached items count: {}", cache.0.len());

        let json = serde_json::json!(cache.dump(None));
        println!("{}", serde_json::to_string_pretty(&json)?);

        anyhow::ensure!(bans.is_empty(), "ban time must have passed");
        anyhow::ensure!(
            json[peer_id.to_string()]["events"]["bad_query"]["tolerated"]
                .as_array()
                .map_or(0, |a| a.len())
                == 1,
            "must not hold more events than to tolerate"
        );

        Ok(())
    }
}
