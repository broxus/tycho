use std::cmp;

use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::models::UnixTime;
use crate::moderator::ban::current_bans::CurrentBans;
use crate::moderator::ban::peer_events::PeerEvents;
use crate::moderator::ban::{CurrentBan, UpdaterQueueItem};
use crate::moderator::journal::item::{BanItem, BanOrigin, JournalItem, JournalItemFull};
use crate::moderator::journal::record_key::RecordKeyFactory;
use crate::moderator::{BanConfig, EventTag, RecordKey, RecordKind, RecordValueShort};

/// Event history and bans are separated because (un)bans should not affect accumulated events:
/// * ban toleration window may be larger than its duration - in that case
///   a new event right after an unban must generate a new ban
/// * manual (un)bans must affect only other manual and auto (un)bans but not event history
pub struct BanCache {
    events: FastHashMap<PeerId, PeerEvents>,
    current_bans: CurrentBans,
    updates_tx: mpsc::UnboundedSender<UpdaterQueueItem>,
}

impl BanCache {
    pub fn new(updates_tx: mpsc::UnboundedSender<UpdaterQueueItem>) -> Self {
        Self {
            events: FastHashMap::default(),
            current_bans: CurrentBans::default(),
            updates_tx,
        }
    }

    pub fn exposed(&self) -> BanCacheExposed<'_> {
        BanCacheExposed(self)
    }

    pub fn restore(
        &mut self,
        kf: &mut RecordKeyFactory,
        now: UnixTime,
        shorts: &[(RecordKey, RecordValueShort)],
        ban_config: &BanConfig,
    ) -> Vec<JournalItemFull> {
        assert!(
            shorts.is_sorted_by_key(|(k, _)| k),
            "keys needed in historical order"
        );
        enum Temp {
            New(BanItem),
            Stored(CurrentBan),
        }
        impl Temp {
            fn until(&self) -> UnixTime {
                match self {
                    Temp::New(item) => item.until,
                    Temp::Stored(q_ban) => q_ban.until,
                }
            }
        }

        let mut temp = FastHashMap::<PeerId, Temp>::default();

        for (key, short) in shorts {
            assert!(short.is_ban_related, "restore only ban related records");
            let peer_events = self.events.entry(short.peer_id).or_default();
            match short.kind {
                RecordKind::NodeStarted => {}
                RecordKind::Event(tag) => {
                    if let Some(until) = peer_events.maybe_ban_until(key, tag, ban_config)
                        && until > now
                        // do not overwrite already created bans if those exist, thus `old.until < until`
                        && (temp.get(&short.peer_id)).is_none_or(|old| old.until() < until)
                    {
                        temp.insert(
                            short.peer_id,
                            Temp::New(BanItem {
                                peer_id: short.peer_id,
                                until,
                                origin: BanOrigin::Parent { key: *key, tag },
                            }),
                        );
                    };
                }
                RecordKind::Banned(until) => {
                    if until > now
                        // overwrite bans newly created in this loop, thus `old.until <= until`
                        && (temp.get(&short.peer_id)).is_none_or(|old| old.until() <= until)
                    {
                        temp.insert(short.peer_id, Temp::Stored(CurrentBan { key: *key, until }));
                    }
                }
                RecordKind::Unbanned => {
                    temp.remove(&short.peer_id);
                }
            }
        }

        let mut new_records = Vec::new();

        for (peer_id, item) in temp {
            match item {
                Temp::New(new) => {
                    let q_ban = CurrentBan {
                        key: kf.new_key(),
                        until: new.until,
                    };

                    self.current_bans.upsert(&peer_id, q_ban).ok();

                    self.updates_tx
                        .send(UpdaterQueueItem::AutoUnban { peer_id, q_ban })
                        .ok();

                    new_records.push(JournalItemFull {
                        key: q_ban.key,
                        item: JournalItem::Banned(new),
                    });
                }
                Temp::Stored(q_ban) => {
                    self.current_bans.upsert(&peer_id, q_ban).ok();

                    self.updates_tx
                        .send(UpdaterQueueItem::AutoUnban { peer_id, q_ban })
                        .ok();
                }
            }
        }

        new_records
    }

    /// * returns `None` in case no ban is applicable
    /// * returns `Some(_, true)` in case a ban is new and should be applied at network level
    pub fn maybe_ban(
        &mut self,
        kf: &mut RecordKeyFactory,
        peer_id: &PeerId,
        key: &RecordKey,
        tag: EventTag,
        ban_config: &BanConfig,
    ) -> Option<(CurrentBan, bool)> {
        let peer_events = self.events.entry(*peer_id).or_default();
        let until = peer_events.maybe_ban_until(key, tag, ban_config)?;
        let q_ban = CurrentBan {
            until,
            key: kf.new_key(),
        };
        let is_first = match self.current_bans.upsert(peer_id, q_ban) {
            Ok(is_first) => {
                self.updates_tx
                    .send(UpdaterQueueItem::AutoUnban {
                        peer_id: *peer_id,
                        q_ban,
                    })
                    .ok();
                is_first
            }
            Err(_) => false,
        };
        Some((q_ban, is_first))
    }

    /// does not affect accumulated penalties in events cache
    /// * returns `Ok(is_first)` if stored a new value that ends later than previous
    ///   * `true` in case a ban is new and should be applied at network level
    ///   * `false` in case a ban at network level already exists
    /// * returns `Err` in case a longer ban exists and this call is a no-op
    pub fn manual_ban(&mut self, peer_id: &PeerId, q_ban: CurrentBan) -> Result<bool, ()> {
        let result = self.current_bans.upsert(peer_id, q_ban);
        if result.is_ok() {
            self.updates_tx
                .send(UpdaterQueueItem::AutoUnban {
                    peer_id: *peer_id,
                    q_ban,
                })
                .ok();
        }
        result
    }

    /// does not affect accumulated penalties in events cache
    /// * returns `Err` in case a longer ban was set concurrently
    pub fn auto_unban(&mut self, peer_id: &PeerId, expected_until: UnixTime) -> Result<(), ()> {
        let ord = self
            .current_bans
            .inner()
            .get(peer_id)
            .map(|current| current.until.cmp(&expected_until));
        match ord {
            Some(cmp::Ordering::Less) => {
                // Note: `2 => NONE => 1` is impossible since bans are removed by a single thread
                panic!("bans cannot be set to lower values")
            }
            Some(cmp::Ordering::Equal) => self.current_bans.remove(peer_id), // Ok
            Some(cmp::Ordering::Greater) => Err(()), // newer auto (un)ban waits in channel
            None => panic!("bans cannot be removed without reset of delay queue"),
        }
    }

    /// does not affect accumulated penalties in events cache
    /// * returns `Err` in case peer was not banned
    pub fn manual_unban(&mut self, peer_id: &PeerId) -> Result<(), ()> {
        self.current_bans.remove(peer_id)
    }
}

pub struct BanCacheExposed<'a>(&'a BanCache);
impl BanCacheExposed<'_> {
    pub fn banned_peers(&self) -> Vec<PeerId> {
        self.0.current_bans.inner().keys().copied().collect()
    }

    pub fn dump_bans(&self) -> FastHashMap<PeerId, CurrentBan> {
        self.0.current_bans.inner().clone()
    }

    pub fn dump_events(&self, peer_id: Option<&PeerId>) -> serde_json::Value {
        let events = &self.0.events;
        match peer_id {
            None => {
                let mut map = serde_json::Map::with_capacity(events.len());
                for (peer_id, item) in events {
                    map.insert(peer_id.to_string(), serde_json::json!(item));
                }
                map.sort_keys();
                serde_json::Value::Object(map)
            }
            Some(peer_id) => {
                let k = peer_id.to_string();
                match events.get(peer_id) {
                    None => serde_json::json!({k: {}}),
                    Some(item) => serde_json::json!({k: item}),
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use super::*;
    use crate::effects::AltFormat;
    use crate::moderator::journal::record_key::RecordKeyFactory;
    use crate::moderator::{BanConfigValue, BanToleration, EventTag};
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

        let mut kf = RecordKeyFactory::default();

        let now = UnixTime::from_millis(1_000_000);
        let peer_id = PeerId([0; _]);

        println!("now {}", now.millis());

        let shorts = [
            (kf.new_millis(0), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Event(EventTag::BadQuery),
                peer_id,
            }),
            (kf.new_millis(700_001), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Unbanned,
                peer_id,
            }),
            (kf.new_millis(700_002), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Event(EventTag::BadQuery),
                peer_id,
            }),
            (kf.new_millis(700_003), RecordValueShort {
                is_ban_related: true,
                kind: RecordKind::Banned(UnixTime::from_millis(100_500)),
                peer_id,
            }),
        ];

        let (tx, _rx) = mpsc::unbounded_channel();
        let mut cache = BanCache::new(tx);

        cache.restore(&mut kf, now, &shorts, &config);

        println!("\nqueued items count: {}", cache.current_bans.inner().len());

        for (peer_id, q_ban) in cache.current_bans.inner() {
            println!("{} => {:?}", peer_id.alt(), q_ban.until.millis());
        }

        println!("\ncached items count: {}", cache.events.len());

        let json = serde_json::json!(cache.exposed().dump_events(None));
        println!("{}", serde_json::to_string_pretty(&json)?);

        anyhow::ensure!(
            cache.current_bans.inner().is_empty(),
            "ban time must have passed"
        );
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
