use std::collections::hash_map;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::time::DelayQueue;
use tycho_network::{Network, PeerId};
use tycho_util::futures::JoinTask;
use tycho_util::{FastDashMap, FastHashMap};

use crate::models::UnixTime;
use crate::moderator::RecordKey;
use crate::moderator::ban::by_peer::ByPeer;
use crate::moderator::journal::{JournalEvent, JournalRecord, RecordData};
use crate::moderator::stored::{RecordAction, RecordKind, RecordOrigin, RecordValueShort};

pub struct BanCore {
    by_peer: FastDashMap<PeerId, ByPeer>,
    ban_queue: mpsc::UnboundedSender<NewBanItem>,
    _updater: JoinTask<()>, // outlives mempool session(s) as a moderator part
}

impl BanCore {
    pub fn new(
        network: &Network,
        delayed_db_writes_tx: &mpsc::UnboundedSender<Vec<JournalRecord>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            by_peer: FastDashMap::default(),
            ban_queue: tx,
            _updater: JoinTask::new(updater(network.clone(), delayed_db_writes_tx.clone(), rx)),
        }
    }

    /// Internal [`ByPeer`] counts which ban is topmost.
    /// May return new ban items that should be stored.
    pub fn restore(&self, shorts: &[(RecordKey, RecordValueShort)]) -> Vec<JournalRecord> {
        assert!(
            shorts.is_sorted_by_key(|(k, _)| k),
            "keys needed in historical order"
        );
        enum State {
            Stored,
            NeedsStore,
        }
        let mut bans = FastHashMap::<PeerId, (State, NewBanItem)>::default();

        for (key, short) in shorts {
            let mut by_peer = self.by_peer.entry(short.peer_id).or_default();
            match short.kind {
                RecordKind::NodeStarted => {}
                RecordKind::Banned { until, origin, .. } => {
                    if by_peer.reset_ban_until(until)
                        // overwrite bans newly created in this loop, thus `old.until <= until`
                        && (bans.get(&short.peer_id)).is_none_or(|(_, old)| old.until <= until)
                    {
                        let new_ban_item = NewBanItem {
                            key: *key,
                            peer_id: short.peer_id,
                            origin,
                            until,
                        };
                        bans.insert(short.peer_id, (State::Stored, new_ban_item));
                    }
                }
                RecordKind::Unbanned(_) => {
                    by_peer.remove_ban();
                    bans.remove(&short.peer_id);
                }
                RecordKind::Event(tag) => {
                    match short.action {
                        RecordAction::Store => continue,
                        RecordAction::CountPenalty | RecordAction::StoreAndCountPenalty => {}
                    }
                    if let Some(until) = by_peer.new_ban_until(*key, tag)
                        // do not overwrite already created bans if those exist, thus `old.until < until`
                        && (bans.get(&short.peer_id)).is_none_or(|(_, old)| old.until < until)
                    {
                        let new_ban_item = NewBanItem {
                            key: RecordKey::new(),
                            peer_id: short.peer_id,
                            origin: RecordOrigin::ParentKeyTag { key: *key, tag },
                            until,
                        };
                        bans.insert(short.peer_id, (State::NeedsStore, new_ban_item));
                    };
                }
            }
        }

        let mut new_records = Vec::with_capacity(
            bans.iter()
                .filter(|(_, (state, _))| matches!(state, State::NeedsStore))
                .count(),
        );

        for (peer_id, (state, ban)) in bans {
            match state {
                State::Stored => {}
                State::NeedsStore => {
                    let data = RecordData::Banned {
                        peer_id,
                        origin: ban.origin,
                        until: ban.until,
                    };
                    new_records.push(JournalRecord { key: ban.key, data });
                }
            }
            self.ban_queue.send(ban).ok();
        }
        new_records
    }

    pub fn maybe_ban_data(&self, key: RecordKey, event: &JournalEvent) -> Option<RecordData> {
        match event.action() {
            RecordAction::Store => return None,
            RecordAction::CountPenalty | RecordAction::StoreAndCountPenalty => {}
        }
        let until = {
            let mut by_peer = self.by_peer.entry(*event.peer_id()).or_default();
            by_peer.new_ban_until(key, event.tag())?
        };

        Some(RecordData::Banned {
            peer_id: *event.peer_id(),
            until,
            origin: RecordOrigin::ParentKeyTag {
                key,
                tag: event.tag(),
            },
        })
    }

    pub fn send_ban_data(&self, key: RecordKey, data: &RecordData) {
        let RecordData::Banned {
            peer_id,
            origin,
            until,
        } = data
        else {
            return;
        };

        let unban = NewBanItem {
            key,
            peer_id: *peer_id,
            origin: *origin,
            until: *until,
        };
        self.ban_queue.send(unban).ok(); // consumer is closed only on shutdown
    }
}

struct NewBanItem {
    key: RecordKey,
    peer_id: PeerId,
    origin: RecordOrigin,
    until: UnixTime,
}
struct QueuedUnbanItem {
    peer_id: PeerId,
    origin: RecordOrigin,
    until: UnixTime,
}

async fn updater(
    network: Network,
    delayed_db_writes_tx: mpsc::UnboundedSender<Vec<JournalRecord>>,
    mut ban_queue: mpsc::UnboundedReceiver<NewBanItem>,
) {
    let mut last_bans = FastHashMap::<PeerId, UnixTime>::default();
    let mut unban_queue = DelayQueue::<QueuedUnbanItem>::new();
    loop {
        tokio::select! {
            Some(ban) = ban_queue.recv() => {
                let unban_wait = Duration::from_millis((ban.until - UnixTime::now()).millis());

                if unban_wait.is_zero() {
                    continue;
                }

                let is_new = match last_bans.entry(ban.peer_id) {
                    hash_map::Entry::Occupied(mut occupied) => {
                        let max_at = ban.until.max(*occupied.get());
                        occupied.insert(max_at);
                        max_at == ban.until
                    }
                    hash_map::Entry::Vacant(vacant) => {
                        vacant.insert(ban.until);
                        network.known_peers().ban(&ban.peer_id);
                        true
                    }
                };
                if is_new {
                    let unban_origin = match ban.origin {
                        RecordOrigin::Manual | RecordOrigin::ParentKey(_) => RecordOrigin::ParentKey(ban.key),
                        RecordOrigin::ParentKeyTag { tag, .. } => RecordOrigin::ParentKeyTag { key: ban.key, tag },
                    };
                    let queued = QueuedUnbanItem {peer_id: ban.peer_id, origin: unban_origin, until: ban.until};
                    unban_queue.insert(queued, unban_wait);
                }
            }
            Some(expired) = unban_queue.next() => {
                let unban = expired.into_inner();
                if last_bans.get(&unban.peer_id).is_some_and(|latest| *latest > unban.until) {
                    // there is a ban in queue with later time
                    continue;
                }
                last_bans.remove(&unban.peer_id);
                network.known_peers().remove(&unban.peer_id);

                let record = JournalRecord {
                    key: RecordKey::new(),
                    data: RecordData::Unbanned {
                        peer_id: unban.peer_id,
                        origin: unban.origin,
                    },
                };
                delayed_db_writes_tx.send(vec![record]).ok();
            }
            else => unreachable!("Ban core updater loop break")
        }
    }
}
