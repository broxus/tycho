use std::collections::hash_map;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::time::DelayQueue;
use tycho_network::{Network, PeerId};
use tycho_util::futures::JoinTask;
use tycho_util::{FastDashMap, FastHashMap};

use crate::engine::NodeConfig;
use crate::models::UnixTime;
use crate::moderator::EventData;
use crate::moderator::event::Event;
use crate::storage::{EventKey, EventSeverity, EventTag, ShortEventData};

pub struct BanCore {
    tolerated: FastDashMap<PeerId, FastHashMap<EventTag, Vec<UnixTime>>>,
    network: Network,
    delayed_db_writes_tx: mpsc::UnboundedSender<Event>,
    schedule_unban: mpsc::UnboundedSender<UnbanItem>,
    _updater: JoinTask<()>,
}

impl BanCore {
    pub fn new(network: &Network, delayed_db_writes_tx: &mpsc::UnboundedSender<Event>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tolerated: FastDashMap::default(),
            network: network.clone(),
            delayed_db_writes_tx: delayed_db_writes_tx.clone(),
            schedule_unban: tx,
            _updater: JoinTask::new(updater(network.clone(), delayed_db_writes_tx.clone(), rx)),
        }
    }

    /// applicable both for newly occurred and replayed on startup
    pub fn maybe_ban(&self, key: EventKey, data: ShortEventData) {
        match data.severity {
            EventSeverity::Info => return,
            EventSeverity::Warn => {}
        }
        let Some(unban_at) = ({
            let conf = NodeConfig::get().bans.get(data.tag);
            conf.toleration
                .is_none_or(|toleration| {
                    let window = toleration.duration.to_time();

                    let mut entry = self.tolerated.entry(data.peer_id).or_default();

                    let times = entry.entry(data.tag).or_default();

                    times.push(key.occurred_at);

                    times.retain(|prev| key.occurred_at - *prev <= window);

                    times.len() >= toleration.count.get() as usize
                })
                .then_some(key.occurred_at + conf.duration.to_time())
        }) else {
            return; // tolerated, no ban this time
        };

        if UnixTime::now() >= unban_at {
            // Note: unban should not reset penalties,
            //   so a ban duration can be shorter than a toleration window.
            // We've finished toleration bookkeeping, now it's time to prevent immediate unbans.
            // That mostly refers to a replay during node start.
            return;
        }

        self.network.known_peers().ban(&data.peer_id);

        self.delayed_db_writes_tx
            .send(Event {
                key,
                data: EventData::Banned {
                    peer_id: data.peer_id,
                    unban_at,
                },
            })
            .ok();

        self.schedule_unban
            .send(UnbanItem {
                peer_id: data.peer_id,
                unban_at,
            })
            .ok(); // consumer is closed only on shoutdown
    }
}

struct UnbanItem {
    peer_id: PeerId,
    unban_at: UnixTime,
}

async fn updater(
    network: Network,
    delayed_db_writes_tx: mpsc::UnboundedSender<Event>,
    mut new_to_unban: mpsc::UnboundedReceiver<UnbanItem>,
) {
    // negligible additional time in order not to loop
    const SKEW_DURATION: Duration = Duration::from_secs(3);
    let mut unban_queue = DelayQueue::<PeerId>::new();
    let mut unban_at = FastHashMap::<PeerId, UnixTime>::default();
    loop {
        tokio::select! {
            Some(expired) = unban_queue.next() => {
                let peer_id = expired.into_inner();
                if let Some(unban_at) = unban_at.get(&peer_id) {
                    let unban_wait = Duration::from_millis((*unban_at - UnixTime::now()).millis());
                    if !unban_wait.is_zero() {
                        // there were two bans with different unban time
                        unban_queue.insert(peer_id, unban_wait + SKEW_DURATION);
                        continue;
                    }
                }
                unban_at.remove(&peer_id);
                network.known_peers().remove(&peer_id);

                delayed_db_writes_tx.send(Event {
                    key: EventKey::new(),
                    data: EventData::Unbanned(peer_id),
                }).ok();
            }
            Some(unban) = new_to_unban.recv() => {
                let unban_wait = Duration::from_millis((unban.unban_at - UnixTime::now()).millis());

                if unban_wait.is_zero() {
                    continue;
                }

                let is_new = match unban_at.entry(unban.peer_id) {
                    hash_map::Entry::Occupied(mut occupied) => {
                        let max_at = unban.unban_at.max(*occupied.get());
                        occupied.insert(max_at);
                        max_at == unban.unban_at
                    }
                    hash_map::Entry::Vacant(vacant) => {
                        vacant.insert(unban.unban_at);
                        true
                    }
                };
                if is_new {
                    unban_queue.insert(unban.peer_id, unban_wait + SKEW_DURATION);
                }
            }
            else => unreachable!("Ban core updater loop break")
        }
    }
}
