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
use crate::storage::{EventKey, EventKind, EventSeverity, ShortEventData};

pub struct BanCore {
    tolerated: FastDashMap<PeerId, FastHashMap<EventKind, Vec<UnixTime>>>,
    network: Network,
    schedule_unban: mpsc::UnboundedSender<UnbanItem>,
    _updater: JoinTask<()>,
}

impl BanCore {
    pub fn new(network: &Network) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tolerated: FastDashMap::default(),
            network: network.clone(),
            schedule_unban: tx,
            _updater: JoinTask::new(updater(network.clone(), rx)),
        }
    }

    /// applicable both for newly occurred and replayed on startup
    pub fn maybe_ban(&self, key: EventKey, data: ShortEventData) {
        match data.severity {
            EventSeverity::Debug => return,
            EventSeverity::Warn => {}
        }
        let unban_at = {
            let conf = NodeConfig::get().bans.get(data.kind);
            let window = conf.toleration.duration.to_time();

            let mut entry = self.tolerated.entry(data.peer_id).or_default();

            let times = entry.entry(data.kind).or_default();

            times.push(key.time);

            times.retain(|prev| key.time - *prev <= window);

            (times.len() >= conf.toleration.count as usize)
                .then_some(key.time + conf.duration.to_time())
        };
        if let Some(unban_at) = unban_at {
            self.network.known_peers().ban(&data.peer_id);
            let unban = UnbanItem {
                peer_id: data.peer_id,
                at: unban_at,
            };
            self.schedule_unban.send(unban).ok(); // consumer is closed only on shoutdown
        }
    }
}

struct UnbanItem {
    peer_id: PeerId,
    at: UnixTime,
}

async fn updater(network: Network, mut new_to_unban: mpsc::UnboundedReceiver<UnbanItem>) {
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
            }
            Some(unban) = new_to_unban.recv() => {
                let unban_wait = Duration::from_millis((unban.at - UnixTime::now()).millis());

                if unban_wait.is_zero() {
                    continue;
                }

                let is_new = match unban_at.entry(unban.peer_id) {
                    hash_map::Entry::Occupied(mut occupied) => {
                        let max_at = unban.at.max(*occupied.get());
                        occupied.insert(max_at);
                        max_at == unban.at
                    }
                    hash_map::Entry::Vacant(vacant) => {
                        vacant.insert(unban.at);
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
