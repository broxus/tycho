use std::collections::hash_map::Entry as HashMapEntry;
use std::ops::RangeInclusive;

use indexmap::IndexMap;
use indexmap::map::Entry as IndexMapEntry;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tycho_consensus::prelude::{MempoolStatsOutput, PeerStats};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::mempool::MempoolAnchorId;

#[derive(Default)]
pub struct StatsCache {
    data: Mutex<IndexMap<MempoolAnchorId, FastHashMap<PeerId, PeerStats>>>,
    anchor_added: Notify,
}

impl StatsCache {
    pub async fn run_updates(&self, mut stats: UnboundedReceiver<MempoolStatsOutput>) {
        while let Some(stats) = stats.recv().await {
            let mut data = self.data.lock();
            let last_round = data.last_entry().map(|e| *e.key()).unwrap_or_default();
            match data.entry(stats.anchor_round) {
                IndexMapEntry::Occupied(_) => continue, // no replacement
                IndexMapEntry::Vacant(vacant) => {
                    assert!(
                        last_round < stats.anchor_round,
                        "out of order stats insert; expected {last_round} < {}",
                        stats.anchor_round
                    );
                    vacant.insert(stats.data);
                }
            }
            drop(data);
            self.anchor_added.notify_waiters();
        }
    }

    pub async fn get(
        &self,
        range: RangeInclusive<MempoolAnchorId>,
    ) -> FastHashMap<PeerId, PeerStats> {
        loop {
            let notified = self.anchor_added.notified();
            let drained = {
                let mut data = self.data.lock();
                // take out data when end is received (it should be shortly after the same anchor)
                data.get_index_of(range.end()).map(|end| {
                    data.drain(..=end)
                        .filter_map(|(anchor, stats_map)| {
                            // just drop data earlier than the requested range
                            (anchor >= *range.start()).then_some(stats_map)
                        })
                        .collect::<Vec<_>>()
                })
            };
            let Some(drained) = drained else {
                notified.await;
                continue;
            };
            let reduced = drained.into_iter().reduce(|mut map_a, map_b| {
                for (peer, stats) in map_b {
                    match map_a.entry(peer) {
                        HashMapEntry::Occupied(mut occupied) => {
                            *occupied.get_mut() += stats;
                        }
                        HashMapEntry::Vacant(vacant) => {
                            vacant.insert(stats);
                        }
                    }
                }
                map_a
            });
            return reduced.unwrap_or_default();
        }
    }
}
