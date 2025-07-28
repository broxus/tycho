use std::collections::hash_map::Entry as HashMapEntry;
use std::ops::RangeInclusive;

use indexmap::IndexMap;
use indexmap::map::Entry as IndexMapEntry;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tycho_network::PeerId;
use tycho_slasher_traits::{MempoolEventsCache, MempoolEventsListener, MempoolPeerStats};
use tycho_util::FastHashMap;

type MempoolAnchorId = u32;

#[derive(Default)]
pub struct MempoolStatsCache {
    data: Mutex<IndexMap<MempoolAnchorId, FastHashMap<PeerId, MempoolPeerStats>>>,
    anchor_added: Notify,
}

impl MempoolEventsListener for MempoolStatsCache {
    fn put_stats(
        &self,
        anchor_round: MempoolAnchorId,
        stats: FastHashMap<PeerId, MempoolPeerStats>,
    ) {
        let mut data = self.data.lock();
        let last_round = data.last_entry().map(|e| *e.key()).unwrap_or_default();
        match data.entry(anchor_round) {
            IndexMapEntry::Occupied(_) => {} // no replacement no merge
            IndexMapEntry::Vacant(vacant) => {
                assert!(
                    last_round < anchor_round,
                    "out of order stats insert; expected {last_round} < {anchor_round}",
                );
                vacant.insert(stats);
            }
        }
        drop(data);
        self.anchor_added.notify_waiters();
    }
}

#[async_trait::async_trait]
impl MempoolEventsCache for MempoolStatsCache {
    async fn get_stats(
        &self,
        rounds: RangeInclusive<MempoolAnchorId>,
    ) -> FastHashMap<PeerId, MempoolPeerStats> {
        loop {
            let notified = self.anchor_added.notified();
            let drained = {
                let mut data = self.data.lock();
                // take out data when end is received (it should be shortly after the same anchor)
                data.get_index_of(rounds.end()).map(|end| {
                    data.drain(..=end)
                        .filter_map(|(anchor, stats_map)| {
                            // just drop data earlier than the requested range
                            (anchor >= *rounds.start()).then_some(stats_map)
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
                            match occupied.get_mut().merge_with(&stats) {
                                Ok(()) => {}
                                Err(error) => {
                                    tracing::error!(%error, %peer, "cannot merge stats, skipping");
                                }
                            }
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
