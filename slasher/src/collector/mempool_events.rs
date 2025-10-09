use std::collections::hash_map::Entry as HashMapEntry;
use std::ops::RangeInclusive;

use indexmap::IndexMap;
use indexmap::map::Entry as IndexMapEntry;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tycho_network::PeerId;
use tycho_slasher_traits::{
    MempoolEventsCache, MempoolEventsListener, MempoolPeerStats, MempoolStatsMergeError,
};
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
            let Some(drained) = ({
                let mut data = self.data.lock();
                let end_i = match data.get_index_of(rounds.end()) {
                    Some(end_i) => Some(end_i),
                    None if data.last().is_none_or(|(anchor, _)| anchor < rounds.end()) => {
                        None // will wait notification, cannot be done here
                    }
                    None if (data.first()).is_some_and(|(anchor, _)| anchor > rounds.end()) => {
                        return FastHashMap::default(); // will never return this range
                    }
                    None => match data.binary_search_keys(rounds.end()) {
                        Ok(_) => unreachable!("key is not in map"),
                        Err(0) => unreachable!("excluded by check of `data.first()`"),
                        Err(next) => Some(next - 1), // some anchor before looked up for
                    },
                };
                // take out data when end is received (it should be shortly after the same anchor)
                end_i.map(|end_i| {
                    data.drain(..=end_i)
                        .filter_map(|(anchor, stats_map)| {
                            // just drop data earlier than the requested range
                            (anchor >= *rounds.start()).then_some(stats_map)
                        })
                        .collect::<Vec<_>>()
                })
            }) else {
                notified.await;
                continue;
            };
            drop(notified);
            let reduced = drained.into_iter().reduce(|mut map_a, map_b| {
                for (peer, stats) in map_b {
                    match map_a.entry(peer) {
                        HashMapEntry::Occupied(mut occupied) => {
                            merge_stats(occupied.get_mut(), &stats);
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

fn merge_stats(acc: &mut MempoolPeerStats, other: &MempoolPeerStats) {
    let error = match acc.merge_with(other) {
        Ok(()) => return,
        Err(error) => error,
    };
    tracing::error!(
        %error,
        "cannot merge stats, skipping"
    );
    let kind = match error {
        MempoolStatsMergeError::RoundOutOfOrder(_, _) => "rounds order",
        MempoolStatsMergeError::OverlappingRanges(_, _) => "ranges overlap",
    };
    metrics::counter!("tycho_mempool_stats_merge_errors", "kind" => kind).increment(1);
}
