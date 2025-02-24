use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{StreamExt, StreamMap};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::effects::AltFormat;
use crate::engine::round_watch::{Commit, RoundWatch, TopKnownAnchor};
use crate::engine::MempoolMergedConfig;
use crate::models::{MempoolOutput, PointId, Round};

#[derive(Default)]
pub struct AnchorConsumer {
    streams: StreamMap<PeerId, UnboundedReceiverStream<MempoolOutput>>,
    // all committers must share the same sequence of anchor points
    anchors: FastHashMap<Round, FastHashMap<PeerId, PointId>>,
    // all committers must share the same anchor history (linearized inclusion dag) for each anchor
    history: FastHashMap<Round, Vec<PointId>>,
    // simulates feedback from collator, as if anchor committed by all peers
    // is immediately confirmed by a top known block
    pub top_known_anchor: RoundWatch<TopKnownAnchor>,
    // Simulates mempool adapter that commits right after collator feedback (may occur in practice
    // if local collation is a bit lagging and the top known block is received from network)
    // Just because it does not require much code and keeps sender alive to avoid panic
    pub commit_round: RoundWatch<Commit>,
    pub common_anchor_count: Arc<AtomicUsize>,
}

impl AnchorConsumer {
    pub fn add(&mut self, committer: PeerId, committed: UnboundedReceiver<MempoolOutput>) {
        self.streams
            .insert(committer, UnboundedReceiverStream::new(committed));
    }

    pub async fn drain(mut self) {
        loop {
            let (_, commit_result) = self
                .streams
                .next()
                .await
                .expect("committed anchor reader must be alive");
            let round = match commit_result {
                MempoolOutput::Running | MempoolOutput::Paused => continue,
                MempoolOutput::NewStartAfterGap(round) => {
                    tracing::warn!("gap in anchor chain, next to commit: {}", round.0);
                    round
                }
                MempoolOutput::NextAnchor(anchor_data) => {
                    let round = anchor_data.anchor.round();
                    tracing::info!("committed anchor {}", round.0);
                    round
                }
            };
            self.top_known_anchor.set_max(round);
            self.commit_round.set_max(round);
        }
    }

    pub async fn check(mut self, merged_conf: &MempoolMergedConfig) {
        let mut next_expected_history_round = None;
        loop {
            let (peer_id, commit_result) = self
                .streams
                .next()
                .await
                .expect("committed anchor reader must be alive");

            let (anchor, history) = match commit_result {
                MempoolOutput::Running | MempoolOutput::Paused => {
                    continue;
                }
                MempoolOutput::NewStartAfterGap(round) => {
                    tracing::warn!("unrecoverable gap at {} for {}", round.0, peer_id.alt());
                    continue;
                }
                MempoolOutput::NextAnchor(data) => (data.anchor, data.history),
            };

            let anchor_id = anchor.id();

            if next_expected_history_round.is_none() {
                // Genesis point is excluded from commit, points only reference it
                next_expected_history_round = Some(merged_conf.conf.genesis_round.next());
            }

            let anchor_round = anchor.round();

            // get last previous anchor round and check if we don't have previous
            for (prev_anchor_round, committers) in self
                .anchors
                .iter()
                .filter(|(prev_anchor_round, _)| **prev_anchor_round < anchor_round)
            {
                assert!(
                    committers.get(&peer_id).is_some(),
                    "Missing anchor for node {} at {:?}, it committed {:?}",
                    peer_id.alt(),
                    prev_anchor_round,
                    anchor_id.alt(),
                );
            }

            match self.anchors.entry(anchor_round) {
                Occupied(mut stored_anchor) => {
                    let committers = stored_anchor.get_mut();
                    assert!(
                        committers.len() < self.streams.len(),
                        "Broken test: we can't store {} committers for total {} peers at round {}",
                        committers.len(),
                        self.streams.len(),
                        anchor_round.0,
                    );
                    assert!(
                        committers.get(&peer_id).is_none(),
                        "Peer {} already committed {:?}, now committed {:?}",
                        peer_id.alt(),
                        committers.get(&peer_id).map(|old_anchor| old_anchor.alt()),
                        anchor_id.alt()
                    );

                    committers.insert(peer_id, anchor_id);
                }
                Vacant(entry) => {
                    let mut peer_map = FastHashMap::default();
                    peer_map.insert(peer_id, anchor_id);
                    entry.insert(peer_map);
                }
            }

            match self.history.get(&anchor_round) {
                Some(stored_history) => {
                    assert_eq!(
                        stored_history.len(),
                        history.len(),
                        "Commited points size differs for {} at round: {}",
                        peer_id.alt(),
                        anchor_round.0,
                    );

                    for (left, right) in stored_history.iter().zip(history.iter()) {
                        assert_eq!(
                            &left.digest,
                            right.digest(),
                            "Points are not equal or order is different for round {anchor_round:?}"
                        );
                    }
                }
                None => {
                    let point_refs = history.iter().map(|x| x.id()).collect::<Vec<_>>();
                    self.history.insert(anchor_round, point_refs);
                }
            }

            let mut common_anchors = vec![];
            let mut common_history = vec![];
            self.anchors.retain(|anchor_round, value| {
                if value.len() == self.streams.len() {
                    let history = self
                        .history
                        .remove(anchor_round)
                        .expect("anchor must have history");
                    assert!(!history.is_empty(), "anchor history cannot be empty");
                    common_anchors.push(anchor_round.0);
                    common_history.extend(history);
                    false
                } else {
                    true
                }
            });

            let minmax_history_round = common_history
                .iter()
                .map(|point_id| point_id.round)
                .minmax()
                .into_option();

            if let Some((min, max)) = minmax_history_round {
                assert!(
                    next_expected_history_round.unwrap() >= min,
                    "anchor history must be contiguous; has a gap between \
                     expected {next_expected_history_round:?} and new oldest {min:?}",
                );
                next_expected_history_round = Some(max.next());
            }

            self.common_anchor_count
                .fetch_add(common_anchors.len(), Ordering::Relaxed);
            common_anchors.sort_unstable();
            tracing::debug!("Anchor hashmap len: {}", self.anchors.len());
            tracing::trace!("History hashmap len: {}", self.history.len());
            if let Some(top_common_anchor) = common_anchors.last() {
                self.top_known_anchor.set_max_raw(*top_common_anchor);
                self.commit_round.set_max_raw(*top_common_anchor);
                tracing::info!(
                    "all nodes committed anchors for rounds {:?}",
                    common_anchors
                );
            }
        }
    }
}
