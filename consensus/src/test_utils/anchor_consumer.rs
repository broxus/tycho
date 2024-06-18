use std::collections::hash_map::Entry::{Occupied, Vacant};

use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{StreamExt, StreamMap};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::effects::AltFormat;
use crate::models::{PointId, Round};
use crate::Point;

#[derive(Default)]
pub struct AnchorConsumer {
    streams: StreamMap<PeerId, UnboundedReceiverStream<(Point, Vec<Point>)>>,
    // all committers must share the same sequence of anchor points
    anchors: FastHashMap<Round, FastHashMap<PeerId, PointId>>,
    // all committers must share the same anchor history (linearized inclusion dag) for each anchor
    history: FastHashMap<Round, Vec<PointId>>,
}

impl AnchorConsumer {
    pub fn add(&mut self, committer: PeerId, committed: UnboundedReceiver<(Point, Vec<Point>)>) {
        self.streams
            .insert(committer, UnboundedReceiverStream::new(committed));
    }

    pub async fn drain(mut self) {
        loop {
            _ = self
                .streams
                .next()
                .await
                .expect("committed anchor reader must be alive");
        }
    }

    pub async fn check(mut self) {
        loop {
            let (peer_id, (anchor, history)) = self
                .streams
                .next()
                .await
                .expect("committed anchor reader must be alive");
            let anchor_id = anchor.id();

            let anchor_round = anchor.body().location.round;

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
            self.anchors.retain(|key, value| {
                if value.len() == self.streams.len() {
                    self.history.remove(key);
                    common_anchors.push(key.0);
                    false
                } else {
                    true
                }
            });

            tracing::debug!("Anchor hashmap len: {}", self.anchors.len());
            tracing::debug!("Refs hashmap ken: {}", self.history.len());
            if !common_anchors.is_empty() {
                tracing::info!("all nodes commited anchors for rounds {:?}", common_anchors);
            }
        }
    }
}
