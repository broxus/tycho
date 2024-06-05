use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use super::dto::ConsensusEvent;
use crate::dag::Verifier;
use crate::effects::{AltFormat, CurrentRoundContext, Effects};
use crate::engine::MempoolConfig;
use crate::intercom::dto::PeerState;
use crate::intercom::PeerSchedule;
use crate::models::{DagPoint, Digest, Location, NodeCount, Point, PointId, Round};

#[derive(Clone)]
pub struct BroadcastFilter {
    inner: Arc<BroadcastFilterInner>,
}

impl BroadcastFilter {
    pub fn new(
        peer_schedule: Arc<PeerSchedule>,
        output: mpsc::UnboundedSender<ConsensusEvent>,
    ) -> Self {
        Self {
            inner: Arc::new(BroadcastFilterInner {
                last_by_peer: Default::default(),
                by_round: Default::default(),
                peer_schedule,
                output,
            }),
        }
    }

    pub fn add(
        &self,
        peer_id: &PeerId,
        point: &Arc<Point>,
        top_dag_round: Round,
        effects: &Effects<CurrentRoundContext>,
    ) {
        let point_round = point.body.location.round;
        if self.inner.add(peer_id, point, top_dag_round, effects) {
            self.inner.advance_round(point_round, effects);
        }
    }

    pub fn advance_round(&self, new_round: Round, round_effects: &Effects<CurrentRoundContext>) {
        self.inner.advance_round(new_round, round_effects);
    }

    pub async fn clear_cache(self) -> ! {
        let mut rx = self.inner.peer_schedule.updates();
        loop {
            match rx.recv().await {
                Ok((peer_id, state)) => {
                    // assume peers aren't removed from DHT immediately
                    let remove = state == PeerState::Unknown;
                    tracing::info!(
                        ignore = !remove,
                        peer = display(peer_id.alt()),
                        new_state = debug(state),
                        "peer state update",
                    );
                    if remove {
                        self.inner.last_by_peer.remove(&peer_id);
                    }
                }
                Err(err @ RecvError::Lagged(_)) => {
                    tracing::error!(error = display(err), "peer state update",);
                }
                Err(err @ RecvError::Closed) => {
                    panic!("peer state update {err}");
                }
            }
        }
    }
}

type SimpleDagLocations = BTreeMap<PeerId, BTreeMap<Digest, Arc<Point>>>;
struct BroadcastFilterInner {
    // defend from spam from future rounds:
    // should keep rounds greater than current dag round
    last_by_peer: FastDashMap<PeerId, Round>,
    // very much like DAG structure, but without dependency check;
    // just to determine reliably that consensus advanced without current node
    by_round: FastDashMap<Round, (NodeCount, SimpleDagLocations)>,
    peer_schedule: Arc<PeerSchedule>,
    output: mpsc::UnboundedSender<ConsensusEvent>,
}

impl BroadcastFilterInner {
    // Note logic still under consideration because of contradiction in requirements:
    //  * we must determine the latest consensus round reliably:
    //    the current approach is to collect 1/3+1 points at the same future round
    //    => we should collect as much points as possible
    //  * we must defend the DAG and current cache from spam from future rounds,
    //    => we should discard points from the far future

    /// channels Vec of points to insert into DAG if consensus round is determined reliably;
    /// returns `true` if round should be advanced;
    fn add(
        &self,
        peer_id: &PeerId,
        point: &Arc<Point>,
        engine_round: Round,
        effects: &Effects<CurrentRoundContext>,
    ) -> bool {
        // for any node @ r+0, its DAG always contains [r-DAG_DEPTH-N; r+1] rounds, where N>=2

        let PointId {
            location: Location { round, author },
            digest,
        } = point.id();

        let verified = if peer_id != author {
            tracing::error!(
                parent: effects.span(),
                peer = display(peer_id.alt()),
                author = display(author.alt()),
                round = round.0,
                digest = display(digest.alt()),
                "sender is not author"
            );
            Err(DagPoint::NotExists(Arc::new(point.id())))
        } else {
            Verifier::verify(point, &self.peer_schedule).inspect_err(|dag_point| {
                tracing::error!(
                    parent: effects.span(),
                    result = display(dag_point.alt()),
                    author = display(author.alt()),
                    round = round.0,
                    digest = display(digest.alt()),
                    "verified"
                );
            })
        };

        if round <= engine_round {
            let event = verified.map_or_else(ConsensusEvent::Invalid, |_| {
                ConsensusEvent::Verified(point.clone())
            });
            self.output.send(event).ok();
            return false;
        } // else: either consensus moved forward without us,
          // or we shouldn't accept the point yet, or it's a spam

        let last_peer_round = *self
            .last_by_peer
            .entry(author)
            .and_modify(|last_by_peer| {
                if *last_by_peer < round {
                    *last_by_peer = round;
                }
            })
            .or_insert(round);
        if last_peer_round > round {
            // we should ban a peer that broadcasts its rounds out of order,
            //   though we cannot prove this decision for other nodes
            tracing::error!(
                parent: effects.span(),
                last_round = last_peer_round.0,
                author = display(author.alt()),
                round = round.0,
                digest = display(digest.alt()),
                "out of order"
            );
            return false;
        };
        if verified.is_err() {
            return false; // do not determine next round by garbage points; it's a ban
        }
        let is_threshold_reached = {
            // note: lock guard inside result
            let try_by_round_entry = self.by_round.entry(round).or_try_insert_with(|| {
                // how many nodes should send broadcasts
                NodeCount::try_from(self.peer_schedule.peers_for(round).len())
                    .map(|node_count| (node_count, Default::default()))
            });
            // Err: will not accept broadcasts from not yet initialized validator set
            match try_by_round_entry {
                Err(_node_count_err) => false,
                Ok(mut entry) => {
                    let (node_count, same_round) = entry.value_mut();
                    // ban the author, if we detect equivocation now; we won't be able to prove it
                    //   if some signatures are invalid (it's another reason for a local ban)
                    same_round
                        .entry(author)
                        .or_default()
                        .insert(digest.clone(), point.clone());
                    same_round.len() >= node_count.reliable_minority()
                }
            }
        };
        if !is_threshold_reached {
            tracing::trace!(
                parent: effects.span(),
                engine_round = engine_round.0,
                author = display(author.alt()),
                round = round.0,
                digest = display(digest.alt()),
                "threshold not reached yet"
            );
        }
        is_threshold_reached
    }

    /// drop everything up to the new round (inclusive), channelling cached points;
    fn advance_round(&self, new_round: Round, effects: &Effects<CurrentRoundContext>) {
        // concurrent callers may break historical order of messages in channel
        // until new_round is channeled, and Collector can cope with it;
        // but engine_round must be set to the greatest value under lock
        let mut rounds = self
            .by_round
            .iter()
            .map(|el| *el.key())
            .filter(|key| *key <= new_round)
            .collect::<Vec<_>>();
        rounds.sort();
        if rounds.is_empty() {
            return;
        }
        let mut released = Vec::new();
        let mut missed = Vec::new();
        for round in rounds {
            let Some((_, (_, points))) = self.by_round.remove(&round) else {
                missed.push(round.0);
                continue;
            };
            if points.is_empty() {
                missed.push(round.0);
                continue;
            };
            self.output
                .send(ConsensusEvent::Forward(round))
                .expect("channel from filter to broadcaster closed");
            for (_, point) in points.into_iter().flat_map(|(_, v)| v.into_iter()) {
                self.output
                    .send(ConsensusEvent::Verified(point))
                    .expect("channel from filter to broadcaster closed");
            }
            released.push(round.0);
        }
        tracing::debug!(
            parent: effects.span(),
            to = new_round.0,
            released = debug(released),
            missed = debug(missed),
            "advance round"
        );

        // TODO there must be some config value - when node needs to sync;
        //   values too far in the future are some garbage, must ban authors
        self.by_round.retain(|round, _| {
            new_round < *round && round.0 <= new_round.0 + MempoolConfig::COMMIT_DEPTH as u32
        });
    }
}
