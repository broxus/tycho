use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use super::dto::ConsensusEvent;
use crate::dag::Verifier;
use crate::engine::MempoolConfig;
use crate::intercom::dto::PeerState;
use crate::intercom::PeerSchedule;
use crate::models::{Digest, Location, NodeCount, Point, PointId, Round};

#[derive(Clone)]
pub struct BroadcastFilter(Arc<BroadcastFilterInner>);

impl BroadcastFilter {
    pub fn new(
        log_id: Arc<String>,
        peer_schedule: Arc<PeerSchedule>,
        output: mpsc::UnboundedSender<ConsensusEvent>,
    ) -> Self {
        Self(Arc::new(BroadcastFilterInner {
            log_id,
            last_by_peer: Default::default(),
            by_round: Default::default(),
            current_dag_round: AtomicU32::new(Round::BOTTOM.0), // will advance with other peers
            peer_schedule,
            output,
        }))
    }

    pub fn add(&self, point: Arc<Point>) {
        self.0.add(point);
    }

    pub fn advance_round(&self, new_round: Round) {
        self.0.advance_round(new_round)
    }

    pub async fn clear_cache(self) -> ! {
        let mut rx = self.0.peer_schedule.updates();
        loop {
            match rx.recv().await {
                Ok((peer_id, PeerState::Unknown)) => {
                    // assume peers aren't removed from DHT immediately
                    self.0.last_by_peer.remove(&peer_id);
                }
                Ok(_) => {}
                Err(err @ RecvError::Lagged(_)) => {
                    tracing::error!("peer schedule updates {err}");
                }
                Err(err @ RecvError::Closed) => {
                    panic!("peer schedule updates {err}");
                }
            }
        }
    }
}

struct BroadcastFilterInner {
    log_id: Arc<String>,
    // defend from spam from future rounds:
    // should keep rounds greater than current dag round
    last_by_peer: FastDashMap<PeerId, Round>,
    // very much like DAG structure, but without dependency check;
    // just to determine reliably that consensus advanced without current node
    by_round: FastDashMap<Round, (NodeCount, BTreeMap<PeerId, BTreeMap<Digest, Arc<Point>>>)>,
    current_dag_round: AtomicU32,
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

    /// returns Vec of points to insert into DAG if consensus round is determined reliably
    fn add(&self, point: Arc<Point>) {
        let local_id = &self.log_id;
        // for any node @ r+0, its DAG always contains [r-DAG_DEPTH-N; r+1] rounds, where N>=0
        let dag_round = Round(self.current_dag_round.load(Ordering::Acquire));
        let PointId {
            location: Location { round, author },
            digest,
        } = point.id();

        tracing::debug!("{local_id} @ {dag_round:?} filter <= bcaster {author:.4?} @ {round:?}");

        let verified = Verifier::verify(&point, &self.peer_schedule);
        if verified.is_err() {
            tracing::error!(
                "{local_id} @ {dag_round:?} filter => bcaster {author:.4?} @ {round:?} : \
                 Invalid {point:.4?}"
            );
        }
        if round <= dag_round {
            let event =
                verified.map_or_else(ConsensusEvent::Invalid, |_| ConsensusEvent::Verified(point));
            self.output.send(event).ok();
            return;
        } // else: either consensus moved forward without us,
          // or we shouldn't accept the point yet, or it's a spam

        if *self
            .last_by_peer
            .entry(author)
            .and_modify(|last_by_peer| {
                if *last_by_peer < round {
                    *last_by_peer = round
                }
            })
            .or_insert(round)
            > round
        {
            // we should ban a peer that broadcasts its rounds out of order,
            //   though we cannot prove this decision for other nodes
            tracing::error!(
                "{local_id} @ {dag_round:?} filter => bcaster {author:.4?} @ {round:?} : \
                 out of order by round"
            );
            return;
        };
        if verified.is_err() {
            return; // do not determine next round by garbage points; it's a ban
        }
        match self.by_round.entry(round).or_try_insert_with(|| {
            // how many nodes should send broadcasts
            NodeCount::try_from(self.peer_schedule.peers_for(round).len())
                .map(|node_count| (node_count, Default::default()))
        }) {
            Err(_) => {
                // will not accept broadcasts from not initialized validator set
                return;
            }
            Ok(mut entry) => {
                let (node_count, ref mut same_round) = entry.value_mut();
                // ban the author, if we detect equivocation now; we won't be able to prove it
                //   if some signatures are invalid (it's another reason for a local ban)
                same_round.entry(author).or_default().insert(digest, point);
                if same_round.len() < node_count.reliable_minority() {
                    tracing::debug!(
                        "{local_id} @ {dag_round:?} filter => bcaster {author:.4?} @ {round:?} : \
                        round is not determined yet",
                    );
                    return;
                };
            }
        }
        self.advance_round(round);
    }

    // drop everything up to the new round (inclusive), channelling cached points
    fn advance_round(&self, new_round: Round) {
        for round in (self.current_dag_round.load(Ordering::Acquire)..=new_round.0).map(Round) {
            self.output.send(ConsensusEvent::Forward(round)).ok();
            // allow filter to channel messages only after Forward was sent
            self.current_dag_round
                .fetch_update(Ordering::Release, Ordering::Relaxed, |old| {
                    Some(round.0).filter(|new| old < *new)
                })
                .ok();
            // map entry is not used by filter anymore
            for point in self
                .by_round
                .remove(&round)
                .into_iter()
                .map(|(_, (_, v))| v.into_iter())
                .flatten()
                .map(|(_, v)| v.into_iter().map(|(_, v)| v))
                .flatten()
            {
                self.output.send(ConsensusEvent::Verified(point)).ok();
            }
        }
        // TODO there must be some config value - when node needs to sync;
        //   values too far in the future are some garbage, must ban authors
        self.by_round.retain(|round, _| {
            new_round < *round && round.0 <= new_round.0 + MempoolConfig::COMMIT_DEPTH as u32
        });
    }
}
