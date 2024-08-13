use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use super::dto::ConsensusEvent;
use crate::dag::{DagRound, Verifier};
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::engine::MempoolConfig;
use crate::intercom::dto::PeerState;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{DagPoint, Digest, PeerCount, Point, PointId, Round};
use crate::outer_round::{Consensus, OuterRound};

#[derive(Clone)]
pub struct BroadcastFilter {
    inner: Arc<BroadcastFilterInner>,
}

impl BroadcastFilter {
    pub fn new(
        peer_schedule: &PeerSchedule,
        consensus_round: &OuterRound<Consensus>,
        output: mpsc::UnboundedSender<ConsensusEvent>,
    ) -> Self {
        Self {
            inner: Arc::new(BroadcastFilterInner {
                peer_schedule: peer_schedule.clone(),
                consensus_round: consensus_round.clone(),
                output,
                last_by_peer: Default::default(),
                by_round: Default::default(),
            }),
        }
    }

    pub fn add(
        &self,
        sender: &PeerId,
        point: &Point,
        top_dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) {
        self.inner
            .add(sender, point, top_dag_round, downloader, store, effects);
    }

    pub fn advance_round(
        &self,
        top_dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        round_effects: &Effects<EngineContext>,
    ) {
        self.inner
            .advance_round(top_dag_round, downloader, store, round_effects);
    }

    /// must be run before broadcaster is set into responder
    /// because of lock on `self.inner.peer_schedule`
    pub async fn clear_cache(self) -> ! {
        let mut rx = self.inner.peer_schedule.read().updates();
        let name = "clear cached points for unresolved peers";
        loop {
            match rx.recv().await {
                Ok((peer_id, PeerState::Unknown)) => {
                    // assume peers aren't removed from DHT immediately
                    let round = self.inner.consensus_round.get();
                    tracing::info!(round = round.0, peer = display(peer_id.alt()), name);
                    self.inner.last_by_peer.remove(&peer_id);
                }
                Ok((_peer_id, PeerState::Resolved)) => {} // skip
                Err(err @ RecvError::Lagged(_)) => {
                    tracing::error!(error = display(err), name);
                }
                Err(err @ RecvError::Closed) => {
                    panic!("{name} {err}");
                }
            }
        }
    }
}

type SimpleDagLocations = BTreeMap<PeerId, BTreeMap<Digest, Point>>;
struct BroadcastFilterInner {
    peer_schedule: PeerSchedule,
    consensus_round: OuterRound<Consensus>,
    output: mpsc::UnboundedSender<ConsensusEvent>,
    // defend from spam from future rounds:
    // should keep rounds greater than current dag round
    last_by_peer: FastDashMap<PeerId, (Round, usize)>,
    // very much like DAG structure, but without dependency check;
    // just to determine reliably that consensus advanced without current node
    by_round: FastDashMap<Round, (PeerCount, SimpleDagLocations)>,
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
        sender: &PeerId,
        point: &Point,
        top_dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) {
        // for any node @ r+0, its DAG always contains [r-DAG_DEPTH-N; r+1] rounds, where N>=2

        let PointId {
            author,
            round,
            digest,
        } = point.id();

        let verified = if sender != author {
            tracing::error!(
                parent: effects.span(),
                sender = display(sender.alt()),
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

        if round <= top_dag_round.round() {
            // commented out: outdated broadcast may resolve download or short-circuit a validation
            // if round < top_dag_round.round().prev() {
            //     return; // will not be certified; look Signer's response `TooOldRound`
            // }
            let Some(point_round) = top_dag_round.scan(point.round()) else {
                // tracing::warn!("DAG is too shallow", round = round.0);
                return; // cannot process anyway
            };
            match verified {
                Ok(()) => self.send_validating(&point_round, point, downloader, store, effects),
                Err(DagPoint::Invalid(_)) => {
                    point_round.insert_invalid_exact(sender, &point, store);
                }
                Err(_not_exists) => {} // FIXME separate failed downloads from broken signatures
            };
            return;
        } // else: either consensus moved forward without us,
          // or we shouldn't accept the point yet, or it's a spam

        let (last_peer_round, duplicates) = *self
            .last_by_peer
            .entry(author)
            .and_modify(|(last_by_peer, duplcates)| {
                match round.cmp(last_by_peer) {
                    cmp::Ordering::Less => {} // do nothing, blame later
                    cmp::Ordering::Equal => *duplcates += 1,
                    cmp::Ordering::Greater => {
                        *last_by_peer = round;
                        *duplcates = 0;
                    }
                }
            })
            .or_insert((round, 0));
        if verified.is_err() || round < last_peer_round || duplicates > 0 {
            // we should ban a peer that broadcasts its rounds out of order,
            //   though we cannot prove this decision for other nodes;
            // rarely a consecutive pair of broadcasts may be reordered during high CPU load
            let level = if verified.is_err() || round < last_peer_round.prev() || duplicates >= 3 {
                // that's severe errors, that require ban
                tracing::Level::ERROR
            } else {
                // sender could have not received our response, thus decides to retry
                tracing::Level::WARN
            };
            dyn_event!(
                parent: effects.span(),
                level,
                malformed = verified.is_err(),
                out_of_order = last_peer_round > round,
                duplicated = duplicates > 0,
                last_round = last_peer_round.0,
                duplicates = duplicates,
                author = display(author.alt()),
                round = round.0,
                digest = display(digest.alt()),
                "misbehaving broadcast"
            );
            // do not determine next round by garbage points; it's a ban
            return;
        };

        let is_threshold_reached = {
            // note: lock guard inside result
            let try_by_round_entry = self.by_round.entry(round).or_try_insert_with(|| {
                // how many nodes should send broadcasts
                PeerCount::try_from(self.peer_schedule.atomic().peers_for(round).len())
                    .map(|peer_count| (peer_count, Default::default()))
            });
            // Err: will not accept broadcasts from not yet initialized validator set
            match try_by_round_entry {
                Err(_peer_count_err) => false,
                Ok(mut entry) => {
                    let (peer_count, same_round) = entry.value_mut();
                    // ban the author, if we detect equivocation now; we won't be able to prove it
                    //   if some signatures are invalid (it's another reason for a local ban)
                    same_round
                        .entry(author)
                        .or_default()
                        .insert(digest.clone(), point.clone());
                    // send `Forward` signal inside `add` just once per round, not for every point
                    same_round.len() == peer_count.reliable_minority()
                }
            }
        };
        if is_threshold_reached {
            self.output
                .send(ConsensusEvent::Forward(round))
                .expect("channel from filter to collector closed");
            self.consensus_round.set_max(round);
        } else {
            tracing::trace!(
                parent: effects.span(),
                author = display(author.alt()),
                round = round.0,
                digest = display(digest.alt()),
                "threshold not reached yet"
            );
        }
    }

    /// drop everything up to the new round (inclusive), channelling cached points;
    fn advance_round(
        &self,
        top_dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) {
        // concurrent callers may break historical order of messages in channel
        // until new_round is channeled, and Collector can cope with it;
        // but engine_round must be set to the greatest value under lock
        let top_round = top_dag_round.round();
        let mut rounds = self
            .by_round
            .iter()
            .map(|el| *el.key())
            .filter(|key| *key <= top_round)
            .collect::<Vec<_>>();
        rounds.sort_unstable();
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
            let Some(point_round) = top_dag_round.scan(round) else {
                missed.push(round.0);
                continue;
            };
            self.output
                .send(ConsensusEvent::Forward(round))
                .expect("channel from filter to collector closed");
            for (_, point) in points.iter().flat_map(|(_, v)| v.iter()) {
                self.send_validating(&point_round, point, downloader, store, effects);
            }
            released.push(round.0);
        }
        tracing::debug!(
            parent: effects.span(),
            to = top_round.0,
            released = debug(released),
            missed = debug(missed),
            "advance round"
        );

        let limit = (top_round.0).saturating_add(MempoolConfig::ROUNDS_LAG_BEFORE_SYNC as u32);
        self.by_round
            .retain(|round, _| top_round < *round && round.0 <= limit);
    }

    fn send_validating(
        &self,
        point_round: &DagRound,
        point: &Point,
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) {
        // this may be not the first insert into DAG - in case some node received it earlier,
        // and we've already received its point that references current broadcast
        if let Some(first_state_fut) =
            point_round.add_broadcast_exact(point, downloader, store, effects)
        {
            let validating = ConsensusEvent::Validating {
                point_id: point.id(),
                task: first_state_fut,
            };
            self.output
                .send(validating)
                .expect("channel from filter to collector closed");
        }
    }
}
