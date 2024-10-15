use std::sync::Arc;

use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::{FastDashMap, FastHashMap};

use super::dto::ConsensusEvent;
use crate::dag::{DagRound, Verifier, VerifyError};
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{Digest, PeerCount, Point, PointId, Round};

#[derive(Clone)]
pub struct BroadcastFilter {
    inner: Arc<BroadcastFilterInner>,
}

impl BroadcastFilter {
    pub fn new(
        peer_schedule: &PeerSchedule,
        consensus_round: &RoundWatch<Consensus>,
        output: mpsc::UnboundedSender<ConsensusEvent>,
    ) -> Self {
        Self {
            inner: Arc::new(BroadcastFilterInner {
                peer_schedule: peer_schedule.clone(),
                consensus_round: consensus_round.clone(),
                output,
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

    pub fn has_point(&self, round: Round, sender: &PeerId) -> bool {
        match self.inner.by_round.get(&round) {
            None => false,
            Some(mapref) => mapref.value().1.contains_key(sender),
        }
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
}

type SimpleDagLocations = FastHashMap<PeerId, FastHashMap<Digest, (Point, usize)>>;
struct BroadcastFilterInner {
    peer_schedule: PeerSchedule,
    consensus_round: RoundWatch<Consensus>,
    output: mpsc::UnboundedSender<ConsensusEvent>,
    // very much like DAG structure, but without dependency check;
    // just to determine reliably that consensus advanced without current node
    // TODO outer by round - scc tree index (?); inner by peer - dash map
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

        let verified_result = if sender != author {
            None // sender is not author
        } else {
            Some(Verifier::verify(point, &self.peer_schedule))
        };

        let (is_threshold_reached, duplicates) = if let Some(verified) = &verified_result {
            enum ExtendMapErr {
                RoundIsInDag,
                CannotExtend,
            }
            // note: lock guard inside result is also taken to advance round and remove the entry
            let try_by_round_entry = {
                self.by_round.entry(round).or_try_insert_with(|| {
                    // if `top dag round` not defined: node start is not finished and dag is not ready,
                    // has to cache incoming points in Filter's map first, then in channel to Collector

                    // if `point round` > `top dag round`: either consensus moved forward without us,
                    // or we shouldn't accept the point into DAG yet, or it's a spam

                    if round <= top_dag_round.round() {
                        // if entry was removed - do not create one, insert right into DAG
                        Err(ExtendMapErr::RoundIsInDag)
                    } else if verified.is_ok() {
                        // how many nodes should send broadcasts
                        PeerCount::try_from(self.peer_schedule.atomic().peers_for(round).len())
                            // Err: will not accept broadcasts from not yet initialized validator set
                            .map_err(|_peer_count_err| ExtendMapErr::CannotExtend)
                            .map(|peer_count| (peer_count, Default::default()))
                    } else {
                        Err(ExtendMapErr::CannotExtend)
                    }
                })
            };
            match try_by_round_entry {
                Err(ExtendMapErr::CannotExtend) => (false, None),
                // grab a lock
                Ok(mut entry) if verified.is_ok() => {
                    let (peer_count, same_round) = entry.value_mut();
                    // ban the author, if we detect equivocation now; we won't be able to prove it
                    //   if some signatures are invalid (it's another reason for a local ban)
                    let duplicates = same_round
                        .entry(author)
                        .or_default()
                        .entry(digest)
                        .and_modify(|(_point, duplicates)| *duplicates += 1)
                        .or_insert((point.clone(), 0))
                        .1;
                    // send `Forward` signal inside `add` just once per round, not for every point
                    let is_threshold_reached = same_round.len() == peer_count.reliable_minority();
                    // allow few duplicates in case of network error
                    (is_threshold_reached, Some(duplicates).filter(|d| *d > 3))
                }
                // points must be channelled to Collector only after signal that threshold is reached
                // (i.e. round is advanced), so had to piggyback on map's locking
                _ => {
                    assert!(
                        verified.is_err() || round <= top_dag_round.round(),
                        "branch clause is broken, expected {round:?} <= top dag {:?}, \
                         also verified {verified:?}",
                        top_dag_round.round()
                    );
                    match verified {
                        Ok(()) => {
                            if let Some(point_round) = top_dag_round.scan(point.round()) {
                                self.send_validating(
                                    &point_round,
                                    point,
                                    downloader,
                                    store,
                                    effects,
                                );
                            }
                        }
                        Err(VerifyError::IllFormed) => {
                            if let Some(point_round) = top_dag_round.scan(point.round()) {
                                point_round.add_ill_formed_broadcast_exact(point, store, effects);
                            }
                        }
                        Err(VerifyError::BadSig) => {
                            if let Some(point_round) = top_dag_round.scan(point.round()) {
                                point_round.set_bad_sig_in_broadcast_exact(&point.data().author);
                            }
                        }
                    }
                    (false, None)
                }
            }
        } else {
            (false, None) // should ban sender
        };

        if is_threshold_reached {
            self.output
                .send(ConsensusEvent::Forward(round))
                .expect("channel from filter to collector closed");
            self.consensus_round.set_max(round);
        }

        // we should ban a peer that broadcasts its rounds out of order,
        //   though we cannot prove this decision for other nodes;
        // rarely a consecutive pair of broadcasts may be reordered during high CPU load
        let level = if verified_result.map_or(true, |vr| vr.is_err()) || duplicates.is_some() {
            // that's severe errors, that require ban
            tracing::Level::ERROR
        } else {
            // sender could have not received our response, thus decides to retry
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent: effects.span(),
            level,
            author = display(author.alt()),
            round = round.0,
            digest = display(digest.alt()),
            sender = verified_result.ok_or(display(sender.alt())).err(),
            malformed = verified_result.and_then(|e| e.err()).map(debug),
            duplicates = duplicates,
            advance = Some(is_threshold_reached).filter(|x| *x),
            "received broadcast"
        );
    }

    /// drop everything up to the new round (inclusive)
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
        let engine_round = top_round.prev(); // cannot add one more `.prev()`
        let limit = (top_round.0).saturating_add(MempoolConfig::CACHE_AHEAD_ENGINE_ROUNDS as u32);

        // Drain points in historical order that cannot be neither included nor signed,
        // thus out of Collector's interest and needed for validation and commit only.
        // We are unlikely to receive such old broadcasts.

        let mut outdated = Vec::new();
        self.by_round.retain(|round, (_, map_by_peer)| {
            let is_to_take = round.next() < engine_round;
            if is_to_take {
                let points = map_by_peer
                    .values()
                    .flat_map(|map_by_digest| map_by_digest.values())
                    .cloned()
                    .collect::<Vec<_>>();
                outdated.push((*round, points));
            }
            !is_to_take && round.0 <= limit
        });
        outdated.sort_unstable_by_key(|(round, _)| *round);

        let mut released = Vec::new();
        let mut missed = Vec::new();
        for (round, points) in outdated {
            let Some(point_round) = top_dag_round.scan(round) else {
                missed.push(round.0);
                continue;
            };
            // Note: no need to channel to Collector, no new local point will reference them anyway
            for (point, _) in points {
                _ = point_round.add_broadcast_exact(&point, downloader, store, effects);
            }
            released.push(round.0);
        }

        // broadcasts of points at these rounds are very likely,
        // we are piggybacking dashmap's lock to channel points right after entry removal

        for round in [engine_round.prev(), engine_round, top_round] {
            self.output
                .send(ConsensusEvent::Forward(round))
                .expect("channel from filter to collector closed");
            if let Some((_, (_, map_by_peer))) = self.by_round.remove(&round) {
                if let Some(point_round) = top_dag_round.scan(round) {
                    let points = map_by_peer
                        .values()
                        .flat_map(|map_by_digest| map_by_digest.values());
                    for (point, _) in points {
                        self.send_validating(&point_round, point, downloader, store, effects);
                    }
                    released.push(round.0);
                } else {
                    missed.push(round.0);
                };
            }
        }

        tracing::debug!(
            parent: effects.span(),
            to = top_round.0,
            released = debug(released),
            missed = debug(missed),
            "advance round"
        );
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
