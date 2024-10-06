use std::cmp;
use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tycho_network::PeerId;
use tycho_util::{FastDashMap, FastHashMap};

use super::dto::ConsensusEvent;
use crate::dag::{DagRound, Verifier, VerifyError};
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::engine::MempoolConfig;
use crate::intercom::dto::PeerState;
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
                last_by_peer: Default::default(),
                by_round: Default::default(),
            }),
        }
    }

    pub fn add(
        &self,
        sender: &PeerId,
        point: &Point,
        top_dag_round: Option<&DagRound>,
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

type SimpleDagLocations = FastHashMap<PeerId, FastHashMap<Digest, Point>>;
struct BroadcastFilterInner {
    peer_schedule: PeerSchedule,
    consensus_round: RoundWatch<Consensus>,
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
        top_dag_round: Option<&DagRound>,
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

        tracing::debug!(
            parent: effects.span(),
            sender = display(sender.alt()),
            author = display(author.alt()),
            round = round.0,
            digest = display(digest.alt()),
            "received broadcast"
        );

        let verified = if sender != author {
            tracing::error!(
                parent: effects.span(),
                sender = display(sender.alt()),
                author = display(author.alt()),
                round = round.0,
                digest = display(digest.alt()),
                "sender is not author"
            );
            Err(None)
        } else {
            Verifier::verify(point, &self.peer_schedule).map_err(|err| {
                tracing::error!(
                    parent: effects.span(),
                    result = debug(&err),
                    author = display(author.alt()),
                    round = round.0,
                    digest = display(digest.alt()),
                    "verified"
                );
                Some(err)
            })
        };

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
            enum ExtendMapErr {
                RoundIsInDag,
                UnknownPeerCount,
            }
            // note: lock guard inside result
            let try_by_round_entry = self.by_round.entry(round).or_try_insert_with(|| {
                // if `top dag round` not defined: node start is not finished and dag is not ready,
                // has to cache incoming points in Filter's map first, then in channel to Collector

                // if `point round` > `top dag round`: either consensus moved forward without us,
                // or we shouldn't accept the point into DAG yet, or it's a spam

                if top_dag_round.map_or(false, |top| round <= top.round()) {
                    // if entry was removed - do not create one, insert right into DAG
                    Err(ExtendMapErr::RoundIsInDag)
                } else {
                    // how many nodes should send broadcasts
                    PeerCount::try_from(self.peer_schedule.atomic().peers_for(round).len())
                        // Err: will not accept broadcasts from not yet initialized validator set
                        .map_err(|_peer_count_err| ExtendMapErr::UnknownPeerCount)
                        .map(|peer_count| (peer_count, Default::default()))
                }
            });
            match try_by_round_entry {
                Err(ExtendMapErr::UnknownPeerCount) => false,
                Err(ExtendMapErr::RoundIsInDag) => {
                    let top_dag_round = top_dag_round.expect("must exist for this branch");
                    assert!(
                        round <= top_dag_round.round(),
                        "branch clause is broken, expected {} <= {}",
                        round.0,
                        top_dag_round.round().0
                    );
                    // commented out: outdated broadcast may resolve download or short-circuit a validation
                    // if round < top_dag_round.round().prev() {
                    //     return; // will not be certified; look Signer's response `TooOldRound`
                    // }
                    let Some(point_round) = top_dag_round.scan(point.round()) else {
                        // tracing::warn!("DAG is too shallow", round = round.0);
                        return; // cannot process anyway
                    };
                    match verified {
                        Ok(()) => {
                            self.send_validating(&point_round, point, downloader, store, effects);
                        }
                        Err(Some(VerifyError::IllFormed)) => {
                            point_round.add_ill_formed_broadcast_exact(point, store, effects);
                        }
                        Err(Some(VerifyError::BadSig)) => {
                            point_round.set_bad_sig_in_broadcast_exact(&point.data().author);
                        }
                        Err(None) => {} // should ban sender, it is not the author
                    };
                    false
                }
                Ok(mut entry) => {
                    let (peer_count, same_round) = entry.value_mut();
                    // ban the author, if we detect equivocation now; we won't be able to prove it
                    //   if some signatures are invalid (it's another reason for a local ban)
                    same_round
                        .entry(author)
                        .or_default()
                        .insert(digest, point.clone());
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
            for point in points {
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
                    for point in points {
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
