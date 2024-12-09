use std::collections::hash_map;
use std::sync::Arc;

use dashmap::mapref::entry::Entry as DashMapEntry;
use tycho_network::PeerId;
use tycho_util::{FastDashMap, FastHashMap};

use crate::dag::{DagHead, Verifier, VerifyError};
use crate::dyn_event;
use crate::effects::{AltFormat, Ctx, MempoolStore, RoundCtx};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::engine::{CachedConfig, ConsensusConfigExt, Genesis};
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{Digest, PeerCount, Point, PointId, Round};

#[derive(Clone)]
pub struct BroadcastFilter {
    inner: Arc<BroadcastFilterInner>,
}

impl BroadcastFilter {
    pub fn new(peer_schedule: &PeerSchedule, consensus_round: &RoundWatch<Consensus>) -> Self {
        Self {
            inner: Arc::new(BroadcastFilterInner {
                peer_schedule: peer_schedule.clone(),
                consensus_round: consensus_round.clone(),
                by_round: Default::default(),
            }),
        }
    }

    pub fn add(
        &self,
        sender: &PeerId,
        point: &Point,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        self.inner
            .add(sender, point, head, downloader, store, round_ctx);
    }

    pub fn has_point(&self, round: Round, sender: &PeerId) -> bool {
        match self.inner.by_round.get(&round) {
            None => false,
            Some(mapref) => mapref.value().1.contains_key(sender),
        }
    }

    pub fn advance_round(
        &self,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        self.inner.advance_round(head, downloader, store, round_ctx);
    }
}

// Result is used to store only `Digest` when point is too far from engine round
type MapByPeer = FastHashMap<PeerId, (Result<Point, Digest>, usize)>;
struct BroadcastFilterInner {
    peer_schedule: PeerSchedule,
    consensus_round: RoundWatch<Consensus>,
    // very much like DAG structure, but without dependency check;
    // just to determine reliably that consensus advanced without current node
    by_round: FastDashMap<Round, (PeerCount, MapByPeer)>,
}

impl BroadcastFilterInner {
    // Note logic still under consideration because of contradiction in requirements:
    //  * we must determine the latest consensus round reliably:
    //    the current approach is to collect 1/3+1 points at the same future round
    //    => we should collect as much points as possible
    //  * we must defend the DAG and current cache from spam from future rounds,
    //    => we should discard points from the far future
    //  * DAG can account equivocated points, but caching future equivocations is an easy OOM
    //  On cache eviction:
    //  * if Engine is [0, CACHE_ROUNDS] behind consensus: BF stores points
    //  * if Engine is CACHE_ROUNDS+ behind consensus: BF stores point digests only
    //  * if Engine is MAX_HISTORY_DEPTH+ behind consensus: BF drops everything up to consensus

    /// channels Vec of points to insert into DAG if consensus round is determined reliably;
    /// returns `true` if round should be advanced;
    fn add(
        &self,
        sender: &PeerId,
        point: &Point,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
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

        let top_round = head.next().round();

        let (is_threshold_reached, duplicates, equivocation) = if let Some(verified) =
            &verified_result
        {
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

                    if round <= top_round {
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
                Err(ExtendMapErr::CannotExtend) => (false, None, None),
                // grab a lock
                Ok(mut entry) if verified.is_ok() && round >= top_round => {
                    let (peer_count, same_round) = entry.value_mut();
                    // ban the author, if we detect equivocation now; we won't be able to prove it
                    //   if some signatures are invalid (it's another reason for a local ban)
                    let (duplicates, equivocation) = match same_round.entry(author) {
                        hash_map::Entry::Occupied(mut existing) => {
                            let (old_digest, duplicates) = match existing.get_mut() {
                                (Ok(old_point), duplicates) => (*old_point.digest(), duplicates),
                                (Err(old_digest), duplicates) => (*old_digest, duplicates),
                            };
                            if &old_digest == point.digest() {
                                *duplicates += 1;
                                // allow some duplicates in case of network error or sender restart
                                (Some(*duplicates).filter(|d| *d > 3), None)
                            } else {
                                (None, Some(old_digest))
                            }
                        }
                        hash_map::Entry::Vacant(vacant) => {
                            let cached = if head.current().round()
                                + (CachedConfig::get().node.cache_future_broadcasts_rounds).get()
                                <= self.consensus_round.get()
                            {
                                Ok(point.clone())
                            } else {
                                Err(*point.digest())
                            };
                            vacant.insert((cached, 0));
                            (None, None)
                        }
                    };
                    // send `Forward` signal inside `add` just once per round, not for every point
                    let is_threshold_reached = same_round.len() == peer_count.reliable_minority();
                    (is_threshold_reached, duplicates, equivocation)
                }
                // points must be channelled to Collector only after signal that threshold is reached
                // (i.e. round is advanced), so had to piggyback on map's locking
                Err(ExtendMapErr::RoundIsInDag) | Ok(_) => {
                    assert!(
                        verified.is_err() || round <= top_round,
                        "branch clause is broken, expected {round:?} <= top dag {:?}, \
                         also verified {verified:?}",
                        top_round
                    );
                    match &verified {
                        Ok(()) => {
                            if let Some(dag_round) = head.next().scan(point.round()) {
                                dag_round.add_broadcast_exact(point, downloader, store, round_ctx);
                            }
                        }
                        Err(
                            VerifyError::IllFormed
                            | VerifyError::MustBeEmpty(_)
                            | VerifyError::LackOfPeers(_)
                            | VerifyError::UnknownPeers(_),
                        ) => {
                            if let Some(dag_round) = head.next().scan(point.round()) {
                                dag_round.add_ill_formed_broadcast_exact(point, store, round_ctx);
                            }
                        }
                        Err(VerifyError::BadSig) => {
                            if let Some(dag_round) = head.next().scan(point.round()) {
                                dag_round.set_bad_sig_in_broadcast_exact(&point.data().author);
                            }
                        }
                        Err(
                            VerifyError::BeforeGenesis
                            | VerifyError::UnknownAuthor
                            | VerifyError::Uninit(_),
                        ) => {} // nothing to do
                    }
                    (false, None, None)
                }
            }
        } else {
            (false, None, None) // should ban sender
        };

        // we should ban a peer that broadcasts its rounds out of order,
        //   though we cannot prove this decision for other nodes;
        // rarely a consecutive pair of broadcasts may be reordered during high CPU load
        let level = if verified_result.as_ref().map_or(true, |vr| vr.is_err())
            || duplicates.is_some()
            || equivocation.is_some()
        {
            // that's severe errors, that require ban
            tracing::Level::ERROR
        } else {
            // sender could have not received our response, thus decides to retry
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent: round_ctx.span(),
            level,
            author = display(author.alt()),
            round = round.0,
            digest = display(digest.alt()),
            sender = verified_result.as_ref().ok_or(display(sender.alt())).err(),
            verified = verified_result.and_then(|e| e.err()).map(display),
            duplicates = duplicates,
            equivocation = equivocation.as_ref().map(|digest| display(digest.alt())),
            advance = Some(is_threshold_reached).filter(|x| *x),
            "received broadcast"
        );

        if is_threshold_reached && round >= top_round {
            // notify collector after max consensus round is updated
            // so engine will be consistent after collector finishes and exits
            self.consensus_round.set_max(round);
            // must not apply peer schedule changes as DagBack may be not ready validating smth old
            // but threshold can be reached only when peer schedule is defined, so it's safe
            self.advance_round(head, downloader, store, round_ctx);
        }
    }

    /// drop everything up to the new round (inclusive)
    fn advance_round(
        &self,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        // Engine round task is not running while collator is syncing blocks
        let history_bottom = (Genesis::id().round)
            .max(self.consensus_round.get() - CachedConfig::get().consensus.max_total_rounds());

        // Drain points in historical order that cannot be neither included nor signed,
        // thus out of Collector's interest and needed for validation and commit only.
        // We are unlikely to receive such old broadcasts.

        let mut outdated = Vec::new();
        let prev_round = head.prev().round();
        self.by_round.retain(|round, (_, map_by_peer)| {
            let is_to_take = *round < prev_round;
            if is_to_take {
                // no need to pass evicted broadcasts, just points to not download them
                let points = map_by_peer
                    .values()
                    .filter(|(r, _)| r.is_ok())
                    .filter_map(|(r, _)| r.clone().ok())
                    .collect::<Vec<_>>();
                outdated.push((*round, points));
            }
            // discard elements whose predicates return false
            !is_to_take || *round >= history_bottom
        });
        outdated.sort_unstable_by_key(|(round, _)| *round);

        let mut released = Vec::new();
        let mut not_found = Vec::new();
        let mut missed = Vec::new();
        for (round, points) in outdated {
            let Some(point_round) = head.prev().scan(round) else {
                missed.push(round.0);
                continue;
            };
            // preserve order by round to not create excessive download tasks
            for point in points {
                point_round.add_broadcast_exact(&point, downloader, store, round_ctx);
            }
            released.push(round.0);
        }

        // broadcasts of points at these rounds are very likely,
        // we are piggybacking dashmap's lock to channel points right after entry removal

        for dag_round in [head.prev(), head.current(), head.next()] {
            let round = dag_round.round();
            let DashMapEntry::Occupied(entry) = self.by_round.entry(round) else {
                not_found.push(round.0);
                continue;
            };
            let (_, map_by_peer) = entry.get();
            for (peer_id, (point_or_digest, _)) in map_by_peer {
                match point_or_digest {
                    Ok(point) => {
                        dag_round.add_broadcast_exact(point, downloader, store, round_ctx);
                    }
                    Err(digest) => {
                        dag_round.add_evicted_broadcast_exact(
                            peer_id, digest, downloader, store, round_ctx,
                        );
                    }
                }
            }
            released.push(round.0);

            entry.remove(); // release lock
        }

        tracing::debug!(
            parent: round_ctx.span(),
            to = head.next().round().0,
            released = debug(released),
            missed = debug(missed),
            not_found = debug(not_found),
            "advance round"
        );
    }
}
