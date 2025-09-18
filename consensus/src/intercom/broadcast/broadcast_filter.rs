use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::{cmp, mem};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::dag::{DagHead, DagRound, IllFormedReason, Verifier, VerifyError, VerifyFailReason};
use crate::dyn_event;
use crate::effects::{AltFormat, Ctx, RoundCtx};
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::engine::{ConsensusConfigExt, NodeConfig};
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{Digest, PeerCount, Point, PointId, Round};
use crate::storage::MempoolStore;

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
            Some(round_item) => match round_item.cached.as_ref() {
                Some(cached) => cached.by_author.contains_key(sender),
                None => false,
            },
        }
    }

    pub fn flush_to_dag(
        &self,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        self.inner.flush_to_dag(head, downloader, store, round_ctx);
    }
}

struct BroadcastFilterInner {
    peer_schedule: PeerSchedule,
    consensus_round: RoundWatch<Consensus>,
    /// very much like DAG structure, but without dependency check;
    /// just to determine reliably that consensus advanced without current node;
    by_round: FastDashMap<Round, ByRoundItem>,
}

/// Both fields cannot be `None` - they are changed under dash map lock
struct ByRoundItem {
    /// values are initialized with `Some` that are taken during cleanup,
    /// because empty [`dashmap::DashMap`] allocates
    cached: Option<Cached>,
    /// when equal or greater round exists, points are send to it directly
    dag_round: Option<DagRound>,
}

struct Cached {
    peer_count: PeerCount,
    /// By-round lock is mostly read lock, so this provides write locks
    by_author: FastDashMap<PeerId, ByAuthor>,
}

#[derive(Clone)]
struct ByAuthor {
    item: ByAuthorItem,
    duplicates: u16,
}
/// store only `Digest` when point is too far from engine round
#[derive(Clone)]
enum ByAuthorItem {
    Ok(Point),
    OkPruned(Digest),
    IllFormed(Point, IllFormedReason),
    IllFormedPruned(Digest, IllFormedReason),
}
impl ByAuthorItem {
    fn digest(&self) -> &Digest {
        match self {
            Self::Ok(point) | Self::IllFormed(point, _) => point.info().digest(),
            Self::OkPruned(digest) | Self::IllFormedPruned(digest, _) => digest,
        }
    }
    fn is_pruned(&self) -> bool {
        match self {
            Self::Ok(_) | Self::IllFormed(_, _) => false,
            Self::OkPruned(_) | Self::IllFormedPruned(_, _) => true,
        }
    }
    fn ill_formed_reason(&self) -> Option<&IllFormedReason> {
        match self {
            Self::Ok(_) | Self::OkPruned(_) => None,
            Self::IllFormed(_, reason) | Self::IllFormedPruned(_, reason) => Some(reason),
        }
    }
}
#[derive(thiserror::Error, Debug)]
enum CheckError {
    #[error("sender {} is not author", .0.alt())]
    SenderNotAuthor(PeerId),
    #[error("failed to verify: {0}")]
    Fail(VerifyFailReason),
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
    //  * if Engine is MAX_HISTORY_DEPTH+ behind consensus: BF keeps MAX_HISTORY_DEPTH items
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
        } = point.info().id();

        // head may be outdated during Engine round switch
        let top_round = head.next().round();

        let checked = if sender != author {
            Err(CheckError::SenderNotAuthor(*sender))
        } else {
            // have to cache every point when the node lags behind consensus
            let prune_after = top_round + NodeConfig::get().cache_future_broadcasts_rounds;
            match Verifier::verify(point.info(), &self.peer_schedule, round_ctx.conf()) {
                Ok(()) => Ok(if round > prune_after {
                    ByAuthorItem::OkPruned(digest)
                } else {
                    ByAuthorItem::Ok(point.clone())
                }),
                Err(VerifyError::IllFormed(reason)) => Ok(if round > prune_after {
                    ByAuthorItem::IllFormedPruned(digest, reason)
                } else {
                    ByAuthorItem::IllFormed(point.clone(), reason)
                }),
                Err(VerifyError::Fail(reason)) => Err(CheckError::Fail(reason)),
            }
        };

        let (is_future_threshold_reached, duplicates, equivocation) = match &checked {
            Ok(verified) => {
                // just don't want to mess with exact type, thus generic
                enum MapSearch<T> {
                    Entry(T),
                    AddToDag,
                    Ignore,
                }
                let map_search = if round <= top_round {
                    if head.last_back_bottom() <= round {
                        // just add to dag directly, Engine removes such BF entries by itself
                        MapSearch::AddToDag
                    } else {
                        // too old and totally useless now
                        MapSearch::Ignore
                    }
                } else {
                    // note: entry lock guard is passed into its ref which cannot remove entry
                    match self.by_round.get(&round) {
                        Some(round_item) => MapSearch::Entry(round_item),
                        None => {
                            // try to create new future round: take write lock later, v_set may be uninit
                            let v_set_len = self.peer_schedule.atomic().peers_for(round).len();
                            match PeerCount::try_from(v_set_len) {
                                Ok(peer_count) => {
                                    let entry =
                                        self.by_round.entry(round).or_insert_with(|| ByRoundItem {
                                            cached: Some(Cached {
                                                peer_count,
                                                by_author: FastDashMap::with_capacity_and_hasher(
                                                    peer_count.full(),
                                                    Default::default(),
                                                ),
                                            }),
                                            dag_round: None,
                                        });
                                    MapSearch::Entry(entry.downgrade())
                                }
                                Err(_) => {
                                    // v_set is not initialized, nothing to do.
                                    // actually such point cannot be successfully verified,
                                    // but we neither have log debounce nor should panic here
                                    MapSearch::Ignore
                                }
                            }
                        }
                    }
                };
                match map_search {
                    MapSearch::Entry(round_item_opt) => match &*round_item_opt {
                        ByRoundItem {
                            cached: Some(cached),
                            dag_round: Option::None,
                        } => {
                            // ban the author, if we detect equivocation now; we won't be able to prove it
                            // if some signatures are invalid (it's another reason for a local ban)
                            let (duplicates, equivocation) = match cached.by_author.entry(author) {
                                dashmap::Entry::Occupied(mut existing) => {
                                    let old_digest = *existing.get().item.digest();
                                    let duplicates = &mut existing.get_mut().duplicates;

                                    let equivocation = if &old_digest == point.info().digest() {
                                        *duplicates = duplicates.saturating_add(1);
                                        None
                                    } else {
                                        Some(old_digest)
                                    };
                                    // allow some duplicates in case of network error or sender restart
                                    // sender could have not received our response, thus retried
                                    (Some(*duplicates).filter(|d| *d > 3), equivocation)
                                }
                                dashmap::Entry::Vacant(vacant) => {
                                    vacant.insert(ByAuthor {
                                        item: verified.clone(),
                                        duplicates: 0,
                                    });
                                    (None, None)
                                }
                            };
                            let is_future_threshold_reached =
                                cached.by_author.len() == cached.peer_count.reliable_minority();
                            (is_future_threshold_reached, duplicates, equivocation)
                        }
                        ByRoundItem {
                            dag_round: Some(start_round),
                            ..
                        } => {
                            // start round is newer than current head
                            if let Some(point_round) = start_round.scan(round) {
                                let iter = std::iter::once((&author, verified));
                                Self::add_all_to_dag(
                                    iter,
                                    &point_round,
                                    downloader,
                                    store,
                                    round_ctx,
                                );
                            }
                            (false, None, None)
                        }
                        ByRoundItem {
                            cached: None,
                            dag_round: None,
                        } => panic!("flush must not leave both fields empty"),
                    },
                    MapSearch::AddToDag => {
                        if let Some(dag_round) = head.next().scan(round) {
                            let iter = std::iter::once((&author, verified));
                            Self::add_all_to_dag(iter, &dag_round, downloader, store, round_ctx);
                        }
                        (false, None, None)
                    }
                    MapSearch::Ignore => (false, None, None),
                }
            }
            Err(_) => (false, None, None),
        };

        let ill_formed_reason = (checked.as_ref().ok()).and_then(|item| item.ill_formed_reason());
        let level = if checked.is_err()
            || ill_formed_reason.is_some()
            || duplicates.is_some()
            || equivocation.is_some()
        {
            tracing::Level::ERROR
        } else {
            tracing::Level::TRACE
        };
        dyn_event!(
            parent: round_ctx.span(),
            level,
            author = display(author.alt()),
            round = round.0,
            digest = display(digest.alt()),
            is_pruned = checked.as_ref().ok().map(|ok| ok.is_pruned()).filter(|x| *x),
            ill_formed = ill_formed_reason.map(display),
            checked = checked.as_ref().err().map(display),
            duplicates = duplicates,
            equivocation = equivocation.as_ref().map(|digest| display(digest.alt())),
            threshold_reached = Some(is_future_threshold_reached).filter(|x| *x),
            "received broadcast"
        );

        if is_future_threshold_reached {
            // notify Collector after max consensus round is updated
            self.consensus_round.set_max(round);

            // round is determined, so clean history;
            // do not flush to DAG as it may have no needed rounds yet
            self.clean(round, head, round_ctx);
        }
    }

    /// just drop unneeded data when Engine is paused and round task is not running
    /// while collator is syncing blocks
    fn clean(&self, round: Round, head: &DagHead, round_ctx: &RoundCtx) {
        if round != self.consensus_round.get() {
            // engine is not paused, let it do its work
            return;
        }

        // inclusive bounds on what should be left in cache
        let history_bottom =
            (head.last_back_bottom()).max(round - round_ctx.conf().consensus.max_total_rounds());
        let prune_after = round + NodeConfig::get().cache_future_broadcasts_rounds;

        let mut past_removed = CleanCounter::default();
        let mut current_kept = CleanCounter::default();
        let mut future_removed = CleanCounter::default();

        self.by_round.retain(|&round, round_item| {
            if round < history_bottom {
                past_removed.add(round, round_item);
                false
            } else if round <= prune_after {
                current_kept.add(round, round_item);
                true
            } else {
                future_removed.add(round, round_item);
                false
            }
        });

        if !past_removed.is_empty() || !future_removed.is_empty() {
            tracing::info!(
                parent: round_ctx.span(),
                consensus_round = round.0,
                keep_range = %format!("[{} ..= {}]", history_bottom.0, prune_after.0),
                kept = %current_kept,
                past_removed = %past_removed,
                future_removed = %future_removed,
                "BF cleaned"
            );
        }
    }

    /// drop everything up to the new round (inclusive) on round task
    fn flush_to_dag(
        &self,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        let back_bottom = head.last_back_bottom();
        let head_prev_round = head.prev().round();
        let head_next_round = head.next().round();

        // Drain points in historical order that cannot be neither included nor signed,
        // thus out of Signer's interest and needed for validation and commit only.
        // We are unlikely to receive such old broadcasts while node is in sync with others.

        let mut past_removed = CleanCounter::default();
        let mut outdated_unordered = Vec::new();
        let mut future_kept = CleanCounter::default();

        self.by_round.retain(|&round, round_item| {
            if round < back_bottom {
                past_removed.add(round, round_item);
                false
            } else if round < head_prev_round {
                if let Some(cached) = mem::take(&mut round_item.cached) {
                    outdated_unordered.push((round, cached.by_author.into_read_only()));
                }
                // no need to set `round_item.dag_round` as item is removed under lock
                false
            } else if round <= head_next_round {
                // keep head rounds for now, remove later in this method
                // do not set `round_item.dag_round` now to flush only in by-round order
                true
            } else {
                future_kept.add(round, round_item);
                true
            }
        });

        // most frequent case: only DagHead.next() round is taken and counter is used only once
        let mut flushed = ByRoundCounter::default();

        let (outdated, not_in_dag) = if outdated_unordered.is_empty() {
            Default::default()
        } else {
            // alloc to the max
            let mut outdated = VecDeque::with_capacity(outdated_unordered.len());
            let mut not_in_dag = VecDeque::with_capacity(outdated_unordered.len());

            // apply reversed historical order to scan dag rounds (back is new and front is old)
            outdated_unordered.sort_unstable_by_key(|(round, _)| cmp::Reverse(*round));
            let outdated_reversed = outdated_unordered;

            let mut last_used_dag_round = Some(head.next().clone());
            for (round, map_by_author) in outdated_reversed {
                last_used_dag_round = last_used_dag_round.and_then(|last| last.scan(round));
                match &last_used_dag_round {
                    Some(found) => outdated.push_front((found.clone(), map_by_author)),
                    None => not_in_dag.push_front(round),
                };
            }
            // results in historical order: back is old and front is new
            (outdated, not_in_dag)
        };

        // preserve historical order by round to not create excessive download tasks
        for (dag_round, map_by_author) in outdated {
            let iter = (map_by_author.iter()).map(|(author, by_author)| (author, &by_author.item));
            let incr = Self::add_all_to_dag(iter, &dag_round, downloader, store, round_ctx);
            flushed.add(dag_round.round(), incr);
        }

        // broadcasts of points at these rounds are very likely,
        // so we remove rounds in order to move points into dag safely for Signer

        for dag_round in [head.prev(), head.current(), head.next()] {
            let round = dag_round.round();
            let map_by_author = {
                let dashmap::Entry::Occupied(mut entry) = self.by_round.entry(round) else {
                    continue; // already in dag
                };
                // set `dag_round` to channel points and drop the write lock for now
                (entry.get_mut().dag_round).get_or_insert_with(|| dag_round.clone());
                match mem::take(&mut entry.get_mut().cached) {
                    Some(cached) => cached.by_author.into_read_only(),
                    None => {
                        entry.remove();
                        continue; // already in dag
                    }
                }
            };
            let iter = (map_by_author.iter()).map(|(author, by_author)| (author, &by_author.item));
            let incr = Self::add_all_to_dag(iter, dag_round, downloader, store, round_ctx);
            flushed.add(round, incr);
            self.by_round.remove(&round);
        }

        if !flushed.is_empty() || !past_removed.is_empty() {
            tracing::info!(
                parent: round_ctx.span(),
                flushed_rounds = flushed.rounds,
                points_total = Some(flushed.points_total).filter(|qnt| *qnt > 0),
                digests_total = Some(flushed.digests_total).filter(|qnt| *qnt > 0),
                points = Some(flushed.points).filter(|vec| !vec.is_empty()).map(debug),
                digests = Some(flushed.digests).filter(|vec| !vec.is_empty()).map(debug),
                not_in_dag = Some(not_in_dag).filter(|vec| !vec.is_empty()).map(debug),
                past_removed = %past_removed,
                future_kept = %future_kept,
                "BF flushed to DAG"
            );
        }
    }

    fn add_all_to_dag<'a>(
        author_item: impl Iterator<Item = (&'a PeerId, &'a ByAuthorItem)>,
        dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> ByRoundIncrement {
        let mut incr = ByRoundIncrement::default();

        for (author, item) in author_item {
            match item {
                ByAuthorItem::Ok(point) => {
                    dag_round.add_broadcast(point, downloader, store, round_ctx);
                    incr.add_point();
                }
                ByAuthorItem::OkPruned(digest) => {
                    dag_round.add_pruned_broadcast(author, digest, downloader, store, round_ctx);
                    incr.add_digest();
                }
                ByAuthorItem::IllFormed(point, reason) => {
                    dag_round.add_ill_formed_broadcast(point, reason, store, round_ctx);
                    incr.add_point();
                }
                ByAuthorItem::IllFormedPruned(_digest, _) => {
                    // do nothing, was stored only to determine round because signature matched
                    incr.add_digest();
                }
            }
        }
        incr
    }
}

/// Every round in BF contains at most one point for each peer from `v_set`, which size
/// cannot exceed [`PeerCount::MAX`]. So `u8` is correct, saturate in case of a bug.
#[derive(Default)]
struct ByRoundIncrement {
    points: u8,
    digests: u8,
}
impl ByRoundIncrement {
    fn add_point(&mut self) {
        self.points = self.points.saturating_add(1);
    }
    fn add_digest(&mut self) {
        self.digests = self.digests.saturating_add(1);
    }
}

/// Keeps [`Round`] as `u32` to be formatted with default `DebugFmt` for `Vec`.
#[derive(Default)]
struct ByRoundCounter {
    rounds: u32,
    // round -> count; values are inserted in historical order
    points: Vec<(u32, u8)>,
    digests: Vec<(u32, u8)>,
    // total sums of the above are very unlikely to exceed even u16
    points_total: u32,
    digests_total: u32,
}
impl ByRoundCounter {
    fn add(&mut self, round: Round, incr: ByRoundIncrement) {
        self.rounds = self.rounds.saturating_add(1);
        if incr.points > 0 {
            self.points.push((round.0, incr.points));
            self.points_total = self.points_total.saturating_add(incr.points as _);
        }
        if incr.digests > 0 {
            self.digests.push((round.0, incr.digests));
            self.digests_total = self.digests_total.saturating_add(incr.digests as _);
        }
    }
    fn is_empty(&self) -> bool {
        self.rounds == 0
    }
}

struct CleanCounter {
    items: usize,
    rounds: u32,
    min: Round,
    max: Round,
}
impl Default for CleanCounter {
    fn default() -> Self {
        Self {
            items: 0,
            rounds: 0,
            min: Round(u32::MAX),
            max: Round::BOTTOM,
        }
    }
}
impl CleanCounter {
    fn add(&mut self, round: Round, by_round_item: &ByRoundItem) {
        self.rounds = self.rounds.saturating_add(1);
        self.min = self.min.min(round);
        self.max = self.max.max(round);
        if let Some(cached) = by_round_item.cached.as_ref() {
            self.items = self.items.saturating_add(cached.by_author.len());
        };
    }
    fn is_empty(&self) -> bool {
        self.rounds == 0
    }
}
impl Display for CleanCounter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            f.write_str("nothing")
        } else {
            write!(f, "{} items in {} rounds ", self.items, self.rounds)?;
            write!(f, "in [{} ..= {}]", self.min.0, self.max.0)
        }
    }
}
