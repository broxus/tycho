use std::cmp;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::FastDashMap;
use tycho_util::metrics::HistogramGuard;

use crate::dag::{DagHead, DagRound, IllFormedReason, Verifier, VerifyError, VerifyFailReason};
use crate::dyn_event;
use crate::effects::{AltFormat, Ctx, RoundCtx};
use crate::engine::{ConsensusConfigExt, NodeConfig};
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{Digest, PeerCount, Point, PointId, Round};
use crate::storage::MempoolStore;

#[derive(Default)]
pub struct BroadcastFilter {
    /// very much like DAG structure, but without dependency check;
    /// just to determine reliably that consensus advanced without current node;
    by_round: FastDashMap<Round, ByRoundItem>,
}

#[derive(Clone)]
struct ByRoundItem {
    /// when defined, new points are send to DAG directly, and `self` will be soon removed
    flush_dag_round: Option<DagRound>,
    peer_count: PeerCount,
    /// `Arc` allows to copy during removal in [`BroadcastFilter::flush_to_dag`]
    by_author: Arc<FastDashMap<PeerId, ByAuthor>>,
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
    fn add_to_dag(
        &self,
        author: &PeerId,
        dag_round: &DagRound,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        match self {
            ByAuthorItem::Ok(point) => {
                dag_round.add_broadcast(point, downloader, store, round_ctx);
            }
            ByAuthorItem::OkPruned(digest) => {
                dag_round.add_pruned_broadcast(author, digest, downloader, store, round_ctx);
            }
            ByAuthorItem::IllFormed(point, reason) => {
                dag_round.add_ill_formed_broadcast(point, reason, store, round_ctx);
            }
            ByAuthorItem::IllFormedPruned(_digest, _) => {
                // do nothing, was stored only to determine round because signature is valid
            }
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

#[derive(Default)]
struct CacheInfo {
    reached_threshold: bool,
    duplicates: Option<u16>,
    equivocation: Option<Digest>,
}

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
impl BroadcastFilter {
    #[allow(clippy::too_many_arguments)]
    pub fn add_check_threshold(
        &self,
        sender: &PeerId,
        point: &Point,
        store: &MempoolStore,
        peer_schedule: &PeerSchedule,
        downloader: &Downloader,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) -> bool {
        let _task_time = HistogramGuard::begin("tycho_mempool_bf_add_time");
        let id = point.info().id();

        let checked = if sender != id.author {
            Err(CheckError::SenderNotAuthor(*sender))
        } else {
            // have to cache every point when the node lags behind consensus;
            let prune_after =
                head.next().round() + NodeConfig::get().cache_future_broadcasts_rounds;
            match Verifier::verify(point.info(), peer_schedule, round_ctx.conf()) {
                Ok(()) => Ok(if id.round > prune_after {
                    ByAuthorItem::OkPruned(id.digest)
                } else {
                    ByAuthorItem::Ok(point.clone())
                }),
                Err(VerifyError::IllFormed(reason)) => Ok(if id.round > prune_after {
                    ByAuthorItem::IllFormedPruned(id.digest, reason)
                } else {
                    ByAuthorItem::IllFormed(point.clone(), reason)
                }),
                Err(VerifyError::Fail(reason)) => Err(CheckError::Fail(reason)),
            }
        };

        let cache_info = self.cache(
            &checked,
            &id,
            store,
            peer_schedule,
            downloader,
            head,
            round_ctx,
        );

        let ill_formed_reason = (checked.as_ref().ok()).and_then(|item| item.ill_formed_reason());
        let level = if checked.is_err()
            || ill_formed_reason.is_some()
            || cache_info.duplicates.is_some()
            || cache_info.equivocation.is_some()
        {
            tracing::Level::ERROR
        } else {
            tracing::Level::TRACE
        };
        dyn_event!(
            parent: round_ctx.span(),
            level,
            dag_top = head.next().round().0,
            author = display(id.author.alt()),
            round = id.round.0,
            digest = display(id.digest.alt()),
            is_pruned = checked.as_ref().ok().map(|ok| ok.is_pruned()).filter(|x| *x),
            ill_formed = ill_formed_reason.map(display),
            checked = checked.as_ref().err().map(display),
            duplicates = cache_info.duplicates,
            equivocation = cache_info.equivocation.as_ref().map(|digest| display(digest.alt())),
            reached_threshold = cache_info.reached_threshold.then_some(true),
            "received broadcast"
        );

        cache_info.reached_threshold
    }

    #[allow(clippy::too_many_arguments)]
    fn cache(
        &self,
        checked: &Result<ByAuthorItem, CheckError>,
        id: &PointId,
        store: &MempoolStore,
        peer_schedule: &PeerSchedule,
        downloader: &Downloader,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) -> CacheInfo {
        let Ok(verified) = checked else {
            return CacheInfo::default();
        };

        if id.round <= head.next().round() {
            if head.last_back_bottom() <= id.round
                && let Some(dag_round) = head.next().scan(id.round)
            {
                // just add to dag directly, Engine removes such BF entries by itself
                verified.add_to_dag(&id.author, &dag_round, downloader, store, round_ctx);
            } // else: too old and totally useless now
            return CacheInfo::default();
        }

        let round_item_read = match self.by_round.get(&id.round) {
            Some(round_item) => round_item,
            None => {
                // try to create new future round: take write lock later, v_set may be uninit
                let v_set_len = peer_schedule.atomic().peers_for(id.round).len();
                match PeerCount::try_from(v_set_len) {
                    Ok(peer_count) => (self.by_round.entry(id.round))
                        .or_insert_with(|| ByRoundItem {
                            flush_dag_round: None,
                            peer_count,
                            by_author: Arc::new(FastDashMap::with_capacity_and_hasher(
                                peer_count.full(),
                                Default::default(),
                            )),
                        })
                        .downgrade(),
                    Err(_) => {
                        // v_set is not initialized, nothing to do.
                        // actually such point cannot be successfully verified,
                        // but we neither have log debounce nor should panic here
                        return CacheInfo::default();
                    }
                }
            }
        };
        match &*round_item_read {
            ByRoundItem {
                flush_dag_round: Option::None,
                peer_count,
                by_author,
            } => {
                let mut cached_info = CacheInfo::default();
                // ban the author, if we detect equivocation now; we won't be able to prove it
                // if some signatures are invalid (it's another reason for a local ban)
                (by_author.entry(id.author))
                    .and_modify(|existing| {
                        let old_digest = *existing.item.digest();
                        if old_digest == id.digest {
                            existing.duplicates = existing.duplicates.saturating_add(1);
                            // allow some duplicates in case of network error or sender restart
                            // sender could have not received our response, thus retried
                            if existing.duplicates > 3 {
                                cached_info.duplicates = Some(existing.duplicates);
                            }
                        } else {
                            cached_info.equivocation = Some(old_digest);
                        };
                    })
                    .or_insert_with(|| ByAuthor {
                        item: verified.clone(),
                        duplicates: 0,
                    });
                // length is approximate, but clean will fire eventually, concurrently is ok too
                cached_info.reached_threshold = by_author.len() == peer_count.reliable_minority();
                cached_info
            }
            ByRoundItem {
                flush_dag_round: Some(flush_dag_round),
                ..
            } => {
                // a rare scenario to handle race with `flush_to_dag()` when `head` arg is older
                let point_round = flush_dag_round.clone();
                drop(round_item_read);
                assert_eq!(id.round, point_round.round(), "wrong round to flush into");
                verified.add_to_dag(&id.author, &point_round, downloader, store, round_ctx);
                CacheInfo::default()
            }
        }
    }

    /// just drop unneeded data when Engine is paused and round task is not running
    /// while collator is syncing blocks
    pub fn clean(&self, round: Round, head: &DagHead, round_ctx: &RoundCtx) {
        let _task_time = HistogramGuard::begin("tycho_mempool_bf_clean_time");
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
                dag_top = head.next().round().0,
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
    pub fn flush_to_dag(
        &self,
        head: &DagHead,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        let _task_time = HistogramGuard::begin("tycho_mempool_bf_flush_time");
        let back_bottom = head.last_back_bottom();
        let head_next_round = head.next().round();

        // Drain points in historical order that cannot be neither included nor signed,
        // thus out of Signer's interest and needed for validation and commit only.
        // We are unlikely to receive such old broadcasts while node is in sync with others.

        let mut past_removed = CleanCounter::default();
        // allocate up to the max
        let mut outdated =
            Vec::with_capacity(1 + head_next_round.0.saturating_sub(back_bottom.0) as usize);
        let mut future_kept = CleanCounter::default();

        self.by_round.retain(|&round, round_item| {
            if round < back_bottom {
                past_removed.add(round, round_item);
                false
            } else if round <= head_next_round {
                // don't set temp dag round yet to route new points to DAG only in historical order
                outdated.push((round, round_item.by_author.clone(), None));
                true
            } else {
                future_kept.add(round, round_item);
                true
            }
        });

        // most frequent case: only DagHead.next() round is taken and counter is used only once
        let mut flushed = ByRoundCounter::with_capacity(outdated.len());
        let mut not_in_dag = Vec::with_capacity(outdated.len());

        // apply reversed historical order to scan dag rounds (first is new and last is old)
        outdated.sort_unstable_by_key(|(round, _, _)| cmp::Reverse(*round));

        let mut last_used_dag_round = head.next();
        for (round, _, dag_round_opt) in outdated.iter_mut() {
            let Some(found) = last_used_dag_round.scan(*round) else {
                break; // reached dag end; nothing can do if dag is not contiguous
            };
            last_used_dag_round = dag_round_opt.insert(found);
        }

        // apply natural historical order (first is old and last is new)
        outdated.reverse();

        // preserve historical order by round to not create excessive download tasks
        for (round, map_by_author, dag_round) in outdated {
            if let Some(dag_round) = &dag_round {
                assert_eq!(round, dag_round.round(), "inserted wrong round");
                if let Some(mut by_round) = self.by_round.get_mut(&round) {
                    let prev = by_round.flush_dag_round.replace(dag_round.clone());
                    if prev.is_some() {
                        let _span = round_ctx.span().enter();
                        panic!("should not flush in parallel, BF round {}", round.0)
                    }
                }
                let mut incr = ByRoundIncrement::default();
                for entry in map_by_author.iter() {
                    let (author, by_author) = entry.pair();
                    incr.count(&by_author.item);
                    (by_author.item).add_to_dag(author, dag_round, downloader, store, round_ctx);
                }
                flushed.add(round, incr);
                self.by_round.remove(&round);
            } else {
                not_in_dag.push(round.0);
            }
        }

        if !flushed.is_empty() || !past_removed.is_empty() {
            tracing::info!(
                parent: round_ctx.span(),
                dag_top = head.next().round().0,
                flushed_rounds = flushed.rounds,
                points_total = (flushed.points_total > 0).then_some(flushed.points_total),
                digests_total = (flushed.digests_total > 0).then_some(flushed.digests_total),
                points = (!flushed.points.is_empty()).then_some(debug(flushed.points)),
                digests = (!flushed.digests.is_empty()).then_some(debug(flushed.digests)),
                rounds_not_in_dag = (!not_in_dag.is_empty()).then_some(debug(not_in_dag)),
                past_removed = %past_removed,
                future_kept = %future_kept,
                "BF flushed to DAG"
            );
        }
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
    fn count(&mut self, item: &ByAuthorItem) {
        if item.is_pruned() {
            self.digests = self.digests.saturating_add(1);
        } else {
            self.points = self.points.saturating_add(1);
        }
    }
}

/// Keeps [`Round`] as `u32` to be formatted with default `DebugFmt` for `Vec`.
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
    fn with_capacity(capacity: usize) -> Self {
        Self {
            rounds: 0,
            points: Vec::with_capacity(capacity),
            digests: Vec::with_capacity(capacity),
            points_total: 0,
            digests_total: 0,
        }
    }
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
        self.items = self.items.saturating_add(by_round_item.by_author.len());
        self.rounds = self.rounds.saturating_add(1);
        self.min = self.min.min(round);
        self.max = self.max.max(round);
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
