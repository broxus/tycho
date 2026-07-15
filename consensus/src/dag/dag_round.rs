use std::sync::{Arc, OnceLock, Weak};

use tokio::sync::mpsc;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::dag::IllFormedReason;
use crate::dag::dag_location::DagLocation;
use crate::dag::dag_point_future::{DagPointFuture, WeakDagPointFuture};
use crate::dag::proof_leader::ProofLeader;
use crate::dag::threshold::Threshold;
use crate::effects::{AltFmt, AltFormat, Ctx, RoundCtx, ValidateCtx};
use crate::engine::{MempoolConfig, NodeConfig};
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{Digest, PeerCount, Point, PointRestore, Round, WeakCert};
use crate::storage::MempoolStore;

#[derive(Clone)]
/// Allows memory allocated by DAG to be freed
pub struct WeakDagRound(Weak<DagRoundInner>);

#[derive(Clone)]
/// do not pass to backwards-recursive async tasks
/// (where gag length is just a logical limit, but is not explicitly applicable)
/// to prevent severe memory leaks of a whole DAG round
/// (in case congested tokio runtime reorders futures), use [`WeakDagRound`] for that
pub struct DagRound(Arc<DagRoundInner>);

/// made public only to support drop via Reclaimer
pub struct DagRoundInner {
    round: Round,
    peer_count: PeerCount,
    proof_leader: Option<PeerId>,
    used_anchor_proof: OnceLock<PeerId>,
    locations: FastDashMap<PeerId, DagLocation>,
    threshold: Threshold,
    triggers_tx: mpsc::UnboundedSender<WeakDagPointFuture>,
    /// sequence of prev rounds: 0 for newest; never empty
    prevs: Vec<WeakDagRound>,
}

impl WeakDagRound {
    pub fn upgrade(&self) -> Option<DagRound> {
        self.0.upgrade().map(DagRound)
    }
}

impl DagRound {
    pub fn new_bottom(
        round: Round,
        triggers_tx: &mpsc::UnboundedSender<WeakDagPointFuture>,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Self {
        Self::new(
            round,
            WeakDagRound(Weak::new()),
            triggers_tx,
            peer_schedule,
            conf,
        )
    }

    pub fn new_next(
        &self,
        triggers_tx: &mpsc::UnboundedSender<WeakDagPointFuture>,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Self {
        Self::new(
            self.round().next(),
            self.downgrade(),
            triggers_tx,
            peer_schedule,
            conf,
        )
    }

    fn new(
        round: Round,
        prev: WeakDagRound,
        triggers_tx: &mpsc::UnboundedSender<WeakDagPointFuture>,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Self {
        let (peers, proof_leader) = {
            let guard = peer_schedule.atomic();
            let peers = guard.peers_for(round).clone();
            (peers, ProofLeader::new(round, &guard, conf))
        };

        let peer_count = if round > conf.genesis_round {
            PeerCount::try_from(peers.len()).unwrap_or_else(|e| panic!("{e} for {round:?}"))
        } else if round >= conf.genesis_round.prev() {
            assert_eq!(peers.len(), 1, "genesis round must have one peer");
            PeerCount::GENESIS
        } else {
            panic!(
                "Coding error: DAG round {} not allowed before genesis",
                round.0
            )
        };

        let prevs = match prev.upgrade() {
            None => {
                let prev_count = NodeConfig::get().weak_dag_round_links.get() as usize;
                vec![WeakDagRound(Weak::default()); prev_count]
            }
            Some(prev_round) => {
                let prev_count = prev_round.0.prevs.len();
                let mut prevs = Vec::with_capacity(prev_count);
                prevs.push(prev);
                for weak in &prev_round.0.prevs[..prev_count - 1] {
                    prevs.push(weak.clone());
                }
                assert_eq!(prevs.len(), prev_count, "same length for all prev links");
                prevs
            }
        };

        let this = Self(Arc::new(DagRoundInner {
            round,
            peer_count,
            proof_leader: proof_leader.finish(),
            used_anchor_proof: OnceLock::new(),
            locations: FastDashMap::with_capacity_and_hasher(peers.len(), Default::default()),
            threshold: Threshold::new(round, peer_count, conf),
            triggers_tx: triggers_tx.clone(),
            prevs,
        }));

        for peer in &*peers {
            (this.0.locations).insert(*peer, DagLocation::new(this.downgrade()));
        }

        this
    }

    pub fn round(&self) -> Round {
        self.0.round
    }

    pub fn peer_count(&self) -> PeerCount {
        self.0.peer_count
    }

    pub fn leader(&self) -> Option<&PeerId> {
        self.0.proof_leader.as_ref()
    }

    pub fn used_anchor_proof(&self) -> &OnceLock<PeerId> {
        &self.0.used_anchor_proof
    }

    pub fn threshold(&self) -> &Threshold {
        &self.0.threshold
    }

    fn edit<F, R>(&self, author: &PeerId, edit: F) -> R
    where
        F: FnOnce(&mut DagLocation) -> R,
    {
        match self.0.locations.get_mut(author) {
            Some(mut loc) => edit(loc.value_mut()),
            None => panic!(
                "DAG must not contain location {} @ {}",
                author.alt(),
                self.round().0
            ),
        }
    }

    pub fn view<F, R>(&self, author: &PeerId, view: F) -> Option<R>
    where
        F: FnOnce(&DagLocation) -> R,
    {
        self.0.locations.view(author, |_, v| view(v))
    }

    #[must_use = "iterators are lazy and do nothing unless consumed"]
    pub fn select<'a, F, R>(&'a self, mut filter_map: F) -> impl Iterator<Item = R> + 'a
    where
        F: FnMut((&PeerId, &DagLocation)) -> Option<R> + 'a,
    {
        self.0
            .locations
            .iter()
            .filter_map(move |a| filter_map(a.pair()))
    }

    pub fn prev(&self) -> &WeakDagRound {
        &self.0.prevs[0]
    }

    pub fn downgrade(&self) -> WeakDagRound {
        WeakDagRound(Arc::downgrade(&self.0))
    }

    pub fn into_inner(self) -> Option<DagRoundInner> {
        Arc::into_inner(self.0)
    }

    /// for locally produced points
    #[must_use = "await for point to resolve in dag"]
    pub fn add_local(
        &self,
        point: &Point,
        key_pair: Option<Arc<KeyPair>>,
        downloader: Downloader,
        store: MempoolStore,
        round_ctx: &RoundCtx,
    ) -> DagPointFuture {
        assert_eq!(
            point.info().round(),
            self.round(),
            "Coding error: point round does not match dag round"
        );
        let mut is_unique = true;
        let dag_point_future = self.edit(point.info().author(), |loc| {
            loc.versions
                .entry(*point.info().digest())
                .and_modify(|_| is_unique = false)
                .or_insert_with(|| {
                    DagPointFuture::new_local_valid(
                        self,
                        point,
                        key_pair,
                        &loc.state,
                        &self.0.triggers_tx,
                        downloader,
                        store,
                        round_ctx,
                    )
                })
                .clone()
        });
        assert!(
            is_unique,
            "local point must be created only once. {:?}",
            point.info().id().alt()
        );
        dag_point_future
    }

    pub fn add_ill_formed_broadcast(
        &self,
        point: &Point,
        reason: &IllFormedReason,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        assert_eq!(
            point.info().round(),
            self.round(),
            "Coding error: point round does not match dag round"
        );
        self.edit(point.info().author(), |loc| {
            loc.versions
                .entry(*point.info().digest())
                .and_modify(|first| first.resolve_download(point, Some(reason)))
                .or_insert_with(|| {
                    DagPointFuture::new_ill_formed_broadcast(
                        point,
                        reason,
                        &loc.state,
                        &self.0.triggers_tx,
                        store,
                        round_ctx,
                    )
                });
        });
    }

    /// Point already verified
    pub fn add_broadcast(
        &self,
        point: &Point,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        let _guard = round_ctx.span().enter();
        assert_eq!(
            point.info().round(),
            self.round(),
            "Coding error: point round does not match dag round"
        );
        self.edit(point.info().author(), |loc| {
            loc.versions
                .entry(*point.info().digest())
                .and_modify(|first| first.resolve_download(point, None))
                .or_insert_with(|| {
                    DagPointFuture::new_broadcast(
                        self,
                        point,
                        &loc.state,
                        &self.0.triggers_tx,
                        downloader,
                        store,
                        round_ctx,
                    )
                });
        });
    }

    /// notice: `round` must exactly match point's round,
    /// otherwise dependency will resolve to [`DagPoint::NotFound`]
    pub fn add_pruned_broadcast(
        &self,
        author: &PeerId,
        digest: &Digest,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) {
        self.edit(author, |loc| {
            loc.versions.entry(*digest).or_insert_with(|| {
                DagPointFuture::new_download(
                    self,
                    author,
                    digest,
                    None,
                    &loc.state,
                    &self.0.triggers_tx,
                    downloader,
                    store,
                    round_ctx,
                )
            });
        });
    }

    /// notice: `round` must exactly match point's round,
    /// otherwise dependency will resolve to [`DagPoint::NotFound`]
    pub fn add_dependency(
        &self,
        author: &PeerId,
        digest: &Digest,
        depender: &PeerId,
        downloader: &Downloader,
        store: &MempoolStore,
        validate_ctx: &ValidateCtx,
    ) -> (WeakDagPointFuture, WeakCert) {
        // dependencies are more often read than written
        if let Some(loc) = self.0.locations.get(author)
            && let Some(first) = loc.versions.get(digest)
        {
            first.add_depender(depender);
            return (first.downgrade(), first.weak_cert());
        };
        self.edit(author, |loc| {
            let first = loc
                .versions
                .entry(*digest)
                .and_modify(|first| first.add_depender(depender))
                .or_insert_with(|| {
                    DagPointFuture::new_download(
                        self,
                        author,
                        digest,
                        Some(depender),
                        &loc.state,
                        &self.0.triggers_tx,
                        downloader,
                        store,
                        validate_ctx,
                    )
                });
            (first.downgrade(), first.weak_cert())
        })
    }

    pub fn restore(
        &self,
        point_restore: PointRestore,
        downloader: &Downloader,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> DagPointFuture {
        let _guard = round_ctx.span().enter();
        assert_eq!(
            point_restore.round(),
            self.round(),
            "Coding error: point restore round does not match dag round"
        );
        let point_id = point_restore.id();
        let mut is_unique = true;
        let dag_point_future = self.edit(&point_id.author, |loc| {
            loc.versions
                .entry(*point_restore.digest())
                .and_modify(|_| is_unique = false)
                .or_insert_with(|| {
                    DagPointFuture::new_restore(
                        self,
                        point_restore,
                        &loc.state,
                        &self.0.triggers_tx,
                        downloader,
                        store,
                        round_ctx,
                    )
                })
                .clone()
        });
        assert!(
            is_unique,
            "points must be restored only once: {:?}",
            point_id.alt()
        );
        dag_point_future
    }

    pub fn scan(&self, round: Round) -> Option<Self> {
        assert!(
            self.round() >= round,
            "Coding error: cannot scan DAG rounds chain for a future round"
        );
        if self.round() == round {
            return Some(self.clone());
        }

        let (hops, pos) = {
            let prev_count = self.0.prevs.len(); // same for all dag rounds
            let contiguous_pos = (self.round().prev().0)
                .checked_sub(round.0)
                .expect("self.round() > round");
            let hops = contiguous_pos / prev_count as u32;
            let pos = contiguous_pos as usize % prev_count;
            (hops, pos)
        };

        if hops == 0 {
            return self.0.prevs[pos].upgrade();
        }

        let mut last_hop_to = self.0.prevs.last().expect("prevs non empty").upgrade()?;
        for _ in 1..hops {
            last_hop_to = (last_hop_to.0.prevs.last().expect("prevs non empty")).upgrade()?;
        }
        let found = last_hop_to.0.prevs[pos].upgrade()?;

        assert_eq!(
            found.round(),
            round,
            "linked list of dag rounds cannot contain gaps, \
             found {} instead of {}, scanned from {}",
            found.round().0,
            round.0,
            self.round().0
        );

        Some(found)
    }
}

impl AltFormat for DagRound {}
impl std::fmt::Debug for AltFmt<'_, DagRound> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(f, "{}[", inner.round().0)?;
        for result in inner.select(|(peer, loc)| Some(write!(f, "{}{:?}", peer.alt(), loc.alt()))) {
            result?;
        }
        write!(f, "]")?;
        Ok(())
    }
}
