use std::cmp;
use std::sync::{Arc, Weak};

use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::dag::IllFormedReason;
use crate::dag::anchor_stage::AnchorStage;
use crate::dag::dag_location::DagLocation;
use crate::dag::dag_point_future::{DagPointFuture, WeakDagPointFuture};
use crate::dag::threshold::Threshold;
use crate::effects::{AltFmt, AltFormat, Ctx, RoundCtx, ValidateCtx};
use crate::engine::MempoolConfig;
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

struct DagRoundInner {
    round: Round,
    peer_count: PeerCount,
    anchor_stage: Option<AnchorStage>,
    locations: FastDashMap<PeerId, DagLocation>,
    threshold: Threshold,
    prev: WeakDagRound,
}

impl WeakDagRound {
    pub fn upgrade(&self) -> Option<DagRound> {
        self.0.upgrade().map(DagRound)
    }
}

impl DagRound {
    pub fn new_bottom(round: Round, peer_schedule: &PeerSchedule, conf: &MempoolConfig) -> Self {
        Self::new(round, peer_schedule, WeakDagRound(Weak::new()), conf)
    }

    pub fn new_next(&self, peer_schedule: &PeerSchedule, conf: &MempoolConfig) -> Self {
        Self::new(self.round().next(), peer_schedule, self.downgrade(), conf)
    }

    fn new(
        round: Round,
        peer_schedule: &PeerSchedule,
        prev: WeakDagRound,
        conf: &MempoolConfig,
    ) -> Self {
        let peers = peer_schedule.atomic().peers_for(round).clone();

        let peer_count = if round > conf.genesis_round {
            PeerCount::try_from(peers.len()).unwrap_or_else(|e| panic!("{e} for {round:?}"))
        } else if round >= conf.genesis_round.prev() {
            PeerCount::GENESIS
        } else {
            panic!(
                "Coding error: DAG round {} not allowed before genesis",
                round.0
            )
        };
        let this = Self(Arc::new(DagRoundInner {
            round,
            peer_count,
            anchor_stage: AnchorStage::of(round, peer_schedule, conf),
            locations: FastDashMap::with_capacity_and_hasher(peers.len(), Default::default()),
            threshold: Threshold::new(round, peer_count, conf),
            prev,
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

    pub fn anchor_stage(&self) -> Option<&AnchorStage> {
        self.0.anchor_stage.as_ref()
    }

    pub fn threshold(&self) -> &Threshold {
        &self.0.threshold
    }

    #[cfg(any(feature = "test", test))]
    pub fn locations(&self) -> &FastDashMap<PeerId, DagLocation> {
        &self.0.locations
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
        &self.0.prev
    }

    pub fn downgrade(&self) -> WeakDagRound {
        WeakDagRound(Arc::downgrade(&self.0))
    }

    /// for locally produced points
    #[must_use = "await for point to resolve in dag"]
    pub fn add_local(
        &self,
        point: &Point,
        key_pair: Option<&Arc<KeyPair>>,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> DagPointFuture {
        assert_eq!(
            point.info().round(),
            self.round(),
            "Coding error: point round does not match dag round"
        );
        self.edit(point.info().author(), |loc| {
            loc.versions
                .entry(*point.info().digest())
                .and_modify(|_| {
                    panic!(
                        "local point must be created only once. {:?}",
                        point.info().id().alt()
                    )
                })
                .or_insert_with(|| {
                    DagPointFuture::new_local_valid(point, &loc.state, store, key_pair, round_ctx)
                })
                .clone()
        })
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
                        point, reason, &loc.state, store, round_ctx,
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
                        self, point, &loc.state, downloader, store, round_ctx,
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
                    self, author, digest, None, &loc.state, downloader, store, round_ctx,
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
        let author = *point_restore.author();
        self.edit(&author, |loc| {
            loc.versions
                .entry(*point_restore.digest())
                .and_modify(|_| {
                    panic!(
                        "points must be restored only once. {:?}",
                        point_restore.alt()
                    )
                })
                .or_insert_with(|| {
                    DagPointFuture::new_restore(
                        self,
                        point_restore,
                        &loc.state,
                        downloader,
                        store,
                        round_ctx,
                    )
                })
                .clone()
        })
    }

    pub fn scan(&self, round: Round) -> Option<Self> {
        assert!(
            round <= self.round(),
            "Coding error: cannot scan DAG rounds chain for a future round"
        );
        let mut visited = self.clone();
        if visited.round() == round {
            return Some(visited);
        }
        while let Some(dag_round) = visited.prev().upgrade() {
            match dag_round.round().cmp(&round) {
                cmp::Ordering::Less => panic!(
                    "Coding error: linked list of dag rounds cannot contain gaps, \
                    found {} to be prev for {}, scanned for {} from {}",
                    dag_round.round().0,
                    visited.round().0,
                    round.0,
                    self.round().0
                ),
                cmp::Ordering::Equal => return Some(dag_round),
                cmp::Ordering::Greater => visited = dag_round,
            }
        }
        None
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
