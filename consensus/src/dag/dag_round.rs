use std::sync::{Arc, Weak};

use everscale_crypto::ed25519::KeyPair;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tracing::Span;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::dag_location::{DagLocation, InclusionState};
use crate::dag::dag_point_future::DagPointFuture;
use crate::effects::{Effects, EngineContext, ValidateContext};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{DagPoint, Digest, PeerCount, Point, Round, ValidPoint};

#[derive(Clone)]
/// Allows memory allocated by DAG to be freed
pub struct WeakDagRound(Weak<DagRoundInner>);

#[derive(Clone)]
/// do not pass to backwards-recursive async tasks
/// (where `DAG_DEPTH` is just a logical limit, but is not explicitly applicable)
/// to prevent severe memory leaks of a whole DAG round
/// (in case congested tokio runtime reorders futures), use [`WeakDagRound`] for that
pub struct DagRound(Arc<DagRoundInner>);

struct DagRoundInner {
    round: Round,          // immutable
    peer_count: PeerCount, // immutable
    /// if `key_pair` is not empty, then the node may produce block at this round,
    /// and also sign broadcasts during previous round
    key_pair: Option<Arc<KeyPair>>, // immutable
    anchor_stage: Option<AnchorStage>, // immutable
    locations: FastDashMap<PeerId, DagLocation>,
    prev: WeakDagRound, // immutable ?
}

impl WeakDagRound {
    const BOTTOM: Self = WeakDagRound(Weak::new());
    pub fn upgrade(&self) -> Option<DagRound> {
        self.0.upgrade().map(DagRound)
    }
}

impl DagRound {
    pub fn next(&self, peer_schedule: &PeerSchedule) -> Self {
        let next_round = self.round().next();
        let (peers_len, key_pair) = {
            let guard = peer_schedule.atomic();
            let peers_len = guard.peers_for(next_round).len();
            (peers_len, guard.local_keys(next_round))
        };
        let locations = FastDashMap::with_capacity_and_hasher(peers_len, Default::default());
        Self(Arc::new(DagRoundInner {
            round: next_round,
            peer_count: PeerCount::try_from(peers_len)
                .unwrap_or_else(|e| panic!("{e} for {next_round:?}")),
            key_pair,
            anchor_stage: AnchorStage::of(next_round, peer_schedule),
            locations,
            prev: self.downgrade(),
        }))
    }

    pub fn genesis(genesis: &Point, peer_schedule: &PeerSchedule) -> Self {
        let locations = FastDashMap::with_capacity_and_hasher(1, Default::default());
        let round = genesis.body().location.round;
        Self(Arc::new(DagRoundInner {
            round,
            peer_count: PeerCount::GENESIS,
            key_pair: None,
            anchor_stage: AnchorStage::of(round, peer_schedule),
            locations,
            prev: WeakDagRound::BOTTOM,
        }))
    }

    pub fn round(&self) -> Round {
        self.0.round
    }

    pub fn peer_count(&self) -> PeerCount {
        self.0.peer_count
    }

    pub fn key_pair(&self) -> Option<&'_ KeyPair> {
        self.0.key_pair.as_deref()
    }

    pub fn anchor_stage(&self) -> Option<&'_ AnchorStage> {
        self.0.anchor_stage.as_ref()
    }

    fn edit<F, R>(&self, author: &PeerId, edit: F) -> R
    where
        F: FnOnce(&mut DagLocation) -> R,
    {
        let mut loc = self.0.locations.entry(*author).or_default();
        edit(loc.value_mut())
    }

    pub fn view<F, R>(&self, author: &PeerId, view: F) -> Option<R>
    where
        F: FnOnce(&DagLocation) -> R,
    {
        self.0.locations.view(author, |_, v| view(v))
    }

    pub fn select<'a, F, R>(&'a self, mut filter_map: F) -> impl Iterator<Item = R> + 'a
    where
        F: FnMut((&PeerId, &DagLocation)) -> Option<R> + 'a,
    {
        self.0
            .locations
            .iter()
            .filter_map(move |a| filter_map(a.pair()))
    }

    pub fn prev(&self) -> &'_ WeakDagRound {
        &self.0.prev
    }

    pub fn downgrade(&self) -> WeakDagRound {
        WeakDagRound(Arc::downgrade(&self.0))
    }

    /// Point already verified
    pub fn add_broadcast_exact(
        &self,
        point: &Point,
        downloader: &Downloader,
        effects: &Effects<EngineContext>,
    ) -> Option<BoxFuture<'static, InclusionState>> {
        let _guard = effects.span().enter();
        assert_eq!(
            point.body().location.round,
            self.round(),
            "Coding error: point round does not match dag round"
        );
        let digest = point.digest();
        self.edit(&point.body().location.author, |loc| {
            let result_state = loc.state().clone();
            loc.init_or_modify(
                digest,
                // FIXME: prior Responder refactor: could not sign during validation,
                //   because current DAG round could advance concurrently;
                //   now current dag round changes consistently,
                //   maybe its possible to reduce locking in 'inclusion state'
                |state| DagPointFuture::new_broadcast(self, point, state, downloader, effects),
                |existing| existing.resolve_download(point),
            )
            .map(|first| first.clone().map(|_| result_state).boxed())
        })
    }

    /// notice: `round` must exactly match point's round,
    /// otherwise dependency will resolve to [`DagPoint::NotExists`]
    pub fn add_dependency_exact(
        &self,
        author: &PeerId,
        digest: &Digest,
        depender: &PeerId,
        downloader: &Downloader,
        effects: &Effects<ValidateContext>,
    ) -> DagPointFuture {
        let future = self.edit(author, |loc| {
            loc.get_or_init(digest, |state| {
                DagPointFuture::new_download(self, author, digest, state, downloader, effects)
            })
            .clone()
        });
        future.add_depender(depender);
        future
    }

    /// for genesis and own points
    pub fn insert_exact_sign(
        &self,
        point: &Point,
        peer_schedule: &PeerSchedule,
        span: &Span,
    ) -> InclusionState {
        let state = self.insert_exact(
            &point.body().location.author,
            &DagPoint::Trusted(ValidPoint::new(point.clone())),
        );
        if let Some(signable) = state.signable() {
            let key_pair = peer_schedule.atomic().local_keys(self.round().next());
            signable.sign(
                self.round(),
                key_pair.as_deref(),
                MempoolConfig::sign_time_range(),
            );
        }
        let _guard = span.enter();
        assert!(
            state.signed_point(self.round()).is_some(),
            "Coding or configuration error: valid point cannot be signed; time issue?"
        );
        state
    }

    pub fn insert_invalid_exact(&self, sender: &PeerId, dag_point: &DagPoint) {
        assert!(
            dag_point.valid().is_none(),
            "Coding error: failed to insert valid point as invalid"
        );
        self.insert_exact(sender, dag_point);
    }

    fn insert_exact(&self, sender: &PeerId, dag_point: &DagPoint) -> InclusionState {
        assert_eq!(
            dag_point.location().round,
            self.round(),
            "Coding error: dag round mismatches point round on insert"
        );
        self.edit(sender, |loc| {
            let _ready = loc.init_or_modify(
                dag_point.digest(),
                |state| DagPointFuture::new_local(dag_point, state),
                |_fut| {},
            );
            loc.state().clone()
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
                core::cmp::Ordering::Less => panic!(
                    "Coding error: linked list of dag rounds cannot contain gaps, \
                    found {} to be prev for {}, scanned for {} from {}",
                    dag_round.round().0,
                    visited.round().0,
                    round.0,
                    self.round().0
                ),
                core::cmp::Ordering::Equal => return Some(dag_round),
                core::cmp::Ordering::Greater => visited = dag_round,
            }
        }
        None
    }
}
