use std::cmp;
use ahash::HashMapExt;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_util::FastHashMap;
use tycho_util::metrics::HistogramGuard;
use crate::dag::dag_location::DagLocation;
use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::dag::{AnchorStage, DagRound, WeakDagRound};
use crate::effects::{AltFormat, Ctx, TaskResult, ValidateCtx};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{
    AnchorStageRole, Cert, CertDirectDeps, DagPoint, Digest, Link, PeerCount, PointId,
    PointInfo, Round, UnixTime,
};
use crate::storage::MempoolStore;
pub struct Verifier;
#[derive(Debug, Copy, Clone)]
pub enum PointMap {
    Evidence,
    Includes,
    Witness,
}
#[derive(thiserror::Error, Debug)]
pub enum VerifyError {
    #[error("cannot verify: {0}")]
    Fail(VerifyFailReason),
    #[error("ill-formed: {0}")]
    IllFormed(IllFormedReason),
}
#[derive(Debug)]
pub enum ValidateResult {
    IllFormed(IllFormedReason),
    Invalid(InvalidReason),
    Valid,
}
#[derive(thiserror::Error, Debug)]
pub enum VerifyFailReason {
    #[error("uninit {:?} peer set of {} len at round {}", .0.2, .0.0, .0.1.0)]
    Uninit((usize, Round, PointMap)),
    #[error("author is not scheduled: outdated peer schedule or author out of nowhere")]
    UnknownAuthor,
}
#[derive(thiserror::Error, Debug, Clone)]
pub enum IllFormedReason {
    #[error("ill-formed after load from DB")]
    AfterLoadFromDb,
    #[error("point before genesis cannot exist in this overlay")]
    BeforeGenesis,
    #[error("too large payload: {0} bytes")]
    TooLargePayload(u32),
    #[error("links anchor across genesis")]
    LinksAcrossGenesis,
    #[error("links both anchor roles to same round")]
    LinksSameRound,
    #[error("{0:?} peer map must be empty")]
    MustBeEmpty(PointMap),
    #[error("unknown peers in {:?} map: {}", .0.1, .0.0.as_slice().alt())]
    UnknownPeers((Vec<PeerId>, PointMap)),
    #[error("{} peers is not enough in {:?} map for 3F+1={}", .0.0, .0.2, .0.1.full())]
    LackOfPeers((usize, PeerCount, PointMap)),
    #[error("anchor time")]
    AnchorTime,
    #[error("anchor stage role {0:?}")]
    SelfAnchorStage(AnchorStageRole),
    #[error("self link")]
    SelfLink,
    #[error("anchor link to {0:?}")]
    AnchorLink(AnchorStageRole),
}
#[derive(thiserror::Error, Debug)]
pub enum InvalidReason {
    #[error("invalid after load from DB")]
    AfterLoadFromDb,
    #[error("cannot validate point, no {0:?} round in DAG")]
    NoRoundInDag(PointMap),
    #[error("cannot validate point, dependency was dropped with its round")]
    DependencyRoundDropped,
    #[error("time is not greater than in prev point {:?}", .0.alt())]
    TimeNotGreaterThanInPrevPoint(PointId),
    #[error("anchor proof does not inherit time from its candidate {:?}", .0.alt())]
    AnchorProofDoesntInheritAnchorTime(PointId),
    #[error("anchor candidate's time is not inherited from its proof {:?}", .0.alt())]
    AnchorTimeNotInheritedFromProof(PointId),
    #[error("must have referenced prev point {:?}", .0.alt())]
    MustHaveReferencedPrevPoint(PointId),
    #[error("must have skipped round after {:?}", .0.alt())]
    MustHaveSkippedRound(PointId),
    #[error("invalid dependency {:?}", .0.alt())]
    InvalidDependency(PointId),
    #[error("dependency time too far in future: {:?}", .0.alt())]
    DependencyTimeTooFarInFuture(PointId),
    #[error("newer anchor {:?} in dependency {:?}", .0.0, .0.1.alt())]
    NewerAnchorInDependency((AnchorStageRole, PointId)),
    #[error(
        "anchor {:?} link leads to other destination through {:?}",
        .0.0,
        .0.1.alt()
    )]
    AnchorLinkBadPath((AnchorStageRole, PointId)),
}
impl Verifier {
    /// the first and mandatory check of any Point received no matter where from
    pub fn verify(
        info: &PointInfo,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Result<(), VerifyError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_verifier_verify_time");
        let result = Self::verify_impl(info, peer_schedule, conf).map_or(Ok(()), Err);
        ValidateCtx::verified(&result);
        result
    }
    /// must be called iff [`Self::verify`] succeeded
    ///
    /// We do not require the whole `Point` to avoid OOM as during sync Dag can grow large.
    pub async fn validate(
        info: PointInfo,
        r_0: WeakDagRound,
        downloader: Downloader,
        store: MempoolStore,
        cert: Cert,
        ctx: ValidateCtx,
    ) -> TaskResult<ValidateResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(validate)),
            file!(),
            150u32,
        );
        let info = info;
        let r_0 = r_0;
        let downloader = downloader;
        let store = store;
        let cert = cert;
        let ctx = ctx;
        let _task_duration = HistogramGuard::begin(
            "tycho_mempool_verifier_validate_time",
        );
        let entered_span = ctx.span().clone().entered();
        let peer_schedule = downloader.peer_schedule();
        match info.round().cmp(&ctx.conf().genesis_round) {
            cmp::Ordering::Less => {
                panic!("Coding error: can only validate points older than genesis")
            }
            cmp::Ordering::Equal => {
                {
                    __guard.end_section(163u32);
                    return ctx.validated(&cert, ValidateResult::Valid);
                };
            }
            cmp::Ordering::Greater => {}
        }
        let Some(r_0) = r_0.upgrade() else {
            if let Some(reason) = Self::check_links(
                &info,
                None,
                peer_schedule,
                ctx.conf(),
            ) {
                {
                    __guard.end_section(171u32);
                    return ctx.validated(&cert, ValidateResult::IllFormed(reason));
                };
            }
            let reason = InvalidReason::NoRoundInDag(PointMap::Evidence);
            {
                __guard.end_section(174u32);
                return ctx.validated(&cert, ValidateResult::Invalid(reason));
            };
        };
        assert_eq!(
            r_0.round(), info.round(), "Coding error: dag round mismatches point round"
        );
        if let Some(reason) = Self::check_links(
            &info,
            Some(&r_0),
            peer_schedule,
            ctx.conf(),
        ) {
            {
                __guard.end_section(183u32);
                return ctx.validated(&cert, ValidateResult::IllFormed(reason));
            };
        }
        let Some(r_1) = r_0.prev().upgrade() else {
            let reason = InvalidReason::NoRoundInDag(PointMap::Includes);
            {
                __guard.end_section(188u32);
                return ctx.validated(&cert, ValidateResult::Invalid(reason));
            };
        };
        let r_2_opt = r_1.prev().upgrade();
        let (mut deps_and_prev, cert_deps) = Self::spawn_direct_deps(
            &info,
            &r_1,
            r_2_opt.as_ref(),
            &downloader,
            &store,
            &ctx,
        );
        if let Some(prev_digest) = info.prev_digest() {
            let weak_cert = (cert_deps.includes.get(prev_digest))
                .expect("prev cert must be included");
            let cert = weak_cert.upgrade().expect("we hold a strong ref to round");
            cert.certify();
        }
        cert.set_deps(cert_deps);
        if r_2_opt.is_none() && !info.witness().is_empty() {
            let reason = InvalidReason::NoRoundInDag(PointMap::Witness);
            {
                __guard.end_section(207u32);
                return ctx.validated(&cert, ValidateResult::Invalid(reason));
            };
        }
        deps_and_prev
            .extend(
                r_1
                    .view(
                        info.author(),
                        |loc| { Self::other_versions(loc, info.prev_digest()) },
                    )
                    .unwrap_or_default(),
            );
        let is_valid_fut = Self::is_valid(info, deps_and_prev, ctx.conf())
            .instrument(entered_span.exit());
        drop(r_0);
        drop(r_1);
        drop(r_2_opt);
        ctx.validated(
            &cert,
            match {
                __guard.end_section(227u32);
                let __result = is_valid_fut.await;
                __guard.start_section(227u32);
                __result
            }? {
                Some(reason) => ValidateResult::Invalid(reason),
                None => ValidateResult::Valid,
            },
        )
    }
    fn check_links(
        info: &PointInfo,
        point_round: Option<&DagRound>,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Option<IllFormedReason> {
        let is_self_link_ok = match point_round
            .ok_or_else(|| AnchorStage::of(info.round(), peer_schedule, conf))
            .as_ref()
            .map_or_else(|fallback| fallback.as_ref(), |round| round.anchor_stage())
        {
            Some(stage) if stage.leader == info.author() => {
                if stage.role == AnchorStageRole::Proof
                    || info.anchor_round(AnchorStageRole::Proof) == info.round().prev()
                {
                    info.prev_digest().is_some()
                        == (info.anchor_link(stage.role) == &Link::ToSelf)
                } else {
                    info.anchor_link(stage.role) != &Link::ToSelf
                }
            }
            Some(_) | None => {
                info.anchor_proof() != &Link::ToSelf
                    && info.anchor_trigger() != &Link::ToSelf
            }
        };
        if !is_self_link_ok {
            return Some(IllFormedReason::SelfLink);
        }
        for link_field in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            let linked_id = info.anchor_id(link_field);
            if linked_id.round == conf.genesis_round {
                continue;
            }
            let is_ok = match point_round.and_then(|dr| dr.scan(linked_id.round)) {
                Some(linked_round) => {
                    linked_round
                        .anchor_stage()
                        .is_some_and(|stage| {
                            stage.role == link_field && stage.leader == linked_id.author
                        })
                }
                None => {
                    AnchorStage::of(linked_id.round, peer_schedule, conf)
                        .is_some_and(|stage| {
                            stage.role == link_field && stage.leader == linked_id.author
                        })
                }
            };
            if !is_ok {
                return Some(IllFormedReason::AnchorLink(link_field));
            }
        }
        None
    }
    fn other_versions(
        dag_location: &DagLocation,
        excluded: Option<&Digest>,
    ) -> Vec<WeakDagPointFuture> {
        let mut others = Vec::with_capacity(
            (dag_location.versions.len()).saturating_sub(excluded.is_some() as usize),
        );
        for (digest, shared) in &dag_location.versions {
            if excluded != Some(digest) {
                others.push(shared.downgrade());
            }
        }
        others
    }
    fn spawn_direct_deps(
        info: &PointInfo,
        r_1: &DagRound,
        r_2_opt: Option<&DagRound>,
        downloader: &Downloader,
        store: &MempoolStore,
        ctx: &ValidateCtx,
    ) -> (FuturesUnordered<WeakDagPointFuture>, CertDirectDeps) {
        let direct_deps = FuturesUnordered::new();
        let mut cert_deps = CertDirectDeps {
            includes: FastHashMap::with_capacity(r_1.peer_count().full()),
            witness: FastHashMap::with_capacity(r_1.peer_count().full()),
        };
        let includes = (info.includes().iter())
            .map(|(author, digest)| (r_1, true, author, digest));
        let witness = r_2_opt
            .into_iter()
            .flat_map(|r_2| {
                (info.witness().iter())
                    .map(move |(author, digest)| (r_2, false, author, digest))
            });
        for (dag_round, is_includes, author, digest) in includes.chain(witness) {
            let (dep, cert) = dag_round
                .add_dependency(author, digest, info.author(), downloader, store, ctx);
            if is_includes {
                cert_deps.includes.insert(*digest, cert);
            } else {
                cert_deps.witness.insert(*digest, cert);
            }
            direct_deps.push(dep);
        }
        (direct_deps, cert_deps)
    }
    /// check only direct dependencies and location for previous point (let it jump over round)
    async fn is_valid(
        info: PointInfo,
        mut deps_and_prev: FuturesUnordered<WeakDagPointFuture>,
        conf: &MempoolConfig,
    ) -> TaskResult<Option<InvalidReason>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(is_valid)),
            file!(),
            352u32,
        );
        let info = info;
        let mut deps_and_prev = deps_and_prev;
        let conf = conf;
        let prev_digest_in_point = info.prev_digest();
        let prev_round = info.round().prev();
        let anchor_trigger_id = info.anchor_id(AnchorStageRole::Trigger);
        let anchor_proof_id = info.anchor_id(AnchorStageRole::Proof);
        let anchor_trigger_link_id = info.anchor_link_id(AnchorStageRole::Trigger);
        let anchor_proof_link_id = info.anchor_link_id(AnchorStageRole::Proof);
        let max_allowed_dep_time = info.time()
            + UnixTime::from_millis(conf.consensus.clock_skew_millis.get() as _);
        while let Some(task_result) = {
            __guard.end_section(370u32);
            let __result = deps_and_prev.next().await;
            __guard.start_section(370u32);
            __result
        } {
            __guard.checkpoint(370u32);
            let Some(dag_point) = task_result? else {
                {
                    __guard.end_section(372u32);
                    return Ok(Some(InvalidReason::DependencyRoundDropped));
                };
            };
            if dag_point.round() == prev_round && dag_point.author() == info.author() {
                match prev_digest_in_point {
                    Some(
                        prev_digest_in_point,
                    ) if prev_digest_in_point == dag_point.digest() => {
                        assert!(
                            dag_point.is_certified(),
                            "prev point was not marked as certified, Cert is broken"
                        );
                        let Some(proven) = dag_point.trusted() else {
                            {
                                __guard.end_section(384u32);
                                return Ok(
                                    Some(InvalidReason::InvalidDependency(dag_point.id())),
                                );
                            };
                        };
                        if let Some(reason) = Self::is_proof_ok(&info, proven) {
                            {
                                __guard.end_section(387u32);
                                return Ok(Some(reason));
                            };
                        }
                    }
                    Some(_) | None => {
                        match dag_point {
                            DagPoint::Valid(valid) => {
                                if valid.is_first_valid() {
                                    {
                                        __guard.end_section(395u32);
                                        return Ok(
                                            Some(
                                                InvalidReason::MustHaveReferencedPrevPoint(
                                                    valid.info().id(),
                                                ),
                                            ),
                                        );
                                    };
                                }
                            }
                            bad @ (DagPoint::Invalid(_) | DagPoint::IllFormed(_)) => {
                                {
                                    __guard.end_section(401u32);
                                    return Ok(
                                        Some(InvalidReason::MustHaveSkippedRound(bad.id())),
                                    );
                                };
                            }
                            DagPoint::NotFound(not_found) => {
                                if not_found.is_certified() {
                                    {
                                        __guard.end_section(405u32);
                                        return Ok(
                                            Some(
                                                InvalidReason::MustHaveReferencedPrevPoint(*not_found.id()),
                                            ),
                                        );
                                    };
                                }
                            }
                        }
                    }
                }
            } else {
                let Some(dep) = dag_point.trusted() else {
                    {
                        __guard.end_section(415u32);
                        return Ok(
                            Some(InvalidReason::InvalidDependency(dag_point.id())),
                        );
                    };
                };
                if dep.time() > max_allowed_dep_time {
                    {
                        __guard.end_section(419u32);
                        return Ok(
                            Some(InvalidReason::DependencyTimeTooFarInFuture(dep.id())),
                        );
                    };
                }
                for (anchor_role, anchor_role_round) in [
                    (AnchorStageRole::Trigger, anchor_trigger_id.round),
                    (AnchorStageRole::Proof, anchor_proof_id.round),
                ] {
                    __guard.checkpoint(421u32);
                    if dep.anchor_round(anchor_role) > anchor_role_round {
                        let tuple = (anchor_role, dep.id());
                        {
                            __guard.end_section(427u32);
                            return Ok(
                                Some(InvalidReason::NewerAnchorInDependency(tuple)),
                            );
                        };
                    }
                }
                let dep_id = dep.id();
                if dep_id == anchor_trigger_link_id
                    && dep.anchor_id(AnchorStageRole::Trigger) != anchor_trigger_id
                {
                    let tuple = (AnchorStageRole::Trigger, dep_id);
                    {
                        __guard.end_section(436u32);
                        return Ok(Some(InvalidReason::AnchorLinkBadPath(tuple)));
                    };
                }
                if dep_id == anchor_proof_link_id {
                    if dep.anchor_id(AnchorStageRole::Proof) != anchor_proof_id {
                        let tuple = (AnchorStageRole::Proof, dep_id);
                        {
                            __guard.end_section(441u32);
                            return Ok(Some(InvalidReason::AnchorLinkBadPath(tuple)));
                        };
                    }
                    if dep.anchor_time() != info.anchor_time() {
                        {
                            __guard.end_section(445u32);
                            return Ok(
                                Some(InvalidReason::AnchorTimeNotInheritedFromProof(dep_id)),
                            );
                        };
                    }
                }
            }
        }
        Ok(None)
    }
    /// blame author and every dependent point's author
    fn verify_impl(
        info: &PointInfo,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Option<VerifyError> {
        fn peer_count_genesis(len: usize, round: Round) -> Result<PeerCount, Round> {
            if len == PeerCount::GENESIS.full() {
                Ok(PeerCount::GENESIS)
            } else {
                Err(round)
            }
        }
        let (same_round_peers, includes_peers, witness_peers) = match (info.round()
            - conf.genesis_round.prev().0)
            .0
        {
            0 => return Some(VerifyError::IllFormed(IllFormedReason::BeforeGenesis)),
            1 => {
                let a = peer_schedule.atomic().peers_for(info.round()).clone();
                ((peer_count_genesis(a.len(), info.round()), a), None, None)
            }
            2 => {
                let rounds = [info.round(), info.round().prev()];
                let [a, b] = peer_schedule.atomic().peers_for_array(rounds);
                (
                    (PeerCount::try_from(a.len()).map_err(|_e| rounds[0]), a),
                    Some((peer_count_genesis(b.len(), rounds[1]), b)),
                    None,
                )
            }
            more => {
                let rounds = [
                    info.round(),
                    info.round().prev(),
                    info.round().prev().prev(),
                ];
                let [a, b, c] = peer_schedule.atomic().peers_for_array(rounds);
                let peer_count_c = if more == 3 {
                    peer_count_genesis(c.len(), rounds[2])
                } else {
                    PeerCount::try_from(c.len()).map_err(|_e| rounds[2])
                };
                (
                    (PeerCount::try_from(a.len()).map_err(|_e| rounds[0]), a),
                    Some((PeerCount::try_from(b.len()).map_err(|_e| rounds[1]), b)),
                    Some((peer_count_c, c)),
                )
            }
        };
        if let Some(reason) = Self::links_across_genesis(info, conf) {
            return Some(VerifyError::IllFormed(reason));
        }
        if let Some(reason) = Self::is_well_formed(info, conf) {
            return Some(VerifyError::IllFormed(reason));
        }
        match same_round_peers {
            (Err(round), scheduled) => {
                let len = scheduled.len();
                let reason = VerifyFailReason::Uninit((len, round, PointMap::Evidence));
                return Some(VerifyError::Fail(reason));
            }
            (Ok(total), scheduled) => {
                if !scheduled.contains(info.author()) {
                    let reason = VerifyFailReason::UnknownAuthor;
                    return Some(VerifyError::Fail(reason));
                }
                let evidence = &info.evidence();
                if !evidence.is_empty() {
                    if total == PeerCount::GENESIS {
                        let reason = IllFormedReason::MustBeEmpty(PointMap::Evidence);
                        return Some(VerifyError::IllFormed(reason));
                    }
                    let unknown = evidence
                        .keys()
                        .filter(|id| !scheduled.contains(id))
                        .copied()
                        .collect::<Vec<_>>();
                    if !unknown.is_empty() {
                        let reason = IllFormedReason::UnknownPeers((
                            unknown,
                            PointMap::Evidence,
                        ));
                        return Some(VerifyError::IllFormed(reason));
                    }
                    let len = evidence.len();
                    if len < total.majority_of_others() {
                        let reason = IllFormedReason::LackOfPeers((
                            len,
                            total,
                            PointMap::Evidence,
                        ));
                        return Some(VerifyError::IllFormed(reason));
                    }
                }
            }
        }
        match includes_peers {
            Some((Err(round), scheduled)) => {
                let len = scheduled.len();
                let reason = VerifyFailReason::Uninit((len, round, PointMap::Includes));
                return Some(VerifyError::Fail(reason));
            }
            None => {
                if !info.includes().is_empty() {
                    let reason = IllFormedReason::MustBeEmpty(PointMap::Includes);
                    return Some(VerifyError::IllFormed(reason));
                }
            }
            Some((Ok(total), scheduled)) => {
                let includes = &info.includes();
                let unknown = includes
                    .keys()
                    .filter(|id| !scheduled.contains(id))
                    .copied()
                    .collect::<Vec<_>>();
                if !unknown.is_empty() {
                    let reason = IllFormedReason::UnknownPeers((
                        unknown,
                        PointMap::Includes,
                    ));
                    return Some(VerifyError::IllFormed(reason));
                }
                let len = includes.len();
                if len < total.majority() {
                    let reason = IllFormedReason::LackOfPeers((
                        len,
                        total,
                        PointMap::Includes,
                    ));
                    return Some(VerifyError::IllFormed(reason));
                }
            }
        }
        match witness_peers {
            Some((Err(round), scheduled)) => {
                let len = scheduled.len();
                let reason = VerifyFailReason::Uninit((len, round, PointMap::Witness));
                return Some(VerifyError::Fail(reason));
            }
            None => {
                if !info.witness().is_empty() {
                    let reason = IllFormedReason::MustBeEmpty(PointMap::Witness);
                    return Some(VerifyError::IllFormed(reason));
                }
            }
            Some((Ok(_), scheduled)) => {
                if !info.witness().is_empty() {
                    let peers = info.witness().keys();
                    let unknown = peers
                        .filter(|peer| !scheduled.contains(peer))
                        .copied()
                        .collect::<Vec<_>>();
                    if !unknown.is_empty() {
                        let reason = IllFormedReason::UnknownPeers((
                            unknown,
                            PointMap::Witness,
                        ));
                        return Some(VerifyError::IllFormed(reason));
                    }
                }
            }
        }
        None
    }
    fn links_across_genesis(
        info: &PointInfo,
        conf: &MempoolConfig,
    ) -> Option<IllFormedReason> {
        let proof_round = info.anchor_round(AnchorStageRole::Proof);
        let trigger_round = info.anchor_round(AnchorStageRole::Trigger);
        match (
            proof_round.cmp(&conf.genesis_round),
            trigger_round.cmp(&conf.genesis_round),
        ) {
            (cmp::Ordering::Less, _) | (_, cmp::Ordering::Less) => {
                Some(IllFormedReason::LinksAcrossGenesis)
            }
            (
                cmp::Ordering::Greater,
                cmp::Ordering::Greater,
            ) if proof_round == trigger_round => Some(IllFormedReason::LinksSameRound),
            _ => None,
        }
    }
    /// counterpart of [`crate::models::PointData::has_well_formed_maps`] that must be called later
    /// and allows to link this [`PointInfo`] with its dependencies for validation and commit;
    /// its decided later in [`Self::check_links`] whether current point belongs to leader
    fn is_well_formed(
        info: &PointInfo,
        conf: &MempoolConfig,
    ) -> Option<IllFormedReason> {
        if info.round() == conf.genesis_round {
            if info.payload_len() > 0 {
                return Some(IllFormedReason::TooLargePayload(info.payload_bytes()));
            }
            if info.anchor_proof() != &Link::ToSelf {
                return Some(IllFormedReason::SelfAnchorStage(AnchorStageRole::Proof));
            }
            if info.anchor_trigger() != &Link::ToSelf {
                return Some(IllFormedReason::SelfAnchorStage(AnchorStageRole::Trigger));
            }
            if info.time() != info.anchor_time() {
                return Some(IllFormedReason::AnchorTime);
            }
        } else {
            if info.payload_bytes() > conf.consensus.payload_batch_bytes.get() {
                return Some(IllFormedReason::TooLargePayload(info.payload_bytes()));
            }
            if info.evidence().is_empty() {
                if info.anchor_proof() == &Link::ToSelf {
                    return Some(
                        IllFormedReason::SelfAnchorStage(AnchorStageRole::Proof),
                    );
                }
                if info.anchor_trigger() == &Link::ToSelf {
                    return Some(
                        IllFormedReason::SelfAnchorStage(AnchorStageRole::Trigger),
                    );
                }
            }
            if info.time() <= info.anchor_time() {
                return Some(IllFormedReason::AnchorTime);
            }
        };
        None
    }
    /// blame author and every dependent point's author
    fn is_proof_ok(info: &PointInfo, proven: &PointInfo) -> Option<InvalidReason> {
        assert_eq!(
            info.author(), proven.author(),
            "Coding error: mismatched authors of proof and its vertex"
        );
        assert_eq!(
            info.round().prev(), proven.round(),
            "Coding error: mismatched rounds of proof and its vertex"
        );
        let prev_digest = info
            .prev_digest()
            .expect(
                "Coding error: passed point doesn't contain proof for a given vertex",
            );
        assert_eq!(
            prev_digest, proven.digest(),
            "Coding error: mismatched previous point of the same author, must have been checked before"
        );
        if info.time() <= proven.time() {
            return Some(InvalidReason::TimeNotGreaterThanInPrevPoint(proven.id()));
        }
        if info.anchor_proof() == &Link::ToSelf && info.anchor_time() != proven.time() {
            return Some(InvalidReason::AnchorProofDoesntInheritAnchorTime(proven.id()));
        }
        None
    }
}
impl ValidateCtx {
    const KIND: &'static str = "kind";
    fn verified(result: &Result<(), VerifyError>) {
        let label = match result {
            Err(VerifyError::Fail(_)) => "failed",
            Err(VerifyError::IllFormed(IllFormedReason::UnknownPeers(_))) => "bad_peer",
            Err(VerifyError::IllFormed(_)) => "ill_formed",
            Ok(_) => {
                metrics::counter!("tycho_mempool_points_verify_ok").increment(1);
                return;
            }
        };
        metrics::counter!("tycho_mempool_points_verify_err", Self::KIND => label)
            .increment(1);
    }
    pub fn resolved(dag_point: &DagPoint) {
        const ORD: &str = "ord";
        let ord = if dag_point.is_first_resolved() { "first" } else { "alt" };
        let kind = match dag_point {
            DagPoint::NotFound(_) => "not_found",
            DagPoint::IllFormed(_) => "ill_formed",
            DagPoint::Invalid(_) => "invalid",
            DagPoint::Valid(_) => {
                metrics::counter!("tycho_mempool_points_resolved_ok", ORD => ord)
                    .increment(1);
                return;
            }
        };
        metrics::counter!(
            "tycho_mempool_points_resolved_err", ORD => ord, Self::KIND => kind
        )
            .increment(1);
    }
    fn validated(
        &self,
        cert: &Cert,
        result: ValidateResult,
    ) -> TaskResult<ValidateResult> {
        match &result {
            ValidateResult::IllFormed(reason) => {
                tracing::error!(
                    parent : self.span(), result = "ill-formed", is_certified = cert
                    .is_certified(), reason = display(reason), "validated",
                );
            }
            ValidateResult::Invalid(reason) => {
                tracing::warn!(
                    parent : self.span(), is_certified = cert.is_certified(), result =
                    "invalid", reason = display(reason), "validated",
                );
            }
            ValidateResult::Valid => {
                tracing::debug!(
                    parent : self.span(), is_certified = cert.is_certified(), result =
                    "valid", "validated",
                );
            }
        };
        Ok(result)
    }
}
