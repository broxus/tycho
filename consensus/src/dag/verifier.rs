use std::cmp;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_util::metrics::HistogramGuard;

use crate::dag::dag_location::DagLocation;
use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::dag::{AnchorStage, DagRound, WeakDagRound};
use crate::effects::{AltFormat, Ctx, TaskResult, ValidateCtx};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{
    AnchorLink, AnchorStageRole, Cert, CertDirectDeps, DagPoint, Digest, IndirectLink, PeerCount,
    PointId, PointInfo, PointMap, Round, StructureIssue, Through, UnixTime,
};
use crate::storage::MempoolStore;
// Note on equivocation.
// Detected point equivocation does not invalidate the point, it just
// prevents us (as a reliable peer) from returning our signature to the author.
// Such a point may be included in our next "includes" or "witnesses",
// but neither its inclusion nor omitting is required: as we don't
// return our signature, our dependencies cannot be validated against it.
// Equally, we immediately stop communicating with the equivocating node,
// without invalidating any of its points (no matter historical or future).
// We will not sign the proof for equivocated point as we ban the author on network layer.
// Anyway, no more than one of equivocated points may become a vertex.

pub struct Verifier;

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
    #[error("structure issue: {0}")]
    Structure(StructureIssue),
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
    // Errors below are thrown from `validate()` because they require DagRound
    #[error("self link")]
    SelfLink,
    #[error("anchor link to {0:?}")]
    AnchorLink(AnchorStageRole),
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum InvalidReason {
    #[error("after load from DB{}", no_dag_round.then_some(" (no dag round)").unwrap_or(""))]
    AfterLoadFromDb { no_dag_round: bool },
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
    #[error("dependency time too far in future: {:?}", .0.alt())]
    DependencyTimeTooFarInFuture(PointId),
    #[error("newer anchor {:?} in dependency {:?}", .0.0, .0.1.alt())]
    NewerAnchorInDependency((AnchorStageRole, PointId)),
    #[error("anchor {:?} link leads to other destination through {:?}", .0.0, .0.1.alt())]
    AnchorLinkBadPath((AnchorStageRole, PointId)),
    #[error("dependency ill-formed {:?}: {}", .0.0.alt(), .0.1)]
    DepIllFormed((PointId, IllFormedReason)),
    #[error("dependency not found {:?}", .0.alt())]
    DepNotFound(PointId),
    #[error("{0:?}")]
    Dependency(InvalidDependency),
}

impl InvalidReason {
    /// root cause is because of no dag round: such points should not be a ban reason
    pub fn no_dag_round(&self) -> bool {
        match self {
            Self::AfterLoadFromDb { no_dag_round } => *no_dag_round,
            Self::NoRoundInDag(_) | Self::DependencyRoundDropped => true,
            InvalidReason::Dependency(inv_dep) => match &*inv_dep.reason {
                Self::AfterLoadFromDb { no_dag_round } => *no_dag_round,
                Self::NoRoundInDag(_) | Self::DependencyRoundDropped => true,
                _ => false,
            },
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct InvalidDependency {
    pub link: IndirectLink,
    pub reason: Box<InvalidReason>,
}

impl std::fmt::Debug for InvalidDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid dependency link {:?}: ", self.link.alt())?;
        std::fmt::Display::fmt(&self.reason, f)
    }
}

// If any round exceeds dag rounds, the arg point @ r+0 is considered valid by itself.
// Any point @ r+0 will be committed, only if it has valid proof @ r+1
// included into valid anchor chain, i.e. validated by consensus.
impl Verifier {
    /// the first and mandatory check of any Point received no matter where from
    pub fn verify(
        info: &PointInfo,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Result<(), VerifyError> {
        let result = Self::verify_impl(info, peer_schedule, conf).map_or(Ok(()), Err);

        ValidateCtx::verified(&result);
        result
    }

    /// must be called iff [`Self::verify`] succeeded
    ///
    /// We do not require the whole `Point` to avoid OOM as during sync Dag can grow large.
    pub async fn validate(
        info: PointInfo,        // @ r+0
        r_0_weak: WeakDagRound, // r+0
        downloader: Downloader,
        store: MempoolStore,
        cert: Cert,
        ctx: ValidateCtx,
    ) -> TaskResult<ValidateResult> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_verifier_validate_time");
        let entered_span = ctx.span().clone().entered();
        let peer_schedule = downloader.peer_schedule();

        match info.round().cmp(&ctx.conf().genesis_round) {
            cmp::Ordering::Less => {
                panic!("Coding error: can only validate points older than genesis")
            }
            cmp::Ordering::Equal => {
                // for genesis point it's sufficient to be well-formed and pass integrity check,
                // it cannot be validated against AnchorStage (as it knows nothing about genesis)
                // and cannot contain dependencies
                return ctx.validated(&cert, ValidateResult::Valid);
            }
            cmp::Ordering::Greater => {} // peer usage is already verified
        }

        let Some(r_0) = r_0_weak.upgrade() else {
            // have to decide between ill-formed and invalid
            if let Some(reason) = Self::check_links(&info, None, peer_schedule, ctx.conf()) {
                return ctx.validated(&cert, ValidateResult::IllFormed(reason));
            }
            let reason = InvalidReason::NoRoundInDag(PointMap::Evidence);
            return ctx.validated(&cert, ValidateResult::Invalid(reason));
        };
        assert_eq!(
            r_0.round(),
            info.round(),
            "Coding error: dag round mismatches point round"
        );

        if let Some(reason) = Self::check_links(&info, Some(&r_0), peer_schedule, ctx.conf()) {
            return ctx.validated(&cert, ValidateResult::IllFormed(reason));
        }

        let Some(r_1) = r_0.prev().upgrade() else {
            let reason = InvalidReason::NoRoundInDag(PointMap::Includes);
            return ctx.validated(&cert, ValidateResult::Invalid(reason));
        };
        let r_2_opt = r_1.prev().upgrade();

        let (mut deps_and_prev, cert_deps, prev_point_cert) =
            Self::spawn_direct_deps(&info, &r_1, r_2_opt.as_ref(), &downloader, &store, &ctx);

        // move strong prev point cert: don't hold it across await
        if let Some(prev_point_cert) = prev_point_cert {
            // point well-formedness is enough to act as a carrier for the majority of signatures
            prev_point_cert.certify(ctx.conf());
        }
        cert.set_deps(cert_deps);

        if r_2_opt.is_none() && !info.witness().is_empty() {
            // to catch history conflict earlier we've spawned deps and certified the prev one
            let reason = InvalidReason::NoRoundInDag(PointMap::Witness);
            return ctx.validated(&cert, ValidateResult::Invalid(reason));
        }

        deps_and_prev.extend(
            // peer has to jump over a round if it could not produce valid point in prev loc
            r_1.view(info.author(), |loc| {
                // do not add same prev_digest twice - it is added as one of 'includes'
                Self::other_versions(loc, info.prev_digest())
            })
            .unwrap_or_default(),
        );

        let is_valid_fut =
            Self::check_valid(&info, deps_and_prev, ctx.conf()).instrument(entered_span.exit());

        // drop strong links before await
        drop(r_0);
        drop(r_1);
        drop(r_2_opt);

        ctx.validated(&cert, is_valid_fut.await?)
    }

    fn check_links(
        info: &PointInfo,
        point_round: Option<&DagRound>,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Option<IllFormedReason> {
        // existence of proofs in leader points is a part of point's well-formedness check
        let is_self_link_ok = match point_round
            .ok_or_else(|| AnchorStage::of(info.round(), peer_schedule, conf))
            .as_ref()
            .map_or_else(|fallback| fallback.as_ref(), |round| round.anchor_stage())
        {
            Some(stage) if stage.leader == info.author() => {
                // either Proof directly points on candidate
                if stage.role == AnchorStageRole::Proof
                    // or Trigger points on Proof
                    || info.anchor_round(AnchorStageRole::Proof) == info.round().prev()
                {
                    // must link to own point if it did not skip rounds
                    info.prev_digest().is_some()
                        == (info.anchor_link(stage.role) == &AnchorLink::ToSelf)
                } else {
                    // skipped either candidate of Proof, but may have prev point
                    info.anchor_link(stage.role) != &AnchorLink::ToSelf
                }
            }
            // others must not pretend to be leaders
            Some(_) | None => {
                info.anchor_proof() != &AnchorLink::ToSelf
                    && info.anchor_trigger() != &AnchorLink::ToSelf
            }
        };
        if !is_self_link_ok {
            return Some(IllFormedReason::SelfLink);
        }

        for link_field in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            let linked_id = info.anchor_id(link_field);

            if linked_id.round == conf.genesis_round {
                // notice that point is required to link to the freshest leader point
                // among all its (in)direct dependencies, which is checked later
                continue;
            }
            let is_ok = match point_round.and_then(|dr| dr.scan(linked_id.round)) {
                Some(linked_round) => linked_round.anchor_stage().is_some_and(|stage| {
                    stage.role == link_field && stage.leader == linked_id.author
                }),
                None => {
                    AnchorStage::of(linked_id.round, peer_schedule, conf).is_some_and(|stage| {
                        stage.role == link_field && stage.leader == linked_id.author
                    })
                }
            };
            if !is_ok {
                // link does not match round's leader, prescribed by AnchorStage
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
        info: &PointInfo,           // @ r+0
        r_1: &DagRound,             // r-1
        r_2_opt: Option<&DagRound>, // r-2
        downloader: &Downloader,
        store: &MempoolStore,
        ctx: &ValidateCtx,
    ) -> (
        FuturesUnordered<WeakDagPointFuture>,
        CertDirectDeps,
        Option<Cert>,
    ) {
        let direct_deps = FuturesUnordered::new();

        let mut prev_point_cert = None;

        // allocate a bit more so it's unlikely to grow during certify procedure
        let mut cert_deps = CertDirectDeps {
            includes: Vec::with_capacity(info.includes().len()),
            witness: Vec::with_capacity(info.witness().len()),
        };

        let point_author = info.author();

        // populate witness before includes: their round is less
        if let Some(r_2) = r_2_opt {
            for (author, digest) in info.witness() {
                let (dep, cert) =
                    r_2.add_dependency(author, digest, point_author, downloader, store, ctx);

                cert_deps.witness.push((*digest, cert));
                direct_deps.push(dep);
            }
        }

        // integrity check passed, so includes contain author's prev point proof
        for (author, digest) in info.includes() {
            let (dep, cert) =
                r_1.add_dependency(author, digest, point_author, downloader, store, ctx);

            if author == point_author
                && let Some(prev_digest) = info.prev_digest()
                && digest == prev_digest
            {
                prev_point_cert = cert.upgrade();
            }

            cert_deps.includes.push((*digest, cert));
            direct_deps.push(dep);
        }

        (direct_deps, cert_deps, prev_point_cert)
    }

    /// check only direct dependencies and location for previous point (let it jump over round)
    async fn check_valid(
        info: &PointInfo,
        mut deps_and_prev: FuturesUnordered<WeakDagPointFuture>,
        conf: &MempoolConfig,
    ) -> TaskResult<ValidateResult> {
        // point is well-formed if we got here, so point.proof matches point.includes
        let prev_digest_in_point = info.prev_digest();
        let prev_round = info.round().prev();

        // Indirect dependencies may be evicted from memory and not participate in this check,
        // but validity of direct dependencies ('links through') ensures inclusion chain is valid.
        // If point under validation is so old, that any dependency download fails,
        // it will not be referenced by the current peer anyway, and it's ok to mark it as invalid
        // until the current peer makes a gap in its far outdated DAG.
        let anchor_trigger_id = info.anchor_id(AnchorStageRole::Trigger);
        let anchor_proof_id = info.anchor_id(AnchorStageRole::Proof);
        let anchor_trigger_through = info.anchor_link_through(AnchorStageRole::Trigger);
        let anchor_proof_through = info.anchor_link_through(AnchorStageRole::Proof);

        let max_allowed_dep_time =
            info.time() + UnixTime::from_millis(conf.consensus.clock_skew_millis.get() as _);

        let mut invalid_reason = None;
        let mut latest_invalid_dep = None;

        // join all dependencies despite the reason to invalidate the point is found
        while let Some(task_result) = deps_and_prev.next().await {
            let Some(dag_point) = task_result? else {
                if invalid_reason.is_none() {
                    invalid_reason = Some(InvalidReason::DependencyRoundDropped);
                }
                break;
            };
            let dep_id = *dag_point.id();

            let is_prev_point = if dep_id.round == prev_round && dep_id.author == info.author() {
                match prev_digest_in_point {
                    Some(prev_digest_in_point) if prev_digest_in_point == dag_point.digest() => {
                        // we validate a point that is a well-formed certificate for the previous,
                        // so we must have already marked the prev as certified
                        assert!(
                            dag_point.is_certified(),
                            "prev point was not marked as certified, Cert is broken"
                        );
                        true
                    }
                    Some(_) | None => {
                        match dag_point {
                            DagPoint::Valid(valid) => {
                                // a requirement to reference the first resolved is reproducible;
                                // if the current point is certifying, then it's prev point
                                // is supported by majority, and we can't invalidate the current
                                if prev_digest_in_point.is_none() && valid.is_first_valid() {
                                    invalid_reason =
                                        Some(InvalidReason::MustHaveReferencedPrevPoint(dep_id));
                                } // else: will not throw err only when first valid is referenced
                            }
                            DagPoint::Invalid(_) | DagPoint::IllFormed(_) => {
                                invalid_reason = Some(InvalidReason::MustHaveSkippedRound(dep_id));
                            }
                            DagPoint::NotFound(not_found) => {
                                if not_found.is_certified() {
                                    invalid_reason =
                                        Some(InvalidReason::MustHaveReferencedPrevPoint(dep_id));
                                } // else: skip, because it may be some other point's dependency
                            }
                        }
                        continue; // it's not among point dependencies
                    }
                }
            } else {
                false
            };

            let dep = match Self::dependency(&mut latest_invalid_dep, &dag_point, None, prev_round)
            {
                Ok(dep) => dep,
                Err(reason) => {
                    invalid_reason = Some(reason);
                    continue; // invalidating deps (ill and not found) are not checked against
                }
            };

            if is_prev_point && let Some(reason) = Self::is_proof_ok(info, dep) {
                invalid_reason = Some(reason);
            }

            if dep.time() > max_allowed_dep_time {
                // dependency time may exceed those in point only by a small value from config
                invalid_reason = Some(InvalidReason::DependencyTimeTooFarInFuture(dep_id));
            }

            for (anchor_role, anchor_role_round) in [
                (AnchorStageRole::Trigger, anchor_trigger_id.round),
                (AnchorStageRole::Proof, anchor_proof_id.round),
            ] {
                if dep.anchor_round(anchor_role) > anchor_role_round {
                    let tuple = (anchor_role, dep_id);
                    invalid_reason = Some(InvalidReason::NewerAnchorInDependency(tuple));
                }
            }

            if dep_id == anchor_trigger_through
                && dep.anchor_id(AnchorStageRole::Trigger) != anchor_trigger_id
            {
                let tuple = (AnchorStageRole::Trigger, dep_id);
                invalid_reason = Some(InvalidReason::AnchorLinkBadPath(tuple));
            }

            if dep_id == anchor_proof_through {
                if dep.anchor_id(AnchorStageRole::Proof) != anchor_proof_id {
                    let tuple = (AnchorStageRole::Proof, dep_id);
                    invalid_reason = Some(InvalidReason::AnchorLinkBadPath(tuple));
                }
                if dep.anchor_time() != info.anchor_time() {
                    // anchor candidate's time is not inherited from its proof
                    invalid_reason = Some(InvalidReason::AnchorTimeNotInheritedFromProof(dep_id));
                }
            }
        }

        Ok(if let Some(invalid) = invalid_reason {
            ValidateResult::Invalid(invalid)
        } else if let Some(inv_dep) = latest_invalid_dep {
            ValidateResult::Invalid(InvalidReason::Dependency(inv_dep))
        } else {
            ValidateResult::Valid
        })
    }

    /// an equivalent to [`DagPoint::trusted`] that also breaks a chain of trans invalid points
    fn dependency<'a>(
        inv_dep: &mut Option<InvalidDependency>,
        dag_point: &'a DagPoint,
        indirect_through: Option<&Through>,
        prev_round: Round,
    ) -> Result<&'a PointInfo, InvalidReason> {
        let (inv_info, root_cause_id, reason) = match dag_point {
            DagPoint::Valid(valid) => return Ok(valid.info()),
            DagPoint::Invalid(invalid) if invalid.is_certified() => return Ok(invalid.info()),
            DagPoint::Invalid(invalid) => (invalid.info(), invalid.info().id(), invalid.reason()),
            DagPoint::IllFormed(ill) => {
                let tuple = (*ill.id(), ill.reason().clone());
                return Err(InvalidReason::DepIllFormed(tuple));
            }
            DagPoint::NotFound(not_found) => {
                return Err(InvalidReason::DepNotFound(*not_found.id()));
            }
        };

        // newer round takes priority; at equal rounds `no_dag_round` has less priority
        if (inv_dep.as_ref()).is_none_or(|old| {
            old.link.to.round < root_cause_id.round
                || (old.link.to.round == root_cause_id.round
                    && old.reason.no_dag_round()
                    && !reason.no_dag_round())
        }) {
            *inv_dep = Some(InvalidDependency {
                reason: Box::new(reason.clone()),
                link: IndirectLink {
                    to: *root_cause_id,
                    path: indirect_through.cloned().unwrap_or_else(|| {
                        if dag_point.round() == prev_round {
                            Through::Includes(*dag_point.author())
                        } else {
                            Through::Witness(*dag_point.author())
                        }
                    }),
                },
            });
        };

        Ok(inv_info)
    }

    /// blame author and every dependent point's author
    fn verify_impl(
        info: &PointInfo, // @ r+0
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

        let (
            same_round_peers, // @ r+0
            includes_peers,   // @ r-1
            witness_peers,    // @ r-2
        ) = match (info.round() - conf.genesis_round.prev().0).0 {
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

        // point belongs to current genesis
        if let Some(reason) = Self::links_across_genesis(info, conf) {
            return Some(VerifyError::IllFormed(reason));
        }

        // check only now, as config seems up to date
        if let Some(reason) = Self::is_well_formed(info, conf) {
            return Some(VerifyError::IllFormed(reason));
        }

        // Every point producer @ r-1 must prove its delivery to 2/3 signers @ r+0
        // inside proving point @ r+0.
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
                        let reason = IllFormedReason::UnknownPeers((unknown, PointMap::Evidence));
                        return Some(VerifyError::IllFormed(reason));
                    }
                    let len = evidence.len();
                    if len < total.majority_of_others() {
                        let reason = IllFormedReason::LackOfPeers((len, total, PointMap::Evidence));
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
                    let reason = IllFormedReason::UnknownPeers((unknown, PointMap::Includes));
                    return Some(VerifyError::IllFormed(reason));
                }
                let len = includes.len();
                if len < total.majority() {
                    let reason = IllFormedReason::LackOfPeers((len, total, PointMap::Includes));
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
                        let reason = IllFormedReason::UnknownPeers((unknown, PointMap::Witness));
                        return Some(VerifyError::IllFormed(reason));
                    }
                }
            }
        }

        None
    }

    fn links_across_genesis(info: &PointInfo, conf: &MempoolConfig) -> Option<IllFormedReason> {
        let proof_round = info.anchor_round(AnchorStageRole::Proof);
        let trigger_round = info.anchor_round(AnchorStageRole::Trigger);
        match (
            proof_round.cmp(&conf.genesis_round),
            trigger_round.cmp(&conf.genesis_round),
        ) {
            (cmp::Ordering::Less, _) | (_, cmp::Ordering::Less) => {
                Some(IllFormedReason::LinksAcrossGenesis)
            }
            (cmp::Ordering::Greater, cmp::Ordering::Greater) if proof_round == trigger_round => {
                // equality is impossible due to commit waves do not start every round;
                // anchor trigger may belong to a later round than proof and vice versa;
                // no indirect links over genesis tombstone
                Some(IllFormedReason::LinksSameRound)
            }
            _ => None, // to validate dependencies
        }
    }

    /// counterpart of [`crate::models::PointData::has_well_formed_maps`] that must be called later
    /// and allows to link this [`PointInfo`] with its dependencies for validation and commit;
    /// its decided later in [`Self::check_links`] whether current point belongs to leader
    fn is_well_formed(info: &PointInfo, conf: &MempoolConfig) -> Option<IllFormedReason> {
        if info.round() == conf.genesis_round {
            if info.payload_len() > 0 {
                return Some(IllFormedReason::TooLargePayload(info.payload_bytes()));
            }
            // evidence map is required to be empty during other peer sets checks
            if info.anchor_proof() != &AnchorLink::ToSelf {
                return Some(IllFormedReason::SelfAnchorStage(AnchorStageRole::Proof));
            }
            if info.anchor_trigger() != &AnchorLink::ToSelf {
                return Some(IllFormedReason::SelfAnchorStage(AnchorStageRole::Trigger));
            }
            if info.time() != info.anchor_time() {
                return Some(IllFormedReason::AnchorTime);
            }
        } else {
            if info.payload_bytes() > conf.consensus.payload_batch_bytes.get() {
                return Some(IllFormedReason::TooLargePayload(info.payload_bytes()));
            }
            // leader must maintain its chain of proofs,
            // while others must link to previous points (checked at the end of this method)
            if info.evidence().is_empty() {
                if info.anchor_proof() == &AnchorLink::ToSelf {
                    return Some(IllFormedReason::SelfAnchorStage(AnchorStageRole::Proof));
                }
                if info.anchor_trigger() == &AnchorLink::ToSelf {
                    return Some(IllFormedReason::SelfAnchorStage(AnchorStageRole::Trigger));
                }
            }
            if info.time() <= info.anchor_time() {
                // point time must be greater than anchor time
                return Some(IllFormedReason::AnchorTime);
            }
        };
        None
    }

    /// blame author and every dependent point's author
    fn is_proof_ok(
        info: &PointInfo,   // @ r+0
        proven: &PointInfo, // @ r-1
    ) -> Option<InvalidReason> {
        assert_eq!(
            info.author(),
            proven.author(),
            "Coding error: mismatched authors of proof and its vertex"
        );
        assert_eq!(
            info.round().prev(),
            proven.round(),
            "Coding error: mismatched rounds of proof and its vertex"
        );
        let prev_digest = info
            .prev_digest()
            .expect("Coding error: passed point doesn't contain proof for a given vertex");
        assert_eq!(
            prev_digest,
            proven.digest(),
            "Coding error: mismatched previous point of the same author, must have been checked before"
        );
        if info.time() <= proven.time() {
            // time must be increasing by the same author until it stops referencing previous points
            return Some(InvalidReason::TimeNotGreaterThanInPrevPoint(*proven.id()));
        }
        if info.anchor_proof() == &AnchorLink::ToSelf && info.anchor_time() != proven.time() {
            // anchor proof must inherit its candidate's time
            return Some(InvalidReason::AnchorProofDoesntInheritAnchorTime(
                *proven.id(),
            ));
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
        metrics::counter!("tycho_mempool_points_verify_err", Self::KIND => label).increment(1);
    }

    pub fn resolved(dag_point: &DagPoint) {
        const ORD: &str = "ord";
        let ord = if dag_point.is_first_resolved() {
            "first"
        } else {
            "alt"
        };
        let kind = match dag_point {
            DagPoint::NotFound(_) => "not_found",
            DagPoint::IllFormed(_) => "ill_formed",
            DagPoint::Invalid(_) => "invalid",
            DagPoint::Valid(_) => {
                metrics::counter!("tycho_mempool_points_resolved_ok", ORD => ord).increment(1);
                return;
            }
        };
        metrics::counter!("tycho_mempool_points_resolved_err", ORD => ord, Self::KIND => kind)
            .increment(1);
    }

    fn validated(&self, cert: &Cert, result: ValidateResult) -> TaskResult<ValidateResult> {
        match &result {
            ValidateResult::IllFormed(reason) => {
                tracing::error!(
                    parent: self.span(),
                    result = "ill-formed",
                    is_certified = cert.is_certified(),
                    reason = display(reason),
                    "validated",
                );
            }
            ValidateResult::Invalid(reason) => {
                tracing::warn!(
                    parent: self.span(),
                    is_certified = cert.is_certified(),
                    result = "invalid",
                    reason = display(reason),
                    "validated",
                );
            }
            ValidateResult::Valid => {
                tracing::debug!(
                    parent: self.span(),
                    is_certified = cert.is_certified(),
                    result = "valid",
                    "validated",
                );
            }
        };
        Ok(result)
    }
}
