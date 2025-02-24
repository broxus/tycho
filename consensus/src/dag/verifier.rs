use std::cmp;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::Instrument;
use tycho_network::PeerId;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run_fifo;

use crate::dag::dag_location::DagLocation;
use crate::dag::dag_point_future::DagPointFuture;
use crate::dag::{DagRound, WeakDagRound};
use crate::effects::{AltFormat, Ctx, MempoolStore, TaskResult, ValidateCtx};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{
    AnchorStageRole, DagPoint, Digest, Link, PeerCount, Point, PointInfo, PrevPointProof, Round,
    UnixTime,
};

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

#[derive(Debug, Copy, Clone)]
pub enum PointMap {
    Evidence, // r+0
    Includes, // r-1
    Witness,  // r-2
}

#[derive(thiserror::Error, Debug)]
pub enum VerifyError {
    #[error("cannot verify: {0}")]
    Fail(VerifyFailReason),
    #[error("signature does not match author")]
    BadSig,
    #[error("ill-formed: {0}")]
    IllFormed(IllFormedReason),
}
#[derive(thiserror::Error, Debug)]
pub enum VerifyFailReason {
    #[error("point before genesis cannot be verified")]
    BeforeGenesis,
    #[error("uninit {:?} peer set of {} len at round {}", .0.2, .0.0, .0.1.0)]
    Uninit((usize, Round, PointMap)),
    #[error("author is not scheduled: outdated peer schedule or author out of nowhere")]
    UnknownAuthor,
}
#[derive(thiserror::Error, Debug, Clone)]
pub enum IllFormedReason {
    #[error("unknown after load from DB")]
    AfterLoadFromDb, // TODO describe all reasons and save them to DB, then remove this stub
    #[error("too large payload: {0} bytes")]
    TooLargePayload(usize),
    #[error("links anchor across genesis")]
    LinksAcrossGenesis,
    #[error("links both anchor roles to same round")]
    LinksSameRound,
    #[error("self link")]
    SelfLink,
    #[error("anchor link")]
    AnchorLink,
    #[error("bad signature in evidence map")]
    EvidenceSig,
    #[error("{0:?} peer map must be empty")]
    MustBeEmpty(PointMap),
    #[error("unknown peers in {:?} map: {}", .0.1, .0.0.as_slice().alt())]
    UnknownPeers((Vec<PeerId>, PointMap)),
    #[error("{} peers is not enough in {:?} map for 3F+1={}", .0.0, .0.2, .0.1.full())]
    LackOfPeers((usize, PeerCount, PointMap)),
    #[error("some structure issue")]
    NotDescribed, // TODO enum for each check
}

#[derive(Debug)]
pub enum ValidateResult {
    Valid { is_certified: bool },
    Invalid { is_certified: bool },
    IllFormed(IllFormedReason),
}

// If any round exceeds dag rounds, the arg point @ r+0 is considered valid by itself.
// Any point @ r+0 will be committed, only if it has valid proof @ r+1
// included into valid anchor chain, i.e. validated by consensus.
impl Verifier {
    /// the first and mandatory check of any Point received no matter where from
    pub fn verify(
        point: &Point,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> Result<(), VerifyError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_verifier_verify_time");

        let result = if !point.is_integrity_ok() {
            Err(VerifyError::BadSig)
        } else {
            Self::verify_impl(point, peer_schedule, conf).map_or(Ok(()), Err)
        };

        ValidateCtx::verified(&result);
        result
    }

    /// must be called iff [`Self::verify`] succeeded
    ///
    /// We do not require the whole `Point` to avoid OOM as during sync Dag can grow large.
    pub async fn validate(
        info: PointInfo, // @ r+0
        prev_proof: Option<PrevPointProof>,
        r_0: WeakDagRound, // r+0
        downloader: Downloader,
        store: MempoolStore,
        mut certified_rx: oneshot::Receiver<()>,
        ctx: ValidateCtx,
    ) -> TaskResult<ValidateResult> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_verifier_validate_time");
        let span_guard = ctx.span().enter();

        match info.round().cmp(&ctx.conf().genesis_round) {
            cmp::Ordering::Less => {
                panic!("Coding error: can only validate points older than genesis")
            }
            cmp::Ordering::Equal => {
                // for genesis point it's sufficient to be well-formed and pass integrity check,
                // it cannot be validated against AnchorStage (as it knows nothing about genesis)
                // and cannot contain dependencies
                return ctx.validated(ValidateResult::Valid {
                    is_certified: false,
                });
            }
            cmp::Ordering::Greater => {} // peer usage is already verified
        }

        let Some(r_0_pre) = r_0.upgrade() else {
            tracing::info!("cannot validate point, no round in local DAG");
            return ctx.validated(ValidateResult::Invalid {
                is_certified: certified_rx.try_recv() != Err(TryRecvError::Empty),
            });
        };
        assert_eq!(
            r_0_pre.round(),
            info.round(),
            "Coding error: dag round mismatches point round"
        );

        if !Self::is_self_links_ok(&info, &r_0_pre) {
            return ctx.validated(ValidateResult::IllFormed(IllFormedReason::SelfLink));
        }

        if ![AnchorStageRole::Proof, AnchorStageRole::Trigger]
            .into_iter()
            .all(|role| Self::is_anchor_link_ok(role, &info, &r_0_pre, ctx.conf()))
        {
            return ctx.validated(ValidateResult::IllFormed(IllFormedReason::AnchorLink));
        };

        drop(r_0_pre);
        drop(span_guard);

        // certified flag aborts proof check; checked proof mark dependencies as certified;
        // check depender's sig before new (down)load point futures are spawned
        let proven_by_cert = if certified_rx.try_recv() != Err(TryRecvError::Empty) {
            // FIXME either sent or sender dropped - all the same; make distinct and do not drop
            Some(true)
        } else if let Some(proof) = prev_proof {
            let mut signatures_fut = std::pin::pin!({
                rayon_run_fifo(move || proof.signatures_match()).instrument(ctx.span().clone())
            });
            let certified = tokio::select! {
                biased;
                _ = &mut certified_rx => {
                    true // certified; certifies
                },
                is_sig_ok = &mut signatures_fut => {
                    if is_sig_ok {
                        false // not certified; certifies
                    } else {
                        return ctx.validated(ValidateResult::IllFormed(IllFormedReason::EvidenceSig));
                    }
                }
            };
            Some(certified)
        } else {
            None
        };

        let span_guard = ctx.span().enter();

        let Some(r_0) = r_0.upgrade() else {
            tracing::info!("cannot validate point, no round in local DAG after proof check");
            return ctx.validated(ValidateResult::Invalid {
                is_certified: proven_by_cert.unwrap_or_default(),
            });
        };

        let Some(r_1) = r_0.prev().upgrade() else {
            tracing::info!("cannot validate point's 'includes', no round in local DAG");
            return ctx.validated(ValidateResult::Invalid {
                is_certified: proven_by_cert.unwrap_or_default(),
            });
        };

        let r_2_opt = r_1.prev().upgrade();
        if r_2_opt.is_none() && !info.data().witness.is_empty() {
            tracing::debug!("cannot validate point's 'witness', no round in local DAG");
            return ctx.validated(ValidateResult::Invalid {
                is_certified: proven_by_cert.unwrap_or_default(),
            });
        }

        let direct_deps = Self::spawn_direct_deps(&info, &r_1, r_2_opt, &downloader, &store, &ctx);

        let (proven_vertex_dep, prev_other_versions) = r_1
            .view(&info.data().author, |loc| {
                Self::versions_partitioned(loc, info.data().prev_digest())
            })
            .unwrap_or_default();

        match proven_by_cert {
            Some(true) => {
                for shared in &direct_deps {
                    shared.mark_certified(); // all dependencies
                }
            }
            Some(false) => {
                if let Some(vertex) = &proven_vertex_dep {
                    vertex.mark_certified(); // just one among all
                }
            }
            None => {}
        }
        let mut is_certified = proven_by_cert.unwrap_or_default();

        let mut is_valid_fut = std::pin::pin!({
            let deps_and_prev = direct_deps
                .iter()
                .cloned()
                // peer has to jump over a round if it could not produce valid point in prev loc;
                // do not add same prev_digest twice - it is added as one of 'includes';
                // do not extend listed dependencies as they may become certified by majority
                .chain(prev_other_versions.into_iter());
            Self::is_valid(info.clone(), deps_and_prev.collect(), ctx.conf())
                .instrument(ctx.span().clone())
        });

        // drop strong links before await
        drop(r_0);
        drop(r_1);
        drop(span_guard);

        let valid = loop {
            tokio::select! {
                biased;
                _ = &mut certified_rx, if !is_certified => {
                    is_certified = true; // oneshot cannot be lagged, only closed
                    for shared in &direct_deps {
                        shared.mark_certified();
                    }
                },
                is_valid = &mut is_valid_fut => {
                    break is_valid?;
                },
            }
        };

        let status = if valid {
            ValidateResult::Valid { is_certified }
        } else {
            ValidateResult::Invalid { is_certified }
        };

        ctx.validated(status)
    }

    fn is_self_links_ok(
        info: &PointInfo,     // @ r+0
        dag_round: &DagRound, // r+0
    ) -> bool {
        // existence of proofs in leader points is a part of point's well-formedness check
        match &dag_round.anchor_stage() {
            Some(stage) if stage.leader == info.data().author => {
                // either Proof directly points on candidate
                if stage.role == AnchorStageRole::Proof
                    // or Trigger points on Proof
                    || info.anchor_round(AnchorStageRole::Proof) == info.round().prev()
                {
                    // must link to own point if it did not skip rounds
                    info.data().prev_digest().is_some()
                        == (info.anchor_link(stage.role) == &Link::ToSelf)
                } else {
                    // skipped either candidate of Proof, but may have prev point
                    info.anchor_link(stage.role) != &Link::ToSelf
                }
            }
            // others must not pretend to be leaders
            Some(_) | None => {
                info.data().anchor_proof != Link::ToSelf
                    && info.data().anchor_trigger != Link::ToSelf
            }
        }
    }

    /// the only method that scans the DAG deeper than 2 rounds
    fn is_anchor_link_ok(
        link_field: AnchorStageRole,
        info: &PointInfo,     // @ r+0
        dag_round: &DagRound, // start with r+0
        conf: &MempoolConfig,
    ) -> bool {
        let linked_id = info.anchor_id(link_field);

        let Some(round) = dag_round.scan(linked_id.round) else {
            // too old indirect reference does not invalidate the point,
            // because its direct dependencies ('link through') will be validated anyway
            return true;
        };

        if round.round() == conf.genesis_round {
            // notice that point is required to link to the freshest leader point
            // among all its (in)direct dependencies, which is checked later
            return true;
        }

        match round.anchor_stage() {
            Some(stage) if stage.role == link_field && stage.leader == linked_id.author => {}
            _ => {
                // link does not match round's leader, prescribed by AnchorStage
                return false;
            }
        };

        true
    }

    fn versions_partitioned(
        dag_location: &DagLocation,
        searched: Option<&Digest>,
    ) -> (Option<DagPointFuture>, Vec<DagPointFuture>) {
        let mut found = None;
        let mut others = Vec::with_capacity(
            dag_location
                .versions
                .len()
                .saturating_sub(searched.is_some() as usize),
        );
        for (digest, shared) in &dag_location.versions {
            if searched == Some(digest) {
                found = Some(shared.clone());
            } else {
                others.push(shared.clone());
            }
        }
        (found, others)
    }

    fn spawn_direct_deps(
        info: &PointInfo,          // @ r+0
        r_1: &DagRound,            // r-1
        r_2_opt: Option<DagRound>, // r-2
        downloader: &Downloader,
        store: &MempoolStore,
        ctx: &ValidateCtx,
    ) -> Vec<DagPointFuture> {
        let mut dependencies =
            Vec::with_capacity(info.data().includes.len() + info.data().witness.len());

        // integrity check passed, so includes contain author's prev point proof
        let includes = info
            .data()
            .includes
            .iter()
            .map(|(author, digest)| (r_1, author, digest));

        let witness = r_2_opt.iter().flat_map(|r_2| {
            info.data()
                .witness
                .iter()
                .map(move |(author, digest)| (r_2, author, digest))
        });

        for (dag_round, author, digest) in includes.chain(witness) {
            let shared = dag_round.add_dependency(
                author,
                digest,
                &info.data().author,
                downloader,
                store,
                ctx,
            );

            dependencies.push(shared);
        }

        dependencies
    }

    /// check only direct dependencies and location for previous point (let it jump over round)
    async fn is_valid(
        info: PointInfo,
        mut deps_and_prev: FuturesUnordered<DagPointFuture>,
        conf: &MempoolConfig,
    ) -> TaskResult<bool> {
        // point is well-formed if we got here, so point.proof matches point.includes
        let prev_digest_in_point = info.data().prev_digest();
        let prev_round = info.round().prev();

        // Indirect dependencies may be evicted from memory and not participate in this check,
        // but validity of direct dependencies ('links through') ensures inclusion chain is valid.
        // If point under validation is so old, that any dependency download fails,
        // it will not be referenced by the current peer anyway, and it's ok to mark it as invalid
        // until the current peer makes a gap in its far outdated DAG.
        let anchor_trigger_id = info.anchor_id(AnchorStageRole::Trigger);
        let anchor_proof_id = info.anchor_id(AnchorStageRole::Proof);
        let anchor_trigger_link_id = info.anchor_link_id(AnchorStageRole::Trigger);
        let anchor_proof_link_id = info.anchor_link_id(AnchorStageRole::Proof);

        let max_allowed_dep_time =
            info.data().time + UnixTime::from_millis(conf.consensus.clock_skew_millis as _);

        while let Some(task_result) = deps_and_prev.next().await {
            let dag_point = task_result?;
            if dag_point.round() == prev_round && dag_point.author() == info.data().author {
                match prev_digest_in_point {
                    Some(prev_digest_in_point) if prev_digest_in_point == dag_point.digest() => {
                        let Some(proven) = dag_point.trusted() else {
                            // author must have skipped current point's round
                            // to clear its bad history
                            return Ok(false);
                        };
                        if !Self::is_proof_ok(&info, proven) {
                            return Ok(false);
                        } // else ok continue
                    }
                    Some(_) | None => {
                        #[allow(clippy::match_same_arms, reason = "comments")]
                        match dag_point {
                            DagPoint::Valid(_) => {
                                // Some: point must have named _this_ point in `prev_digest`
                                // None: point must have filled `prev_digest` and `includes`
                                return Ok(false);
                            }
                            DagPoint::Invalid(_) | DagPoint::IllFormed(_) => {
                                // Some: point must have named _this_ point in `prev_digest`,
                                //       just to be invalid for an invalid dependency
                                // None: author must have skipped current point's round
                                return Ok(false);
                            }
                            DagPoint::NotFound(not_found) if not_found.is_certified() => {
                                // same as for valid
                                return Ok(false);
                            }
                            DagPoint::NotFound(_) => {
                                // failed download is ok for both Some and None:
                                // it's other point's dependency, that really may not exist
                            }
                        }
                    }
                }
            } else {
                let Some(dep) = dag_point.trusted() else {
                    // just invalid dependency
                    return Ok(false);
                };
                if dep.data().time > max_allowed_dep_time {
                    // dependency time may exceed those in point only by a small value from config
                    return Ok(false);
                }
                if dep.anchor_round(AnchorStageRole::Trigger) > anchor_trigger_id.round
                    || dep.anchor_round(AnchorStageRole::Proof) > anchor_proof_id.round
                {
                    // did not actualize the chain
                    return Ok(false);
                }

                let dep_id = dep.id();
                if dep_id == anchor_trigger_link_id
                    && dep.anchor_id(AnchorStageRole::Trigger) != anchor_trigger_id
                {
                    // path does not lead to destination
                    return Ok(false);
                }
                if dep_id == anchor_proof_link_id {
                    if dep.anchor_id(AnchorStageRole::Proof) != anchor_proof_id {
                        // path does not lead to destination
                        return Ok(false);
                    }
                    if dep.data().anchor_time != info.data().anchor_time {
                        // anchor candidate's time is not inherited from its proof
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// blame author and every dependent point's author
    fn verify_impl(
        point: &Point, // @ r+0
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
        ) = match (point.round() - conf.genesis_round.prev().0).0 {
            0 => return Some(VerifyError::Fail(VerifyFailReason::BeforeGenesis)),
            1 => {
                let a = peer_schedule.atomic().peers_for(point.round()).clone();
                ((peer_count_genesis(a.len(), point.round()), a), None, None)
            }
            2 => {
                let rounds = [point.round(), point.round().prev()];
                let [a, b] = peer_schedule.atomic().peers_for_array(rounds);
                (
                    (PeerCount::try_from(a.len()).map_err(|_e| rounds[0]), a),
                    Some((peer_count_genesis(b.len(), rounds[1]), b)),
                    None,
                )
            }
            more => {
                let rounds = [
                    point.round(),
                    point.round().prev(),
                    point.round().prev().prev(),
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
        if let Some(reason) = Self::links_across_genesis(point, conf) {
            return Some(VerifyError::IllFormed(reason));
        }

        // check size only now, as config seems up to date
        let payload_bytes: usize = point.payload().iter().fold(0, |acc, msg| acc + msg.len());
        if payload_bytes > conf.consensus.payload_batch_bytes as usize {
            let reason = IllFormedReason::TooLargePayload(payload_bytes);
            return Some(VerifyError::IllFormed(reason));
        }

        if !point.is_well_formed(conf) {
            return Some(VerifyError::IllFormed(IllFormedReason::NotDescribed));
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
                if !scheduled.contains(&point.data().author) {
                    let reason = VerifyFailReason::UnknownAuthor;
                    return Some(VerifyError::Fail(reason));
                }
                if !point.evidence().is_empty() {
                    if total == PeerCount::GENESIS {
                        let reason = IllFormedReason::MustBeEmpty(PointMap::Evidence);
                        return Some(VerifyError::IllFormed(reason));
                    }
                    let evidence = point.evidence();
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
                if !point.data().includes.is_empty() {
                    let reason = IllFormedReason::MustBeEmpty(PointMap::Includes);
                    return Some(VerifyError::IllFormed(reason));
                }
            }
            Some((Ok(total), scheduled)) => {
                let includes = &point.data().includes;
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
                if !point.data().witness.is_empty() {
                    let reason = IllFormedReason::MustBeEmpty(PointMap::Witness);
                    return Some(VerifyError::IllFormed(reason));
                }
            }
            Some((Ok(_), scheduled)) => {
                if !point.data().witness.is_empty() {
                    let peers = point.data().witness.keys();
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

    fn links_across_genesis(point: &Point, conf: &MempoolConfig) -> Option<IllFormedReason> {
        let genesis_round = conf.genesis_round;
        let proof_round = point.anchor_round(AnchorStageRole::Proof);
        let trigger_round = point.anchor_round(AnchorStageRole::Trigger);
        match (
            proof_round.cmp(&genesis_round),
            trigger_round.cmp(&genesis_round),
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

    /// blame author and every dependent point's author
    fn is_proof_ok(
        info: &PointInfo,   // @ r+0
        proven: &PointInfo, // @ r-1
    ) -> bool {
        assert_eq!(
            info.data().author,
            proven.data().author,
            "Coding error: mismatched authors of proof and its vertex"
        );
        assert_eq!(
            info.round().prev(),
            proven.round(),
            "Coding error: mismatched rounds of proof and its vertex"
        );
        let prev_digest = info
            .data()
            .prev_digest()
            .expect("Coding error: passed point doesn't contain proof for a given vertex");
        assert_eq!(
            prev_digest, proven.digest(),
            "Coding error: mismatched previous point of the same author, must have been checked before"
        );
        if info.data().time <= proven.data().time {
            // time must be increasing by the same author until it stops referencing previous points
            return false;
        }
        if info.data().anchor_proof == Link::ToSelf && info.data().anchor_time != proven.data().time
        {
            // anchor proof must inherit its candidate's time
            return false;
        }
        true
    }
}

impl ValidateCtx {
    const KIND: &'static str = "kind";

    fn verified(result: &Result<(), VerifyError>) {
        let label = match result {
            Err(VerifyError::Fail(_)) => "failed",
            Err(VerifyError::IllFormed(IllFormedReason::UnknownPeers(_))) => "bad_peer",
            Err(VerifyError::BadSig) => "bad_sig",
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

    fn validated(&self, result: ValidateResult) -> TaskResult<ValidateResult> {
        match &result {
            ValidateResult::IllFormed(reason) => {
                tracing::error!(
                    parent: self.span(),
                    result = "ill-formed",
                    reason = display(reason),
                    "validated",
                );
            }
            ValidateResult::Invalid { is_certified } => {
                tracing::warn!(
                    parent: self.span(),
                    %is_certified,
                    result = "invalid",
                    "validated",
                );
            }
            ValidateResult::Valid { is_certified } => {
                tracing::debug!(
                    parent: self.span(),
                    %is_certified,
                    result = "valid",
                    "validated",
                );
            }
        };
        Ok(result)
    }
}
