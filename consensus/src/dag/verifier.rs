use std::sync::Arc;

use futures_util::stream::FuturesUnordered;
use futures_util::{future, StreamExt};
use tokio::sync::oneshot;
use tracing::Instrument;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use crate::dag::dag_location::DagLocation;
use crate::dag::dag_point_future::DagPointFuture;
use crate::dag::{DagRound, WeakDagRound};
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, MempoolStore, ValidateContext};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{
    AnchorStageRole, DagPoint, Digest, Link, PeerCount, Point, PointInfo, ValidPoint,
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

#[derive(Debug)]
pub enum VerifyError {
    BadSig,
    IllFormed,
}

// If any round exceeds dag depth, the arg point @ r+0 is considered valid by itself.
// Any point @ r+0 will be committed, only if it has valid proof @ r+1
// included into valid anchor chain, i.e. validated by consensus.
impl Verifier {
    /// the first and mandatory check of any Point received no matter where from
    pub fn verify(point: &Point, peer_schedule: &PeerSchedule) -> Result<(), VerifyError> {
        let _task_duration = HistogramGuard::begin("tycho_mempool_verifier_verify_time");
        let result = if !point.is_integrity_ok() {
            Err(VerifyError::BadSig)
        } else if !(point.is_well_formed() && Self::is_peer_usage_ok(point, peer_schedule)) {
            Err(VerifyError::IllFormed)
        } else {
            Ok(())
        };
        ValidateContext::verified(&result);
        result
    }

    /// must be called iff [`Self::verify`] succeeded
    pub async fn validate(
        point: Point,      // @ r+0
        r_0: WeakDagRound, // r+0
        downloader: Downloader,
        store: MempoolStore,
        mut certified_rx: oneshot::Receiver<()>,
        effects: Effects<ValidateContext>,
    ) -> DagPoint {
        let _task_duration = HistogramGuard::begin("tycho_mempool_verifier_validate_time");
        let span_guard = effects.span().enter();

        // for genesis point it's sufficient to be well-formed and pass integrity check,
        // it cannot be validated against AnchorStage (as it knows nothing about genesis)
        // and cannot contain dependencies
        assert!(
            point.round() > MempoolConfig::GENESIS_ROUND,
            "Coding error: can only validate points older than genesis"
        );
        let Some(r_0) = r_0.upgrade() else {
            tracing::info!("cannot (in)validate point, no round in local DAG");
            let dag_point = DagPoint::Suspicious(ValidPoint::new(&point));
            return ValidateContext::validated(dag_point);
        };
        assert_eq!(
            point.round(),
            r_0.round(),
            "Coding error: dag round mismatches point round"
        );

        let mut dependencies = Vec::with_capacity(
            point.data().includes.len() + point.data().witness.len() + 2, // +2 for anchor fields
        );

        if !(Self::is_self_links_ok(&point, &r_0)
            && Self::add_anchor_link_if_ok(
                AnchorStageRole::Proof,
                &point,
                &r_0,
                &downloader,
                &store,
                &effects,
                &mut dependencies,
            )
            && Self::add_anchor_link_if_ok(
                AnchorStageRole::Trigger,
                &point,
                &r_0,
                &downloader,
                &store,
                &effects,
                &mut dependencies,
            ))
        {
            return ValidateContext::validated(DagPoint::IllFormed(Arc::new(point.id())));
        }

        let Some(r_1) = r_0.prev().upgrade() else {
            tracing::info!("cannot (in)validate point's 'includes', no round in local DAG");
            let dag_point = DagPoint::Suspicious(ValidPoint::new(&point));
            return ValidateContext::validated(dag_point);
        };

        let mut proven_vertex_dep = None;
        Self::gather_deps(
            &point,
            &r_1,
            &downloader,
            &store,
            &effects,
            &mut dependencies,
            &mut proven_vertex_dep,
        );

        let mut signatures_fut = std::pin::pin!(match point.prev_proof() {
            None => future::Either::Left(future::ready(true)),
            Some(proof) => future::Either::Right(
                rayon_run(move || proof.signatures_match()).instrument(effects.span().clone()),
            ),
        });

        let mut is_unique_in_loc_fut = std::pin::pin!(Self::is_unique_in_loc(
            &point,
            r_0.view(&point.data().author, |loc| {
                let other_versions = Self::versions_except(loc, Some(point.digest()));
                (loc.bad_sig_in_broadcast, other_versions)
            })
        )
        .instrument(effects.span().clone()));

        let mut is_valid_fut = std::pin::pin!(Self::is_valid(
            &point,
            dependencies
                .iter()
                .cloned()
                // do not extend listed dependencies as they may become trusted by consensus
                .chain(
                    // peer has to jump over a round if it had some invalid point in prev loc
                    r_1.view(&point.data().author, |loc| {
                        // do not add same prev_digest twice - it is added as one of 'includes'
                        Self::versions_except(loc, point.data().prev_digest.as_ref())
                    })
                    .into_iter()
                    .flatten()
                )
                .collect()
        )
        .instrument(effects.span().clone()));

        // drop strong links before await
        drop(r_0);
        drop(r_1);
        drop(span_guard);

        let mut certified = None;
        let mut valid = None;
        let mut sig_ok = None;
        let mut unique_in_loc = None;

        loop {
            if certified.unwrap_or_default()
                || (valid.is_some() && unique_in_loc.is_some() && sig_ok.is_some())
            {
                break;
            }
            tokio::select! {
                biased;
                recv_result = &mut certified_rx, if certified.is_none() => {
                    certified = Some(recv_result.is_ok()); // oneshot cannot be lagged, only closed
                    if recv_result.is_ok() {
                        for shared in &dependencies {
                            shared.mark_certified();
                        }
                    }
                },
                is_sig_ok = &mut signatures_fut, if sig_ok.is_none() => {
                    sig_ok = Some(is_sig_ok);
                    if is_sig_ok {
                        // it's a noop if the point doesn't have a proof in its body
                        if let Some(vertex) = &proven_vertex_dep {
                            vertex.mark_certified();
                        }
                    } else {
                        break;
                    }
                },
                is_valid = &mut is_valid_fut, if valid.is_none() => {
                    valid = Some(is_valid);
                    if !is_valid {
                        break;
                    }
                },
                is_unique_in_loc = &mut is_unique_in_loc_fut, if unique_in_loc.is_none() => {
                    unique_in_loc = Some(is_unique_in_loc);
                }
            }
        }

        let (dag_point, level) = match (certified.unwrap_or_default(), valid, sig_ok, unique_in_loc)
        {
            (true, _, _, _) => (
                // here "trust consensus" call chain resolves;
                // ignore other flags, though it looks like a race condition:
                // follow majority's decision now, otherwise will follow it via sync
                DagPoint::Trusted(ValidPoint::new(&point)),
                tracing::Level::INFO,
            ),
            (false, Some(true), Some(true), Some(true)) => (
                DagPoint::Trusted(ValidPoint::new(&point)),
                tracing::Level::TRACE,
            ),
            (false, Some(true), Some(true), Some(false)) => (
                DagPoint::Suspicious(ValidPoint::new(&point)),
                tracing::Level::WARN,
            ),
            (false, Some(false), _, _) | (false, _, Some(false), _) => {
                (DagPoint::Invalid((&point).into()), tracing::Level::ERROR)
            }
            (false, _, _, _) => {
                let _guard = effects.span().enter();
                unreachable!(
                    "unexpected pattern in loop break: \
                     certified={certified:?} valid={valid:?} \
                     sig_ok={sig_ok:?} unique_in_loc={unique_in_loc:?}"
                );
            }
        };
        dyn_event!(
            parent: effects.span(),
            level,
            result = display(dag_point.alt()),
            certified = debug(certified),
            valid = debug(valid),
            sig_ok = debug(sig_ok),
            unique_in_loc = debug(unique_in_loc),
            "validated",
        );
        ValidateContext::validated(dag_point)
    }

    fn is_self_links_ok(
        point: &Point,        // @ r+0
        dag_round: &DagRound, // r+0
    ) -> bool {
        // existence of proofs in leader points is a part of point's well-form-ness check
        (match &dag_round.anchor_stage() {
            // no one may link to self
            None => {
                point.data().anchor_proof != Link::ToSelf
                    && point.data().anchor_trigger != Link::ToSelf
            }
            // leader must link to own point while others must not
            Some(stage) => {
                (stage.leader == point.data().author)
                    == (point.anchor_link(stage.role) == &Link::ToSelf)
            }
        }) || point.round() == MempoolConfig::GENESIS_ROUND
    }

    /// the only method that scans the DAG deeper than 2 rounds
    fn add_anchor_link_if_ok(
        link_field: AnchorStageRole,
        point: &Point,        // @ r+0
        dag_round: &DagRound, // start with r+0
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<ValidateContext>,
        dependencies: &mut Vec<DagPointFuture>,
    ) -> bool {
        let linked_id = point.anchor_id(link_field);

        let Some(round) = dag_round.scan(linked_id.round) else {
            // too old indirect reference does not invalidate the point,
            // because its direct dependencies ('link through') will be validated anyway
            return true;
        };

        if round.round() == MempoolConfig::GENESIS_ROUND {
            // notice that point is required to link to the freshest leader point
            // among all its (in)direct dependencies, which is checked later
            return linked_id == crate::test_utils::genesis_point_id();
        }

        match round.anchor_stage() {
            Some(stage) if stage.role == link_field && stage.leader == linked_id.author => {}
            _ => {
                // link does not match round's leader, prescribed by AnchorStage
                return false;
            }
        };

        #[allow(clippy::match_same_arms)] // for comments
        match point.anchor_link(link_field) {
            Link::ToSelf => {
                // do not search in DAG the point that is currently under validation;
                // link's destination is already checked by AnchorStage above, cannot be reordered
            }
            Link::Direct(_) => {
                // will be added in a search list as a member of `witness` or `includes`
            }
            Link::Indirect { .. } => {
                // actually no need to check indirect dependencies, but let's reinsure while we can
                dependencies.push(round.add_dependency_exact(
                    &linked_id.author,
                    &linked_id.digest,
                    &point.data().author,
                    downloader,
                    store,
                    effects,
                ));
            }
        };

        true
    }

    fn versions_except(dag_location: &DagLocation, except: Option<&Digest>) -> Vec<DagPointFuture> {
        // this is a synchronization point as whole closure runs under DashMap's lock
        dag_location
            .versions()
            .iter()
            .filter(|(digest, _)| except.map_or(true, |except| except != *digest))
            .map(|(_, shared)| shared.clone())
            .collect::<Vec<_>>()
    }

    fn gather_deps(
        point: &Point,  // @ r+0
        r_1: &DagRound, // r-1
        downloader: &Downloader,
        store: &MempoolStore,
        effects: &Effects<ValidateContext>,
        dependencies: &mut Vec<DagPointFuture>,
        proven_vertex_dep: &mut Option<DagPointFuture>,
    ) {
        let r_2_opt = r_1.prev().upgrade();
        if r_2_opt.is_none() {
            tracing::debug!("cannot (in)validate point's 'witness', no round in local DAG");
        }

        // integrity check passed, so includes contain author's prev point proof
        let includes = point
            .data()
            .includes
            .iter()
            .map(|(author, digest)| (r_1, author, digest));

        let witness = r_2_opt.iter().flat_map(|r_2| {
            point
                .data()
                .witness
                .iter()
                .map(move |(author, digest)| (r_2, author, digest))
        });

        for (dag_round, author, digest) in includes.chain(witness) {
            let shared = dag_round.add_dependency_exact(
                author,
                digest,
                &point.data().author,
                downloader,
                store,
                effects,
            );

            // it's sufficient to check only author to get prev (proven) point from well-formed one:
            // * includes map contains same author at most once - and it matches proven point
            // * witness map cannot contain same author
            if author == point.data().author {
                *proven_vertex_dep = Some(shared.clone());
            }

            dependencies.push(shared);
        }
    }

    async fn is_unique_in_loc(
        point: &Point,
        loc_data: Option<(bool, Vec<DagPointFuture>)>,
    ) -> bool {
        let Some((broadcasted_bad_sig, other_versions)) = loc_data else {
            return true;
        };
        if broadcasted_bad_sig {
            return false;
        };
        let mut other_versions = other_versions.into_iter().collect::<FuturesUnordered<_>>();
        while let Some(dag_point) = other_versions.next().await {
            assert_eq!(
                dag_point.author(),
                point.data().author,
                "this method checks only points by the same author"
            );
            assert_eq!(
                dag_point.round(),
                point.round(),
                "this method checks only points at the same round"
            );
            assert_ne!(
                dag_point.digest(),
                point.digest(),
                "impossible to validate the same point multiple times concurrently"
            );
            match dag_point {
                DagPoint::Trusted(_)
                | DagPoint::Suspicious(_)
                | DagPoint::Invalid(_)
                | DagPoint::IllFormed(_) => {
                    return false;
                }
                DagPoint::NotFound(_) => {
                    // failed download is ok here:
                    // it's other point's dependency, that really may not exist
                }
            }
        }
        true
    }

    /// check only direct dependencies and location for previous point (let it jump over round)
    async fn is_valid(point: &Point, mut deps_and_prev: FuturesUnordered<DagPointFuture>) -> bool {
        // point is well-formed if we got here, so point.proof matches point.includes
        let prev_digest_in_point = point.data().prev_digest.as_ref();
        let prev_round = point.round().prev();

        // Indirect dependencies may be evicted from memory and not participate in this check,
        // but validity of direct dependencies ('links through') ensures inclusion chain is valid.
        // If point under validation is so old, that any dependency download fails,
        // it will not be referenced by the current peer anyway, and it's ok to mark it as invalid
        // until the current peer syncs its far outdated DAG (when the lag exceeds `DAG_DEPTH`).
        let anchor_trigger_id = point.anchor_id(AnchorStageRole::Trigger);
        let anchor_proof_id = point.anchor_id(AnchorStageRole::Proof);
        let anchor_trigger_link_id = point.anchor_link_id(AnchorStageRole::Trigger);
        let anchor_proof_link_id = point.anchor_link_id(AnchorStageRole::Proof);

        while let Some(dag_point) = deps_and_prev.next().await {
            if dag_point.round() == prev_round && dag_point.author() == point.data().author {
                match prev_digest_in_point {
                    Some(prev_digest_in_point) if prev_digest_in_point == dag_point.digest() => {
                        match dag_point {
                            DagPoint::Trusted(valid) | DagPoint::Suspicious(valid) => {
                                if !Self::is_proof_ok(point, &valid.info) {
                                    return false;
                                } // else ok continue
                            }
                            DagPoint::Invalid(_)
                            | DagPoint::IllFormed(_)
                            | DagPoint::NotFound(_) => {
                                // author must have skipped current point's round
                                // to clear its bad history
                                return false;
                            }
                        }
                    }
                    Some(_) | None => {
                        #[allow(clippy::match_same_arms)] // for comments
                        match dag_point {
                            DagPoint::Trusted(_) | DagPoint::Suspicious(_) => {
                                // Some: point must have named _this_ point in `prev_digest`
                                // None: point must have filled `prev_digest` and `includes`
                                return false;
                            }
                            DagPoint::Invalid(_) | DagPoint::IllFormed(_) => {
                                // Some: point must have named _this_ point in `prev_digest`,
                                //       just to be invalid for an invalid dependency
                                // None: author must have skipped current point's round
                                return false;
                            }
                            DagPoint::NotFound(_) => {
                                // failed download is ok for both Some and None:
                                // it's other point's dependency, that really may not exist
                            }
                        }
                    }
                }
            } else {
                match dag_point {
                    DagPoint::Trusted(valid) | DagPoint::Suspicious(valid) => {
                        if valid.info.anchor_round(AnchorStageRole::Trigger)
                            > anchor_trigger_id.round
                            || valid.info.anchor_round(AnchorStageRole::Proof)
                                > anchor_proof_id.round
                        {
                            // did not actualize the chain
                            return false;
                        }
                        let valid_point_id = valid.info.id();
                        if ({
                            valid_point_id == anchor_trigger_link_id
                                && valid.info.anchor_id(AnchorStageRole::Trigger)
                                    != anchor_trigger_id
                        }) || ({
                            valid_point_id == anchor_proof_link_id
                                && valid.info.anchor_id(AnchorStageRole::Proof) != anchor_proof_id
                        }) {
                            // path does not lead to destination
                            return false;
                        }
                        if valid_point_id == anchor_proof_link_id
                            && valid.info.data().anchor_time != point.data().anchor_time
                        {
                            // anchor candidate's time is not inherited from its proof
                            return false;
                        }
                    }
                    DagPoint::Invalid(_) | DagPoint::IllFormed(_) | DagPoint::NotFound(_) => {
                        return false; // just invalid dependency
                    }
                }
            }
        }
        true
    }

    /// blame author and every dependent point's author
    fn is_peer_usage_ok(
        point: &Point, // @ r+0
        peer_schedule: &PeerSchedule,
    ) -> bool {
        if point.round() == MempoolConfig::GENESIS_ROUND {
            // `is_well_formed()` ensured that genesis has empty includes, witness and evidence
            return true;
        }
        let [
            witness_peers/* @ r-2 */ ,
            includes_peers /* @ r-1 */ ,
            proof_peers /* @ r+0 */
        ] = peer_schedule.atomic().peers_for_array([
                point.round().prev().prev(),
                point.round().prev(),
                point.round(),
            ]);
        for peer_id in point.data().witness.keys() {
            if !witness_peers.contains(peer_id) {
                return false;
            }
        }
        let peer_count = if point.round().prev() == MempoolConfig::GENESIS_ROUND {
            PeerCount::GENESIS
        } else {
            match PeerCount::try_from(proof_peers.len()) {
                Ok(peer_count) => peer_count,
                // reject the point in case we don't know validators for its round
                Err(_) => return false,
            }
        };
        if point.data().includes.len() < peer_count.majority() {
            return false;
        };
        for peer_id in point.data().includes.keys() {
            if !includes_peers.contains(peer_id) {
                return false;
            }
        }
        let Some(evidence /* @ r-1 */) = point.evidence() else {
            return true;
        };
        // Every point producer @ r-1 must prove its delivery to 2/3 signers @ r+0
        // inside proving point @ r+0.

        let Ok(peer_count) = PeerCount::try_from(proof_peers.len()) else {
            return false;
        };
        if evidence.len() < peer_count.majority_of_others() {
            return false;
        }
        for (peer_id, _) in evidence.iter() {
            if !proof_peers.contains(peer_id) {
                return false;
            }
        }
        true
    }

    /// blame author and every dependent point's author
    fn is_proof_ok(
        point: &Point,      // @ r+0
        proven: &PointInfo, // @ r-1
    ) -> bool {
        assert_eq!(
            point.data().author,
            proven.data().author,
            "Coding error: mismatched authors of proof and its vertex"
        );
        assert_eq!(
            point.round().prev(),
            proven.round(),
            "Coding error: mismatched rounds of proof and its vertex"
        );
        let prev_digest = point
            .data()
            .prev_digest
            .as_ref()
            .expect("Coding error: passed point doesn't contain proof for a given vertex");
        assert_eq!(
            prev_digest, proven.digest(),
            "Coding error: mismatched previous point of the same author, must have been checked before"
        );
        if point.data().time < proven.data().time {
            // time must be non-decreasing by the same author
            return false;
        }
        if point.data().anchor_proof == Link::ToSelf
            && point.data().anchor_time != proven.data().time
        {
            // anchor proof must inherit its candidate's time
            return false;
        }
        true
    }
}

impl ValidateContext {
    const KIND: &'static str = "kind";

    const VERIFY_LABELS: [(&'static str, &'static str); 2] =
        [(Self::KIND, "bad_sig"), (Self::KIND, "ill_formed")];

    const VALIDATE_LABELS: [(&'static str, &'static str); 4] = [
        (Self::KIND, "not_found"),
        (Self::KIND, "ill_formed"),
        (Self::KIND, "invalid"),
        (Self::KIND, "suspicious"),
    ];

    fn verified(result: &Result<(), VerifyError>) {
        let (labels, count) = match result {
            Err(VerifyError::BadSig) => (&Self::VERIFY_LABELS[0..=0], 1),
            Err(VerifyError::IllFormed) => (&Self::VERIFY_LABELS[1..=1], 1),
            Ok(_) => (&Self::VERIFY_LABELS[..], 0),
        };
        metrics::counter!("tycho_mempool_verifier_verify", labels).increment(count);
    }

    fn validated(result: DagPoint) -> DagPoint {
        let (labels, count) = match result {
            DagPoint::NotFound(_) => (&Self::VALIDATE_LABELS[0..=0], 1),
            DagPoint::IllFormed(_) => (&Self::VALIDATE_LABELS[1..=1], 1),
            DagPoint::Invalid(_) => (&Self::VALIDATE_LABELS[2..=2], 1),
            DagPoint::Suspicious(_) => (&Self::VALIDATE_LABELS[3..=3], 1),
            DagPoint::Trusted(_) => (&Self::VALIDATE_LABELS[..], 0),
        };
        metrics::counter!("tycho_mempool_verifier_validate", labels).increment(count);
        result
    }
}
