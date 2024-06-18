use std::sync::Arc;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tracing::{Instrument, Span};
use tycho_util::sync::rayon_run;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::{DagPointFuture, DagRound, WeakDagRound};
use crate::effects::{AltFormat, Effects, EffectsContext, ValidateContext};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{DagPoint, Link, LinkField, Location, NodeCount, Point, ValidPoint};

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

// If any round exceeds dag depth, the arg point @ r+0 is considered valid by itself.
// Any point @ r+0 will be committed, only if it has valid proof @ r+1
// included into valid anchor chain, i.e. validated by consensus.

impl Verifier {
    /// the first and mandatory check of any Point received no matter where from
    pub fn verify(point: &Point, peer_schedule: &PeerSchedule) -> Result<(), DagPoint> {
        if !point.is_integrity_ok() {
            Err(DagPoint::NotExists(Arc::new(point.id()))) // cannot use point body
        } else if !(point.is_well_formed() && Self::is_list_of_signers_ok(point, peer_schedule)) {
            // point links, etc. will not be used
            Err(DagPoint::Invalid(point.clone()))
        } else {
            Ok(())
        }
    }

    /// must be called iff [`Self::verify`] succeeded
    pub async fn validate(
        point: Point,      // @ r+0
        r_0: WeakDagRound, // r+0
        downloader: Downloader,
        parent_span: Span,
    ) -> DagPoint {
        let effects = Effects::<ValidateContext>::new(&parent_span, &point);
        let span_guard = effects.span().enter();

        // for genesis point it's sufficient to be well-formed and pass integrity check,
        // it cannot be validated against AnchorStage (as it knows nothing about genesis)
        // and cannot contain dependencies
        assert!(
            point.body().location.round > MempoolConfig::GENESIS_ROUND,
            "Coding error: can only validate points older than genesis"
        );
        let Some(r_0) = r_0.upgrade() else {
            tracing::info!("cannot (in)validate point, no round in local DAG");
            return DagPoint::Suspicious(ValidPoint::new(point.clone()));
        };
        assert_eq!(
            point.body().location.round,
            r_0.round(),
            "Coding error: dag round mismatches point round"
        );

        let dependencies = FuturesUnordered::new();

        if !(Self::is_self_links_ok(&point, &r_0)
            && Self::add_anchor_link_if_ok(
                LinkField::Proof,
                &point,
                &r_0,
                &downloader,
                &effects,
                &dependencies,
            )
            && Self::add_anchor_link_if_ok(
                LinkField::Trigger,
                &point,
                &r_0,
                &downloader,
                &effects,
                &dependencies,
            ))
        {
            return DagPoint::Invalid(point.clone());
        }

        let Some(r_1) = r_0.prev().upgrade() else {
            tracing::info!("cannot (in)validate point's 'includes', no round in local DAG");
            return DagPoint::Suspicious(ValidPoint::new(point.clone()));
        };
        Self::gather_deps(&point, &r_1, &downloader, &effects, &dependencies);

        // drop strong links before await
        drop(r_0);
        drop(r_1);
        drop(span_guard);

        let mut signatures_fut = std::pin::pin!(match point.body().proof.as_ref() {
            None => futures_util::future::Either::Left(futures_util::future::ready(true)),
            Some(proof) => futures_util::future::Either::Right(
                rayon_run({
                    let proof = proof.clone();
                    move || proof.signatures_match()
                })
                .instrument(effects.span().clone()),
            ),
        });
        let mut check_deps_fut = std::pin::pin!(
            Self::check_deps(&point, dependencies).instrument(effects.span().clone())
        );

        let mut sig_checked = false;
        let mut deps_checked = None;
        loop {
            tokio::select! {
                is_sig_ok = &mut signatures_fut, if !sig_checked => if is_sig_ok {
                    match deps_checked {
                        None => sig_checked = true,
                        Some(result) => break result
                    }
                } else {
                    break DagPoint::Invalid(point.clone())
                },
                dag_point = &mut check_deps_fut, if deps_checked.is_none() => {
                    if sig_checked || dag_point.valid().is_none() {
                        // either invalid or signature check passed
                        // this cancels `rayon_run` task as receiver is dropped
                        break dag_point;
                    } else {
                        deps_checked = Some(dag_point);
                    }
                }
            }
        }
    }

    fn is_self_links_ok(
        point: &Point,        // @ r+0
        dag_round: &DagRound, // r+0
    ) -> bool {
        // existence of proofs in leader points is a part of point's well-form-ness check
        (match &dag_round.anchor_stage() {
            // no one may link to self
            None => {
                point.body().anchor_proof != Link::ToSelf
                    && point.body().anchor_trigger != Link::ToSelf
            }
            // leader must link to own point while others must not
            Some(AnchorStage::Proof { leader, .. }) => {
                (leader == point.body().location.author)
                    == (point.body().anchor_proof == Link::ToSelf)
            }
            Some(AnchorStage::Trigger { leader, .. }) => {
                (leader == point.body().location.author)
                    == (point.body().anchor_trigger == Link::ToSelf)
            }
        }) || point.body().location.round == MempoolConfig::GENESIS_ROUND
    }

    /// the only method that scans the DAG deeper than 2 rounds
    fn add_anchor_link_if_ok(
        link_field: LinkField,
        point: &Point,        // @ r+0
        dag_round: &DagRound, // start with r+0
        downloader: &Downloader,
        effects: &Effects<ValidateContext>,
        dependencies: &FuturesUnordered<DagPointFuture>,
    ) -> bool {
        let linked_id = point.anchor_id(link_field);

        let Some(round) = dag_round.scan(linked_id.location.round) else {
            // too old indirect reference does not invalidate the point,
            // because its direct dependencies ('link through') will be validated anyway
            return true;
        };

        if round.round() == MempoolConfig::GENESIS_ROUND {
            // notice that point is required to link to the freshest leader point
            // among all its (in)direct dependencies, which is checked later
            return linked_id == crate::test_utils::genesis_point_id();
        }

        match (round.anchor_stage(), link_field) {
            (Some(AnchorStage::Proof { leader, .. }), LinkField::Proof)
            | (Some(AnchorStage::Trigger { leader, .. }), LinkField::Trigger)
                if leader == linked_id.location.author => {}
            _ => {
                // link does not match round's leader, prescribed by AnchorStage
                return false;
            }
        };

        #[allow(clippy::match_same_arms)]
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
                    &linked_id.location.author,
                    &linked_id.digest,
                    &point.body().location.author,
                    downloader,
                    effects,
                ));
            }
        };

        true
    }

    fn gather_deps(
        point: &Point,  // @ r+0
        r_1: &DagRound, // r-1
        downloader: &Downloader,
        effects: &Effects<ValidateContext>,
        dependencies: &FuturesUnordered<DagPointFuture>,
    ) {
        r_1.view(&point.body().location.author, |loc| {
            // to check for equivocation
            let prev_digest = point.body().proof.as_ref().map(|p| &p.digest);
            for (digest, shared) in loc.versions() {
                if prev_digest.as_ref().map_or(true, |prev| *prev != digest) {
                    dependencies.push(shared.clone());
                }
            }
        });

        for (author, digest) in &point.body().includes {
            // integrity check passed, so includes contain author's prev point proof
            dependencies.push(r_1.add_dependency_exact(
                author,
                digest,
                &point.body().location.author,
                downloader,
                effects,
            ));
        }
        let Some(r_2) = r_1.prev().upgrade() else {
            tracing::info!("cannot (in)validate point's 'witness', no round in local DAG");
            return;
        };
        for (author, digest) in &point.body().witness {
            dependencies.push(r_2.add_dependency_exact(
                author,
                digest,
                &point.body().location.author,
                downloader,
                effects,
            ));
        }
    }

    async fn check_deps(
        point: &Point,
        mut dependencies: FuturesUnordered<DagPointFuture>,
    ) -> DagPoint {
        // point is well-formed if we got here, so point.proof matches point.includes
        let proven_vertex = point.body().proof.as_ref().map(|p| &p.digest);
        let prev_loc = Location {
            round: point.body().location.round.prev(),
            author: point.body().location.author,
        };

        // The node must have no points in previous location
        // in case it provide no proof for previous point.
        // But equivocation does not invalidate the point.
        // Invalid dependency is the author's fault.
        let mut is_suspicious = false;

        // Indirect dependencies may be evicted from memory and not participate in this check,
        // but validity of direct dependencies ('links through') ensures inclusion chain is valid.
        // If point under validation is so old, that any dependency download fails,
        // it will not be referenced by the current peer anyway, and it's ok to mark it as invalid
        // until the current peer syncs its far outdated DAG (when the lag exceeds `DAG_DEPTH`).
        let anchor_trigger_id = point.anchor_id(LinkField::Trigger);
        let anchor_proof_id = point.anchor_id(LinkField::Proof);
        let anchor_trigger_link_id = point.anchor_link_id(LinkField::Trigger);
        let anchor_proof_link_id = point.anchor_link_id(LinkField::Proof);

        while let Some(dag_point) = dependencies.next().await {
            match dag_point {
                DagPoint::Trusted(valid) | DagPoint::Suspicious(valid) => {
                    if prev_loc == valid.point.body().location {
                        match proven_vertex {
                            Some(vertex_digest) if valid.point.digest() == vertex_digest => {
                                if !Self::is_proof_ok(point, &valid.point) {
                                    return DagPoint::Invalid(point.clone());
                                } // else: ok proof
                            }
                            Some(_) => is_suspicious = true, // equivocation
                            // the author must have provided the proof in current point
                            None => return DagPoint::Invalid(point.clone()),
                        }
                    } // else: valid dependency
                    if valid.point.anchor_round(LinkField::Trigger)
                        > anchor_trigger_id.location.round
                        || valid.point.anchor_round(LinkField::Proof)
                            > anchor_proof_id.location.round
                    {
                        // did not actualize the chain
                        return DagPoint::Invalid(point.clone());
                    }
                    let valid_point_id = valid.point.id();
                    if ({
                        valid_point_id == anchor_trigger_link_id
                            && valid.point.anchor_id(LinkField::Trigger) != anchor_trigger_id
                    }) || ({
                        valid_point_id == anchor_proof_link_id
                            && valid.point.anchor_id(LinkField::Proof) != anchor_proof_id
                    }) {
                        // path does not lead to destination
                        return DagPoint::Invalid(point.clone());
                    }
                    if valid_point_id == anchor_proof_link_id
                        && valid.point.body().anchor_time != point.body().anchor_time
                    {
                        // anchor candidate's time is not inherited from its proof
                        return DagPoint::Invalid(point.clone());
                    }
                }
                DagPoint::Invalid(invalid) => {
                    if prev_loc == invalid.body().location {
                        match proven_vertex {
                            Some(vertex_digest) if invalid.digest() == vertex_digest => {
                                return DagPoint::Invalid(point.clone())
                            }
                            Some(_) => is_suspicious = true, // equivocation
                            // the author must have skipped previous round
                            None => return DagPoint::Invalid(point.clone()),
                        }
                    } else {
                        return DagPoint::Invalid(point.clone()); // just invalid dependency
                    }
                }
                DagPoint::NotExists(not_exists) => {
                    if prev_loc == not_exists.location {
                        match proven_vertex {
                            Some(vertex_digest) if &not_exists.digest == vertex_digest => {
                                return DagPoint::Invalid(point.clone())
                            }
                            _ => {} // dependency of some other point; ban the sender
                        }
                    } else {
                        return DagPoint::Invalid(point.clone()); // just invalid dependency
                    }
                }
            }
        }

        if is_suspicious {
            DagPoint::Suspicious(ValidPoint::new(point.clone()))
        } else {
            DagPoint::Trusted(ValidPoint::new(point.clone()))
        }
    }

    /// blame author and every dependent point's author
    fn is_list_of_signers_ok(
        point: &Point, // @ r+0
        peer_schedule: &PeerSchedule,
    ) -> bool {
        let [
            witness_peers/* @ r-2 */ ,
            includes_peers /* @ r-1 */ ,
            proof_peers /* @ r+0 */
        ] = peer_schedule.peers_for_array([
                point.body().location.round.prev().prev(),
                point.body().location.round.prev(),
                point.body().location.round,
            ]);
        for (peer_id, _) in point.body().witness.iter() {
            if !witness_peers.contains_key(peer_id) {
                return false;
            }
        }
        let node_count = NodeCount::new(includes_peers.len());
        if point.body().includes.len() < node_count.majority() {
            return false;
        };
        for (peer_id, _) in point.body().includes.iter() {
            if !includes_peers.contains_key(peer_id) {
                return false;
            }
        }
        let Some(proven /* @ r-1 */) = &point.body().proof else {
            return true;
        };
        // Every point producer @ r-1 must prove its delivery to 2/3+1 producers @ r+0
        // inside proving point @ r+0.

        // If author was in validator set @ r-1 and is not in validator set @ r+0,
        // its point @ r-1 won't become a vertex because its proof point @ r+0 cannot be valid.
        // That means: payloads from the last round of validation epoch are never collated.

        // reject point in case this node is not ready to accept: the point is from far future
        let Ok(node_count) = NodeCount::try_from(proof_peers.len()) else {
            return false;
        };
        if proven.evidence.len() < node_count.majority_of_others() {
            return false;
        }
        for (peer_id, _) in proven.evidence.iter() {
            if !proof_peers.contains_key(peer_id) {
                return false;
            }
        }
        true
    }

    /// blame author and every dependent point's author
    fn is_proof_ok(
        point: &Point,  // @ r+0
        proven: &Point, // @ r-1
    ) -> bool {
        assert_eq!(
            point.body().location.author,
            proven.body().location.author,
            "Coding error: mismatched authors of proof and its vertex"
        );
        assert_eq!(
            point.body().location.round.prev(),
            proven.body().location.round,
            "Coding error: mismatched rounds of proof and its vertex"
        );
        let proof = point
            .body()
            .proof
            .as_ref()
            .expect("Coding error: passed point doesn't contain proof for a given vertex");
        assert_eq!(
            &proof.digest, proven.digest(),
            "Coding error: mismatched previous point of the same author, must have been checked before"
        );
        if point.body().time < proven.body().time {
            // time must be non-decreasing by the same author
            return false;
        }
        if point.body().anchor_proof == Link::ToSelf
            && point.body().anchor_time != proven.body().time
        {
            // anchor proof must inherit its candidate's time
            return false;
        }
        true
    }
}
impl EffectsContext for ValidateContext {}

impl Effects<ValidateContext> {
    fn new(parent_span: &Span, point: &Point) -> Self {
        Self::new_child(parent_span, || {
            tracing::error_span!(
                "validate",
                author = display(point.body().location.author.alt()),
                round = point.body().location.round.0,
                digest = display(point.digest().alt()),
            )
        })
    }
}
