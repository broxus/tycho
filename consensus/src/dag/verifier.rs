use std::sync::Arc;

use futures_util::FutureExt;
use tokio::task::JoinSet;
use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::{DagRound, WeakDagRound};
use crate::engine::MempoolConfig;
use crate::intercom::{Downloader, PeerSchedule};
use crate::models::{
    DagPoint, Digest, Link, LinkField, Location, NodeCount, Point, PointId, Ugly, ValidPoint,
};

// Note on equivocation.
// Detected point equivocation does not invalidate the point, it just
// prevents us (as a fair actor) from returning our signature to the author.
// Such a point may be included in our next "includes" or "witnesses",
// but neither its inclusion nor omitting is required: as we don't
// return our signature, our dependencies cannot be validated against it.
// Equally, we immediately stop communicating with the equivocating node,
// without invalidating any of its points (no matter historical or future).
// We will not sign the proof for equivocated point
// as we've banned the author on network layer.
// Anyway, no more than one of equivocated points may become a vertex.

pub struct Verifier;

impl Verifier {
    // FIXME outside, for points to sign only: check time bounds before validation, sign only Trusted
    // FIXME shallow verification during sync to close a gap, trusting first vertex contents:
    //       take any vertex and its proof point, check signatures for the vertex,
    //       and use all vertex dependencies recursively as Trusted without any checks
    //       with 3-rounds-wide sliding window that moves backwards

    /// the first and mandatory check of any Point received no matter where from
    pub fn verify(point: &Arc<Point>, peer_schedule: &PeerSchedule) -> Result<(), DagPoint> {
        if !point.is_integrity_ok() {
            Err(DagPoint::NotExists(Arc::new(point.id()))) // cannot use point body
        } else if !(point.is_well_formed() && Self::is_list_of_signers_ok(point, peer_schedule)) {
            // the last task spawns if ok - in order not to walk through every dag round twice
            Err(DagPoint::Invalid(point.clone()))
        } else {
            Ok(())
        }
    }

    /// must be called iff [Self::verify] succeeded
    pub async fn validate(
        point: Arc<Point>, // @ r+0
        r_0: WeakDagRound, // r+0
        downloader: Downloader,
    ) -> DagPoint {
        let Some(r_0) = r_0.get() else {
            tracing::warn!(
                "cannot validate {:?}, local DAG moved far forward",
                point.id().ugly()
            );
            return DagPoint::NotExists(Arc::new(point.id()));
        };
        // TODO upgrade Weak whenever used to let Dag Round drop if some future hangs up for long
        if &point.body.location.round != r_0.round() {
            panic!("Coding error: dag round mismatches point round")
        }

        let mut dependencies = JoinSet::new();
        if !({
            Self::is_self_links_ok(&point, &r_0)
                // the last task spawns if ok - in order not to walk through every dag round twice
                && Self::add_anchor_links_if_ok(&point, &r_0, &downloader, &mut dependencies)
        }) {
            return DagPoint::Invalid(point.clone());
        }
        let Some(r_1) = r_0.prev().get() else {
            // If r-1 exceeds dag depth, the arg point @ r+0 is considered valid by itself.
            // Any point @ r+0 will be committed, only if it has valid proof @ r+1
            //   included into valid anchor chain, i.e. validated by consensus.
            return DagPoint::Trusted(ValidPoint::new(point.clone()));
        };
        Self::gather_deps(&point, &r_1, &downloader, &mut dependencies);
        // drop strong links before await
        _ = r_0;
        _ = r_1;
        Self::check_deps(&point, dependencies).await
    }

    fn is_self_links_ok(
        point: &Point,        // @ r+0
        dag_round: &DagRound, // r+0
    ) -> bool {
        // existence of proofs in leader points is a part of point's well-form-ness check
        match &dag_round.anchor_stage() {
            // no one may link to self
            None => {
                (point.body.anchor_proof != Link::ToSelf
                    && point.body.anchor_trigger != Link::ToSelf)
                    || point.body.location.round == MempoolConfig::GENESIS_ROUND
            }
            // leader must link to own point while others must not
            Some(AnchorStage::Proof { leader, .. }) => {
                (leader == point.body.location.author) == (point.body.anchor_proof == Link::ToSelf)
            }
            Some(AnchorStage::Trigger { leader, .. }) => {
                (leader == point.body.location.author)
                    == (point.body.anchor_trigger == Link::ToSelf)
            }
        }
    }

    /// may visit every DAG round kept in memory
    fn add_anchor_links_if_ok(
        point: &Point,        // @ r+0
        dag_round: &DagRound, // start with r+0
        downloader: &Downloader,
        dependencies: &mut JoinSet<DagPoint>,
    ) -> bool {
        let mut links = vec![
            (point.anchor_id(LinkField::Proof), LinkField::Proof),
            (point.anchor_id(LinkField::Trigger), LinkField::Trigger),
        ];
        let mut linked_with_round = Vec::with_capacity(2);
        let mut dag_round = dag_round.clone();
        while !links.is_empty() {
            links.retain(|(linked, link_field)| {
                let found = &linked.location.round == dag_round.round();
                if found {
                    match (&dag_round.anchor_stage(), link_field) {
                        // AnchorStage::Candidate(_) requires nothing special
                        (Some(AnchorStage::Proof { leader, .. }), LinkField::Proof)
                            if leader == linked.location.author => {}
                        (Some(AnchorStage::Trigger { leader, .. }), LinkField::Trigger)
                            if leader == linked.location.author => {}
                        _ => return false, // link not to round's leader
                    }
                    linked_with_round.push((
                        linked.location.author.clone(),
                        linked.digest.clone(),
                        dag_round.clone(),
                    ));
                }
                !found
            });
            match dag_round.prev().get() {
                Some(r) => dag_round = r,
                None => {
                    // if links in point exceed DAG depth, consider them valid by now;
                    // either dependencies have more recent link and point will be invalidated later,
                    // or author was less successful to get fresh data and did not commit for long
                    // (thus keeps more history in its local Dag)
                    break;
                }
            }
        }
        // valid linked points will be in dag without this addition by recursion,
        // while we need to get invalid ones to blame current point
        for (author, digest, dag_round) in linked_with_round {
            // skip self links
            if dag_round.round() < &point.body.location.round {
                // TODO will add the same point from direct dependencies twice,
                // we can do better but nothing terrible
                Self::add_dependency(
                    &author,
                    &digest,
                    &dag_round,
                    &point.body.location.author,
                    downloader,
                    dependencies,
                );
            }
        }
        true
    }

    fn add_dependency(
        author: &PeerId,
        digest: &Digest,
        round: &DagRound,
        dependant: &PeerId,
        downloader: &Downloader,
        dependencies: &mut JoinSet<DagPoint>,
    ) {
        let downloader = downloader.clone();
        let shared = round.edit(author, |loc| {
            loc.get_or_init(digest, move || {
                let point_id = PointId {
                    location: Location {
                        author: author.clone(),
                        round: round.round().clone(),
                    },
                    digest: digest.clone(),
                };
                downloader.run(point_id, round.as_weak(), dependant.clone())
            })
        });
        dependencies.spawn(shared.map(|(dag_point, _)| dag_point));
    }

    fn gather_deps(
        point: &Point,  // @ r+0
        r_1: &DagRound, // r-1
        downloader: &Downloader,
        dependencies: &mut JoinSet<DagPoint>,
    ) {
        let author = &point.body.location.author;
        r_1.view(author, |loc| {
            for (_, shared) in loc.versions() {
                dependencies.spawn(shared.clone().map(|(dag_point, _)| dag_point));
            }
        });
        for (node, digest) in &point.body.includes {
            // integrity check passed, so includes contain author's prev point proof
            Self::add_dependency(&node, &digest, &r_1, author, downloader, dependencies);
        }

        if let Some(r_2) = r_1.prev().get() {
            for (node, digest) in &point.body.witness {
                Self::add_dependency(&node, &digest, &r_2, author, downloader, dependencies);
            }
        };
    }

    async fn check_deps(point: &Arc<Point>, mut dependencies: JoinSet<DagPoint>) -> DagPoint {
        // point is well-formed if we got here, so point.proof matches point.includes
        let proven_vertex = point.body.proof.as_ref().map(|p| &p.digest).clone();
        let prev_loc = Location {
            round: point.body.location.round.prev(),
            author: point.body.location.author,
        };

        // The node must have no points in previous location
        //   in case it provide no proof for previous point.
        // But equivocation does not invalidate the point.
        // Invalid dependency is the author's fault.
        let mut is_suspicious = false;
        // last is meant to be the last among all dependencies
        let anchor_trigger_id = point.anchor_id(LinkField::Trigger);
        let anchor_proof_id = point.anchor_id(LinkField::Proof);
        let anchor_trigger_link_id = point.anchor_link_id(LinkField::Trigger);
        let anchor_proof_link_id = point.anchor_link_id(LinkField::Proof);
        while let Some(res) = dependencies.join_next().await {
            match res {
                Ok(DagPoint::Trusted(valid) | DagPoint::Suspicious(valid)) => {
                    if prev_loc == valid.point.body.location {
                        match proven_vertex {
                            Some(vertex_digest) if &valid.point.digest == vertex_digest => {
                                if !Self::is_proof_ok(&point, &valid.point) {
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
                    if valid_point_id == anchor_proof_id && point.body.time < valid.point.body.time
                    {
                        // Any point that includes anchor candidate through its proof
                        // must provide the time not less than candidate's to maintain
                        // non-decreasing time in committed anchor chain.
                        // The time of candidate's valid proof exactly satisfies such requirement:
                        // it either will be signed by majority (that unblocks the commit trigger),
                        // or the valid trigger will not be created.
                        // FIXME better use the time from the proven anchor candidate -
                        //   though it's an additional dependency during validation,
                        //   it is proven and can't be manipulated
                        //   (i.e. do not rely on anything unless 2F+1 signatures provided)
                        return DagPoint::Invalid(point.clone());
                    }
                }
                Ok(DagPoint::Invalid(invalid)) => {
                    if prev_loc == invalid.body.location {
                        match proven_vertex {
                            Some(vertex_digest) if &invalid.digest == vertex_digest => {
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
                Ok(DagPoint::NotExists(not_exists)) => {
                    if prev_loc == not_exists.location {
                        match proven_vertex {
                            Some(vertex_digest) if &not_exists.digest == vertex_digest => {
                                return DagPoint::Invalid(point.clone())
                            }
                            _ => {} // dependency of some other point; we've banned that sender
                        }
                    } else {
                        return DagPoint::Invalid(point.clone()); // just invalid dependency
                    }
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
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
        if point.body.location.round == MempoolConfig::GENESIS_ROUND {
            return true; // all maps are empty for a well-formed genesis
        }
        let [
            witness_peers/* @ r-2 */ ,
            includes_peers /* @ r-1 */ ,
            proof_peers /* @ r+0 */
        ] = peer_schedule.peers_for_array([
                point.body.location.round.prev().prev(),
                point.body.location.round.prev(),
                point.body.location.round.clone(),
            ]);
        for (peer_id, _) in point.body.witness.iter() {
            if !witness_peers.contains_key(peer_id) {
                return false;
            }
        }
        let node_count = NodeCount::new(includes_peers.len());
        if point.body.includes.len() < node_count.majority() {
            return false;
        };
        for (peer_id, _) in point.body.includes.iter() {
            if !includes_peers.contains_key(peer_id) {
                return false;
            }
        }
        let Some(proven /* @ r-1 */) = &point.body.proof else {
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
        if point.body.location.author != proven.body.location.author {
            panic!("Coding error: mismatched authors of proof and its vertex")
        }
        if point.body.location.round.prev() != proven.body.location.round {
            panic!("Coding error: mismatched rounds of proof and its vertex")
        }
        let Some(proof) = &point.body.proof else {
            panic!("Coding error: passed point doesn't contain proof for a given vertex")
        };
        if proof.digest != proven.digest {
            panic!("Coding error: mismatched previous point of the same author")
        }
        if point.body.time < proven.body.time {
            return false; // time must be non-decreasing by the same author
        }
        for (peer, sig) in proof.evidence.iter() {
            if !sig.verifies(peer, &proof.digest) {
                return false;
            }
        }
        true
    }
}
