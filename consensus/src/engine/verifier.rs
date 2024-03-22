use std::sync::Arc;

use futures_util::future;
use futures_util::FutureExt;
use tokio::task::JoinSet;

use tycho_network::PeerId;
use tycho_util::futures::{JoinTask, Shared};

use crate::engine::dag::{AnchorStage, DagPoint, DagRound, IndexedPoint};
use crate::engine::node_count::NodeCount;
use crate::engine::peer_schedule::PeerSchedule;
use crate::models::point::{Digest, Link, Location, Point};
use crate::tasks::downloader::DownloadTask;

/*
Note on equivocation.
Detected point equivocation does not invalidate the point, it just
  prevents us (as a fair actor) from returning our signature to the author.
Such a point may be included in our next "includes" or "witnesses",
  but neither its inclusion nor omitting is required: as we don't
  return our signature, our dependencies cannot be validated against it.
Equally, we immediately stop communicating with the equivocating node,
  without invalidating any of its points (no matter historical or future).
We will not sign the proof for equivocated point
  as we've banned the author on network layer.
Anyway, no more than one of equivocated points may become a vertex.
*/

pub struct Verifier;

impl Verifier {
    // todo outside, for points to sign only: check time bounds before validation, sign only Trusted
    // todo: shallow verification during sync to close a gap, trusting first vertex contents:
    //       take any vertex and its proof point, check signatures for the vertex,
    //       and use all vertex dependencies recursively as Trusted without any checks
    //       with 3-rounds-wide sliding window that moves backwards

    pub fn verify(
        r_0 /* r+0 */: Arc<DagRound>,
        point /* @ r+0 */: Box<Point>,
        peer_schedule: &PeerSchedule,
    ) -> JoinTask<DagPoint> {
        if &point.body.location.round != &r_0.round {
            panic! {"Coding error: dag round mismatches point round"}
        }
        if !point.is_integrity_ok() {
            let not_exists = DagPoint::NotExists(Arc::new(point.id())); // cannot use point body
            return JoinTask::new(future::ready(not_exists));
        }
        let mut dependencies = JoinSet::new();
        if !({
            point.is_well_formed()
                && Self::is_self_links_ok(&point, &r_0)
                && Self::is_list_of_signers_ok(&point, peer_schedule)
                // the last task spawns if ok - in order not to walk through every dag round twice
                && Self::add_anchor_links_if_ok(&point, r_0.clone(), &mut dependencies)
        }) {
            let invalid = DagPoint::Invalid(Arc::new(*point));
            return JoinTask::new(future::ready(invalid));
        }
        if let Some(r_1) = r_0.prev.upgrade() {
            Self::gather_deps(&point, &r_1, &mut dependencies);
            return JoinTask::new(Self::check_deps(point, dependencies));
        }
        // If r-1 exceeds dag depth, the arg point @ r+0 is considered valid by itself.
        // Any point @ r+0 will be committed, only if it has valid proof @ r+1
        //   included into valid anchor chain, i.e. validated by consensus.
        let trusted = DagPoint::Trusted(Arc::new(IndexedPoint::new(*point)));
        JoinTask::new(future::ready(trusted))
    }

    fn is_self_links_ok(point /* @ r+0 */: &Point, dag_round /* r+0 */: &DagRound) -> bool {
        // existence of proofs in leader blocks is a part of point's well-form-ness check
        match &dag_round.anchor_stage {
            // no one may link to self
            None | Some(AnchorStage::Candidate(_)) => {
                point.body.last_anchor_proof != Link::ToSelf
                    && point.body.last_anchor_trigger != Link::ToSelf
            }
            // leader must link to own point while others must not
            Some(AnchorStage::Proof(leader_id)) => {
                (leader_id == point.body.location.author)
                    == (point.body.last_anchor_proof == Link::ToSelf)
            }
            Some(AnchorStage::Trigger(leader_id)) => {
                (leader_id == point.body.location.author)
                    == (point.body.last_anchor_trigger == Link::ToSelf)
            }
        }
    }

    /// may visit every DAG round kept in memory
    fn add_anchor_links_if_ok(
        point: &Point,                // @ r+0
        mut dag_round: Arc<DagRound>, // start with r+0
        dependencies: &mut JoinSet<DagPoint>,
    ) -> bool {
        let mut links = vec![
            (point.last_anchor_proof_id(), false),
            (point.last_anchor_trigger_id(), true),
        ];
        let mut linked_with_round = Vec::with_capacity(2);
        while !links.is_empty() {
            links.retain(|(linked, is_trigger)| {
                let found = linked.location.round == dag_round.round;
                if found {
                    match (&dag_round.anchor_stage, is_trigger) {
                        // AnchorStage::Candidate(_) requires nothing special
                        (Some(AnchorStage::Proof(leader_id)), false)
                            if leader_id == linked.location.author => {}
                        (Some(AnchorStage::Trigger(leader_id)), true)
                            if leader_id == linked.location.author => {}
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
            if dag_round.prev.upgrade().map(|r| dag_round = r).is_none() {
                // if links in point exceed DAG depth, consider them valid by now;
                // either dependencies have more recent link and point will be invalidated later,
                // or author was less successful to get fresh data and did not commit for long
                // (thus keeps more history in its local Dag)
                break;
            }
        }
        // valid linked points will be in dag without this addition by recursion,
        // while we need to get invalid ones to blame current point
        for (author, digest, dag_round) in linked_with_round {
            // skip self links
            if dag_round.round < point.body.location.round {
                // will add the same point from direct dependencies twice,
                // we can do better but nothing terrible
                Self::add_dependency(&author, &digest, &dag_round, dependencies);
            }
        }
        true
    }

    fn add_dependency(
        node: &PeerId,
        digest: &Digest,
        round: &DagRound,
        dependencies: &mut JoinSet<DagPoint>,
    ) {
        let mut loc = round.locations.entry(*node).or_default();
        let fut = loc
            .versions
            .entry(digest.clone())
            .or_insert_with(|| Shared::new(JoinTask::new(DownloadTask {})))
            .clone();
        dependencies.spawn(fut.map(|a| a.0));
    }

    fn gather_deps(
        point /* @ r+0 */: &Point,
        r_1 /* r-1 */: &DagRound,
        dependencies: &mut JoinSet<DagPoint>,
    ) {
        if let Some(loc) = r_1.locations.get(&point.body.location.author) {
            // to check for equivocation or mandatory skip of a round
            for version in loc.versions.values() {
                dependencies.spawn(version.clone().map(|a| a.0));
            }
        }
        for (node, digest) in &point.body.includes {
            // integrity check passed, so includes contain author's prev point proof
            Self::add_dependency(&node, &digest, &r_1, dependencies);
        }

        if let Some(r_2) = r_1.prev.upgrade() {
            for (node, digest) in &point.body.witness {
                Self::add_dependency(&node, &digest, &r_2, dependencies);
            }
        };
    }

    async fn check_deps(point: Box<Point>, mut dependencies: JoinSet<DagPoint>) -> DagPoint {
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
        let anchor_trigger_id = point.last_anchor_trigger_id();
        let anchor_proof_id = point.last_anchor_proof_id();
        let anchor_trigger_through = point.last_anchor_trigger_through();
        let anchor_proof_through = point.last_anchor_proof_through();
        while let Some(res) = dependencies.join_next().await {
            match res {
                Ok(DagPoint::Trusted(valid) | DagPoint::Suspicious(valid)) => {
                    if prev_loc == valid.point.body.location {
                        match proven_vertex {
                            Some(vertex_digest) if &valid.point.digest == vertex_digest => {
                                if !Self::is_proof_ok(&point, &valid.point) {
                                    return DagPoint::Invalid(Arc::new(*point));
                                } // else: ok proof
                            }
                            Some(_) => is_suspicious = true, // equivocation
                            // the author must have provided the proof in current point
                            None => return DagPoint::Invalid(Arc::new(*point)),
                        }
                    } // else: valid dependency
                    if valid.point.last_anchor_trigger_round() > anchor_trigger_id.location.round
                        || valid.point.last_anchor_proof_round() > anchor_proof_id.location.round
                    {
                        // did not actualize the chain
                        return DagPoint::Invalid(Arc::new(*point));
                    }
                    let valid_point_id = valid.point.id();
                    if ({
                        valid_point_id == anchor_trigger_through
                            && valid.point.last_anchor_trigger_id() != anchor_trigger_id
                    }) || ({
                        valid_point_id == anchor_proof_through
                            && valid.point.last_anchor_proof_id() != anchor_proof_id
                    }) {
                        // path does not lead to destination
                        return DagPoint::Invalid(Arc::new(*point));
                    }
                    if valid_point_id == anchor_proof_id && point.body.time < valid.point.body.time
                    {
                        // Any point that (in)directly includes anchor candidate through its proof
                        // must provide the time not less than candidate's to maintain
                        // non-decreasing time in committed anchor chain.
                        // The time of candidate's valid proof exactly satisfies such requirement:
                        // it either will be signed by majority (what unblocks the commit trigger),
                        // or the valid trigger will not be created.
                        return DagPoint::Invalid(Arc::new(*point));
                    }
                }
                Ok(DagPoint::Invalid(invalid)) => {
                    if prev_loc == invalid.body.location {
                        match proven_vertex {
                            Some(vertex_digest) if &invalid.digest == vertex_digest => {
                                return DagPoint::Invalid(Arc::new(*point))
                            }
                            Some(_) => is_suspicious = true, // equivocation
                            // the author must have skipped previous round
                            None => return DagPoint::Invalid(Arc::new(*point)),
                        }
                    } else {
                        return DagPoint::Invalid(Arc::new(*point)); // just invalid dependency
                    }
                }
                Ok(DagPoint::NotExists(not_exists)) => {
                    if prev_loc == not_exists.location {
                        match proven_vertex {
                            Some(vertex_digest) if &not_exists.digest == vertex_digest => {
                                return DagPoint::Invalid(Arc::new(*point))
                            }
                            _ => {} // dependency of some other point; we've banned that sender
                        }
                    } else {
                        return DagPoint::Invalid(Arc::new(*point)); // just invalid dependency
                    }
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                    unreachable!();
                }
            }
        }
        if is_suspicious {
            DagPoint::Suspicious(Arc::new(IndexedPoint::new(*point)))
        } else {
            DagPoint::Trusted(Arc::new(IndexedPoint::new(*point)))
        }
    }

    /// blame author and every dependent point's author
    fn is_list_of_signers_ok(point /* @ r+0 */: &Point, peer_schedule: &PeerSchedule) -> bool {
        let Some(proof /* @ r-1 */) = &point.body.proof else {
            return true;
        };
        let [
            same_round_peers/* @ r-1 */,
            next_round_peers/* @ r+0 */
        ] = peer_schedule.peers_for_array([
                point.body.location.round.prev(),
                point.body.location.round
            ]);
        //TODO may there be a problem ?
        // the size of required validator set is determined by point's round,
        // but if the next round is a new epoch start, amount of available signers may change

        // may include author's signature already contained in proven point, no matter
        if proof.evidence.len() < NodeCount::new(same_round_peers.len()).into() {
            return false;
        }

        for (peer, _) in proof.evidence.iter() {
            if !(same_round_peers.contains_key(peer) || next_round_peers.contains_key(peer)) {
                // two validator sets are the same except the first round of a new epoch;
                // unexpected peer, thus invalid
                return false;
            }
        }
        true
    }

    /// blame author and every dependent point's author
    fn is_proof_ok(point /* @ r+0 */: &Point, proven: &Point /* @ r-1 */) -> bool {
        if point.body.location.author != proven.body.location.author {
            unreachable! {"Coding error: mismatched authors of proof and its vertex"}
        }
        if point.body.location.round.prev() != proven.body.location.round {
            unreachable! {"Coding error: mismatched rounds of proof and its vertex"}
        }
        let Some(proof) = &point.body.proof else {
            unreachable! {"Coding error: passed point doesn't contain proof for a given vertex"}
        };
        if proof.digest != proven.digest {
            unreachable! {"Coding error: mismatched previous point of the same author"}
        }
        if point.body.time < proven.body.time {
            return false; // time must be non-decreasing by the same author
        }
        let Some(body) = bincode::serialize(&proven.body).ok() else {
            // should be removed after move to TL
            unreachable! {"Library error: failed to serialize point body"}
        };
        for (peer, sig) in proof.evidence.iter() {
            let Some(pubkey) = peer.as_public_key() else {
                // should have been validated outside mempool
                unreachable! {"Config error: failed to convert peer id into public key"}
            };
            let sig: Result<[u8; 64], _> = sig.0.to_vec().try_into();
            let Some(sig) = sig.ok() else {
                // unexpected bytes used as a signature, thus invalid
                return false;
            };
            if !pubkey.verify_raw(body.as_slice(), &sig) {
                return false;
            }
        }
        true
    }
}
