use std::sync::Arc;

use futures_util::future;
use futures_util::FutureExt;
use tokio::task::JoinSet;

use tycho_network::PeerId;
use tycho_util::futures::{JoinTask, Shared};

use crate::engine::dag::{DagPoint, DagRound, IndexedPoint};
use crate::engine::peer_schedule::PeerSchedule;
use crate::models::point::{Digest, Location, Point};
use crate::tasks::downloader::DownloadTask;

pub struct Verifier {
    peer_schedule: PeerSchedule,
}

impl Verifier {
    pub fn verify(
        &self,
        r_0 /* r+0 */: &DagRound,
        point /* @ r+0 */: Box<Point>,
    ) -> JoinTask<DagPoint> {
        if &point.body.location.round != &r_0.round {
            panic! {"Coding error: dag round mismatches point round"}
        }
        if !point.is_integrity_ok() {
            let not_exists = DagPoint::NotExists(Arc::new(point.id())); // cannot use point body
            JoinTask::new(future::ready(not_exists))
        } else if !(point.is_well_formed() && self.is_list_of_signers_ok(&point)) {
            let invalid = DagPoint::Invalid(Arc::new(*point));
            JoinTask::new(future::ready(invalid))
        } else if let Some(r_1) = r_0.prev.upgrade() {
            let dependencies = Self::gather_deps(r_1, &point);
            JoinTask::new(Self::check_deps(point, dependencies))
        } else {
            // If r-1 exceeds dag depth, the arg point @ r+0 is considered valid by itself.
            // Any point @ r+0 will be committed, only if it has valid proof @ r+1
            //   included into valid anchor chain, i.e. validated by consensus.
            let trusted = DagPoint::Trusted(Arc::new(IndexedPoint::new(*point)));
            JoinTask::new(future::ready(trusted))
        }
    }

    fn gather_deps(r_1 /* r-1 */: Arc<DagRound>, point /* @ r+0 */: &Point) -> JoinSet<DagPoint> {
        fn add_dependency(
            round: &Arc<DagRound>,
            node: &PeerId,
            digest: &Digest,
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

        let mut dependencies = JoinSet::new();
        let author = &point.body.location.author;

        if let Some(loc) = r_1.locations.get(author) {
            // to check for equivocation or mandatory skip of a round
            for version in loc.versions.values() {
                dependencies.spawn(version.clone().map(|a| a.0));
            }
        }
        for (node, digest) in &point.body.includes {
            // integrity check passed, so includes contain author's prev point proof
            add_dependency(&r_1, &node, &digest, &mut dependencies);
        }
        if let Some(r_2) = r_1.prev.upgrade() {
            for (node, digest) in &point.body.witness {
                add_dependency(&r_2, &node, &digest, &mut dependencies);
            }
        };
        dependencies
    }

    async fn check_deps(point: Box<Point>, mut dependencies: JoinSet<DagPoint>) -> DagPoint {
        // point is well-formed if we got here, so point.proof matches point.includes
        let proven = point.body.proof.as_ref().map(|p| &p.digest).clone();
        let prev_loc = Location {
            round: point.body.location.round.prev(),
            author: point.body.location.author,
        };

        // The node must have no points in previous location
        //   in case it provide no proof for previous point.
        // But equivocation does not invalidate the point.
        // Invalid dependency is the author's fault.
        let mut is_suspicious = false;
        while let Some(res) = dependencies.join_next().await {
            match res {
                Ok(DagPoint::Trusted(valid) | DagPoint::Suspicious(valid)) => {
                    if prev_loc == valid.point.body.location {
                        match proven {
                            None => return DagPoint::Invalid(Arc::new(*point)),
                            Some(v) if v == &valid.point.digest => {
                                if !Self::is_proof_ok(&point, &valid.point) {
                                    return DagPoint::Invalid(Arc::new(*point));
                                } // else: ok proof
                            }
                            Some(_) => is_suspicious = true, // equivocation
                        }
                    } // else: valid dependency
                }
                Ok(DagPoint::Invalid(invalid)) => {
                    if prev_loc == invalid.body.location {
                        match proven {
                            // node must have skipped prev_loc.round
                            None => return DagPoint::Invalid(Arc::new(*point)),
                            Some(v) if v == &invalid.digest => {
                                return DagPoint::Invalid(Arc::new(*point))
                            }
                            Some(_) => is_suspicious = true, // equivocation
                        }
                    } else {
                        return DagPoint::Invalid(Arc::new(*point)); // just invalid dependency
                    }
                }
                Ok(DagPoint::NotExists(not_exists)) => {
                    if prev_loc == not_exists.location {
                        match proven {
                            Some(v) if v == &not_exists.digest => {
                                return DagPoint::Invalid(Arc::new(*point))
                            }
                            _ => {} // dependency of some other point; we've banned the sender
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
    fn is_list_of_signers_ok(&self, point /* @ r+0 */: &Point) -> bool {
        let Some(proof /* @ r-1 */) = &point.body.proof else {
            return true;
        };
        let [
            same_round_peers/* @ r-1 */,
            next_round_peers/* @ r+0 */
        ] = self.peer_schedule.peers_for_array([
                point.body.location.round.prev(),
                point.body.location.round
            ]);
        //TODO may there be a problem ?
        // the size of required validator set is determined by point's round,
        // but if the next round is a new epoch start, amount of available signers may change

        // may include author's signature already contained in proven point, no matter
        if proof.evidence.len() < ((same_round_peers.len() + 2) / 3) * 3 + 1 {
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
        if !(point.body.time >= proven.body.time) {
            return false; // time must be non-decreasing
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

    // Todo: leader chain validation - for leaders only (including time)

    // Todo: anchor inclusion validation and time based on it

    // todo: time validation based on now() - for directly received (for signature) points (roots)
    //   leave time only in leader (anchor) blocks?

    // todo: shallow validation during sync ?
}
