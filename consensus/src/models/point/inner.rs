use serde::{Deserialize, Serialize};

use crate::models::{AnchorStageRole, Digest, Link, PointBody, PointId, Round, Signature, Through};
use crate::MempoolConfig;

#[derive(Serialize, Deserialize, Debug)]
pub struct PointInner {
    // hash of the point's body (includes author peer id)
    pub digest: Digest,
    // author's signature for the digest
    pub signature: Signature,
    pub body: PointBody,
}

impl PointInner {
    pub fn id(&self) -> PointId {
        PointId {
            author: self.body.author,
            round: self.body.round,
            digest: self.digest.clone(),
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        let digest = self.body.proof.as_ref().map(|p| &p.digest)?;
        Some(PointId {
            author: self.body.author,
            round: self.body.round.prev(),
            digest: digest.clone(),
        })
    }

    pub fn is_integrity_ok(&self) -> bool {
        self.signature.verifies(&self.body.author, &self.digest)
            && self.digest == Digest::new(&self.body)
    }

    pub fn is_well_formed(&self) -> bool {
        // any genesis is suitable, round number may be taken from configs
        let author = &self.body.author;
        let is_special_ok = match self.body.round {
            MempoolConfig::GENESIS_ROUND => {
                self.body.time == self.body.anchor_time
                    && self.body.anchor_trigger == Link::ToSelf
                    && self.body.anchor_proof == Link::ToSelf
                    && self.body.includes.is_empty()
                    && self.body.witness.is_empty()
                    && self.body.proof.is_none()
                    && self.body.payload.is_empty()
            }
            round if round > MempoolConfig::GENESIS_ROUND => {
                // no witness is possible at the round right after genesis;
                // the other way: we may panic on round.prev().prev() while extracting link's round
                (round > MempoolConfig::GENESIS_ROUND.next() || self.body.witness.is_empty())
                // leader must maintain its chain of proofs,
                // while others must link to previous points (checked at the end of this method);
                // its decided later (using dag round data) whether current point belongs to leader
                && !(self.body.anchor_proof == Link::ToSelf && self.body.proof.is_none())
                && !(self.body.anchor_trigger == Link::ToSelf && self.body.proof.is_none())
            }
            _ => false,
        };
        is_special_ok
            // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
            && self.body.proof.as_ref().map(|p| &p.digest) == self.body.includes.get(author)
            // in contrast, evidence must contain only signatures of others
            && self.body.proof.as_ref().map_or(true, |p| !p.evidence.contains_key(author))
            // also cannot witness own point
            && !self.body.witness.contains_key(&self.body.author)
            && self.is_link_well_formed(AnchorStageRole::Proof)
            && self.is_link_well_formed(AnchorStageRole::Trigger)
            && self.body.time >= self.body.anchor_time
            && MempoolConfig::PAYLOAD_BATCH_BYTES >= self.body.payload.iter().map(|x| x.len()).sum()
            && match (
                self.anchor_round(AnchorStageRole::Proof),
                self.anchor_round(AnchorStageRole::Trigger)
            ) {
                (x, MempoolConfig::GENESIS_ROUND) => x >= MempoolConfig::GENESIS_ROUND,
                (MempoolConfig::GENESIS_ROUND, y) => y >= MempoolConfig::GENESIS_ROUND,
                // equality is impossible due to commit waves do not start every round;
                // anchor trigger may belong to a later round than proof and vice versa;
                // no indirect links over genesis tombstone
                (x, y) => x != y && x > MempoolConfig::GENESIS_ROUND && y > MempoolConfig::GENESIS_ROUND,
            }
    }

    pub fn is_link_well_formed(&self, link_field: AnchorStageRole) -> bool {
        match self.anchor_link(link_field) {
            Link::ToSelf => true,
            Link::Direct(Through::Includes(peer)) => self.body.includes.contains_key(peer),
            Link::Direct(Through::Witness(peer)) => self.body.witness.contains_key(peer),
            Link::Indirect {
                path: Through::Includes(peer),
                to,
            } => self.body.includes.contains_key(peer) && to.round.next() < self.body.round,
            Link::Indirect {
                path: Through::Witness(peer),
                to,
            } => self.body.witness.contains_key(peer) && to.round.next().next() < self.body.round,
        }
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
        match link_field {
            AnchorStageRole::Trigger => &self.body.anchor_trigger,
            AnchorStageRole::Proof => &self.body.anchor_proof,
        }
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        match self.anchor_link(link_field) {
            Link::ToSelf => self.body.round,
            Link::Direct(Through::Includes(_)) => self.body.round.prev(),
            Link::Direct(Through::Witness(_)) => self.body.round.prev().prev(),
            Link::Indirect { to, .. } => to.round,
        }
    }

    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        match self.anchor_link(link_field) {
            Link::Indirect { to, .. } => to.clone(),
            _direct => self.anchor_link_id(link_field),
        }
    }

    pub fn anchor_link_id(&self, link_field: AnchorStageRole) -> PointId {
        let (digest, author, round) = match self.anchor_link(link_field) {
            Link::ToSelf => return self.id(),
            Link::Direct(Through::Includes(peer))
            | Link::Indirect {
                path: Through::Includes(peer),
                ..
            } => (self.body.includes.get(peer), *peer, self.body.round.prev()),
            Link::Direct(Through::Witness(peer))
            | Link::Indirect {
                path: Through::Witness(peer),
                ..
            } => (
                self.body.witness.get(peer),
                *peer,
                self.body.round.prev().prev(),
            ),
        };
        PointId {
            author,
            round,
            digest: digest
                .expect("Coding error: usage of ill-formed point")
                .clone(),
        }
    }
}
