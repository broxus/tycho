use std::cmp;
use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;
use crate::models::point::{AnchorStageRole, Digest, Link, PointData, Round, Signature, Through};

#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(test, derive(Clone))]
pub struct PointBody {
    pub round: Round, // let it be @ r+0
    pub data: PointData,
    /// signatures for [`self.body.prev_digest`] if one exists:
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter;
    /// point author is excluded: everyone must use the proven point to validate its proof
    pub evidence: Option<BTreeMap<PeerId, Signature>>,
    pub payload: Vec<Bytes>,
}

impl PointBody {
    pub fn make_digest(&self) -> Digest {
        let bytes = bincode::serialize(self).expect("serialize point body");
        Digest::new(bytes.as_slice())
    }

    pub fn is_well_formed(&self) -> bool {
        // any genesis is suitable, round number may be taken from configs
        let is_special_ok = match self.round.cmp(&MempoolConfig::genesis_round()) {
            cmp::Ordering::Equal => {
                self.data.time == self.data.anchor_time
                    && self.data.anchor_trigger == Link::ToSelf
                    && self.data.anchor_proof == Link::ToSelf
                    && self.data.includes.is_empty()
                    && self.data.witness.is_empty()
                    && self.data.prev_digest.is_none()
                    && self.evidence.is_none()
                    && self.payload.is_empty()
            }
            cmp::Ordering::Greater => {
                // no witness is possible at the round right after genesis;
                // the other way: we may panic on round.prev().prev() while extracting link's round
                (self.round > MempoolConfig::genesis_round().next() || self.data.witness.is_empty())
                    // leader must maintain its chain of proofs,
                    // while others must link to previous points (checked at the end of this method);
                    // its decided later (using dag round data) whether current point belongs to leader
                    && !(self.data.anchor_proof == Link::ToSelf && self.data.prev_digest.is_none())
                    && !(self.data.anchor_trigger == Link::ToSelf && self.data.prev_digest.is_none())
            }

            cmp::Ordering::Less => false,
        };
        is_special_ok
            // proof for previous point consists of digest and 2F++ evidences
            && self.evidence.is_none() == self.data.prev_digest.is_none()
            // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
            && self.data.prev_digest.as_ref() == self.data.includes.get(&self.data.author)
            // in contrast, evidence must contain only signatures of others
            && self.evidence.as_ref().map_or(true, |map| !map.contains_key(&self.data.author))
            // also cannot witness own point
            && !self.data.witness.contains_key(&self.data.author)
            && self.is_link_well_formed(AnchorStageRole::Proof)
            && self.is_link_well_formed(AnchorStageRole::Trigger)
            && self.data.time >= self.data.anchor_time
            && MempoolConfig::PAYLOAD_BATCH_BYTES >= self.payload.iter().map(|x| x.len()).sum()
            && match (
            self.data.anchor_round(AnchorStageRole::Proof, self.round),
            self.data.anchor_round(AnchorStageRole::Trigger, self.round)
        ) {
            (x, y) if y == MempoolConfig::genesis_round() => x >= MempoolConfig::genesis_round(),
            (x, y) if x == MempoolConfig::genesis_round() => y >= MempoolConfig::genesis_round(),
            // equality is impossible due to commit waves do not start every round;
            // anchor trigger may belong to a later round than proof and vice versa;
            // no indirect links over genesis tombstone
            (x, y) => x != y && x > MempoolConfig::genesis_round() && y > MempoolConfig::genesis_round(),
        }
    }

    pub fn is_link_well_formed(&self, link_field: AnchorStageRole) -> bool {
        match self.data.anchor_link(link_field) {
            Link::ToSelf => true,
            Link::Direct(Through::Includes(peer)) => self.data.includes.contains_key(peer),
            Link::Direct(Through::Witness(peer)) => self.data.witness.contains_key(peer),
            Link::Indirect {
                path: Through::Includes(peer),
                to,
            } => self.data.includes.contains_key(peer) && to.round.next() < self.round,
            Link::Indirect {
                path: Through::Witness(peer),
                to,
            } => self.data.witness.contains_key(peer) && to.round.next().next() < self.round,
        }
    }
}
