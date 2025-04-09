use std::collections::BTreeMap;

use bytes::Bytes;
use everscale_types::models::ConsensusConfig;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;
use crate::models::point::{AnchorStageRole, Digest, Link, PointData, Round, Signature, Through};
use crate::models::proto_utils::evidence_btree_map;
use crate::models::{PeerCount, UnixTime};

#[derive(TlWrite, TlRead, Debug)]
#[cfg_attr(test, derive(Clone))]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBody {
    pub round: Round, // let it be @ r+0
    pub payload: Vec<Bytes>,
    pub data: PointData,
    #[tl(with = "evidence_btree_map")]
    /// signatures for own point from previous round (if one exists, else empty map):
    /// the node may prove its vertex@r-1 with its point@r+0 only; contains signatures from
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter, author is excluded;
    pub evidence: BTreeMap<PeerId, Signature>,
}

impl PointBody {
    pub fn max_byte_size(consensus_config: &ConsensusConfig) -> usize {
        // 4 bytes of PointBody tag
        // 4 bytes of Round
        // payload bytes_max_size_hint

        // 4 bytes of PointData tag
        // 32 bytes of author
        // Max peer count * (32 + 32) of includes
        // Max peer count * (32 + 32) of witness
        // 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor_trigger Link
        // 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor proof Link
        // 8 bytes of time
        // 8 bytes of anchor time

        // Max peer size * (32 + 64) bytes of evidence

        const EXT_IN_BOC_MIN: usize = 48;

        let max_possible_includes_witness: usize =
            PeerCount::MAX.full() * (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES);

        let evidence_size: usize =
            PeerCount::MAX.full() * (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES);

        let point_data_size: usize = 4
            + PeerId::MAX_TL_BYTES
            + (2 * max_possible_includes_witness)
            + 2 * Link::MAX_TL_BYTES
            + 2 * UnixTime::MAX_TL_BYTES;

        4 + Round::MAX_TL_SIZE
            + tl_proto::bytes_max_size_hint(EXT_IN_BOC_MIN)
                * (1 + consensus_config.payload_batch_bytes as usize / EXT_IN_BOC_MIN)
            + point_data_size
            + evidence_size
    }
    pub fn make_digest(&self, conf: &MempoolConfig) -> Digest {
        let mut data = Vec::<u8>::with_capacity(conf.point_max_bytes);
        self.write_to(&mut data);
        Digest::new(data.as_ref())
    }

    pub fn payload_bytes(&self) -> u32 {
        (self.payload.iter()).fold(0, |acc, msg| acc.saturating_add(msg.len() as u32))
    }

    pub fn is_well_formed(&self, conf: &MempoolConfig) -> bool {
        // check for being earlier than genesis takes place with other peer checks
        #[allow(clippy::nonminimal_bool, reason = "independent logical checks")]
        let is_special_ok = if self.round == conf.genesis_round {
            self.payload.is_empty()
                && self.data.anchor_trigger == Link::ToSelf
                && self.data.anchor_proof == Link::ToSelf
                && self.data.time == self.data.anchor_time
        } else {
            // leader must maintain its chain of proofs,
            // while others must link to previous points (checked at the end of this method);
            // its decided later (using dag round data) whether current point belongs to leader
            !(self.data.anchor_proof == Link::ToSelf && self.evidence.is_empty())
                && !(self.data.anchor_trigger == Link::ToSelf && self.evidence.is_empty())
                && self.data.time > self.data.anchor_time
        };
        is_special_ok
            // proof for previous point consists of digest and 2F++ evidences
            // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
            && self.evidence.is_empty() != self.data.includes.contains_key(&self.data.author)
            // evidence must contain only signatures of others
            && !self.evidence.contains_key(&self.data.author)
            // also cannot witness own point
            && !self.data.witness.contains_key(&self.data.author)
            && self.is_link_well_formed(AnchorStageRole::Trigger)
            && self.is_link_well_formed(AnchorStageRole::Proof)
    }

    fn is_link_well_formed(&self, link_field: AnchorStageRole) -> bool {
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
