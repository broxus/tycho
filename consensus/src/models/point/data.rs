use std::fmt::Debug;

use serde::Serialize;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use super::link::*;
use crate::engine::MempoolConfig;
use crate::models::point::proto_utils::{digests_map, signatures_map, u8_as_u32};
use crate::models::point::{Digest, Round, UnixTime, proto_utils};
use crate::models::{PeerCount, PointId, Signature};

#[derive(Clone, Debug, TlRead, TlWrite, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, id = "consensus.pointData", scheme = "proto.tl")]
pub struct PointData {
    /// `>= 2F+1` points @ r-1,
    /// signed by author @ r-1 with some additional points just mentioned;
    /// mandatory includes author's own vertex iff proof is given.
    /// Repeatable order on every node is needed for commit; map is used during validation
    #[tl(with = "digests_map")]
    pub includes: FastHashMap<PeerId, Digest>,
    /// `>= 0` points @ r-2, signed by author @ r-1
    /// Repeatable order on every node needed for commit; map is used during validation
    #[tl(with = "digests_map")]
    pub witness: FastHashMap<PeerId, Digest>,
    /// signatures for own point from previous round (if one exists, else empty map):
    /// the node may prove its vertex@r-1 with its point@r+0 only; contains signatures from
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter, author is excluded;
    #[tl(with = "signatures_map")]
    pub evidence: FastHashMap<PeerId, Signature>,
    /// last included by author; defines author's last committed anchor
    pub anchor_proof: AnchorLink,
    /// last included by author; maintains anchor chain linked without explicit DAG traverse
    pub anchor_trigger: AnchorLink,
    pub role: PointRole,
    /// local peer time at the moment of point creation, cannot be less than `anchor_time`
    pub time: UnixTime,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

#[derive(Clone, Debug, TlRead, TlWrite, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, scheme = "proto.tl")]
pub enum PointRole {
    #[tl(id = "consensus.pointRole.regular")]
    Regular,
    #[tl(id = "consensus.pointRole.proof")]
    AnchorProof {
        #[tl(with = "u8_as_u32")]
        seq_no: u8,
        is_last: bool,
    },
    /// the last trigger in sticky chain; the single one if no sticky anchors
    #[tl(id = "consensus.pointRole.trigger")]
    AnchorTrigger,
    #[tl(id = "consensus.pointRole.genesis")]
    Genesis,
}

impl PointRole {
    pub(super) const MAX_BYTE_SIZE: usize = 4 + 4 + 4;

    pub fn is_anchor_proof(&self) -> bool {
        match self {
            Self::Regular | Self::AnchorTrigger => false,
            Self::AnchorProof { .. } | Self::Genesis => true,
        }
    }

    pub fn is_anchor_trigger(&self) -> bool {
        match self {
            Self::Regular => false,
            Self::AnchorProof { seq_no, .. } => *seq_no > 0,
            Self::AnchorTrigger | Self::Genesis => true,
        }
    }

    fn requires_prev_point(&self) -> bool {
        match self {
            Self::Regular | Self::Genesis => false,
            Self::AnchorProof { .. } | Self::AnchorTrigger => true,
        }
    }
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
pub enum StructureIssue {
    #[error("{}expected genesis", if *.0 { "" } else { "Un" })]
    ExpectedGenesis(bool),
    #[error("genesis pseudo prev point links")]
    BadGenesisPrevPoint,
    #[error("{0:?} map must not contain author")]
    AuthorInMap(PointMap),
    #[error("{0:?} must have prev point")]
    RolePrevPoint(AnchorStageRole),
    #[error("Trigger must link prev point as anchor proof")]
    TriggerBadProofLink,
    #[error("bad {0:?} link through {1:?} map")]
    Link(AnchorStageRole, PointMap),
    #[error("anchor stage role {0:?}")]
    SelfAnchorStage(AnchorStageRole),
    #[error("anchor time")]
    AnchorTime,
    #[error("must have prev point")]
    NoPrevPoint,
}

#[derive(Debug, Copy, Clone)]
pub enum PointMap {
    Evidence, // r+0
    Includes, // r-1
    Witness,  // r-2
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnchorStageRole {
    Trigger,
    Proof,
}

impl PointData {
    pub(super) const MAX_BYTE_SIZE: usize = {
        let max_possible_maps: usize = PeerCount::MAX.full()
            * ((PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES) // includes map
                + (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES) // evidence map
                + (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES)) // signatures map
            + 3 * proto_utils::MAP_LEN_BYTES; // maps lengths

        4 + max_possible_maps
            + 2 * AnchorLink::MAX_TL_BYTES
            + PointRole::MAX_BYTE_SIZE
            + 2 * UnixTime::MAX_TL_BYTES
    };

    pub(super) fn is_wave_link_ok(
        &self,
        is_leader: bool,
        has_prev_point: bool,
        round: Round,
        conf: &MempoolConfig,
    ) -> bool {
        match &self.role {
            PointRole::Regular => !{
                is_leader
                    && has_prev_point // optional for Regular and encoded for AnchorProof
                    && self.anchor_proof.is_wave_far_enough(round, conf)
            },
            PointRole::AnchorProof { seq_no: 0, .. } => {
                is_leader && self.anchor_proof.is_wave_far_enough(round, conf)
            }
            PointRole::AnchorProof { .. } | PointRole::AnchorTrigger | PointRole::Genesis => true,
        }
    }

    /// counterpart of [`crate::dag::BasicVerifier::verify`] that must be called earlier,
    /// does not require config and allows to use [`crate::models::Point`] methods
    pub(super) fn check_non_genesis_structure(
        &self,
        author: &PeerId,
        round: Round,
    ) -> Result<(), StructureIssue> {
        let has_prev_point = self.includes.contains_key(author);

        if self.role.requires_prev_point() && !has_prev_point {
            return Err(StructureIssue::NoPrevPoint);
        };

        // proof for previous point consists of digest and 2F++ evidences
        // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
        ((self.evidence.is_empty() == has_prev_point).then_some(PointMap::Includes))
            // evidence must contain only signatures of others
            .or((self.evidence.contains_key(author)).then_some(PointMap::Evidence))
            // also cannot witness own point
            .or((self.witness.contains_key(author)).then_some(PointMap::Witness))
            .map(StructureIssue::AuthorInMap)
            .map_or(Ok(()), Err)?;

        if self.role.is_anchor_trigger()
            && !matches!(
                &self.anchor_proof,
                AnchorLink::Direct(Through::Includes(peer)) if peer == author
            )
        {
            return Err(StructureIssue::TriggerBadProofLink);
        }

        for role in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            let is_in_role = match role {
                AnchorStageRole::Trigger => self.role.is_anchor_trigger(),
                AnchorStageRole::Proof => self.role.is_anchor_proof(),
            };
            if is_in_role {
                if !has_prev_point {
                    return Err(StructureIssue::RolePrevPoint(role));
                }
                // leader must maintain its chain of proofs,
                // while others must link to previous points (checked at the end of this method)
                if self.evidence.is_empty() {
                    return Err(StructureIssue::SelfAnchorStage(role));
                }
            }
            let link = match role {
                AnchorStageRole::Trigger => &self.anchor_trigger,
                AnchorStageRole::Proof => &self.anchor_proof,
            };
            if let Some(map) = match link {
                AnchorLink::Direct(Through::Includes(peer)) => {
                    (!self.includes.contains_key(peer)).then_some(PointMap::Includes)
                }
                AnchorLink::Direct(Through::Witness(peer)) => {
                    (!self.witness.contains_key(peer)).then_some(PointMap::Witness)
                }
                AnchorLink::Indirect(IndirectLink { to, path }) => match path {
                    Through::Includes(peer) => {
                        { !self.includes.contains_key(peer) || to.round >= round.prev() }
                            .then_some(PointMap::Includes)
                    }
                    Through::Witness(peer) => {
                        { !self.witness.contains_key(peer) || to.round >= round.prev().prev() }
                            .then_some(PointMap::Witness)
                    }
                },
            } {
                return Err(StructureIssue::Link(role, map));
            }
        }

        if self.time <= self.anchor_time {
            // point time must be greater than anchor time
            return Err(StructureIssue::AnchorTime);
        }
        Ok(())
    }

    pub(super) fn through_id(&self, through: &Through, round: Round) -> Option<PointId> {
        let (map, author, round) = match through {
            Through::Includes(peer) => (&self.includes, *peer, round.prev()),
            Through::Witness(peer) => (&self.witness, *peer, round.prev().prev()),
        };
        Some(PointId {
            author,
            round,
            digest: *map.get(&author)?,
        })
    }
}
