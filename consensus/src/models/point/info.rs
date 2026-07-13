use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use serde::Serialize;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::engine::MempoolConfig;
use crate::models::{
    AnchorStageRole, AnyLink, ChainedProofLink, Digest, EvidenceSigError, IndirectLink, PointData,
    PointKey, PointRole, Round, Signature, StructureIssue, Through, UnixTime,
};

#[derive(Clone, TlRead, TlWrite)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, id = "consensus.pointInfo", scheme = "proto.tl")]
pub struct PointInfo(Arc<PointInfoInner>);

// The only such case doesn't deserve `rc` feature on `serde` crate to be enabled
impl Serialize for &PointInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[derive(TlRead, TlWrite, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
struct PointInfoInner {
    id: PointId,
    signature: Signature,
    payload_len: u32,
    payload_bytes: u32,
    data: PointData,
}

impl Debug for PointInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PointInfo")
            .field("digest", self.digest())
            .field("signature", self.signature())
            .field("author", self.author())
            .field("round", &self.round())
            .field("payload_len", &self.payload_len())
            .field("payload_bytes", &self.payload_bytes())
            .field("data", self.data())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, TlRead, TlWrite, Serialize)]
#[tl(boxed, id = "consensus.pointId", scheme = "proto.tl")]
pub struct PointId {
    pub round: Round,
    pub digest: Digest,
    pub author: PeerId,
}

impl PointId {
    pub const MAX_TL_BYTES: usize =
        4 + PeerId::MAX_TL_BYTES + Round::MAX_TL_BYTES + Digest::MAX_TL_BYTES;

    pub fn key(&self) -> PointKey {
        PointKey::new(self.round, self.digest)
    }
}

impl PointInfo {
    pub const MAX_BYTE_SIZE: usize =
        4 + PointId::MAX_TL_BYTES + Signature::MAX_TL_BYTES + 4 + 4 + PointData::MAX_BYTE_SIZE;

    pub(super) fn new(
        id: PointId,
        signature: Signature,
        payload_len: u32,
        payload_bytes: u32,
        data: PointData,
    ) -> Self {
        Self(Arc::new(PointInfoInner {
            id,
            signature,
            payload_len,
            payload_bytes,
            data,
        }))
    }

    pub fn id(&self) -> &PointId {
        &self.0.id
    }

    pub fn key(&self) -> PointKey {
        PointKey::new(self.round(), *self.digest())
    }

    pub fn digest(&self) -> &Digest {
        &self.0.id.digest
    }

    pub fn signature(&self) -> &Signature {
        &self.0.signature
    }

    pub fn author(&self) -> &PeerId {
        &self.0.id.author
    }

    pub fn round(&self) -> Round {
        self.0.id.round
    }

    pub fn payload_len(&self) -> u32 {
        self.0.payload_len
    }

    pub fn payload_bytes(&self) -> u32 {
        self.0.payload_bytes
    }

    pub(super) fn data(&self) -> &PointData {
        &self.0.data
    }

    pub fn includes(&self) -> &FastHashMap<PeerId, Digest> {
        &(self.0.data).includes
    }

    pub fn witness(&self) -> &FastHashMap<PeerId, Digest> {
        &(self.0.data).witness
    }
    pub fn evidence(&self) -> &FastHashMap<PeerId, Signature> {
        &(self.0.data).evidence
    }

    pub fn time(&self) -> UnixTime {
        (self.0.data).time
    }

    pub fn anchor_time(&self) -> UnixTime {
        (self.0.data).anchor_time
    }

    pub fn prev_digest(&self) -> Option<&Digest> {
        (self.0.data).includes.get(self.author())
    }

    pub fn prev_id(&self) -> Option<PointId> {
        Some(PointId {
            author: *self.author(),
            round: self.round().prev(),
            digest: *self.prev_digest()?,
        })
    }

    pub fn check_evidence(&self) -> Result<(), EvidenceSigError> {
        let is_ok = self.prev_id().is_none_or(|prev_id| {
            (self.evidence().iter()).all(|(peer, sig)| sig.verify(peer, &prev_id))
        });
        if is_ok { Ok(()) } else { Err(EvidenceSigError) }
    }

    pub fn is_proof_link_ok(&self, is_leader: bool, conf: &MempoolConfig) -> bool {
        (self.0.data).is_proof_link_ok(is_leader, self.prev_digest().is_some(), self.round(), conf)
    }

    pub fn check_structure(&self, is_genesis: bool) -> Result<(), StructureIssue> {
        if is_genesis {
            if !matches!(self.0.data.role, PointRole::Genesis) {
                return Err(StructureIssue::ExpectedGenesis(true));
            }
            if self.time() != self.anchor_time() {
                return Err(StructureIssue::AnchorTime);
            }
            Ok(())
        } else {
            if matches!(self.0.data.role, PointRole::Genesis) {
                return Err(StructureIssue::ExpectedGenesis(false));
            }
            (self.0.data).check_non_genesis_structure(self.author(), self.round())
        }
    }

    pub fn anchor_trigger(&self) -> AnyLink<'_> {
        self.0.data.role.anchor_trigger()
    }

    pub fn anchor_proof(&self) -> AnyLink<'_> {
        self.0.data.role.anchor_proof(self.author())
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> AnyLink<'_> {
        (self.0.data).anchor_link(link_field, self.author())
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        (self.0.data).anchor_round(link_field, self.round())
    }

    pub fn indirect_anchor_proof(&self) -> Option<&IndirectLink> {
        match self.0.data.role.chained_anchor_proof() {
            ChainedProofLink::Inapplicable | ChainedProofLink::PrevPoint { .. } => None,
            ChainedProofLink::Indirect(link) => Some(link),
        }
    }

    pub fn sticky_anchors(&self) -> Option<u8> {
        match self.0.data.role.chained_anchor_proof() {
            ChainedProofLink::Inapplicable | ChainedProofLink::Indirect(_) => None,
            ChainedProofLink::PrevPoint { chained } => Some(chained),
        }
    }

    pub fn chained_anchor_proof_to_round(&self) -> Option<Round> {
        match &self.0.data.role.chained_anchor_proof() {
            ChainedProofLink::Inapplicable => None,
            ChainedProofLink::PrevPoint { .. } => Some(self.round().prev()),
            ChainedProofLink::Indirect(link) => Some(link.to.round),
        }
    }

    pub fn chained_anchor_proof_to(&self) -> Option<PointId> {
        match &self.0.data.role.chained_anchor_proof() {
            ChainedProofLink::Inapplicable => None,
            ChainedProofLink::PrevPoint { .. } => {
                Some((self.prev_id()).expect("Coding error: usage of ill-formed point"))
            }
            ChainedProofLink::Indirect(link) => Some(link.to),
        }
    }

    pub fn chained_anchor_proof_to_through(&self) -> Option<(PointId, Option<PointId>)> {
        match &self.0.data.role.chained_anchor_proof() {
            ChainedProofLink::Inapplicable => None,
            ChainedProofLink::PrevPoint { .. } => {
                let prev_id = self
                    .prev_id()
                    .expect("Coding error: usage of ill-formed point");
                Some((prev_id, None))
            }
            ChainedProofLink::Indirect(link) => {
                let through = self
                    .through_id(&link.path)
                    .expect("Coding error: usage of ill-formed point");
                Some((link.to, Some(through)))
            }
        }
    }

    /// Returns `Some(anchor proof id)` that must be defined by trigger link (its prev point).
    pub fn trigger_bound_proof_id(&self) -> Option<PointId> {
        let proof_id = self.data().inherited_anchor_proof_id(self.round())?;
        (proof_id.round < self.anchor_round(AnchorStageRole::Trigger)).then_some(proof_id)
    }

    /// Well-formed point may return `None` if attribute belongs to another point
    pub fn through_id(&self, through: &Through) -> Option<PointId> {
        self.0.data.through_id(through, self.round())
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        (self.0.data)
            .anchor_id(link_field, self.author(), self.round())
            .unwrap_or_else(|| *self.id())
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_through(&self, link_field: AnchorStageRole) -> PointId {
        (self.0.data)
            .anchor_link_id(link_field, self.author(), self.round())
            .unwrap_or_else(|| *self.id())
    }
}

#[cfg(any(test, feature = "test"))]
impl PointId {
    pub fn random() -> Self {
        Self {
            author: PeerId(rand::random()),
            round: Round(rand::random()),
            digest: Digest::random(),
        }
    }
}
