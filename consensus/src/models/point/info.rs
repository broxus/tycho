use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::models::{
    AnchorLink, AnchorStageRole, ChainedAnchorProof, Digest, EvidenceSigError, IndirectLink,
    PointData, PointId, PointKey, Round, Signature, StructureIssue, UnixTime,
};

#[derive(Clone, TlRead, TlWrite)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, id = "consensus.pointInfo", scheme = "proto.tl")]
pub struct PointInfo(Arc<PointInfoInner>);

#[derive(TlWrite, TlRead)]
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

    pub fn anchor_trigger(&self) -> &AnchorLink {
        &(self.0.data).anchor_trigger
    }

    pub fn anchor_proof(&self) -> &AnchorLink {
        &(self.0.data).anchor_proof
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
        let is_ok = self.prev_digest().is_none_or(|prev_proof| {
            (self.evidence().iter()).all(|(peer, sig)| sig.verifies(peer, prev_proof))
        });
        if is_ok { Ok(()) } else { Err(EvidenceSigError) }
    }

    pub fn check_structure(&self) -> Result<(), StructureIssue> {
        (self.0.data).check_maps(self.author(), self.round())
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &AnchorLink {
        (self.0.data).anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        (self.0.data).anchor_round(link_field, self.round())
    }

    pub fn chained_anchor_proof(&self) -> Option<&IndirectLink> {
        match &self.0.data.chained_anchor_proof {
            ChainedAnchorProof::Inapplicable => None,
            ChainedAnchorProof::Chained(link) => Some(link),
        }
    }

    pub fn chained_proof_to_through(&self) -> Option<(PointId, PointId)> {
        self.chained_anchor_proof().map(|link| {
            let through = (self.0.data.through_id(&link.path, self.round()))
                .expect("Coding error: usage of ill-formed point");
            (link.to, through)
        })
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        (self.0.data)
            .anchor_id(link_field, self.round())
            .unwrap_or_else(|| *self.id())
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_through(&self, link_field: AnchorStageRole) -> PointId {
        (self.0.data)
            .anchor_link_id(link_field, self.round())
            .unwrap_or_else(|| *self.id())
    }
}
