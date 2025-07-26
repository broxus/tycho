use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::models::{
    AnchorStageRole, Digest, Link, PointData, PointId, Round, Signature, UnixTime,
};

#[derive(Clone, TlRead, TlWrite)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, id = "consensus.pointInfo", scheme = "proto.tl")]
pub struct PointInfo(Arc<PointInfoInner>);

#[derive(TlWrite, TlRead)]
#[cfg_attr(test, derive(PartialEq))]
struct PointInfoInner {
    digest: Digest,
    signature: Signature,
    author: PeerId,
    round: Round,
    payload_len: u32,
    payload_bytes: u32,
    data: PointData,
}

impl Debug for PointInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PointInfo")
            .field("digest", self.digest())
            .field("signature", &self.signature())
            .field("author", &self.author())
            .field("round", &self.round())
            .field("payload_len", &self.payload_len())
            .field("payload_bytes", &self.payload_bytes())
            .field("data", self.data())
            .finish()
    }
}

impl PointInfo {
    pub const MAX_BYTE_SIZE: usize = {
        // 4 bytes of PointInfo tag
        // 32 bytes of Digest
        // 64 bytes of Signature
        // 32 bytes of Author
        // 4 bytes for round, payload len and bytes
        // point data max_size_hint

        4 + Digest::MAX_TL_BYTES
            + Signature::MAX_TL_BYTES
            + PeerId::MAX_TL_BYTES
            + (4 + 4 + 4)
            + PointData::MAX_BYTE_SIZE
    };

    pub(super) fn new(
        digest: Digest,
        signature: Signature,
        author: PeerId,
        round: Round,
        payload_len: u32,
        payload_bytes: u32,
        data: PointData,
    ) -> Self {
        Self(Arc::new(PointInfoInner {
            digest,
            signature,
            author,
            round,
            payload_len,
            payload_bytes,
            data,
        }))
    }

    pub fn digest(&self) -> &Digest {
        &self.0.digest
    }

    pub fn signature(&self) -> &Signature {
        &self.0.signature
    }

    pub fn author(&self) -> PeerId {
        self.0.author
    }

    pub fn round(&self) -> Round {
        self.0.round
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

    pub fn includes(&self) -> &BTreeMap<PeerId, Digest> {
        &(self.0.data).includes
    }

    pub fn witness(&self) -> &BTreeMap<PeerId, Digest> {
        &(self.0.data).witness
    }
    pub fn evidence(&self) -> &BTreeMap<PeerId, Signature> {
        &(self.0.data).evidence
    }

    pub fn anchor_trigger(&self) -> &Link {
        &(self.0.data).anchor_trigger
    }

    pub fn anchor_proof(&self) -> &Link {
        &(self.0.data).anchor_proof
    }

    pub fn time(&self) -> UnixTime {
        (self.0.data).time
    }

    pub fn anchor_time(&self) -> UnixTime {
        (self.0.data).anchor_time
    }

    pub fn prev_digest(&self) -> Option<&Digest> {
        (self.0.data).includes.get(&self.0.author)
    }

    pub(super) fn has_well_formed_maps(&self) -> bool {
        (self.0.data).has_well_formed_maps(self.0.author, self.0.round)
    }

    pub fn id(&self) -> PointId {
        PointId {
            author: self.0.author,
            round: self.0.round,
            digest: self.0.digest,
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        Some(PointId {
            author: self.0.author,
            round: self.0.round.prev(),
            digest: *self.prev_digest()?,
        })
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
        (self.0.data).anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        (self.0.data).anchor_round(link_field, self.0.round)
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        (self.0.data)
            .anchor_id(link_field, self.0.round)
            .unwrap_or(self.id())
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_id(&self, link_field: AnchorStageRole) -> PointId {
        (self.0.data)
            .anchor_link_id(link_field, self.0.round)
            .unwrap_or(self.id())
    }
}
