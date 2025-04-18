use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tl_proto::{TlRead, TlWrite};

use crate::models::{
    AnchorStageRole, Digest, Link, Point, PointData, PointDataWrite, PointId, Round,
};

#[derive(Clone, TlRead, TlWrite)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PointInfo(Arc<PointInfoInner>);

#[derive(TlWrite, TlRead)]
#[cfg_attr(test, derive(PartialEq))]
struct PointInfoInner {
    digest: Digest,
    round: Round,
    payload_len: u32,
    payload_bytes: u32,
    data: PointData,
}

#[derive(TlWrite)]
/// Note: fields and their order must be the same with [`PointInfoInner`]
pub struct PointInfoWrite<'a> {
    digest: &'a Digest,
    round: Round,
    payload_len: u32,
    payload_bytes: u32,
    data: PointDataWrite<'a>,
}

impl Debug for PointInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PointInfo")
            .field("digest", self.digest())
            .field("round", &self.round().0)
            .field("payload_len", &self.payload_len())
            .field("payload_bytes", &self.payload_bytes())
            .field("data", self.data())
            .finish()
    }
}

impl From<&Point> for PointInfo {
    fn from(point: &Point) -> Self {
        PointInfo(Arc::new(PointInfoInner {
            digest: *point.digest(),
            round: point.round(),
            payload_len: point.payload_len(),
            payload_bytes: point.payload_bytes(),
            data: point.data().clone(),
        }))
    }
}

impl PointInfo {
    pub const MAX_BYTE_SIZE: usize = {
        // 4 bytes of PointInfo tag
        // 32 bytes of Digest
        // 4 bytes for round, payload len and bytes

        // payload bytes max_size_hint
        // point data max_size_hint

        4 + Digest::MAX_TL_BYTES + 4 + 4 + 4 + PointData::MAX_BYTE_SIZE
    };

    pub fn digest(&self) -> &Digest {
        &self.0.digest
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

    pub fn data(&self) -> &PointData {
        &self.0.data
    }

    pub fn id(&self) -> PointId {
        PointId {
            author: self.0.data.author,
            round: self.0.round,
            digest: self.0.digest,
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        Some(PointId {
            author: self.0.data.author,
            round: self.0.round.prev(),
            digest: *self.0.data.prev_digest()?,
        })
    }

    pub fn serializable_from(point: &Point) -> PointInfoWrite<'_> {
        PointInfoWrite {
            digest: point.digest(),
            round: point.round(),
            payload_len: point.payload_len(),
            payload_bytes: point.payload_bytes(),
            data: PointDataWrite::from(point.data()),
        }
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
        self.0.data.anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        self.0.data.anchor_round(link_field, self.0.round)
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        self.0
            .data
            .anchor_id(link_field, self.0.round)
            .unwrap_or(self.id())
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_id(&self, link_field: AnchorStageRole) -> PointId {
        self.0
            .data
            .anchor_link_id(link_field, self.0.round)
            .unwrap_or(self.id())
    }
}
