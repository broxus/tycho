use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tl_proto::{TlRead, TlWrite};

use crate::models::{
    AnchorStageRole, Digest, Link, Point, PointData, PointDataRef, PointId, Round,
};

#[derive(Clone, TlRead, TlWrite)]
pub struct PointInfo(Arc<PointInfoInner>);

#[derive(TlWrite, TlRead)]
struct PointInfoInner {
    round: Round,
    digest: Digest,
    data: PointData,
}

impl Ord for PointInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.0.round, &self.0.digest).cmp(&(&other.0.round, &other.0.digest))
    }
}

impl PartialOrd for PointInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PointInfo {
    fn eq(&self, other: &Self) -> bool {
        self.0.digest == other.0.digest
    }
}

impl Eq for PointInfo {}

#[derive(TlWrite)]
/// Note: fields and their order must be the same with [`PointInfoInner`]
pub struct PointInfoRef<'a> {
    round: Round,
    digest: &'a Digest,
    data: PointDataRef<'a>,
}

impl Debug for PointInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PointInfo")
            .field("round", &self.round().0)
            .field("digest", self.digest())
            .field("data", self.data())
            .finish()
    }
}

impl From<&Point> for PointInfo {
    fn from(point: &Point) -> Self {
        PointInfo(Arc::new(PointInfoInner {
            round: point.round(),
            digest: *point.digest(),
            data: point.data().clone(),
        }))
    }
}

impl PointInfo {
    pub fn round(&self) -> Round {
        self.0.round
    }

    pub fn digest(&self) -> &Digest {
        &self.0.digest
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

    pub fn serializable_from(point: &Point) -> PointInfoRef<'_> {
        PointInfoRef {
            round: point.round(),
            digest: point.digest(),
            data: PointDataRef::from(point.data()),
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
