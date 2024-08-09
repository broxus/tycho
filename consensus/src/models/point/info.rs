use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::models::point::*;

#[derive(Clone)]
pub struct PointInfo(Arc<PointInfoInner>);

#[derive(Serialize, Deserialize)]
struct PointInfoInner {
    round: Round,
    digest: Digest,
    data: PointData,
}

impl Serialize for PointInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PointInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(PointInfo(Arc::new(PointInfoInner::deserialize(
            deserializer,
        )?)))
    }
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
            digest: point.digest().clone(),
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
            digest: self.0.digest.clone(),
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        Some(PointId {
            author: self.0.data.author,
            round: self.0.round.prev(),
            digest: self.0.data.prev_digest.as_ref()?.clone(),
        })
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
