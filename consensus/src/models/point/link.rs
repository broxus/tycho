use std::fmt::{Debug, Formatter};

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::effects::{AltFmt, AltFormat};
use crate::models::{PointId, PointMap};

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum AnchorLink {
    #[tl(id = "consensus.anchorLink.to_self")]
    ToSelf,
    #[tl(id = "consensus.anchorLink.direct")]
    Direct(Through),
    #[tl(id = "consensus.anchorLink.indirect")]
    Indirect(IndirectLink),
}

impl AnchorLink {
    pub const MAX_TL_BYTES: usize = 4 + IndirectLink::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, id = "consensus.link.indirect", scheme = "proto.tl")]
pub struct IndirectLink {
    pub to: PointId,
    pub path: Through,
}

impl IndirectLink {
    pub const MAX_TL_BYTES: usize = 4 + PointId::MAX_TL_BYTES + Through::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Through {
    #[tl(id = "consensus.link.through.includes")]
    Includes(PeerId),
    #[tl(id = "consensus.link.through.witness")]
    Witness(PeerId),
}

impl Through {
    pub const MAX_TL_BYTES: usize = 4 + PeerId::MAX_TL_BYTES;
}

impl AltFormat for AnchorLink {}
impl Debug for AltFmt<'_, AnchorLink> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            AnchorLink::ToSelf => write!(f, "to self"),
            AnchorLink::Direct(through) => write!(f, "direct {:?}", through.alt()),
            AnchorLink::Indirect(indirect) => write!(f, "indirect {:?}", indirect.alt()),
        }
    }
}

impl AltFormat for IndirectLink {}
impl Debug for AltFmt<'_, IndirectLink> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(f, "to {:?} {:?}", &inner.to.alt(), &inner.path.alt())
    }
}

impl AltFormat for Through {}
impl Debug for AltFmt<'_, Through> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        let (map, peer_id) = match inner {
            Through::Witness(peer_id) => (PointMap::Witness, peer_id),
            Through::Includes(peer_id) => (PointMap::Includes, peer_id),
        };
        write!(f, "through {map:?} {}", peer_id.alt())
    }
}

#[cfg(any(test, feature = "test"))]
impl AnchorLink {
    pub fn random() -> Self {
        match rand::random_range(0..5) {
            0 => Self::ToSelf,
            1 | 2 => Self::Direct(Through::random()),
            3 | 4 => Self::Indirect(IndirectLink::random()),
            _ => unreachable!(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
impl IndirectLink {
    pub fn random() -> Self {
        Self {
            to: PointId::random(),
            path: Through::random(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
impl Through {
    pub fn random() -> Self {
        if rand::random_bool(0.5) {
            Through::Includes(PeerId(rand::random()))
        } else {
            Through::Witness(PeerId(rand::random()))
        }
    }
}
