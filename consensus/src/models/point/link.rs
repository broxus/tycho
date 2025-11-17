use std::fmt::{Debug, Formatter};

use serde::Serialize;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::effects::{AltFmt, AltFormat};
use crate::models::{Digest, PointId, PointMap, Round};

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite, Serialize)]
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

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite, Serialize)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ChainedAnchorProof {
    #[tl(id = "consensus.chainedAnchorProof.inapplicable")]
    Inapplicable,
    #[tl(id = "consensus.chainedAnchorProof.chained")]
    Chained(IndirectLink),
}

impl ChainedAnchorProof {
    pub const MAX_TL_BYTES: usize = 4 + IndirectLink::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite, Serialize)]
#[tl(boxed, id = "consensus.link.indirect", scheme = "proto.tl")]
pub struct IndirectLink {
    pub to: PointId,
    pub path: Through,
}

impl IndirectLink {
    pub const MAX_TL_BYTES: usize = 4 + PointId::MAX_TL_BYTES + Through::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite, Serialize)]
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

impl IndirectLink {
    pub fn fill(&self, dest: &mut [u8]) -> anyhow::Result<()> {
        let len = dest.len();
        anyhow::ensure!(len == Self::MAX_TL_BYTES, "fill buf len {len}");

        let mut start: usize = 0;
        let mut end: usize = 0;

        let mut fill_next = |len: usize, source: &[u8]| {
            end += len;
            dest[start..end].copy_from_slice(source);
            start = end;
        };

        fill_next(4, &Self::TL_ID.to_le_bytes());
        fill_next(4, &PointId::TL_ID.to_le_bytes());
        fill_next(Round::MAX_TL_SIZE, &self.to.round.0.to_le_bytes());
        fill_next(Digest::MAX_TL_BYTES, self.to.digest.inner());
        fill_next(4, &PeerId::TL_ID.to_le_bytes());
        fill_next(PeerId::MAX_TL_BYTES - 4, &self.to.author.0);

        let (tl_id, peer_id) = match &self.path {
            Through::Includes(peer_id) => (Through::TL_ID_INCLUDES, peer_id),
            Through::Witness(peer_id) => (Through::TL_ID_WITNESS, peer_id),
        };

        fill_next(4, &tl_id.to_le_bytes());
        fill_next(4, &PeerId::TL_ID.to_le_bytes());
        fill_next(PeerId::MAX_TL_BYTES - 4, &peer_id.0);

        Ok(())
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::effects::AltFormat;

    #[test]
    fn indirect_link_fill_mimics_tl() -> anyhow::Result<()> {
        let link = IndirectLink::random();

        let mut buf: [u8; IndirectLink::MAX_TL_BYTES] = [0; _];
        link.fill(&mut buf)?;

        let tl = tl_proto::serialize(&link);

        anyhow::ensure!(
            buf.as_slice() == tl.as_slice(),
            "got:\n{}\nexpected:\n{}",
            buf.as_slice().alt(),
            tl.as_slice().alt()
        );

        let l_2 = tl_proto::deserialize::<IndirectLink>(&buf[..])?;

        anyhow::ensure!(l_2 == link);

        Ok(())
    }
}
