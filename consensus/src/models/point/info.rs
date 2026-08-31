use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use serde::Serialize;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::engine::MempoolConfig;
use crate::models::{
    AnchorLink, AnchorStageRole, Digest, EvidenceSigError, IndirectLink, PointData, PointKey,
    PointRole, Round, Signature, StructureIssue, Through, UnixTime,
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

    pub fn is_wave_link_ok(&self, is_leader: bool, conf: &MempoolConfig) -> bool {
        (self.0.data).is_wave_link_ok(is_leader, self.prev_digest().is_some(), self.round(), conf)
    }

    pub fn check_structure(&self, is_genesis: bool) -> Result<(), StructureIssue> {
        if is_genesis {
            if !matches!(self.0.data.role, PointRole::Genesis) {
                return Err(StructureIssue::ExpectedGenesis(true));
            }
            if self.time() != self.anchor_time() {
                return Err(StructureIssue::AnchorTime);
            }
            if self.prev_digest().is_none_or(|pd| pd != &Digest::ZERO)
                || self.0.data.anchor_proof != AnchorLink::Direct(Through::Includes(*self.author()))
                || self.0.data.anchor_proof != self.0.data.anchor_trigger
            {
                return Err(StructureIssue::BadGenesisPrevPoint);
            }

            Ok(())
        } else {
            if matches!(self.0.data.role, PointRole::Genesis) {
                return Err(StructureIssue::ExpectedGenesis(false));
            }
            (self.0.data).check_non_genesis_structure(self.author(), self.round())
        }
    }

    pub fn is_anchor_proof(&self) -> bool {
        self.0.data.role.is_anchor_proof()
    }

    pub fn is_anchor_trigger(&self) -> bool {
        self.0.data.role.is_anchor_trigger()
    }

    pub fn anchor_proof(&self) -> AnchorView<'_> {
        self.anchor(AnchorStageRole::Proof)
    }

    pub fn anchor_trigger(&self) -> AnchorView<'_> {
        self.anchor(AnchorStageRole::Trigger)
    }

    /// the final destination of an anchor link
    pub fn anchor(&self, link_field: AnchorStageRole) -> AnchorView<'_> {
        let link = match link_field {
            AnchorStageRole::Proof => &self.0.data.anchor_proof,
            AnchorStageRole::Trigger => &self.0.data.anchor_trigger,
        };
        AnchorView {
            through: AnchorViewThrough { info: self, link },
            role: link_field,
        }
    }

    pub fn indirect_anchor_links(&self) -> [Option<&IndirectLink>; 2] {
        let proof = match &self.0.data.anchor_proof {
            AnchorLink::Direct(_) => None,
            AnchorLink::Indirect(link) => Some(link),
        };
        let trigger = match &self.0.data.anchor_trigger {
            AnchorLink::Direct(_) => None,
            AnchorLink::Indirect(link) => Some(link),
        };
        [proof, trigger]
    }

    pub fn sticky_anchors(&self) -> Option<u8> {
        match &self.0.data.role {
            PointRole::AnchorProof { seq_no } => Some(*seq_no),
            _ => None,
        }
    }

    /// Well-formed point may return `None` if attribute belongs to another point
    pub fn through_id(&self, through: &Through) -> Option<PointId> {
        self.0.data.through_id(through, self.round())
    }
}

pub struct AnchorView<'a> {
    through: AnchorViewThrough<'a>,
    role: AnchorStageRole,
}

impl<'a> AnchorView<'a> {
    pub fn top(self) -> AnchorViewTop<'a> {
        AnchorViewTop {
            is_top: match self.role {
                AnchorStageRole::Trigger => self.through.info.is_anchor_trigger(),
                AnchorStageRole::Proof => self.through.info.is_anchor_proof(),
            },
            linked: self.linked(),
        }
    }

    pub fn linked(self) -> AnchorViewLinked<'a> {
        AnchorViewLinked(self.through)
    }

    pub fn through(self) -> AnchorViewThrough<'a> {
        self.through
    }
}

pub struct AnchorViewTop<'a> {
    is_top: bool,
    linked: AnchorViewLinked<'a>,
}

impl AnchorViewTop<'_> {
    pub fn id(&self) -> PointId {
        if self.is_top {
            *self.linked.0.info.id()
        } else {
            self.linked.id()
        }
    }

    pub fn round(&self) -> Round {
        if self.is_top {
            self.linked.0.info.round()
        } else {
            self.linked.round()
        }
    }

    pub fn author(&self) -> &PeerId {
        if self.is_top {
            self.linked.0.info.author()
        } else {
            self.linked.author()
        }
    }

    pub fn digest(&self) -> &Digest {
        if self.is_top {
            self.linked.0.info.digest()
        } else {
            self.linked.digest()
        }
    }
}

pub struct AnchorViewLinked<'a>(AnchorViewThrough<'a>);

impl AnchorViewLinked<'_> {
    pub fn id(&self) -> PointId {
        PointId {
            round: self.round(),
            author: *self.author(),
            digest: *self.digest(),
        }
    }

    pub fn round(&self) -> Round {
        match self.0.link {
            AnchorLink::Indirect(link) => link.to.round,
            AnchorLink::Direct(_) => self.0.round(),
        }
    }

    pub fn author(&self) -> &PeerId {
        match self.0.link {
            AnchorLink::Indirect(link) => &link.to.author,
            AnchorLink::Direct(_) => self.0.author(),
        }
    }

    pub fn digest(&self) -> &Digest {
        match self.0.link {
            AnchorLink::Indirect(link) => &link.to.digest,
            AnchorLink::Direct(_) => self.0.digest(),
        }
    }
}

pub struct AnchorViewThrough<'a> {
    info: &'a PointInfo,
    link: &'a AnchorLink,
}

impl AnchorViewThrough<'_> {
    pub fn id(&self) -> PointId {
        PointId {
            round: self.round(),
            author: *self.author(),
            digest: *self.digest(),
        }
    }

    pub fn round(&self) -> Round {
        let through = match self.link {
            AnchorLink::Direct(through) => through,
            AnchorLink::Indirect(link) => &link.path,
        };
        match through {
            Through::Includes(_) => self.info.round().prev(),
            Through::Witness(_) => self.info.round().prev().prev(),
        }
    }

    pub fn author(&self) -> &PeerId {
        let through = match self.link {
            AnchorLink::Direct(through) => through,
            AnchorLink::Indirect(link) => &link.path,
        };
        match through {
            Through::Includes(peer_id) | Through::Witness(peer_id) => peer_id,
        }
    }

    pub fn digest(&self) -> &Digest {
        self.digest_safe().expect("usage of ill-formed point")
    }

    pub fn digest_safe(&self) -> Option<&Digest> {
        let through = match self.link {
            AnchorLink::Direct(through) => through,
            AnchorLink::Indirect(link) => &link.path,
        };
        match through {
            Through::Includes(peer_id) => self.info.data().includes.get(peer_id),
            Through::Witness(peer_id) => self.info.data().witness.get(peer_id),
        }
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
