use std::collections::BTreeMap;

use tl_proto::{RawBytes, TlRead, TlWrite};
use tycho_network::PeerId;

use crate::models::point::{Digest, PointData, Round, Signature};
use crate::models::proto_utils::evidence_btree_map;

#[derive(TlWrite)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointWrite<'a, T>
where
    T: AsRef<[u8]>,
{
    pub digest: &'a Digest,
    pub signature: &'a Signature,
    pub body: PointBodyWrite<'a, T>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointRead<'tl> {
    pub digest: Digest,
    pub signature: Signature,
    pub body: PointBodyRead<'tl>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointRawRead<'tl> {
    pub digest: Digest,
    _signature: Signature,
    pub body: RawBytes<'tl, tl_proto::Boxed>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.point", scheme = "proto.tl")]
pub struct PointPrefixRead<'tl> {
    _digest: Digest,
    _signature: Signature,
    pub body: PointBodyPrefixRead<'tl>,
}

#[derive(TlWrite)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyWrite<'a, T>
where
    T: AsRef<[u8]>,
{
    pub round: Round, // let it be @ r+0
    pub payload: &'a [T],
    pub data: &'a PointData,
    #[tl(with = "evidence_btree_map")]
    /// signatures for own point from previous round (if one exists, else empty map):
    /// the node may prove its vertex@r-1 with its point@r+0 only; contains signatures from
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter, author is excluded;
    pub evidence: &'a BTreeMap<PeerId, Signature>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyRead<'tl> {
    pub round: Round,
    pub payload: Vec<&'tl [u8]>,
    pub data: PointData,
    #[tl(with = "evidence_btree_map")]
    pub evidence: BTreeMap<PeerId, Signature>,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyPrefixRead<'tl> {
    _round: Round,
    pub payload: Vec<&'tl [u8]>,
}
