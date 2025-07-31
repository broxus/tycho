use tl_proto::{RawBytes, TlRead, TlResult, TlWrite};
use tycho_network::PeerId;

use crate::models::Round;
use crate::models::point::{Digest, PointData, Signature};

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
    pub signature: Signature,
    pub body: RawBytes<'tl, tl_proto::Boxed>,
}

#[derive(TlWrite)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyWrite<'a, T>
where
    T: AsRef<[u8]>,
{
    pub author: &'a PeerId,
    pub round: Round,
    pub payload: &'a [T],
    pub data: &'a PointData,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
pub struct PointBodyRead<'tl> {
    pub author: PeerId,
    pub round: Round,
    pub payload: Vec<&'tl [u8]>,
    pub data: PointData,
}

impl PointRawRead<'_> {
    pub fn author(&self) -> TlResult<&PeerId> {
        #[derive(TlRead)]
        #[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
        struct PointBodyPrefix<'tl> {
            author: &'tl PeerId,
        }
        let body = <PointBodyPrefix<'_>>::read_from(&mut self.body.as_ref())?;
        Ok(body.author)
    }

    pub fn payload(&self) -> TlResult<Vec<&[u8]>> {
        #[derive(TlRead)]
        #[tl(boxed, id = "consensus.pointBody", scheme = "proto.tl")]
        struct PointBodyPrefix<'tl> {
            _author: &'tl PeerId,
            _round: Round,
            payload: Vec<&'tl [u8]>,
        }
        let body = <PointBodyPrefix<'_>>::read_from(&mut self.body.as_ref())?;
        Ok(body.payload)
    }
}
