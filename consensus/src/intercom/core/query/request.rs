use bytes::{Buf, Bytes};
use tl_proto::{RawBytes, TlError, TlRead, TlWrite};
use tycho_network::Request;

use crate::models::{Point, PointId, Round, StructureIssue};

#[derive(Copy, Clone, Debug, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum QueryRequestTag {
    #[tl(id = "intercom.queryTag.broadcast")]
    Broadcast,
    #[tl(id = "intercom.queryTag.signature")]
    Signature,
    #[tl(id = "intercom.queryTag.download")]
    Download,
}

pub enum QueryRequest {
    Broadcast(Point, Option<StructureIssue>),
    Signature(Round),
    Download(PointId),
}

impl QueryRequest {
    pub fn broadcast(point: &Point) -> Request {
        Request::from_tl(QueryRequestWrite {
            tag: QueryRequestTag::Broadcast,
            body: &RawBytes::<tl_proto::Boxed>::new(point.serialized()),
        })
    }

    pub fn signature(round: Round) -> Request {
        Request::from_tl(QueryRequestWrite {
            tag: QueryRequestTag::Signature,
            body: &round,
        })
    }

    pub fn download(id: &PointId) -> Request {
        Request::from_tl(QueryRequestWrite {
            tag: QueryRequestTag::Download,
            body: id,
        })
    }
}

#[derive(TlWrite, Debug)]
#[tl(boxed, id = "intercom.queryRequest", scheme = "proto.tl")]
struct QueryRequestWrite<'a, T> {
    tag: QueryRequestTag,
    body: &'a T,
}

#[derive(TlRead, Debug)]
#[tl(boxed, id = "intercom.queryRequest", scheme = "proto.tl")]
struct QueryRequestRead<'tl> {
    tag: QueryRequestTag,
    body: RawBytes<'tl, tl_proto::Boxed>,
}

pub struct QueryRequestRaw {
    pub tag: QueryRequestTag,
    request_body: Bytes,
}

impl QueryRequestRaw {
    pub fn new(mut request_body: Bytes) -> Result<Self, TlError> {
        let QueryRequestRead { tag, body } = tl_proto::deserialize::<_>(&request_body)?;
        let data_offset = request_body.len() - body.as_ref().len();
        request_body.advance(data_offset);
        Ok(Self { request_body, tag })
    }

    pub fn parse(self) -> Result<QueryRequestMedium, TlError> {
        Ok(match self.tag {
            QueryRequestTag::Broadcast => QueryRequestMedium::Broadcast(self.request_body),
            QueryRequestTag::Signature => {
                QueryRequestMedium::Signature(tl_proto::deserialize(&self.request_body)?)
            }
            QueryRequestTag::Download => {
                QueryRequestMedium::Download(tl_proto::deserialize(&self.request_body)?)
            }
        })
    }
}

pub enum QueryRequestMedium {
    Broadcast(Bytes),
    Signature(Round),
    Download(PointId),
}
