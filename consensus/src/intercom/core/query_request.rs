use bytes::{Buf, Bytes};
use tl_proto::{RawBytes, TlError, TlRead, TlWrite};
use tycho_network::Request;

use crate::effects::MempoolRayon;
use crate::models::{Point, PointId, Round};

#[derive(Copy, Clone, Debug, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum QueryRequestTag {
    #[tl(id = "intercom.queryTag.broadcast")]
    Broadcast,
    #[tl(id = "intercom.queryTag.pointById")]
    PointById,
    #[tl(id = "intercom.queryTag.signature")]
    Signature,
}

pub enum QueryRequest {
    Broadcast(Point),
    PointById(PointId),
    Signature(Round),
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

    pub fn point_by_id(id: &PointId) -> Request {
        Request::from_tl(QueryRequestWrite {
            tag: QueryRequestTag::PointById,
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

    pub async fn parse(self, mempool_rayon: &MempoolRayon) -> anyhow::Result<QueryRequest> {
        Ok(match self.tag {
            QueryRequestTag::Broadcast => {
                let request_body = self.request_body;
                let point = mempool_rayon
                    .run_fifo(|| Point::parse(request_body.into()))
                    .await???;
                QueryRequest::Broadcast(point)
            }
            QueryRequestTag::PointById => {
                QueryRequest::PointById(tl_proto::deserialize::<PointId>(&self.request_body)?)
            }
            QueryRequestTag::Signature => {
                QueryRequest::Signature(tl_proto::deserialize::<Round>(&self.request_body)?)
            }
        })
    }
}
