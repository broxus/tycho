use bytes::{Buf, Bytes};
use tl_proto::{TlError, TlRead, TlWrite};
use tycho_network::PrefixedRequest;

use crate::intercom::Dispatcher;
use crate::models::{EvidenceSigError, Point, PointId, Round};

#[derive(Copy, Clone, Debug, PartialEq, TlRead, TlWrite)]
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
    Broadcast(Point, Option<EvidenceSigError>),
    Signature(Round),
    Download(PointId),
}

impl QueryRequest {
    pub fn broadcast(dispatcher: &Dispatcher, point: &Point) -> PrefixedRequest {
        dispatcher.request_from_tl(QueryRequestWrite {
            tag: QueryRequestTag::Broadcast,
            body: &point.serialized(),
        })
    }

    pub fn signature(dispatcher: &Dispatcher, round: Round) -> PrefixedRequest {
        dispatcher.request_from_tl(QueryRequestWrite {
            tag: QueryRequestTag::Signature,
            body: &round,
        })
    }

    pub fn download(dispatcher: &Dispatcher, id: &PointId) -> PrefixedRequest {
        dispatcher.request_from_tl(QueryRequestWrite {
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
struct QueryRequestTagRead {
    tag: QueryRequestTag,
}

pub struct QueryRequestRaw {
    pub tag: QueryRequestTag,
    request_body: Bytes,
}

impl QueryRequestRaw {
    pub fn new(mut request_body: Bytes) -> Result<Self, TlError> {
        let mut remaining = &request_body[..];

        let QueryRequestTagRead { tag } = <_>::read_from(&mut remaining)?;

        request_body.advance(request_body.len() - remaining.len());

        Ok(Self { request_body, tag })
    }

    pub fn parse(mut self) -> Result<QueryRequestMedium, TlError> {
        Ok(match self.tag {
            QueryRequestTag::Broadcast => {
                let mut remaining = &self.request_body[..];
                let data = <&[u8]>::read_from(&mut remaining)?;

                let data_len = data.len();
                let data_end = self.request_body.len() - remaining.len();
                let data_start = data_end - data.len();

                self.request_body.advance(data_start);
                self.request_body.truncate(data_len);

                QueryRequestMedium::Broadcast(self.request_body)
            }
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
