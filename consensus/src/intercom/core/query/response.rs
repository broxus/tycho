use std::fmt::{Display, Formatter};
use std::time::Instant;

use bytes::{Buf, Bytes};
use tl_proto::{TlError, TlRead, TlResult, TlWrite};
use tycho_network::Response;

use crate::effects::{AltFmt, AltFormat};
use crate::intercom::core::query::request::QueryRequestTag;
use crate::models::Signature;

#[derive(Debug, TlWrite, TlRead)]
#[tl(boxed, id = "intercom.broadcastResponse", scheme = "proto.tl")]
pub struct BroadcastResponse;

#[derive(Debug, TlWrite, TlRead)]
#[tl(boxed, scheme = "proto.tl")]
pub enum SignatureResponse {
    #[tl(id = "intercom.signatureResponse.signature")]
    Signature(Signature),
    #[tl(id = "intercom.signatureResponse.noPoint")]
    /// peer dropped its state or just reached point's round
    NoPoint,
    // TimeOut (still verifying or disconnect) is also a reason to retry
    #[tl(id = "intercom.signatureResponse.tryLater")]
    /// * signer did not reach the point's round yet - lighter weight broadcast retry loop;
    /// * signer still validates the point;
    /// * clock skew: signer's wall time lags the time from point's body
    TryLater,

    #[tl(id = "intercom.signatureResponse.rejected")]
    /// * malformed point
    /// * equivocation
    /// * invalid dependency
    /// * signer is more than 1 round in front of us
    Rejected(SignatureRejectedReason),
}

#[derive(Debug, TlWrite, TlRead)]
#[tl(boxed, scheme = "proto.tl")]
pub enum SignatureRejectedReason {
    #[tl(id = "intercom.signatureRejectedReason.tooOldRound")]
    TooOldRound,
    #[tl(id = "intercom.signatureRejectedReason.cannotSign")]
    CannotSign,
    #[tl(id = "intercom.signatureRejectedReason.unknownPeer")]
    UnknownPeer,
}

#[derive(Clone, Debug, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum DownloadResponse<T> {
    #[tl(id = "intercom.downloadResponse.defined")]
    Defined(T),
    #[tl(id = "intercom.downloadResponse.definedNone")]
    DefinedNone,
    #[tl(id = "intercom.downloadResponse.tryLater")]
    TryLater,
}

pub struct QueryResponse {
    start: Instant,
    tag: Option<QueryRequestTag>,
    result: Result<metrics::Histogram, ()>,
}

impl Drop for QueryResponse {
    fn drop(&mut self) {
        match &self.result {
            Ok(histogram) => {
                histogram.record(self.start.elapsed());
            }
            Err(()) => {
                let kind = match self.tag {
                    None => "unknown",
                    Some(QueryRequestTag::Broadcast) => "broadcast",
                    Some(QueryRequestTag::Signature) => "signature",
                    Some(QueryRequestTag::Download) => "download",
                };
                metrics::counter!("tycho_mempool_failed_query_responder", "kind" => kind)
                    .increment(1);
            }
        }
    }
}

impl QueryResponse {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            tag: None,
            result: Err(()),
        }
    }

    pub fn set_tag(&mut self, tag: QueryRequestTag) {
        self.tag = Some(tag);
    }

    pub fn broadcast(mut self) -> Response {
        debug_assert_eq!(self.tag, Some(QueryRequestTag::Broadcast));
        self.result = Ok(metrics::histogram!(
            "tycho_mempool_broadcast_query_responder_time"
        ));
        Response::from_tl(BroadcastResponse)
    }

    pub fn parse_broadcast(response: &Response) -> Result<BroadcastResponse, TlError> {
        BroadcastResponse::read_from(&mut &response.body[..])
    }

    pub fn signature(mut self, response: SignatureResponse) -> Response {
        debug_assert_eq!(self.tag, Some(QueryRequestTag::Signature));
        self.result = Ok(match response {
            SignatureResponse::NoPoint | SignatureResponse::TryLater => {
                metrics::histogram!("tycho_mempool_signature_query_responder_pong_time")
            }
            SignatureResponse::Signature(_) | SignatureResponse::Rejected(_) => {
                metrics::histogram!("tycho_mempool_signature_query_responder_data_time")
            }
        });
        Response::from_tl(response)
    }

    pub fn parse_signature(response: &Response) -> TlResult<SignatureResponse> {
        SignatureResponse::read_from(&mut &response.body[..])
    }

    pub fn download(mut self, response: DownloadResponse<Bytes>) -> Response {
        debug_assert_eq!(self.tag, Some(QueryRequestTag::Download));
        self.result = Ok(match response {
            DownloadResponse::Defined(_) => {
                metrics::histogram!("tycho_mempool_download_query_responder_some_time")
            }
            DownloadResponse::DefinedNone | DownloadResponse::TryLater => {
                metrics::histogram!("tycho_mempool_download_query_responder_none_time")
            }
        });
        Response::from_tl(response)
    }

    pub fn parse_download(mut response: Response) -> Result<DownloadResponse<Bytes>, TlError> {
        let interim = DownloadResponse::<&[u8]>::read_from(&mut &response.body[..])?;
        Ok(match interim {
            DownloadResponse::Defined(data) => {
                let data_offset = response.body.len() - data.len();
                response.body.advance(data_offset);
                DownloadResponse::Defined(response.body)
            }
            DownloadResponse::DefinedNone => DownloadResponse::DefinedNone,
            DownloadResponse::TryLater => DownloadResponse::TryLater,
        })
    }
}

impl AltFormat for SignatureResponse {}
impl Display for AltFmt<'_, SignatureResponse> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            SignatureResponse::Signature(_) => f.write_str("Signature"),
            SignatureResponse::NoPoint => f.write_str("NoPoint"),
            SignatureResponse::TryLater => f.write_str("TryLater"),
            SignatureResponse::Rejected(reason) => f.debug_tuple("Rejected").field(reason).finish(),
        }
    }
}

impl<T: AsRef<[u8]>> AltFormat for DownloadResponse<T> {}
impl<T: AsRef<[u8]>> Display for AltFmt<'_, DownloadResponse<T>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            DownloadResponse::Defined(_) => f.write_str("Some"),
            DownloadResponse::DefinedNone => f.write_str("None"),
            DownloadResponse::TryLater => f.write_str("TryLater"),
        }
    }
}
