use std::fmt::{Display, Formatter};
use std::time::Instant;

use bytes::{Buf, Bytes};
use tl_proto::{TlError, TlRead, TlResult, TlWrite};
use tycho_network::Response;

use crate::effects::{AltFmt, AltFormat};
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

pub struct QueryResponse;
impl QueryResponse {
    pub fn broadcast(start: Instant) -> Response {
        let histogram = metrics::histogram!("tycho_mempool_broadcast_query_responder_time");
        histogram.record(start.elapsed());
        Response::from_tl(BroadcastResponse)
    }

    pub fn parse_broadcast(response: &Response) -> Result<BroadcastResponse, TlError> {
        BroadcastResponse::read_from(&mut &response.body[..])
    }

    pub fn signature(start: Instant, body: SignatureResponse) -> Response {
        let response = Response::from_tl(&body);
        let histogram = match body {
            SignatureResponse::NoPoint | SignatureResponse::TryLater => {
                metrics::histogram!("tycho_mempool_signature_query_responder_pong_time")
            }
            SignatureResponse::Signature(_) | SignatureResponse::Rejected(_) => {
                metrics::histogram!("tycho_mempool_signature_query_responder_data_time")
            }
        };
        histogram.record(start.elapsed());
        response
    }

    pub fn parse_signature(response: &Response) -> TlResult<SignatureResponse> {
        SignatureResponse::read_from(&mut &response.body[..])
    }

    pub fn download(start: Instant, body: DownloadResponse<Bytes>) -> Response {
        let response = Response::from_tl(&body);
        let histogram = match body {
            DownloadResponse::Defined(_) => {
                metrics::histogram!("tycho_mempool_download_query_responder_some_time")
            }
            DownloadResponse::DefinedNone | DownloadResponse::TryLater => {
                metrics::histogram!("tycho_mempool_download_query_responder_none_time")
            }
        };
        histogram.record(start.elapsed());
        response
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
