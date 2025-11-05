use std::fmt::{Display, Formatter};
use std::time::Instant;
use bytes::Buf;
use tl_proto::{TlError, TlRead, TlResult, TlWrite};
use tycho_network::Response;
use tycho_util::sync::rayon_run_fifo;
use crate::effects::{AltFmt, AltFormat};
use crate::models::{Point, PointIntegrityError, Signature};
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
pub enum PointByIdResponse<T> {
    #[tl(id = "intercom.pointByIdResponse.defined")]
    Defined(T),
    #[tl(id = "intercom.pointByIdResponse.definedNone")]
    DefinedNone,
    #[tl(id = "intercom.pointByIdResponse.tryLater")]
    TryLater,
}
pub struct QueryResponse;
impl QueryResponse {
    pub fn broadcast(start: Instant) -> Response {
        let histogram = metrics::histogram!(
            "tycho_mempool_broadcast_query_responder_time"
        );
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
    pub fn point_by_id<T>(start: Instant, body: PointByIdResponse<T>) -> Response
    where
        T: AsRef<[u8]> + TlWrite,
    {
        let response = Response::from_tl(&body);
        let histogram = match body {
            PointByIdResponse::Defined(_) => {
                metrics::histogram!("tycho_mempool_download_query_responder_some_time")
            }
            PointByIdResponse::DefinedNone | PointByIdResponse::TryLater => {
                metrics::histogram!("tycho_mempool_download_query_responder_none_time")
            }
        };
        histogram.record(start.elapsed());
        response
    }
    pub async fn parse_point_by_id(
        mut response: Response,
    ) -> TlResult<PointByIdResponse<Result<Point, PointIntegrityError>>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(parse_point_by_id)),
            file!(),
            110u32,
        );
        let mut response = response;
        let interim = PointByIdResponse::<&[u8]>::read_from(&mut &response.body[..])?;
        Ok(
            match interim {
                PointByIdResponse::Defined(data) => {
                    let data_offset = response.body.len() - data.len();
                    response.body.advance(data_offset);
                    let response_body = response.body;
                    PointByIdResponse::Defined(
                        {
                            __guard.end_section(118u32);
                            let __result = rayon_run_fifo(|| Point::parse(
                                    response_body.into(),
                                ))
                                .await;
                            __guard.start_section(118u32);
                            __result
                        }?,
                    )
                }
                PointByIdResponse::DefinedNone => PointByIdResponse::DefinedNone,
                PointByIdResponse::TryLater => PointByIdResponse::TryLater,
            },
        )
    }
}
impl AltFormat for SignatureResponse {}
impl Display for AltFmt<'_, SignatureResponse> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            SignatureResponse::Signature(_) => f.write_str("Signature"),
            SignatureResponse::NoPoint => f.write_str("NoPoint"),
            SignatureResponse::TryLater => f.write_str("TryLater"),
            SignatureResponse::Rejected(reason) => {
                f.debug_tuple("Rejected").field(reason).finish()
            }
        }
    }
}
impl<T: AsRef<[u8]>> AltFormat for PointByIdResponse<T> {}
impl<T: AsRef<[u8]>> Display for AltFmt<'_, PointByIdResponse<T>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            PointByIdResponse::Defined(_) => f.write_str("Some"),
            PointByIdResponse::DefinedNone => f.write_str("None"),
            PointByIdResponse::TryLater => f.write_str("TryLater"),
        }
    }
}
