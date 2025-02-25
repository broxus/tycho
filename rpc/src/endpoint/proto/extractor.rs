use std::borrow::Cow;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::endpoint::{proto, APPLICATION_PROTOBUF, PARSE_ERROR_CODE};

// Counters
const METRIC_IN_REQ_FAIL_TOTAL: &str = "tycho_rpc_in_req_fail_total";

pub struct Protobuf<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for Protobuf<T>
where
    T: Message + Default,
    S: Send + Sync,
{
    type Rejection = ProtoErrorResponse;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = match Bytes::from_request(req, state).await {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(ProtoErrorResponse {
                    code: PARSE_ERROR_CODE,
                    message: e.to_string().into(),
                });
            }
        };
        let message = match T::decode(bytes) {
            Ok(message) => message,
            Err(e) => {
                return Err(ProtoErrorResponse {
                    code: PARSE_ERROR_CODE,
                    message: e.to_string().into(),
                });
            }
        };
        Ok(Protobuf(message))
    }
}

impl<T> IntoResponse for Protobuf<T>
where
    T: Message,
{
    fn into_response(self) -> Response {
        let body = self.0.encode_to_vec();
        let mut res = Response::new(body.into());
        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(APPLICATION_PROTOBUF));
        res
    }
}

pub struct ProtobufRef<'a, T>(pub &'a T);

impl<T> IntoResponse for ProtobufRef<'_, T>
where
    T: Message,
{
    fn into_response(self) -> Response {
        let body = self.0.encode_to_vec();
        let mut res = Response::new(body.into());
        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(APPLICATION_PROTOBUF));
        res
    }
}

pub struct ProtoOkResponse(proto::rpc::Response);

impl ProtoOkResponse {
    pub fn new(result: proto::rpc::response::Result) -> Self {
        ProtoOkResponse(proto::rpc::Response {
            result: Some(result),
        })
    }

    pub fn into_raw(self) -> RawProtoOkResponse {
        RawProtoOkResponse::from(self)
    }

    pub fn as_raw(&self) -> RawProtoOkResponse {
        RawProtoOkResponse::from(self)
    }
}

impl IntoResponse for ProtoOkResponse {
    fn into_response(self) -> Response {
        (StatusCode::OK, Protobuf(self.0)).into_response()
    }
}

#[derive(Clone)]
pub struct RawProtoOkResponse(pub bytes::Bytes);

impl From<ProtoOkResponse> for RawProtoOkResponse {
    fn from(value: ProtoOkResponse) -> Self {
        RawProtoOkResponse(value.0.encode_to_vec().into())
    }
}

impl From<&ProtoOkResponse> for RawProtoOkResponse {
    fn from(value: &ProtoOkResponse) -> Self {
        RawProtoOkResponse(value.0.encode_to_vec().into())
    }
}

impl IntoResponse for RawProtoOkResponse {
    fn into_response(self) -> Response {
        let mut res = Response::new(self.0.into());
        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(APPLICATION_PROTOBUF));
        res
    }
}

pub struct ProtoErrorResponse {
    pub code: i32,
    pub message: Cow<'static, str>,
}

impl IntoResponse for ProtoErrorResponse {
    fn into_response(self) -> Response {
        metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);

        Protobuf(proto::rpc::Error {
            code: self.code,
            message: self.message.into(),
        })
        .into_response()
    }
}
