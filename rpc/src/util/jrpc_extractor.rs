use std::borrow::Cow;
use std::marker::PhantomData;

use axum::async_trait;
use axum::extract::{FromRequest, Request};
use axum::http::{self, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::util::error_codes::*;

// Counters
const METRIC_IN_REQ_FAIL_TOTAL: &str = "tycho_rpc_in_req_fail_total";

pub const JSON_HEADERS: [(HeaderName, HeaderValue); 1] = [(
    http::header::CONTENT_TYPE,
    HeaderValue::from_static("application/json"),
)];

pub trait ParseParams {
    type Params;

    fn parse_params(self, params: &RawValue) -> Result<Self::Params, serde_json::Error>;

    fn method_name(&self) -> &'static str;
}

#[macro_export]
macro_rules! declare_jrpc_method {
    (
        $(#[$($meta:tt)*])*
        $vis:vis enum $method_enum:ident: $method_name_enum:ident {
            $(
                $(#[$($method_meta:tt)*])*
                $method_name:ident($method_params:ty)
            ),*$(,)?
        }
    ) => {
        #[allow(clippy::enum_variant_names)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, $crate::__internal::serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        $vis enum $method_name_enum {
            $(
                $(#[$($method_meta)*])*
                $method_name,
            )*
        }

        impl $crate::util::jrpc_extractor::ParseParams for $method_name_enum {
            type Params = $method_enum;

            fn parse_params(self, params: &$crate::__internal::serde_json::value::RawValue) -> Result<Self::Params, serde_json::Error> {
                let params = params.get();
                match self {
                    $(Self::$method_name => $crate::__internal::serde_json::from_str(params).map($method_enum::$method_name),)*
                }
            }

             fn method_name(&self) -> &'static str {
                match self {
                    $(Self::$method_name => stringify!($method_name),)*
                }
            }
        }

        #[derive(Debug)]
        $vis enum $method_enum {
            $(
                $(#[$($method_meta)*])*
                $method_name($method_params),
            )*
        }
    };
}

pub use declare_jrpc_method;

pub struct Jrpc<B: JrpcBehaviour, T: ParseParams> {
    pub id: B::Id,
    pub params: <T as ParseParams>::Params,
    pub method: &'static str,
    pub behaviour: PhantomData<B>,
}

#[async_trait]
impl<S, T, B> FromRequest<S> for Jrpc<B, T>
where
    B: JrpcBehaviour,
    JrpcErrorResponse<B>: IntoResponse,
    B::Id: for<'de> Deserialize<'de>,
    T: ParseParams + for<'de> Deserialize<'de>,
    S: Send + Sync,
{
    type Rejection = JrpcErrorResponse<B>;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        #[derive(Deserialize)]
        enum Unknown {
            #[serde(other)]
            Unknown,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ParsedMethod<T> {
            Known(T),
            Unknown(Unknown),
        }

        #[derive(Deserialize)]
        struct Request<'a, T, ID> {
            jsonrpc: &'a str,
            id: ID,
            method: ParsedMethod<T>,
            #[serde(borrow)]
            params: &'a RawValue,
        }

        let bytes = match Bytes::from_request(req, state).await {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(JrpcErrorResponse {
                    id: None,
                    code: PARSE_ERROR_CODE,
                    message: e.to_string().into(),
                    behaviour: PhantomData::<B>,
                });
            }
        };

        let (id, code, message) = match serde_json::from_slice::<Request<'_, T, B::Id>>(&bytes) {
            Ok(req) if req.jsonrpc == JSONRPC_VERSION => match req.method {
                ParsedMethod::Known(known) => {
                    let method = known.method_name();
                    match known.parse_params(req.params) {
                        Ok(params) => {
                            return Ok(Self {
                                id: req.id,
                                method,
                                params,
                                behaviour: PhantomData,
                            })
                        }
                        Err(e) => (Some(req.id), INVALID_PARAMS_CODE, e.to_string().into()),
                    }
                }
                ParsedMethod::Unknown(Unknown::Unknown) => {
                    (Some(req.id), METHOD_NOT_FOUND_CODE, "unknown method".into())
                }
            },
            Ok(req) => (
                Some(req.id),
                INVALID_REQUEST_CODE,
                "invalid jronrpc version".into(),
            ),
            Err(e) => {
                let code = match e.classify() {
                    serde_json::error::Category::Data => INVALID_REQUEST_CODE,
                    serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                        PARSE_ERROR_CODE
                    }
                    serde_json::error::Category::Io => {
                        // we don't use `serde_json::from_reader` and instead always buffer
                        // bodies first, so we shouldn't encounter any IO errors
                        unreachable!()
                    }
                };
                (None, code, e.to_string().into())
            }
        };

        Err(JrpcErrorResponse {
            id,
            code,
            message,
            behaviour: PhantomData,
        })
    }
}

pub trait JrpcBehaviour: Sized {
    type Id;

    fn serialize_ok_response<T, S>(
        response: &JrpcOkResponse<T, Self>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: serde::Serialize;

    fn serialize_error_response<S>(
        response: &JrpcErrorResponse<Self>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer;
}

pub struct RfcBehaviour;

impl JrpcBehaviour for RfcBehaviour {
    type Id = i64;

    fn serialize_ok_response<T, S>(
        response: &JrpcOkResponse<T, Self>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: serde::Serialize,
    {
        use serde::ser::SerializeStruct;

        let mut ser = serializer.serialize_struct("JrpcResponse", 3)?;
        ser.serialize_field(JSONRPC_FIELD, JSONRPC_VERSION)?;
        ser.serialize_field("id", &response.id)?;
        ser.serialize_field("result", &response.result)?;
        ser.end()
    }

    fn serialize_error_response<S>(
        response: &JrpcErrorResponse<Self>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut ser = serializer.serialize_struct("JrpcResponse", 3)?;
        ser.serialize_field(JSONRPC_FIELD, JSONRPC_VERSION)?;
        ser.serialize_field("id", &response.id)?;
        ser.serialize_field("error", &JrpcError {
            code: response.code,
            message: &response.message,
        })?;
        ser.end()
    }
}

pub struct JrpcOkResponse<T, B: JrpcBehaviour> {
    pub id: B::Id,
    pub result: T,
    pub behaviour: PhantomData<B>,
}

impl<T, B: JrpcBehaviour> JrpcOkResponse<T, B> {
    pub fn new(id: B::Id, result: T) -> Self {
        Self {
            id,
            result,
            behaviour: PhantomData,
        }
    }
}

impl<T, B> Serialize for JrpcOkResponse<T, B>
where
    T: Serialize,
    B: JrpcBehaviour,
{
    #[inline]
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        B::serialize_ok_response(self, serializer)
    }
}

impl<T, B: JrpcBehaviour> IntoResponse for JrpcOkResponse<T, B>
where
    Self: Serialize,
    JrpcErrorResponse<B>: Serialize,
{
    fn into_response(self) -> Response {
        let mut buf = BytesMut::with_capacity(128).writer();
        let data = match serde_json::to_writer(&mut buf, &self) {
            Ok(()) => buf.into_inner().freeze(),
            Err(e) => {
                buf.get_mut().clear();
                serde_json::to_writer(&mut buf, &JrpcErrorResponse {
                    id: Some(self.id),
                    code: BAD_RESPONSE_CODE,
                    message: format!("failed to serialize response: {e}").into(),
                    behaviour: PhantomData::<B>,
                })
                .unwrap();

                buf.into_inner().freeze()
            }
        };
        (JSON_HEADERS, data).into_response()
    }
}

pub struct JrpcErrorResponse<B: JrpcBehaviour> {
    pub id: Option<B::Id>,
    pub code: i32,
    pub message: Cow<'static, str>,
    pub behaviour: PhantomData<B>,
}

impl<B: JrpcBehaviour> Serialize for JrpcErrorResponse<B> {
    #[inline]
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        B::serialize_error_response(self, serializer)
    }
}

impl<B: JrpcBehaviour> IntoResponse for JrpcErrorResponse<B>
where
    Self: Serialize,
{
    fn into_response(self) -> Response {
        metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);

        (StatusCode::OK, axum::Json(self)).into_response()
    }
}

#[derive(Serialize)]
pub struct JrpcError<'a> {
    pub code: i32,
    pub message: &'a str,
}

pub const JSONRPC_FIELD: &str = "jsonrpc";
pub const JSONRPC_VERSION: &str = "2.0";
