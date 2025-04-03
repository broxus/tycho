use std::borrow::Cow;

use axum::async_trait;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::util::error_codes::*;

// Counters
const METRIC_IN_REQ_FAIL_TOTAL: &str = "tycho_rpc_in_req_fail_total";

pub trait ParseParams {
    type Params;

    fn parse_params(self, params: &RawValue) -> Result<Self::Params, serde_json::Error>;

    fn method_name(&self) -> &'static str;
}

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
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
        #[serde(rename_all = "camelCase")]
        $vis enum $method_name_enum {
            $(
                $(#[$($method_meta)*])*
                $method_name,
            )*
        }

        impl $crate::util::jrpc_extractor::ParseParams for $method_name_enum {
            type Params = $method_enum;

            fn parse_params(self, params: &serde_json::value::RawValue) -> Result<Self::Params, serde_json::Error> {
                let params = params.get();
                match self {
                    $(Self::$method_name => serde_json::from_str(params).map($method_enum::$method_name),)*
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

pub(crate) use declare_jrpc_method;

pub struct Jrpc<ID, T: ParseParams> {
    pub id: ID,
    pub params: <T as ParseParams>::Params,
    pub method: &'static str,
}

#[async_trait]
impl<S, T, ID> FromRequest<S> for Jrpc<ID, T>
where
    ID: Serialize + for<'de> Deserialize<'de>,
    T: ParseParams + for<'de> Deserialize<'de>,
    S: Send + Sync,
{
    type Rejection = JrpcErrorResponse<ID>;

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
                });
            }
        };

        let (id, code, message) = match serde_json::from_slice::<Request<'_, T, ID>>(&bytes) {
            Ok(req) if req.jsonrpc == JSONRPC_VERSION => match req.method {
                ParsedMethod::Known(known) => {
                    let method = known.method_name();
                    match known.parse_params(req.params) {
                        Ok(params) => {
                            return Ok(Self {
                                id: req.id,
                                method,
                                params,
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

        Err(JrpcErrorResponse { id, code, message })
    }
}

pub struct JrpcOkResponse<ID, T> {
    pub id: ID,
    pub result: T,
}

impl<ID, T> JrpcOkResponse<ID, T> {
    pub fn new(id: ID, result: T) -> Self {
        Self { id, result }
    }
}

impl<ID: Serialize, T: Serialize> Serialize for JrpcOkResponse<ID, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut ser = serializer.serialize_struct("JrpcResponse", 3)?;
        ser.serialize_field(JSONRPC_FIELD, JSONRPC_VERSION)?;
        ser.serialize_field("id", &self.id)?;
        ser.serialize_field("result", &self.result)?;
        ser.end()
    }
}

impl<ID: Serialize, T: Serialize> IntoResponse for JrpcOkResponse<ID, T> {
    fn into_response(self) -> Response {
        (StatusCode::OK, axum::Json(self)).into_response()
    }
}

pub struct JrpcErrorResponse<ID> {
    pub id: Option<ID>,
    pub code: i32,
    pub message: Cow<'static, str>,
}

impl<ID: Serialize> Serialize for JrpcErrorResponse<ID> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        #[derive(Serialize)]
        struct Error<'a> {
            code: i32,
            message: &'a str,
        }

        let mut ser = serializer.serialize_struct("JrpcResponse", 3)?;
        ser.serialize_field(JSONRPC_FIELD, JSONRPC_VERSION)?;
        ser.serialize_field("id", &self.id)?;
        ser.serialize_field("error", &Error {
            code: self.code,
            message: &self.message,
        })?;
        ser.end()
    }
}

impl<ID: Serialize> IntoResponse for JrpcErrorResponse<ID> {
    fn into_response(self) -> Response {
        metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);

        (StatusCode::OK, axum::Json(self)).into_response()
    }
}

const JSONRPC_FIELD: &str = "jsonrpc";
const JSONRPC_VERSION: &str = "2.0";
