use std::time::Duration;

use anyhow::Result;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::RequestExt;
use tokio::net::TcpListener;

pub use self::jrpc::JrpcEndpointCache;
pub use self::proto::ProtoEndpointCache;
use crate::state::RpcState;

mod jrpc;
mod proto;
mod tonapi;

pub struct RpcEndpoint {
    listener: TcpListener,
    state: RpcState,
}

impl RpcEndpoint {
    pub async fn bind(state: RpcState) -> Result<Self> {
        let listener = TcpListener::bind(state.config().listen_addr).await?;
        Ok(Self { listener, state })
    }

    pub async fn serve(self) -> std::io::Result<()> {
        use tower::ServiceBuilder;
        use tower_http::cors::CorsLayer;
        use tower_http::timeout::TimeoutLayer;

        // Prepare middleware
        let service = ServiceBuilder::new()
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(CorsLayer::permissive())
            .layer(TimeoutLayer::new(Duration::from_secs(25)));

        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        // Prepare routes
        let router = axum::Router::new()
            .route("/", get(health_check))
            .route("/", post(common_route))
            .route("/rpc", post(common_route))
            .route("/proto", post(common_route))
            .nest("/v2", tonapi::router())
            .layer(service)
            .with_state(self.state);

        // Start server
        axum::serve(self.listener, router).await
    }
}

fn health_check() -> futures_util::future::Ready<impl IntoResponse> {
    futures_util::future::ready(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_millis()
            .to_string(),
    )
}

async fn common_route(state: State<RpcState>, req: Request) -> Response {
    use axum::http::StatusCode;

    match get_mime_type(&req) {
        Some(mime) if mime.starts_with(APPLICATION_JSON) => match req.extract().await {
            Ok(method) => jrpc::route(state, method).await,
            Err(e) => e.into_response(),
        },
        Some(mime) if mime.starts_with(APPLICATION_PROTOBUF) => match req.extract().await {
            Ok(request) => proto::route(state, request).await,
            Err(e) => e.into_response(),
        },
        _ => StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response(),
    }
}

fn get_mime_type(req: &Request) -> Option<&str> {
    req.headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
}

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_PROTOBUF: &str = "application/x-protobuf";

const MAX_REQUEST_SIZE: usize = 2 << 17; // 256kb

// === Error codes ===

const INTERNAL_ERROR_CODE: i32 = -32000;
const NOT_READY_CODE: i32 = -32001;
const NOT_SUPPORTED_CODE: i32 = -32002;
const INVALID_BOC_CODE: i32 = -32003;
const TOO_LARGE_LIMIT_CODE: i32 = -32004;

const PARSE_ERROR_CODE: i32 = -32700;
const INVALID_REQUEST_CODE: i32 = -32600;
const METHOD_NOT_FOUND_CODE: i32 = -32601;
const INVALID_PARAMS_CODE: i32 = -32602;
