use std::time::Duration;

use anyhow::Result;
use axum::extract::{DefaultBodyLimit, FromRef, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, RequestExt, middleware};
use axum_client_ip::ClientIpSource;
use tokio::net::TcpListener;

pub use self::jrpc::JrpcEndpointCache;
pub use self::proto::ProtoEndpointCache;
pub use self::rate_limits::RpcRateLimitsConfig;
use crate::state::RpcState;
use crate::util::mime::{APPLICATION_JSON, APPLICATION_PROTOBUF, get_mime_type};

pub mod jrpc;
pub mod proto;
pub mod rate_limits;

pub struct RpcEndpointBuilder<C = ()> {
    common: RpcEndpointBuilderCommon,
    custom_routes: C,
}

impl Default for RpcEndpointBuilder {
    #[inline]
    fn default() -> Self {
        Self {
            common: Default::default(),
            custom_routes: (),
        }
    }
}

impl RpcEndpointBuilder<()> {
    pub fn empty() -> Self {
        Self {
            common: RpcEndpointBuilderCommon::empty(),
            custom_routes: (),
        }
    }

    pub fn with_custom_routes<S>(
        self,
        routes: axum::Router<S>,
    ) -> RpcEndpointBuilder<axum::Router<S>>
    where
        RpcState: FromRef<S>,
        S: Send + Sync,
    {
        RpcEndpointBuilder {
            common: self.common,
            custom_routes: routes,
        }
    }

    pub async fn bind(self, state: RpcState) -> Result<RpcEndpoint> {
        let listener = state.bind_socket().await?;

        let rl_config = &state.config().rate_limits;
        let ip_source = state.config().real_ip_source.clone();
        let rate_limiter = rl_config.clone().map(Into::into);
        let active_streams = rate_limits::ActiveStreamLimiter::new(
            state.config().subscriptions.max_streams_per_addr,
            rl_config.clone().map(|c| c.whitelist).unwrap_or_default(),
        );

        Ok(RpcEndpoint::from_parts(
            listener,
            self.common.build(),
            ip_source,
            rate_limiter,
            active_streams,
            state,
        ))
    }
}

impl<C> RpcEndpointBuilder<C> {
    pub fn with_healthcheck_route<T: Into<String>>(mut self, route: T) -> Self {
        self.common.healthcheck_route = Some(route.into());
        self
    }

    pub fn with_base_routes<I, T>(mut self, routes: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.common.base_routes = routes.into_iter().map(Into::into).collect();
        self
    }
}

impl<S> RpcEndpointBuilder<axum::Router<S>>
where
    RpcState: FromRef<S>,
    S: Send + Sync + Clone + 'static,
{
    pub async fn bind(self, state: S) -> Result<RpcEndpoint> {
        let rpc_state = RpcState::from_ref(&state);

        let listener = rpc_state.bind_socket().await?;

        let rl_config = &rpc_state.config().rate_limits;
        let ip_source = rpc_state.config().real_ip_source.clone();
        let rate_limiter = rl_config.clone().map(Into::into);
        let active_streams = rate_limits::ActiveStreamLimiter::new(
            rpc_state.config().subscriptions.max_streams_per_addr,
            rl_config.clone().map(|c| c.whitelist).unwrap_or_default(),
        );

        Ok(RpcEndpoint::from_parts(
            listener,
            self.common.build::<S>().merge(self.custom_routes),
            ip_source,
            rate_limiter,
            active_streams,
            state,
        ))
    }
}

struct RpcEndpointBuilderCommon {
    healthcheck_route: Option<String>,
    base_routes: Vec<String>,
}

impl Default for RpcEndpointBuilderCommon {
    fn default() -> Self {
        Self {
            healthcheck_route: Some("/".to_owned()),
            base_routes: vec!["/".to_owned(), "/rpc".to_owned(), "/proto".to_owned()],
        }
    }
}

impl RpcEndpointBuilderCommon {
    pub fn empty() -> Self {
        Self {
            healthcheck_route: None,
            base_routes: Vec::new(),
        }
    }

    fn build<S>(self) -> axum::Router<S>
    where
        RpcState: FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let mut router = axum::Router::new();

        if let Some(route) = self.healthcheck_route {
            router = router.route(&route, get(health_check));
        }
        for route in self.base_routes {
            router = router.route(&route, post(common_route));
        }
        router = router.merge(jrpc::stream_router::<S>());

        router
    }
}

pub struct RpcEndpoint {
    listener: TcpListener,
    router: axum::Router<()>,
}

impl RpcEndpoint {
    pub fn builder() -> RpcEndpointBuilder {
        RpcEndpointBuilder::default()
    }

    pub fn empty_builder() -> RpcEndpointBuilder {
        RpcEndpointBuilder::empty()
    }

    pub fn from_parts<S>(
        listener: TcpListener,
        router: axum::Router<S>,
        ip_source: ClientIpSource,
        rate_limiter: Option<rate_limits::RpcRateLimiter>,
        active_streams: rate_limits::ActiveStreamLimiter,
        state: S,
    ) -> Self
    where
        S: Clone + Send + Sync + 'static,
    {
        use tower::ServiceBuilder;
        use tower_http::cors::CorsLayer;
        use tower_http::timeout::TimeoutLayer;

        // Prepare middleware
        let service = ServiceBuilder::new()
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(CorsLayer::permissive())
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                Duration::from_secs(25),
            ));

        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        // Prepare routes
        let router = match rate_limiter {
            Some(rate_limiter) => router.layer(middleware::from_fn_with_state(
                rate_limiter,
                rate_limits::rate_limit,
            )),
            None => router,
        };

        let router = router
            .layer(Extension(ip_source))
            .layer(Extension(active_streams))
            .layer(service)
            .with_state(state);

        // Done
        Self { listener, router }
    }

    pub async fn serve(self) -> std::io::Result<()> {
        axum::serve(
            self.listener,
            self.router
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
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

const MAX_REQUEST_SIZE: usize = 2 << 17; // 256kb
