use std::time::Duration;
use anyhow::Result;
use axum::RequestExt;
use axum::extract::{DefaultBodyLimit, FromRef, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::net::TcpListener;
pub use self::jrpc::JrpcEndpointCache;
pub use self::proto::ProtoEndpointCache;
use crate::state::RpcState;
use crate::util::mime::{APPLICATION_JSON, APPLICATION_PROTOBUF, get_mime_type};
pub mod jrpc;
pub mod proto;
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(bind)),
            file!(),
            55u32,
        );
        let state = state;
        let listener = {
            __guard.end_section(56u32);
            let __result = state.bind_socket().await;
            __guard.start_section(56u32);
            __result
        }?;
        Ok(RpcEndpoint::from_parts(listener, self.common.build(), state))
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(bind)),
            file!(),
            86u32,
        );
        let state = state;
        let listener = {
            __guard.end_section(87u32);
            let __result = RpcState::from_ref(&state).bind_socket().await;
            __guard.start_section(87u32);
            __result
        }?;
        Ok(
            RpcEndpoint::from_parts(
                listener,
                self.common.build::<S>().merge(self.custom_routes),
                state,
            ),
        )
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
        state: S,
    ) -> Self
    where
        S: Clone + Send + Sync + 'static,
    {
        use tower::ServiceBuilder;
        use tower_http::cors::CorsLayer;
        use tower_http::timeout::TimeoutLayer;
        let service = ServiceBuilder::new()
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(CorsLayer::permissive())
            .layer(TimeoutLayer::new(Duration::from_secs(25)));
        #[cfg(feature = "compression")]
        let service = service
            .layer(tower_http::compression::CompressionLayer::new().gzip(true));
        let router = router.layer(service).with_state(state);
        Self { listener, router }
    }
    pub async fn serve(self) -> std::io::Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(serve)),
            file!(),
            174u32,
        );
        {
            __guard.end_section(175u32);
            let __result = axum::serve(self.listener, self.router).await;
            __guard.start_section(175u32);
            __result
        }
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
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(common_route)),
        file!(),
        189u32,
    );
    let state = state;
    let req = req;
    use axum::http::StatusCode;
    match get_mime_type(&req) {
        Some(mime) if mime.starts_with(APPLICATION_JSON) => {
            match {
                __guard.end_section(193u32);
                let __result = req.extract().await;
                __guard.start_section(193u32);
                __result
            } {
                Ok(method) => {
                    __guard.end_section(194u32);
                    let __result = jrpc::route(state, method).await;
                    __guard.start_section(194u32);
                    __result
                }
                Err(e) => e.into_response(),
            }
        }
        Some(mime) if mime.starts_with(APPLICATION_PROTOBUF) => {
            match {
                __guard.end_section(197u32);
                let __result = req.extract().await;
                __guard.start_section(197u32);
                __result
            } {
                Ok(request) => {
                    __guard.end_section(198u32);
                    let __result = proto::route(state, request).await;
                    __guard.start_section(198u32);
                    __result
                }
                Err(e) => e.into_response(),
            }
        }
        _ => StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response(),
    }
}
const MAX_REQUEST_SIZE: usize = 2 << 17;
