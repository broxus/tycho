use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use futures_util::future::{Ready, ready};
use tokio::time::sleep;
use tycho_network::{
    Connection, Network, NetworkConfig, NetworkExt, Request, Response, Routable, Router,
    Service, ServiceRequest,
};
use tycho_util::test::init_logger;
mod common;
#[derive(Clone, Copy)]
struct SlowService {
    delay: Duration,
}
impl Service<ServiceRequest> for SlowService {
    type QueryResponse = Response;
    type OnQueryFuture = std::pin::Pin<
        Box<dyn Future<Output = Option<Self::QueryResponse>> + Send>,
    >;
    type OnMessageFuture = Ready<()>;
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let peer_id = req.metadata.peer_id;
        let delay = self.delay;
        tracing::info!(
            % peer_id, "SlowService: Handling request, delaying for {:?}", delay
        );
        match req.parse_tl::<common::Ping>() {
            Ok(ping) => {
                let value_to_echo = ping.value;
                Box::pin(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        35u32,
                    );
                    {
                        __guard.end_section(36u32);
                        let __result = sleep(delay).await;
                        __guard.start_section(36u32);
                        __result
                    };
                    tracing::info!(
                        % peer_id, "SlowService: Delay finished for value {}",
                        value_to_echo
                    );
                    Some(
                        Response::from_tl(common::Pong {
                            value: value_to_echo,
                        }),
                    )
                })
            }
            Err(e) => {
                tracing::error!(
                    % peer_id, "SlowService: Failed to parse request: {}", e
                );
                Box::pin(async {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        45u32,
                    );
                    None
                })
            }
        }
    }
    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        ready(())
    }
}
impl Routable for SlowService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [common::Ping::TL_ID]
    }
}
fn create_network_with_limit<S>(
    max_concurrent_requests: usize,
    service: S,
) -> Result<Network>
where
    S: Service<ServiceRequest, QueryResponse = Response> + Routable + Clone + Send + Sync
        + 'static,
{
    let mut config = NetworkConfig::default();
    config.max_concurrent_requests_per_peer = max_concurrent_requests;
    let network = Network::builder()
        .with_random_private_key()
        .with_config(config)
        .build(
            (std::net::Ipv4Addr::LOCALHOST, 0),
            Router::builder().route(service).build(),
        )?;
    Ok(network)
}
#[tokio::test]
async fn test_inbound_request_rate_limit() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(test_inbound_request_rate_limit)),
        file!(),
        81u32,
    );
    init_logger("test_inbound_request_rate_limit", "debug");
    let receiver_node = create_network_with_limit(
        1,
        SlowService {
            delay: Duration::from_secs(3),
        },
    )?;
    #[derive(Clone)]
    struct EchoService;
    impl Service<ServiceRequest> for EchoService {
        type QueryResponse = Response;
        type OnQueryFuture = Ready<Option<Response>>;
        type OnMessageFuture = Ready<()>;
        fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
            ready(
                req
                    .parse_tl::<common::Ping>()
                    .ok()
                    .map(|p| Response::from_tl(common::Pong { value: p.value })),
            )
        }
        fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
            ready(())
        }
    }
    impl Routable for EchoService {
        fn query_ids(&self) -> impl IntoIterator<Item = u32> {
            [common::Ping::TL_ID]
        }
    }
    let sender_node = create_network_with_limit(10, EchoService)?;
    let receiver_peer_info = Arc::new(receiver_node.sign_peer_info(0, u32::MAX));
    let sender_peer_info = Arc::new(sender_node.sign_peer_info(0, u32::MAX));
    sender_node.known_peers().insert(receiver_peer_info.clone(), false)?;
    receiver_node.known_peers().insert(sender_peer_info, false)?;
    let receiver_addr = receiver_peer_info.iter_addresses().next().unwrap().clone();
    tracing::info!("Connecting sender to receiver...");
    {
        __guard.end_section(130u32);
        let __result = sender_node.connect(receiver_addr, receiver_node.peer_id()).await;
        __guard.start_section(130u32);
        __result
    }?;
    {
        __guard.end_section(132u32);
        let __result = sleep(Duration::from_millis(200)).await;
        __guard.start_section(132u32);
        __result
    };
    tracing::info!("Connection established.");
    tracing::info!("Sending first request (will block the handler slot)...");
    let first_request_handle = tokio::spawn({
        let sender = sender_node.clone();
        let receiver_id = *receiver_node.peer_id();
        async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                140u32,
            );
            {
                __guard.end_section(143u32);
                let __result = sender
                    .query(&receiver_id, Request::from_tl(common::Ping { value: 1 }))
                    .await;
                __guard.start_section(143u32);
                __result
            }
        }
    });
    {
        __guard.end_section(148u32);
        let __result = sleep(Duration::from_millis(500)).await;
        __guard.start_section(148u32);
        __result
    };
    tracing::info!("Sending second request (should be rejected)...");
    let second_request_result = {
        __guard.end_section(158u32);
        let __result = sender_node
            .query(receiver_node.peer_id(), Request::from_tl(common::Ping { value: 2 }))
            .await;
        __guard.start_section(158u32);
        __result
    };
    tracing::info!("Verifying second request failed...");
    assert!(
        second_request_result.is_err(),
        "Second request should have failed due to rate limit."
    );
    let err = second_request_result.err().unwrap();
    let is_rate_limit_error = err
        .to_string()
        .contains(&Connection::LIMIT_EXCEEDED_ERROR_CODE.to_string());
    assert!(is_rate_limit_error, "Error type indicates rate limit rejection: {err:?}");
    tracing::info!("Waiting for the first request to complete successfully...");
    let first_result = {
        __guard.end_section(178u32);
        let __result = first_request_handle.await;
        __guard.start_section(178u32);
        __result
    }??;
    let first_response: common::Pong = first_result.parse_tl()?;
    assert_eq!(
        first_response.value, 1, "First request did not succeed or returned wrong value."
    );
    tracing::info!("First request completed as expected.");
    {
        __guard.end_section(186u32);
        let __result = sleep(Duration::from_millis(100)).await;
        __guard.start_section(186u32);
        __result
    };
    tracing::info!("Sending third request (should succeed now)...");
    let third_result = {
        __guard.end_section(195u32);
        let __result = sender_node
            .query(receiver_node.peer_id(), Request::from_tl(common::Ping { value: 3 }))
            .await;
        __guard.start_section(195u32);
        __result
    }?;
    let third_response: common::Pong = third_result.parse_tl()?;
    assert_eq!(third_response.value, 3, "Third request should have succeeded.");
    tracing::info!("Third request completed as expected.");
    tracing::info!("Test finished successfully.");
    Ok(())
}
