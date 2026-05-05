use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use anyhow::Result;
use tycho_core::global_config::GlobalConfig;
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_core::proto::overlay;
use tycho_network::{
    DhtConfig, DhtService, Network, OverlayConfig, OverlayService, PublicOverlay, Response, Router,
    Service, ServiceRequest,
};
use tycho_util::futures::BoxFutureOrNoop;

const GLOBAL_CONFIG_PATH: &str = "./global-config.json";

struct NoopService;

impl Service<ServiceRequest> for NoopService {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Response>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;

    fn on_query(&self, _: ServiceRequest) -> Self::OnQueryFuture {
        BoxFutureOrNoop::Noop
    }

    fn on_message(&self, _: ServiceRequest) -> Self::OnMessageFuture {
        BoxFutureOrNoop::Noop
    }
}

async fn make_client(global_config: &GlobalConfig) -> Result<PublicOverlayClient> {
    let key = rand::random::<tycho_crypto::ed25519::SecretKey>();
    let local_id = tycho_crypto::ed25519::PublicKey::from(&key).into();

    let (dht_tasks, dht_service) = DhtService::builder(local_id)
        .with_config(DhtConfig::default())
        .build();

    let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
        .with_config(OverlayConfig::default())
        .with_dht_service(dht_service.clone())
        .build();

    let router = Router::builder()
        .route(dht_service.clone())
        .route(overlay_service.clone())
        .build();

    let network = Network::builder()
        .with_private_key(key.to_bytes())
        .build((Ipv4Addr::UNSPECIFIED, 0), router)?;

    dht_tasks.spawn(&network, &global_config.bootstrap_peers)?;
    overlay_tasks.spawn(&network);

    let peer_resolver = dht_service.make_peer_resolver().build(&network);

    let overlay_id = global_config.zerostate.compute_public_overlay_id();
    let public_overlay = PublicOverlay::builder(overlay_id)
        .with_peer_resolver(peer_resolver)
        .build(NoopService);

    overlay_service.add_public_overlay(&public_overlay);

    let client = PublicOverlayClient::new(network, public_overlay, Default::default());

    Ok(client)
}

async fn wait_for_neighbours(client: &PublicOverlayClient, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    loop {
        if !client.neighbours().get_active_neighbours().is_empty() {
            return Ok(());
        }
        if start.elapsed() > timeout {
            anyhow::bail!("timeout waiting for neighbours");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn overlay_ping(
    client: &PublicOverlayClient,
    peer_id: &tycho_network::PeerId,
) -> Result<Duration, String> {
    let start = Instant::now();
    let req = tycho_network::Request::from_tl(overlay::Ping);
    let resp = client
        .overlay()
        .query(client.network(), peer_id, req)
        .await
        .map_err(|e| format!("{e}"))?;

    tl_proto::deserialize::<overlay::Pong>(&resp.body).map_err(|e| format!("{e}"))?;
    Ok(start.elapsed())
}

#[tokio::test]
#[ignore = "requires global config and reachable network"]
async fn overlay_rate_limits() -> Result<()> {
    tycho_util::test::init_logger("overlay_rate_limits", "info");

    let config_path =
        std::env::var("GLOBAL_CONFIG").unwrap_or_else(|_| GLOBAL_CONFIG_PATH.to_owned());
    let global_config = GlobalConfig::from_file(&config_path)?;
    tracing::info!(
        overlay_id = %global_config.zerostate.compute_public_overlay_id(),
        bootstrap_peers = global_config.bootstrap_peers.len(),
        "loaded global config"
    );

    let client = make_client(&global_config).await?;

    tracing::info!("waiting for neighbours...");
    wait_for_neighbours(&client, Duration::from_secs(30)).await?;
    let neighbours = client.neighbours().get_active_neighbours();
    tracing::info!(count = neighbours.len(), "neighbours discovered");

    // pick one neighbour for all requests
    let target = *neighbours.first().unwrap().peer_id();
    tracing::info!(%target, "pinning to single neighbour");

    // verify connectivity
    let rtt = overlay_ping(&client, &target)
        .await
        .expect("initial ping failed");
    tracing::info!(?rtt, "initial ping ok");

    // concurrent burst to single peer
    let n = 100;
    tracing::info!(n, "sending concurrent burst");
    let start = Instant::now();

    let futs: Vec<_> = (0..n)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { overlay_ping(&client, &target).await })
        })
        .collect();

    let mut ok = 0usize;
    let mut errors = 0usize;
    for fut in futs {
        match fut.await? {
            Ok(_) => ok += 1,
            Err(_) => errors += 1,
        }
    }
    let elapsed = start.elapsed();

    tracing::info!(ok, errors, ?elapsed, "burst results");
    println!("concurrent burst {n}: ok={ok}, errors={errors}, elapsed={elapsed:?}");

    if errors > 0 {
        println!("rate limiting kicked in after ~{ok} requests");
    } else {
        println!("no rate limiting observed");
    }

    // wait for cooldown
    println!("waiting 35s for cooldown...");
    tokio::time::sleep(Duration::from_secs(35)).await;

    // single request should work
    match overlay_ping(&client, &target).await {
        Ok(rtt) => println!("after cooldown: ok, rtt={rtt:?}"),
        Err(e) => println!("after cooldown: still failing: {e}"),
    }

    // sustained 1 rps
    let mut ok_count = 0;
    for _ in 0..5 {
        if overlay_ping(&client, &target).await.is_ok() {
            ok_count += 1;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!("sustained 1rps: {ok_count}/5 ok");

    Ok(())
}
