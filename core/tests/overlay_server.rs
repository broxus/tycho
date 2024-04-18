use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytesize::ByteSize;
use everscale_crypto::ed25519;
use everscale_types::models::BlockId;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tl_proto::{TlRead, TlWrite};
use tycho_core::blockchain_client::BlockchainClient;
use tycho_core::overlay_client::public_overlay_client::PublicOverlayClient;
use tycho_core::overlay_client::settings::OverlayClientSettings;
use tycho_core::overlay_server::OverlayServer;
use tycho_core::proto::overlay::{
    ArchiveInfo, BlockFull, Data, KeyBlockIds, PersistentStatePart, Response,
};
use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayConfig, OverlayId, OverlayService, PeerId,
    PeerResolver, PublicOverlay, Request, Router, Service, ServiceRequest,
};
use tycho_storage::{Db, DbOptions, Storage};

mod node {
    use everscale_crypto::ed25519;
    use std::net::Ipv4Addr;
    use std::time::Duration;
    use tycho_network::{
        DhtConfig, DhtService, Network, OverlayConfig, OverlayService, PeerResolver, Router,
    };

    pub struct NodeBase {
        pub network: Network,
        pub dht_service: DhtService,
        pub overlay_service: OverlayService,
        pub peer_resolver: PeerResolver,
    }

    impl NodeBase {
        pub fn with_random_key() -> Self {
            let key = ed25519::SecretKey::generate(&mut rand::thread_rng());
            let local_id = ed25519::PublicKey::from(&key).into();

            let (dht_tasks, dht_service) = DhtService::builder(local_id)
                .with_config(make_fast_dht_config())
                .build();

            let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
                .with_config(make_fast_overlay_config())
                .with_dht_service(dht_service.clone())
                .build();

            let router = Router::builder()
                .route(dht_service.clone())
                .route(overlay_service.clone())
                .build();

            let network = Network::builder()
                .with_private_key(key.to_bytes())
                .with_service_name("test-service")
                .build((Ipv4Addr::LOCALHOST, 0), router)
                .unwrap();

            dht_tasks.spawn(&network);
            overlay_tasks.spawn(&network);

            let peer_resolver = dht_service.make_peer_resolver().build(&network);

            Self {
                network,
                dht_service,
                overlay_service,
                peer_resolver,
            }
        }
    }

    pub fn make_fast_dht_config() -> DhtConfig {
        DhtConfig {
            local_info_announce_period: Duration::from_secs(1),
            local_info_announce_period_max_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            routing_table_refresh_period_max_jitter: Duration::from_secs(1),
            ..Default::default()
        }
    }

    pub fn make_fast_overlay_config() -> OverlayConfig {
        OverlayConfig {
            public_overlay_peer_store_period: Duration::from_secs(1),
            public_overlay_peer_store_max_jitter: Duration::from_secs(1),
            public_overlay_peer_exchange_period: Duration::from_secs(1),
            public_overlay_peer_exchange_max_jitter: Duration::from_secs(1),
            public_overlay_peer_discovery_period: Duration::from_secs(1),
            public_overlay_peer_discovery_max_jitter: Duration::from_secs(1),
            ..Default::default()
        }
    }
}

mod storage {
    use anyhow::Result;
    use bytesize::ByteSize;
    use everscale_types::boc::Boc;
    use everscale_types::cell::Cell;
    use everscale_types::models::ShardState;
    use std::sync::Arc;
    use tycho_storage::{Db, DbOptions, Storage};

    #[derive(Clone)]
    struct ShardStateCombined {
        cell: Cell,
        state: ShardState,
    }

    impl ShardStateCombined {
        fn from_file(path: impl AsRef<str>) -> Result<Self> {
            let bytes = std::fs::read(path.as_ref())?;
            let cell = Boc::decode(&bytes)?;
            let state = cell.parse()?;
            Ok(Self { cell, state })
        }

        fn gen_utime(&self) -> Option<u32> {
            match &self.state {
                ShardState::Unsplit(s) => Some(s.gen_utime),
                ShardState::Split(_) => None,
            }
        }

        fn min_ref_mc_seqno(&self) -> Option<u32> {
            match &self.state {
                ShardState::Unsplit(s) => Some(s.min_ref_mc_seqno),
                ShardState::Split(_) => None,
            }
        }
    }

    pub(crate) async fn init_storage() -> Result<Arc<Storage>> {
        let tmp_dir = tempfile::tempdir()?;
        let root_path = tmp_dir.path();

        // Init rocksdb
        let db_options = DbOptions {
            rocksdb_lru_capacity: ByteSize::kb(1024),
            cells_cache_size: ByteSize::kb(1024),
        };
        let db = Db::open(root_path.join("db_storage"), db_options)?;

        // Init storage
        let storage = Storage::new(
            db,
            root_path.join("file_storage"),
            db_options.cells_cache_size.as_u64(),
        )?;
        assert!(storage.node_state().load_init_mc_block_id().is_err());

        Ok(storage)
    }
}

struct Node {
    network: Network,
    public_overlay: PublicOverlay,
    dht_client: DhtClient,
}

impl Node {
    fn with_random_key(storage: Arc<Storage>) -> Self {
        let node::NodeBase {
            network,
            dht_service,
            overlay_service,
            peer_resolver,
        } = node::NodeBase::with_random_key();
        let public_overlay = PublicOverlay::builder(PUBLIC_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .build(OverlayServer::new(storage, true));
        overlay_service.add_public_overlay(&public_overlay);

        let dht_client = dht_service.make_client(&network);

        Self {
            network,
            public_overlay,
            dht_client,
        }
    }

    async fn public_overlay_query<Q, A>(&self, peer_id: &PeerId, req: Q) -> anyhow::Result<A>
    where
        Q: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        self.public_overlay
            .query(&self.network, peer_id, Request::from_tl(req))
            .await?
            .parse_tl::<A>()
            .map_err(Into::into)
    }
}

fn make_network(storage: Arc<Storage>, node_count: usize) -> Vec<Node> {
    let nodes = (0..node_count)
        .map(|_| Node::with_random_key(storage.clone()))
        .collect::<Vec<_>>();

    let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);

    for node in &nodes {
        node.dht_client
            .add_peer(Arc::new(common_peer_info.clone()))
            .unwrap();
    }

    nodes
}

#[tokio::test]
async fn overlay_server_with_empty_storage() -> Result<()> {
    tycho_util::test::init_logger("public_overlays_accessible");

    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }

    let storage = storage::init_storage().await?;

    const NODE_COUNT: usize = 10;
    let nodes = make_network(storage, NODE_COUNT);

    tracing::info!("discovering nodes");
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut peer_states = BTreeMap::<&PeerId, PeerState>::new();

        for (i, left) in nodes.iter().enumerate() {
            for (j, right) in nodes.iter().enumerate() {
                if i == j {
                    continue;
                }

                let left_id = left.network.peer_id();
                let right_id = right.network.peer_id();

                if left.public_overlay.read_entries().contains(right_id) {
                    peer_states.entry(left_id).or_default().knows_about += 1;
                    peer_states.entry(right_id).or_default().known_by += 1;
                }
            }
        }

        tracing::info!("{peer_states:#?}");

        let total_filled = peer_states
            .values()
            .filter(|state| state.knows_about == nodes.len() - 1)
            .count();

        tracing::info!(
            "peers with filled overlay: {} / {}",
            total_filled,
            nodes.len()
        );
        if total_filled == nodes.len() {
            break;
        }
    }

    tracing::info!("resolving entries...");
    for node in &nodes {
        let resolved = FuturesUnordered::new();
        for entry in node.public_overlay.read_entries().iter() {
            let handle = entry.resolver_handle.clone();
            resolved.push(async move { handle.wait_resolved().await });
        }

        // Ensure all entries are resolved.
        resolved.collect::<Vec<_>>().await;
        tracing::info!(
            peer_id = %node.network.peer_id(),
            "all entries resolved",
        );
    }

    tracing::info!("making overlay requests...");
    for node in nodes {
        let client = BlockchainClient::new(
            PublicOverlayClient::new(
                node.network,
                node.public_overlay,
                OverlayClientSettings::default(),
            )
            .await,
        );

        let result = client.get_block_full(BlockId::default()).await;
        assert!(result.is_ok());

        if let Ok(response) = &result {
            assert_eq!(response.data(), &Response::Ok(BlockFull::Empty));
        }

        let result = client.get_next_block_full(BlockId::default()).await;
        assert!(result.is_ok());

        if let Ok(response) = &result {
            assert_eq!(response.data(), &Response::Ok(BlockFull::Empty));
        }

        let result = client.get_next_key_block_ids(BlockId::default(), 10).await;
        assert!(result.is_ok());

        if let Ok(response) = &result {
            let ids = KeyBlockIds {
                blocks: vec![],
                incomplete: true,
            };
            assert_eq!(response.data(), &Response::Ok(ids));
        }

        let result = client
            .get_persistent_state_part(BlockId::default(), BlockId::default(), 0, 0)
            .await;
        assert!(result.is_ok());

        if let Ok(response) = &result {
            assert_eq!(
                response.data(),
                &Response::Ok(PersistentStatePart::NotFound)
            );
        }

        let result = client.get_archive_info(0).await;
        assert!(result.is_ok());

        if let Ok(response) = &result {
            assert_eq!(response.data(), &Response::Err);
        }

        let result = client.get_archive_slice(0, 0, 100).await;
        assert!(result.is_ok());

        if let Ok(response) = &result {
            assert_eq!(response.data(), &Response::Err);
        }

        break;
    }

    tracing::info!("done!");
    Ok(())
}

static PUBLIC_OVERLAY_ID: OverlayId = OverlayId([1; 32]);
