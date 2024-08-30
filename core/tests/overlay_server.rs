use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tycho_block_util::block::{BlockProofStuff, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_core::blockchain_rpc::{BlockchainRpcClient, BlockchainRpcService, BroadcastListener};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_core::proto::blockchain::{BlockFull, KeyBlockIds, PersistentStateInfo};
use tycho_network::{DhtClient, InboundRequestMeta, Network, OverlayId, PeerId, PublicOverlay};
use tycho_storage::Storage;

mod network;
mod storage;
mod utils;

trait TestNode {
    fn network(&self) -> &Network;
    fn public_overlay(&self) -> &PublicOverlay;
    fn force_update_validators(&self, peers: Vec<PeerId>);
}

impl TestNode for network::Node {
    fn network(&self) -> &Network {
        self.network()
    }

    fn public_overlay(&self) -> &PublicOverlay {
        self.public_overlay()
    }

    fn force_update_validators(&self, _: Vec<PeerId>) {}
}

async fn discover<N: TestNode>(nodes: &[N]) -> Result<()> {
    tracing::info!("discovering nodes");
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut peer_states = BTreeMap::<&PeerId, PeerState>::new();

        for (i, left) in nodes.iter().enumerate() {
            for (j, right) in nodes.iter().enumerate() {
                if i == j {
                    continue;
                }

                let left_id = left.network().peer_id();
                let right_id = right.network().peer_id();

                if left.public_overlay().read_entries().contains(right_id) {
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
    for node in nodes {
        let resolved = FuturesUnordered::new();
        for entry in node.public_overlay().read_entries().iter() {
            let handle = entry.resolver_handle.clone();
            resolved.push(async move { handle.wait_resolved().await });
        }

        // Ensure all entries are resolved.
        resolved.collect::<Vec<_>>().await;
        tracing::info!(
            peer_id = %node.network().peer_id(),
            "all entries resolved",
        );
    }

    Ok(())
}

#[derive(Debug, Default)]
struct PeerState {
    knows_about: usize,
    known_by: usize,
}

#[tokio::test]
async fn overlay_server_msg_broadcast() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_msg_broadcast", "info");

    #[derive(Default, Clone)]
    struct BroadcastCounter {
        total_received: Arc<AtomicUsize>,
    }

    impl BroadcastListener for BroadcastCounter {
        type HandleMessageFut<'a> = futures_util::future::Ready<()>;

        fn handle_message(
            &self,
            meta: Arc<InboundRequestMeta>,
            message: bytes::Bytes,
        ) -> Self::HandleMessageFut<'_> {
            tracing::info!(
                peer_id = %meta.peer_id,
                remote_addr = %meta.remote_address,
                len = message.len(),
                "received broadcast from peer",
            );
            self.total_received.fetch_add(1, Ordering::Release);
            futures_util::future::ready(())
        }
    }

    struct Node {
        base: network::NodeBase,
        dht_client: DhtClient,
        blockchain_client: BlockchainRpcClient,
    }

    impl Node {
        fn with_random_key(storage: Storage, broadcast_counter: BroadcastCounter) -> Self {
            const OVERLAY_ID: OverlayId = OverlayId([0x33; 32]);

            let base = network::NodeBase::with_random_key();
            let public_overlay = PublicOverlay::builder(OVERLAY_ID)
                .with_peer_resolver(base.peer_resolver.clone())
                .build(
                    BlockchainRpcService::builder()
                        .with_storage(storage)
                        .with_broadcast_listener(broadcast_counter)
                        .build(),
                );
            base.overlay_service.add_public_overlay(&public_overlay);

            let dht_client = base.dht_service.make_client(&base.network);
            let client =
                PublicOverlayClient::new(base.network.clone(), public_overlay, Default::default());

            let blockchain_client = BlockchainRpcClient::builder()
                .with_public_overlay_client(client.clone())
                .build();

            Self {
                base,
                dht_client,
                blockchain_client,
            }
        }
    }

    impl TestNode for Node {
        fn network(&self) -> &Network {
            &self.base.network
        }

        fn public_overlay(&self) -> &PublicOverlay {
            self.blockchain_client.overlay()
        }

        fn force_update_validators(&self, peers: Vec<PeerId>) {
            self.blockchain_client
                .overlay_client()
                .update_validator_set(&peers);
        }
    }

    let broadcast_counter = BroadcastCounter::default();
    let (storage, _tmp_dir) = storage::init_storage().await?;

    let nodes = (0..10)
        .map(|_| Node::with_random_key(storage.clone(), broadcast_counter.clone()))
        .collect::<Vec<_>>();

    {
        let first_peer = nodes.first().unwrap();
        let first_peer_info = first_peer.base.network.sign_peer_info(0, u32::MAX);
        for node in &nodes {
            node.dht_client
                .add_peer(Arc::new(first_peer_info.clone()))
                .unwrap();
        }
    }

    discover(&nodes).await?;

    let peers = nodes
        .iter()
        .map(|x| *x.dht_client.network().peer_id())
        .collect::<Vec<PeerId>>();

    tracing::info!("broadcasting messages...");
    for node in &nodes {
        node.force_update_validators(peers.clone());

        // NOTE: Yield is required to wait until validators are resolved.
        //       Might fail on multi-threaded tests. In that cast use sleep.
        tokio::task::yield_now().await;

        node.blockchain_client
            .broadcast_external_message(b"hello world")
            .await;
    }
    let total_received = broadcast_counter.total_received.load(Ordering::Acquire);

    assert!(total_received > nodes.len() * 4 && total_received <= nodes.len() * 5);

    Ok(())
}

#[tokio::test]
async fn overlay_server_with_empty_storage() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_with_empty_storage", "info");

    let (storage, _tmp_dir) = Storage::new_temp()?;

    let nodes = network::make_network(storage, 10);

    discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    let result = client.get_block_full(&BlockId::default()).await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert_eq!(response.data(), &BlockFull::NotFound);
    }

    let result = client.get_next_block_full(&BlockId::default()).await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert_eq!(response.data(), &BlockFull::NotFound);
    }

    let result = client.get_next_key_block_ids(&BlockId::default(), 10).await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        let ids = KeyBlockIds {
            block_ids: vec![],
            incomplete: true,
        };
        assert_eq!(response.data(), &ids);
    }

    let result = client.get_persistent_state_info(&BlockId::default()).await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert_eq!(response.data(), &PersistentStateInfo::NotFound);
    }

    let result = client.get_archive_info(0).await;
    assert!(result.is_err());

    let result = client.get_archive_chunk(0, 0).await;
    assert!(result.is_err());

    tracing::info!("done!");
    Ok(())
}

#[tokio::test]
async fn overlay_server_blocks() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_blocks", "info");

    let (storage, _tmp_dir) = storage::init_storage().await?;

    let nodes = network::make_network(storage, 10);

    discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    let archive_data = utils::read_file("archive.bin")?;
    let archive = utils::parse_archive(&archive_data)?;

    for block_id in archive.blocks.keys() {
        if block_id.shard.is_masterchain() {
            let result = client.get_block_full(block_id).await;
            assert!(result.is_ok());

            let (archive_block, archive_proof, archive_queue_diff) =
                archive.get_entry_by_id(block_id)?;

            if let Ok(response) = &result {
                match response.data() {
                    BlockFull::Found {
                        block_id,
                        block,
                        proof,
                        queue_diff,
                    } => {
                        let block = BlockStuff::deserialize_checked(block_id, block)?;
                        assert_eq!(block.as_ref(), archive_block.block());

                        let proof = BlockProofStuff::deserialize(block_id, proof)?;
                        assert_eq!(proof.as_ref().proof_for, archive_proof.as_ref().proof_for);
                        assert_eq!(proof.as_ref().root, archive_proof.as_ref().root);

                        let queue_diff = QueueDiffStuff::deserialize(block_id, queue_diff)?;
                        assert_eq!(queue_diff.diff(), archive_queue_diff.diff());
                    }
                    BlockFull::NotFound => anyhow::bail!("block not found"),
                }
            }
        }
    }

    tracing::info!("done!");
    Ok(())
}
