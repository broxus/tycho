use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tycho_core::blockchain_rpc::{BlockchainRpcClient, BlockchainRpcService, BroadcastListener};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_core::proto::blockchain::{BlockFull, KeyBlockIds, PersistentStateInfo};
use tycho_network::{DhtClient, InboundRequestMeta, Network, OverlayId, PeerId, PublicOverlay};
use tycho_storage::Storage;

use crate::common::archive::*;

mod common;

trait TestNode {
    fn network(&self) -> &Network;
    fn public_overlay(&self) -> &PublicOverlay;
}

impl TestNode for common::node::Node {
    fn network(&self) -> &Network {
        self.network()
    }

    fn public_overlay(&self) -> &PublicOverlay {
        self.public_overlay()
    }
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
    tycho_util::test::init_logger("overlay_server_msg_broadcast", "debug");

    #[derive(Default, Clone)]
    struct BroadcastCounter {
        total_received: Arc<AtomicUsize>,
    }

    impl BroadcastListener for BroadcastCounter {
        type HandleMessageFut<'a> = futures_util::future::Ready<()>;

        fn handle_message<'a>(
            &'a self,
            meta: Arc<InboundRequestMeta>,
            message: bytes::Bytes,
        ) -> Self::HandleMessageFut<'a> {
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
        base: common::node::NodeBase,
        dht_client: DhtClient,
        blockchain_client: BlockchainRpcClient,
    }

    impl Node {
        fn with_random_key(storage: Storage, broadcast_counter: BroadcastCounter) -> Self {
            const OVERLAY_ID: OverlayId = OverlayId([0x33; 32]);

            let base = common::node::NodeBase::with_random_key();
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

            let blockchain_client = BlockchainRpcClient::new(PublicOverlayClient::new(
                base.network.clone(),
                public_overlay,
                Default::default(),
            ));

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
    }

    let broadcast_counter = BroadcastCounter::default();
    let (storage, _tmp_dir) = common::storage::init_storage().await?;

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

    tracing::info!("broadcasting messages...");
    for node in &nodes {
        node.blockchain_client
            .broadcast_external_message(b"hello world")
            .await;
    }

    // TODO: Replace `* 5` with a proper number of expected messages, possibly a range check
    assert_eq!(
        broadcast_counter.total_received.load(Ordering::Acquire),
        nodes.len() * 5
    );

    Ok(())
}

#[tokio::test]
async fn overlay_server_with_empty_storage() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_with_empty_storage", "debug");

    let (storage, _tmp_dir) = Storage::new_temp()?;

    let nodes = common::node::make_network(storage, 10);

    discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::new(PublicOverlayClient::new(
        node.network().clone(),
        node.public_overlay().clone(),
        Default::default(),
    ));

    let result = client.get_block_full(&BlockId::default()).await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert_eq!(response.data(), &BlockFull::Empty);
    }

    let result = client.get_next_block_full(&BlockId::default()).await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert_eq!(response.data(), &BlockFull::Empty);
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

    let result = client.get_archive_slice(0, 0, 100).await;
    assert!(result.is_err());

    tracing::info!("done!");
    Ok(())
}

#[tokio::test]
async fn overlay_server_blocks() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_blocks", "debug");

    let (storage, _tmp_dir) = common::storage::init_storage().await?;

    let nodes = common::node::make_network(storage, 10);

    discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::new(PublicOverlayClient::new(
        node.network().clone(),
        node.public_overlay().clone(),
        Default::default(),
    ));

    let archive = common::storage::get_archive()?;
    for (block_id, archive_data) in archive.blocks {
        if block_id.shard.is_masterchain() {
            let result = client.get_block_full(&block_id).await;
            assert!(result.is_ok());

            if let Ok(response) = &result {
                match response.data() {
                    BlockFull::Found {
                        block_id,
                        block,
                        proof,
                        ..
                    } => {
                        let block = deserialize_block(block_id, block)?;
                        assert_eq!(block, archive_data.block.unwrap().data);

                        let proof = deserialize_block_proof(block_id, proof, false)?;
                        let archive_proof = archive_data.proof.unwrap();
                        assert_eq!(proof.proof_for, archive_proof.data.proof_for);
                        assert_eq!(proof.root, archive_proof.data.root);
                    }
                    _ => anyhow::bail!("block not found"),
                }
            }
        }
    }

    tracing::info!("done!");
    Ok(())
}
