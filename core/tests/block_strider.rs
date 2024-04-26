use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tycho_core::block_strider::provider::BlockProvider;
use tycho_core::blockchain_client::BlockchainClient;
use tycho_core::overlay_client::{PublicOverlayClient, PublicOverlayClientConfig};
use tycho_network::PeerId;

mod common;

#[tokio::test]
async fn storage_block_strider() -> anyhow::Result<()> {
    tycho_util::test::init_logger("storage_block_strider");

    let (storage, tmp_dir) = common::storage::init_storage().await?;

    let archive = common::storage::get_archive()?;
    for (block_id, data) in archive.blocks {
        if block_id.shard.is_masterchain() {
            let block = storage.get_block(&block_id).await;
            assert!(block.is_some());

            if let Some(block) = block {
                let block = block?;

                assert_eq!(&block_id, block.id());
                assert_eq!(&data.block.unwrap().data, block.block());
            }
        }
    }

    tmp_dir.close()?;

    tracing::info!("done!");
    Ok(())
}

#[tokio::test]
async fn overlay_block_strider() -> anyhow::Result<()> {
    tycho_util::test::init_logger("overlay_block_strider");

    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }

    let (storage, tmp_dir) = common::storage::init_storage().await?;

    const NODE_COUNT: usize = 10;
    let nodes = common::node::make_network(storage, NODE_COUNT);

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
    for node in &nodes {
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

    tracing::info!("making overlay requests...");
    let node = nodes.first().unwrap();

    let client = BlockchainClient::new(
        PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            PublicOverlayClientConfig::default(),
        ),
        Default::default(),
    );

    let archive = common::storage::get_archive()?;
    for (block_id, data) in archive.blocks {
        if block_id.shard.is_masterchain() {
            let block = client.get_block(&block_id).await;
            assert!(block.is_some());

            if let Some(block) = block {
                let block = block?;

                assert_eq!(&block_id, block.id());
                assert_eq!(&data.block.unwrap().data, block.block());
            }
        }
    }

    tmp_dir.close()?;

    tracing::info!("done!");
    Ok(())
}
