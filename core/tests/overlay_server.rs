use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_core::proto::blockchain::{BlockFull, KeyBlockIds, PersistentStatePart};
use tycho_network::PeerId;
use tycho_storage::Storage;

use crate::common::archive::*;

mod common;

#[tokio::test]
async fn overlay_server_with_empty_storage() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_with_empty_storage", "debug");

    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }

    let (storage, _tmp_dir) = Storage::new_temp()?;

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

    let result = client
        .get_persistent_state_part(&BlockId::default(), &BlockId::default(), 0, 0)
        .await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert_eq!(response.data(), &PersistentStatePart::NotFound);
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

    tmp_dir.close()?;

    tracing::info!("done!");
    Ok(())
}
