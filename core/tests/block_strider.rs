use std::collections::BTreeMap;
use std::time::Duration;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tycho_block_util::block::{BlockIdExt, BlockStuff};
use tycho_core::block_strider::{
    BlockProvider, BlockProviderExt, BlockchainBlockProvider, RetryConfig,
    StorageBlockProvider,
};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_core::overlay_client::{PublicOverlayClient, PublicOverlayClientConfig};
use tycho_network::PeerId;
mod network;
mod storage;
mod utils;
#[tokio::test]
async fn storage_block_strider() -> anyhow::Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(storage_block_strider)),
        file!(),
        19u32,
    );
    tycho_util::test::init_logger("storage_block_strider", "info");
    let (storage, _tmp_dir) = {
        __guard.end_section(22u32);
        let __result = storage::init_storage().await;
        __guard.start_section(22u32);
        __result
    }?;
    let storage_provider = StorageBlockProvider::new(storage);
    let archive_data = utils::read_file("archive_1.bin")?;
    let archive = utils::parse_archive(&archive_data)?;
    for (block_id, data) in archive.blocks {
        __guard.checkpoint(28u32);
        if block_id.shard.is_masterchain() {
            let block = {
                __guard.end_section(32u32);
                let __result = storage_provider
                    .get_block(&block_id.relative_to_self())
                    .await;
                __guard.start_section(32u32);
                __result
            };
            assert!(block.is_some());
            if let Some(block) = block {
                let block = block?;
                assert_eq!(& block_id, block.id());
                let archive_block = BlockStuff::deserialize_checked(
                    &block_id,
                    data.block.unwrap().as_ref(),
                )?;
                assert_eq!(archive_block.block(), block.block());
            }
        }
    }
    tracing::info!("done!");
    Ok(())
}
#[tokio::test]
async fn overlay_block_strider() -> anyhow::Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(overlay_block_strider)),
        file!(),
        51u32,
    );
    tycho_util::test::init_logger("overlay_block_strider", "info");
    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }
    let (storage, tmp_dir) = {
        __guard.end_section(60u32);
        let __result = storage::init_storage().await;
        __guard.start_section(60u32);
        __result
    }?;
    const NODE_COUNT: usize = 10;
    let nodes = network::make_network(storage.clone(), NODE_COUNT);
    tracing::info!("discovering nodes");
    loop {
        __guard.checkpoint(66u32);
        {
            __guard.end_section(67u32);
            let __result = tokio::time::sleep(Duration::from_secs(1)).await;
            __guard.start_section(67u32);
            __result
        };
        let mut peer_states = BTreeMap::<&PeerId, PeerState>::new();
        for (i, left) in nodes.iter().enumerate() {
            __guard.checkpoint(71u32);
            for (j, right) in nodes.iter().enumerate() {
                __guard.checkpoint(72u32);
                if i == j {
                    {
                        __guard.end_section(74u32);
                        __guard.start_section(74u32);
                        continue;
                    };
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
        tracing::info!("peers with filled overlay: {} / {}", total_filled, nodes.len());
        if total_filled == nodes.len() {
            {
                __guard.end_section(100u32);
                __guard.start_section(100u32);
                break;
            };
        }
    }
    tracing::info!("resolving entries...");
    for node in &nodes {
        __guard.checkpoint(105u32);
        let resolved = FuturesUnordered::new();
        for entry in node.public_overlay().read_entries().iter() {
            __guard.checkpoint(107u32);
            let handle = entry.resolver_handle.clone();
            resolved
                .push(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        109u32,
                    );
                    {
                        __guard.end_section(109u32);
                        let __result = handle.wait_resolved().await;
                        __guard.start_section(109u32);
                        __result
                    }
                });
        }
        {
            __guard.end_section(113u32);
            let __result = resolved.collect::<Vec<_>>().await;
            __guard.start_section(113u32);
            __result
        };
        tracing::info!(peer_id = % node.network().peer_id(), "all entries resolved",);
    }
    tracing::info!("making overlay requests...");
    let node = nodes.first().unwrap();
    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(
            PublicOverlayClient::new(
                node.network().clone(),
                node.public_overlay().clone(),
                PublicOverlayClientConfig::default(),
            ),
        )
        .build();
    let provider = BlockchainBlockProvider::new(
            client,
            storage.clone(),
            Default::default(),
        )
        .retry(RetryConfig {
            attempts: 10,
            interval: Duration::from_millis(100),
        });
    let archive_data = utils::read_file("archive_1.bin")?;
    let archive = utils::parse_archive(&archive_data)?;
    for block_id in archive.mc_block_ids.values() {
        __guard.checkpoint(139u32);
        let block = {
            __guard.end_section(140u32);
            let __result = provider.get_block(&block_id.relative_to_self()).await;
            __guard.start_section(140u32);
            __result
        };
        assert!(block.is_some());
        if let Some(block) = block {
            let block = block?;
            assert_eq!(block_id, block.id());
            let archive_block = BlockStuff::deserialize_checked(
                block_id,
                block.as_new_archive_data()?,
            );
            assert_eq!(archive_block ?.block(), block.block());
        }
    }
    tmp_dir.close()?;
    tracing::info!("done!");
    Ok(())
}
