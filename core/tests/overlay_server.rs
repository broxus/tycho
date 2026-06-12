use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use tycho_block_util::block::{BlockProofStuff, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::blockchain_rpc::{
    BlockchainRpcClient, BlockchainRpcService, BroadcastListener, DataRequirement,
};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_core::proto::blockchain::{KeyBlockIds, PersistentStateInfo};
use tycho_core::storage::{
    CoreStorage, CoreStorageConfig, NewBlockMeta, PersistentStateKind, PersistentStateStorage,
};
use tycho_network::{DhtClient, InboundRequestMeta, Network, OverlayId, PeerId, PublicOverlay};
use tycho_storage::StorageContext;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::models::{BlockId, ExtInMsgInfo, OwnedMessage, ShardIdent};
use tycho_util::compression::zstd_decompress_simple;
use tycho_util::fs::MappedFile;

use crate::network::TestNode;

mod network;
mod storage;
mod utils;

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
        fn with_random_key(storage: CoreStorage, broadcast_counter: BroadcastCounter) -> Self {
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

    network::discover(&nodes).await?;

    let peers = nodes
        .iter()
        .map(|x| *x.dht_client.network().peer_id())
        .collect::<Vec<PeerId>>();

    for node in &nodes {
        node.force_update_validators(peers.clone());
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    tracing::info!("broadcasting messages...");
    let msg = BocRepr::encode(OwnedMessage {
        info: ExtInMsgInfo::default().into(),
        init: None,
        body: Default::default(),
        layout: None,
    })?;

    for node in &nodes {
        node.blockchain_client
            .broadcast_external_message(&msg)
            .await;
    }

    let total_received = broadcast_counter.total_received.load(Ordering::Acquire);

    assert!(total_received > nodes.len() * 4 && total_received <= nodes.len() * 5);

    Ok(())
}

#[tokio::test]
async fn overlay_server_with_empty_storage() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_with_empty_storage", "info");

    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
    let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

    let nodes = network::make_network(storage, 10);

    network::discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    let result = client
        .get_block_full(&BlockId::default(), DataRequirement::Optional)
        .await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert!(response.data.is_none());
    }

    let result = client
        .get_next_block_full(&BlockId::default(), DataRequirement::Optional)
        .await;
    assert!(result.is_ok());

    if let Ok(response) = &result {
        assert!(response.data.is_none());
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

    tracing::info!("done!");
    Ok(())
}

#[tokio::test]
async fn overlay_server_blocks() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_blocks", "info");

    let (storage, _tmp_dir) = storage::init_storage().await?;

    let nodes = network::make_network(storage, 10);

    network::discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    let archive_file = "archive_1.bin";
    let archive_data = utils::read_file(archive_file)?;
    let archive = Arc::new(
        utils::parse_archive(&archive_data)
            .with_context(|| format!("Failed to parse archive {}", archive_file))?,
    );

    for block_id in archive.blocks.keys() {
        if block_id.shard.is_masterchain() {
            let result = client
                .get_block_full(block_id, DataRequirement::Required)
                .await?;

            let (archive_block, archive_proof, archive_queue_diff) =
                archive.get_entry_by_id(block_id).await?;

            if let Some(block_full) = &result.data {
                let block = BlockStuff::deserialize_checked(block_id, &block_full.block_data)?;
                assert_eq!(block.as_ref(), archive_block.block());

                let proof = BlockProofStuff::deserialize(block_id, &block_full.proof_data)?;
                assert_eq!(proof.as_ref().proof_for, archive_proof.as_ref().proof_for);
                assert_eq!(proof.as_ref().root, archive_proof.as_ref().root);

                let queue_diff =
                    QueueDiffStuff::deserialize(block_id, &block_full.queue_diff_data)?;
                assert_eq!(queue_diff.diff(), archive_queue_diff.diff());
            }
        }
    }

    tracing::info!("done!");
    Ok(())
}

#[tokio::test]
async fn overlay_server_persistent_state() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_persistent_state", "info");

    let (storage, _tmp_dir) = storage::init_storage().await?;

    let shard_states = storage.shard_state_storage();
    let persistent_states = storage.persistent_state_storage();

    // Prepare zerostate
    static ZEROSTATE_BOC: &[u8] = include_bytes!("../tests/data/zerostate.boc");
    let zerostate_root = Boc::decode(ZEROSTATE_BOC)?;
    let zerostate_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
        root_hash: *zerostate_root.repr_hash(),
        file_hash: Boc::file_hash_blake(ZEROSTATE_BOC),
    };

    // Write zerostate to db
    let (zerostate_handle, _) = storage
        .block_handle_storage()
        .create_or_load_handle(&zerostate_id, NewBlockMeta::zero_state(0, true));

    let zerostate = ShardStateStuff::from_root(
        &zerostate_id,
        zerostate_root,
        shard_states.min_ref_mc_state().insert_untracked(),
    )?;
    shard_states
        .store_state_ignore_cache(&zerostate_handle, &zerostate, Default::default())
        .await?;

    {
        let mut zerostate_file = storage.context().temp_files().unnamed_file().open()?;
        std::io::copy(
            &mut std::convert::identity(ZEROSTATE_BOC),
            &mut zerostate_file,
        )?;

        persistent_states
            .store_shard_state_file(0, &zerostate_handle, zerostate_file)
            .await?;
    }

    assert!(zerostate_handle.has_persistent_shard_state());

    persistent_states
        .get_state_info(&zerostate_id, PersistentStateKind::Shard)
        .unwrap();

    // Prepare network
    let nodes = network::make_network(storage.clone(), 10);

    network::discover(&nodes).await?;

    tracing::info!("making overlay requests...");

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    let pending_state = client
        .find_persistent_state(&zerostate_id, PersistentStateKind::Shard)
        .await?;

    assert_eq!(pending_state.split_depth, 0);
    assert!(pending_state.parts.is_empty());

    let temp_file = client
        .download_persistent_state(
            pending_state,
            None,
            storage.context().temp_files().unnamed_file().open()?,
        )
        .await?;

    let mapped = MappedFile::from_existing_file(temp_file)?;
    assert_eq!(mapped.as_slice(), ZEROSTATE_BOC);

    tracing::info!("done!");
    Ok(())
}

#[tokio::test]
async fn overlay_server_split_persistent_state() -> Result<()> {
    tycho_util::test::init_logger("overlay_server_split_persistent_state", "info");

    const DUMP_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../test/data/dump/persistents/",
        "0:8000000000000000:36:",
        "78d7e559cf68d9d1821520d1b53b16ba5cf9cef139106efd388af2bfe2016817:",
        "a875088e0557ab41241aaf07db46e9422e070d7d3842fe4f7269ae6a0bd69ae5.boc",
    );

    // load a real shard state dump that produces split persistent parts
    let dump_path = std::path::Path::new(DUMP_PATH);
    let block_id: BlockId = dump_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap()
        .parse()?;
    let full_boc = zstd_decompress_simple(&std::fs::read(dump_path)?)?;
    let expected_state_root_hash = *Boc::decode(&full_boc)?.repr_hash();

    // open storage with split persistent states enabled
    let mut config = CoreStorageConfig::new_potato();
    config.persistent_state_split_depth = 2;
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
    let storage = CoreStorage::open(ctx, config).await?;
    let shard_states = storage.shard_state_storage();

    // import the full state into shard storage before writing the persistent bundle
    shard_states.begin_raw_import()?;
    let root_hash = shard_states
        .store_state_bytes(
            &block_id,
            bytes::Bytes::from(full_boc),
            Some(&expected_state_root_hash),
        )
        .await?;
    anyhow::ensure!(
        root_hash == expected_state_root_hash,
        "dump state root hash mismatch"
    );
    shard_states.finish_raw_import()?;

    // create a block handle that can be used by the persistent-state writer
    let loaded_state = shard_states.load_state(block_id.seqno, &block_id).await?;
    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(&block_id, NewBlockMeta {
                is_key_block: block_id.is_masterchain(),
                gen_utime: loaded_state.as_ref().gen_utime,
                ref_by_mc_seqno: block_id.seqno,
            });
    storage.block_handle_storage().set_has_shard_state(&handle);
    storage.block_handle_storage().set_skip_states_gc(&handle);

    // write the split persistent bundle using the existing storage flow
    let persistent_states = storage.persistent_state_storage();
    persistent_states
        .store_shard_state(block_id.seqno, &handle)
        .await?;

    // capture local decompressed bytes to compare with overlay downloads
    let state_info = persistent_states
        .get_state_info(&block_id, PersistentStateKind::Shard)
        .unwrap();
    assert_eq!(state_info.split_depth, 2);
    assert!(!state_info.parts.is_empty());
    let first_part = state_info.parts[0];
    let expected_main = read_persistent_state_decompressed(
        persistent_states,
        &block_id,
        PersistentStateKind::Shard,
        None,
        state_info.size,
        state_info.chunk_size,
    )
    .await?;
    let expected_part = read_persistent_state_decompressed(
        persistent_states,
        &block_id,
        PersistentStateKind::Shard,
        Some(first_part.prefix),
        first_part.size,
        state_info.chunk_size,
    )
    .await?;

    // expose the storage through the regular overlay test network
    let nodes = network::make_network(storage.clone(), 10);
    network::discover(&nodes).await?;

    tracing::info!("making split overlay requests...");

    let node = nodes.first().unwrap();
    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    // verify that overlay discovery returns split metadata
    let pending_state = client
        .find_persistent_state(&block_id, PersistentStateKind::Shard)
        .await?;
    assert_eq!(pending_state.split_depth, 2);
    assert_eq!(pending_state.parts.len(), state_info.parts.len());
    assert!(pending_state.part(first_part.prefix).is_some());

    // download and verify the split main state
    let main_file = client
        .download_persistent_state(
            pending_state.clone(),
            None,
            storage.context().temp_files().unnamed_file().open()?,
        )
        .await?;
    let main_file = MappedFile::from_existing_file(main_file)?;
    assert_eq!(main_file.as_slice(), expected_main);

    // download and verify one advertised split part
    let part_file = client
        .download_persistent_state(
            pending_state,
            Some(first_part.prefix),
            storage.context().temp_files().unnamed_file().open()?,
        )
        .await?;
    let part_file = MappedFile::from_existing_file(part_file)?;
    assert_eq!(part_file.as_slice(), expected_part);

    tracing::info!("done!");
    Ok(())
}

async fn read_persistent_state_decompressed(
    persistent_states: &PersistentStateStorage,
    block_id: &BlockId,
    state_kind: PersistentStateKind,
    part_shard_prefix: Option<u64>,
    size: std::num::NonZeroU64,
    chunk_size: std::num::NonZeroU32,
) -> Result<Vec<u8>> {
    let mut compressed = Vec::new();
    for offset in (0..size.get()).step_by(chunk_size.get() as usize) {
        let chunk = persistent_states
            .read_state_chunk(block_id, offset, state_kind, part_shard_prefix)
            .await
            .context("failed to read persistent state chunk")?;
        compressed.extend_from_slice(&chunk);
    }
    Ok(zstd_decompress_simple(&compressed)?)
}
