use anyhow::Result;
use bytes::BytesMut;
use bytesize::ByteSize;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use everscale_types::prelude::*;
use futures_util::future;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::{Archive, ArchiveVerifier};
use tycho_block_util::block::{BlockIdExt, BlockIdRelation, BlockProofStuff, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, ArchiveHandler, ArchiveSubscriber,
    ArchiveSubscriberContext, BlockProvider, BlockProviderExt, BlockStrider, CheckProof,
    OptionalBlockStuff, PersistentBlockStriderState, ProofChecker, ShardStateApplier,
    StateSubscriber, StateSubscriberContext,
};
use tycho_core::blockchain_rpc::{BlockchainRpcClient, DataRequirement};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_network::PeerId;
use tycho_storage::{ArchiveId, ArchivesGcConfig, NewBlockMeta, Storage, StorageConfig};
use tycho_util::compression::{zstd_decompress, ZstdDecompressStream};
use tycho_util::project_root;

use crate::network::TestNode;

mod network;
mod utils;

const BLOCK_DATA_CHUNK_SIZE: usize = 1024 * 1024; // 1MB

#[derive(Default, Debug, Clone, Copy)]
struct DummySubscriber;

impl StateSubscriber for DummySubscriber {
    type HandleStateFut<'a> = future::Ready<Result<()>>;

    fn handle_state(&self, _cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
        future::ready(Ok(()))
    }
}

pub struct ArchiveProvider {
    archive: Archive,
    proof_checker: ProofChecker,
}

impl ArchiveProvider {
    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        let BlockIdRelation {
            mc_block_id,
            block_id,
        } = block_id_relation;

        let (ref block, ref proof, ref queue_diff) = match self.archive.get_entry_by_id(block_id) {
            Ok(entry) => entry,
            Err(e) => return Some(Err(e.into())),
        };

        match self
            .proof_checker
            .check_proof(CheckProof {
                mc_block_id,
                block,
                proof,
                queue_diff,
                store_on_success: true,
            })
            .await
        {
            Ok(_) => Some(Ok(block.clone())),
            Err(e) => Some(Err(e)),
        }
    }
}

impl BlockProvider for ArchiveProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        let id = match self.archive.mc_block_ids.get(&(prev_block_id.seqno + 1)) {
            Some(id) => id,
            None => return Box::pin(future::ready(None)),
        };
        Box::pin(async move { self.get_block(&id.relative_to_self()).await })
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id_relation))
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        future::ready(Ok(()))
    }
}

pub struct ArchiveHandlerSubscriber {}

impl ArchiveSubscriber for ArchiveHandlerSubscriber {
    type HandleArchiveFut<'a> = future::Ready<Result<()>>;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a> {
        futures_util::future::ready(ArchiveHandlerInner::handle(cx))
    }
}

struct ArchiveHandlerInner {}

impl ArchiveHandlerInner {
    fn handle(cx: &ArchiveSubscriberContext<'_>) -> Result<()> {
        let iterator = cx.archive_iterator();

        let mut zstd_decoder = ZstdDecompressStream::new(BLOCK_DATA_CHUNK_SIZE)?;

        // Reuse buffer for decompressed data
        let mut decompressed_chunk = Vec::new();

        let mut verifier = ArchiveVerifier::default();

        let mut archive_bytes = BytesMut::new();

        for (key, chunk) in iterator {
            let id = u32::from_be_bytes(key[..4].try_into()?);
            assert_eq!(id, cx.archive_id);

            decompressed_chunk.clear();
            zstd_decoder.write(chunk.as_ref(), &mut decompressed_chunk)?;

            verifier.write_verify(decompressed_chunk.as_ref())?;

            archive_bytes.extend_from_slice(decompressed_chunk.as_ref());
        }

        verifier.final_check()?;

        // Build archive
        Archive::new(archive_bytes)?;

        Ok(())
    }
}

async fn prepare_storage(config: StorageConfig, zerostate: ShardStateStuff) -> Result<Storage> {
    let storage = Storage::builder().with_config(config).build().await?;

    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(zerostate.block_id(), NewBlockMeta {
                is_key_block: zerostate.block_id().is_masterchain(),
                gen_utime: zerostate.state().gen_utime,
                ref_by_mc_seqno: 0,
            });

    let shard_states = storage.shard_state_storage();
    shard_states
        .store_state(&handle, &zerostate, Default::default())
        .await?;

    let global_id = zerostate.state().global_id;
    let gen_utime = zerostate.state().gen_utime;

    for entry in zerostate.shards()?.iter() {
        let (shard_ident, _) = entry?;

        let state = ShardStateUnsplit {
            global_id,
            shard_ident,
            gen_utime,
            min_ref_mc_seqno: u32::MAX,
            ..Default::default()
        };

        let root = CellBuilder::build_from(&state)?;
        let root_hash = *root.repr_hash();
        let file_hash = Boc::file_hash_blake(Boc::encode(&root));

        let block_id = BlockId {
            shard: state.shard_ident,
            seqno: state.seqno,
            root_hash,
            file_hash,
        };

        let state = ShardStateStuff::from_root(&block_id, root, shard_states.min_ref_mc_state())?;

        let (handle, _) =
            storage
                .block_handle_storage()
                .create_or_load_handle(state.block_id(), NewBlockMeta {
                    is_key_block: state.block_id().is_masterchain(),
                    gen_utime,
                    ref_by_mc_seqno: 0,
                });

        storage
            .shard_state_storage()
            .store_state(&handle, &state, Default::default())
            .await?;
    }

    Ok(storage)
}

#[tokio::test]
async fn archives() -> Result<()> {
    tycho_util::test::init_logger("archives", "debug");

    // Prepare directory
    let tmp_dir = tempfile::tempdir()?;

    // Init storage
    let config = StorageConfig {
        root_dir: tmp_dir.path().to_owned(),
        rocksdb_lru_capacity: ByteSize::kb(1024),
        cells_cache_size: ByteSize::kb(1024),
        rocksdb_enable_metrics: false,
        archives_gc: Some(ArchivesGcConfig::default()),
        states_gc: None,
        blocks_gc: None,
        blocks_cache: Default::default(),
    };

    let zerostate_data = utils::read_file("zerostate.boc")?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
    let zerostate_id = *zerostate.block_id();
    let storage = prepare_storage(config, zerostate).await?;

    // Init state applier
    let state_subscriber = DummySubscriber;

    let state_applier = ShardStateApplier::new(storage.clone(), state_subscriber);

    // Archive provider
    let archive_data = utils::read_file("archive_1.bin")?;
    let archive = utils::parse_archive(&archive_data)?;

    let archive_provider = ArchiveProvider {
        archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Next archive provider
    let next_archive_data = utils::read_file("archive_2.bin")?;
    let next_archive = utils::parse_archive(&next_archive_data)?;

    let next_archive_provider = ArchiveProvider {
        archive: next_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Last archive provider
    let last_archive_data = utils::read_file("archive_3.bin")?;
    let last_archive = utils::parse_archive(&last_archive_data)?;

    let last_archive_provider = ArchiveProvider {
        archive: last_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Strider state
    let strider_state = PersistentBlockStriderState::new(zerostate_id, storage.clone());

    // Init block strider
    let block_strider = BlockStrider::builder()
        .with_provider(
            archive_provider
                .chain(next_archive_provider)
                .chain(last_archive_provider),
        )
        .with_state(strider_state)
        .with_block_subscriber(state_applier)
        .build();

    block_strider.run().await?;

    let archive_id = storage.block_storage().get_archive_id(1);

    let ArchiveId::Found(archive_id) = archive_id else {
        anyhow::bail!("archive not found")
    };

    // Check archive size
    let archive_size = storage
        .block_storage()
        .get_archive_size(archive_id)?
        .unwrap();
    assert_eq!(archive_size, archive_data.len());

    // Check archive data
    let archive_chunk_size = storage.block_storage().archive_chunk_size().get() as usize;

    let mut expected_archive_data = vec![];
    for offset in (0..archive_size).step_by(archive_chunk_size) {
        let chunk = storage
            .block_storage()
            .get_archive_chunk(archive_id, offset as u64)
            .await?;
        expected_archive_data.extend_from_slice(&chunk);
    }
    assert_eq!(archive_data, expected_archive_data);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn heavy_archives() -> Result<()> {
    tycho_util::test::init_logger("heavy_archives", "debug");

    // Prepare directory
    let project_root = project_root()?.join(".scratch");
    let integration_test_path = project_root.join("integration_tests");
    let current_test_path = integration_test_path.join("heavy_archives");
    std::fs::remove_dir_all(&current_test_path).ok();
    std::fs::create_dir_all(&current_test_path)?;

    // Init storage
    let config = StorageConfig {
        root_dir: current_test_path.join("db"),
        rocksdb_lru_capacity: ByteSize::kb(1024 * 1024),
        cells_cache_size: ByteSize::kb(1024 * 1024),
        rocksdb_enable_metrics: false,
        archives_gc: Some(ArchivesGcConfig::default()),
        states_gc: None,
        blocks_gc: None,
        blocks_cache: Default::default(),
    };

    let zerostate_path = integration_test_path.join("zerostate.boc");
    let zerostate_data = std::fs::read(zerostate_path)?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
    let zerostate_id = *zerostate.block_id();
    let storage = prepare_storage(config, zerostate).await?;

    // Init state applier
    let state_subscriber = DummySubscriber;

    let state_applier = ShardStateApplier::new(storage.clone(), state_subscriber);
    let archive_handler = ArchiveHandler::new(storage.clone(), ArchiveHandlerSubscriber {})?;

    // Archive provider
    let archive_path = integration_test_path.join("archive_1.bin");
    let archive_data = std::fs::read(archive_path)?;
    let archive = utils::parse_archive(&archive_data)?;

    let archive_provider = ArchiveProvider {
        archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Next archive provider
    let next_archive_path = integration_test_path.join("archive_2.bin");
    let next_archive_data = std::fs::read(next_archive_path)?;
    let next_archive = utils::parse_archive(&next_archive_data)?;

    let next_archive_provider = ArchiveProvider {
        archive: next_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Last archive provider
    let last_archive_path = integration_test_path.join("archive_3.bin");
    let last_archive_data = std::fs::read(last_archive_path)?;
    let last_archive = utils::parse_archive(&last_archive_data)?;

    let last_archive_provider = ArchiveProvider {
        archive: last_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Strider state
    let strider_state = PersistentBlockStriderState::new(zerostate_id, storage.clone());

    // Init block strider
    let block_strider = BlockStrider::builder()
        .with_provider(
            archive_provider
                .chain(next_archive_provider)
                .chain(last_archive_provider),
        )
        .with_state(strider_state)
        .with_block_subscriber((state_applier, archive_handler))
        .build();

    block_strider.run().await?;
    storage.block_storage().wait_for_archive_commit().await?;

    // Check archive data
    let archive_chunk_size = storage.block_storage().archive_chunk_size().get() as usize;

    check_archive(&storage, &archive_data, archive_chunk_size, 1).await?;
    check_archive(&storage, &next_archive_data, archive_chunk_size, 101).await?;

    // Make network
    let nodes = network::make_network(storage.clone(), 10);
    network::discover(&nodes).await?;

    let peers = nodes
        .iter()
        .map(|x| *x.network().peer_id())
        .collect::<Vec<PeerId>>();

    for node in &nodes {
        node.force_update_validators(peers.clone());
    }

    let node = nodes.first().unwrap();

    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(PublicOverlayClient::new(
            node.network().clone(),
            node.public_overlay().clone(),
            Default::default(),
        ))
        .build();

    // Get archive
    let archive_path = integration_test_path.join("archive_1.bin");
    let archive_data = std::fs::read(archive_path)?;
    let archive = utils::parse_archive(&archive_data)?;

    // Request blocks from network and check with archive blocks
    let mut sorted: Vec<_> = archive.blocks.into_iter().collect();
    sorted.sort_by_key(|(x, _)| *x);

    // Blockchain client
    {
        // getBlockFull
        for (block_id, data_entry) in &sorted {
            let result = client
                .get_block_full(block_id, DataRequirement::Required)
                .await?;

            let block_full = result.data.unwrap();

            let block = BlockStuff::deserialize_checked(block_id, &block_full.block_data)?;
            let proof = BlockProofStuff::deserialize(block_id, &block_full.proof_data)?;
            let queue_diff = QueueDiffStuff::deserialize(block_id, &block_full.queue_diff_data)?;

            // Block
            {
                let data = data_entry.block.as_ref().unwrap();
                let archive_block = BlockStuff::deserialize_checked(block_id, data)?;
                assert_eq!(block.block(), archive_block.block());
            }

            // Proof
            {
                let data = data_entry.proof.as_ref().unwrap();
                let archive_proof = BlockProofStuff::deserialize(block_id, data)?;
                assert_eq!(proof.proof().root, archive_proof.proof().root);
            }

            // Queue diff
            {
                let data = data_entry.queue_diff.as_ref().unwrap();
                let archive_queue_diff = QueueDiffStuff::deserialize(block_id, data)?;
                assert_eq!(queue_diff.diff(), archive_queue_diff.diff());
            }
        }

        // getNextBlockFull
        for (block_id, _) in &sorted {
            let result = client
                .get_next_block_full(block_id, DataRequirement::Required)
                .await?;

            let block_full = result.data.unwrap();

            let block =
                BlockStuff::deserialize_checked(&block_full.block_id, &block_full.block_data);
            assert!(block.is_ok());

            let proof = BlockProofStuff::deserialize(&block_full.block_id, &block_full.proof_data);
            assert!(proof.is_ok());

            let queue_diff =
                QueueDiffStuff::deserialize(&block_full.block_id, &block_full.queue_diff_data);
            assert!(queue_diff.is_ok());
        }
    }

    // Get archive
    let archive_path = integration_test_path.join("archive_1.bin");
    let archive_data = std::fs::read(archive_path)?;
    let archive = utils::parse_archive(&archive_data)?;

    // Archive provider
    {
        let archive_block_provider =
            ArchiveBlockProvider::new(client, storage, ArchiveBlockProviderConfig::default());

        // getBlock
        for (_, mc_block_id) in archive.mc_block_ids.iter() {
            let result = archive_block_provider
                .get_block(&mc_block_id.relative_to_self())
                .await;
            let mc_block = result.unwrap()?;

            let data_entry = archive.blocks.get(mc_block_id).unwrap();
            let mc_block_data = data_entry.block.as_ref().unwrap();
            let archive_mc_block = BlockStuff::deserialize_checked(mc_block_id, mc_block_data)?;

            assert_eq!(mc_block.block(), archive_mc_block.block());

            let custom = mc_block.load_custom()?;
            for entry in custom.shards.latest_blocks() {
                let shard_block_id = entry?;

                // Skip zero block
                if shard_block_id.seqno != 0 {
                    let block = archive_block_provider
                        .get_block(&BlockIdRelation {
                            mc_block_id: *mc_block.id(),
                            block_id: shard_block_id,
                        })
                        .await
                        .unwrap()?;

                    let data_entry = archive.blocks.get(&shard_block_id).unwrap();
                    let block_data = data_entry.block.as_ref().unwrap();
                    let archive_block =
                        BlockStuff::deserialize_checked(&shard_block_id, block_data)?;

                    assert_eq!(block.block(), archive_block.block());
                }
            }
        }

        // getNextBlock
        for (block_id, _) in &sorted {
            if block_id.is_masterchain() {
                let result = archive_block_provider.get_next_block(block_id).await;
                assert!(result.is_some());

                if let Some(block) = result {
                    assert!(block.is_ok());
                }
            }
        }
    }

    Ok(())
}

async fn check_archive(
    storage: &Storage,
    original_archive: &[u8],
    archive_chunk_size: usize,
    seqno: u32,
) -> Result<()> {
    tracing::info!("Checking archive {}", seqno);
    let archive_id = storage.block_storage().get_archive_id(seqno);

    let ArchiveId::Found(archive_id) = archive_id else {
        anyhow::bail!("archive {seqno} not found")
    };

    // Check archive size
    let archive_size = storage
        .block_storage()
        .get_archive_size(archive_id)?
        .unwrap();

    let mut got_archive = vec![];
    for offset in (0..archive_size).step_by(archive_chunk_size) {
        let chunk = storage
            .block_storage()
            .get_archive_chunk(archive_id, offset as u64)
            .await?;
        got_archive.extend_from_slice(&chunk);
    }

    let original_decompressed = decompress(original_archive);
    let got_decompressed = decompress(&got_archive);

    let original_len = original_decompressed.len();
    let got_len = got_decompressed.len();

    assert_eq!(archive_size, original_archive.len(), "Size mismatch");
    assert_eq!(got_archive.len(), archive_size, "Retrieved size mismatch");
    assert_eq!(original_archive, &got_archive, "Content mismatch");
    assert_eq!(
        original_decompressed, got_decompressed,
        "Decompressed mismatch"
    );
    assert_eq!(original_len, got_len, "Decompressed size mismatch");

    Ok(())
}

fn decompress(data: &[u8]) -> Vec<u8> {
    let mut decompressed = Vec::new();
    zstd_decompress(data, &mut decompressed).unwrap();
    decompressed
}
