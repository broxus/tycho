use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;
use anyhow::Result;
use bytesize::ByteSize;
use futures_util::future;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::Archive;
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
use tycho_core::storage::{
    ArchiveId, BlockFlags, BlockMeta, CoreStorage, CoreStorageConfig, NewBlockMeta,
    PackageEntryKey,
};
use tycho_network::PeerId;
use tycho_storage::kv::StoredValue;
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::models::{BlockId, ShardStateUnsplit};
use tycho_types::prelude::*;
use tycho_util::compression::{ZstdCompressStream, zstd_decompress_simple};
use tycho_util::project_root;
use crate::network::TestNode;
mod network;
mod utils;
#[derive(Default, Debug, Clone, Copy)]
struct DummySubscriber;
impl StateSubscriber for DummySubscriber {
    type HandleStateFut<'a> = future::Ready<Result<()>>;
    fn handle_state(&self, _cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
        future::ready(Ok(()))
    }
}
type ArchivesSet = BTreeMap<u32, Arc<Archive>>;
pub struct ArchiveProvider {
    archives: parking_lot::Mutex<ArchivesSet>,
    proof_checker: ProofChecker,
}
impl ArchiveProvider {
    async fn get_block_impl(
        &self,
        block_id_relation: &BlockIdRelation,
    ) -> OptionalBlockStuff {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_block_impl)),
            file!(),
            56u32,
        );
        let block_id_relation = block_id_relation;
        let BlockIdRelation { mc_block_id, block_id } = block_id_relation;
        let archive = {
            let mut archive = None;
            let guard = self.archives.lock();
            for (_, value) in guard.iter() {
                __guard.checkpoint(66u32);
                if value.mc_block_ids.contains_key(&mc_block_id.seqno) {
                    archive = Some(value.clone());
                }
            }
            archive
        };
        match archive {
            Some(archive) => {
                let (ref block, ref proof, ref queue_diff) = match {
                    __guard.end_section(78u32);
                    let __result = archive.get_entry_by_id(block_id).await;
                    __guard.start_section(78u32);
                    __result
                } {
                    Ok(entry) => entry,
                    Err(e) => {
                        __guard.end_section(80u32);
                        return Some(Err(e.into()));
                    }
                };
                match {
                    __guard.end_section(92u32);
                    let __result = self
                        .proof_checker
                        .check_proof(CheckProof {
                            mc_block_id,
                            block,
                            proof,
                            queue_diff,
                            store_on_success: true,
                        })
                        .await;
                    __guard.start_section(92u32);
                    __result
                } {
                    Ok(_) => Some(Ok(block.clone())),
                    Err(e) => Some(Err(e)),
                }
            }
            None => None,
        }
    }
}
impl BlockProvider for ArchiveProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        let guard = self.archives.lock();
        let mut block_id = None;
        for (_, value) in guard.iter() {
            if let Some(id) = value.mc_block_ids.get(&(prev_block_id.seqno + 1)) {
                block_id = Some(*id);
            }
        }
        let id = match block_id {
            Some(id) => id,
            None => return Box::pin(future::ready(None)),
        };
        Box::pin(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                122u32,
            );
            {
                __guard.end_section(122u32);
                let __result = self.get_block(&id.relative_to_self()).await;
                __guard.start_section(122u32);
                __result
            }
        })
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
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
        let archive_size = cx
            .storage
            .block_storage()
            .get_archive_size(cx.archive_id)?
            .ok_or_else(|| anyhow::anyhow!("Archive {} not found", cx.archive_id))?;
        if archive_size == 0 {
            return Err(anyhow::anyhow!("Archive {} is empty", cx.archive_id));
        }
        tracing::info!(
            "Archive {} exists with size {} bytes", cx.archive_id, archive_size
        );
        Ok(())
    }
}
async fn prepare_storage(
    config: StorageConfig,
    zerostate: ShardStateStuff,
) -> Result<CoreStorage> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(prepare_storage)),
        file!(),
        174u32,
    );
    let config = config;
    let zerostate = zerostate;
    let ctx = {
        __guard.end_section(175u32);
        let __result = StorageContext::new(config).await;
        __guard.start_section(175u32);
        __result
    }?;
    let storage = {
        __guard.end_section(176u32);
        let __result = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await;
        __guard.start_section(176u32);
        __result
    }?;
    let (handle, _) = storage
        .block_handle_storage()
        .create_or_load_handle(
            zerostate.block_id(),
            NewBlockMeta {
                is_key_block: zerostate.block_id().is_masterchain(),
                gen_utime: zerostate.state().gen_utime,
                ref_by_mc_seqno: 0,
            },
        );
    let shard_states = storage.shard_state_storage();
    {
        __guard.end_section(190u32);
        let __result = shard_states
            .store_state(&handle, &zerostate, Default::default())
            .await;
        __guard.start_section(190u32);
        __result
    }?;
    let global_id = zerostate.state().global_id;
    let gen_utime = zerostate.state().gen_utime;
    for entry in zerostate.shards()?.iter() {
        __guard.checkpoint(195u32);
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
        let state = ShardStateStuff::from_root(
            &block_id,
            root,
            shard_states.min_ref_mc_state().insert_untracked(),
        )?;
        let (handle, _) = storage
            .block_handle_storage()
            .create_or_load_handle(
                state.block_id(),
                NewBlockMeta {
                    is_key_block: state.block_id().is_masterchain(),
                    gen_utime,
                    ref_by_mc_seqno: 0,
                },
            );
        {
            __guard.end_section(235u32);
            let __result = storage
                .shard_state_storage()
                .store_state(&handle, &state, Default::default())
                .await;
            __guard.start_section(235u32);
            __result
        }?;
    }
    Ok(storage)
}
#[tokio::test]
async fn archives() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(archives)),
        file!(),
        242u32,
    );
    tycho_util::test::init_logger("archives", "debug");
    let tmp_dir = tempfile::tempdir()?;
    let config = StorageConfig::new_potato(tmp_dir.path());
    let zerostate_data = utils::read_file("zerostate.boc")?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
    let zerostate_id = *zerostate.block_id();
    let storage = {
        __guard.end_section(254u32);
        let __result = prepare_storage(config.clone(), zerostate.clone()).await;
        __guard.start_section(254u32);
        __result
    }?;
    let state_subscriber = DummySubscriber;
    let state_applier = ShardStateApplier::new(storage.clone(), state_subscriber);
    let first_archive_data = utils::read_file("archive_1.bin")?;
    let first_archive = utils::parse_archive(&first_archive_data).map(Arc::new)?;
    let next_archive_data = utils::read_file("archive_2.bin")?;
    let next_archive = utils::parse_archive(&next_archive_data).map(Arc::new)?;
    let last_archive_data = utils::read_file("archive_3.bin")?;
    let last_archive = utils::parse_archive(&last_archive_data).map(Arc::new)?;
    let mut first_provider_archives = ArchivesSet::new();
    first_provider_archives.insert(1, first_archive.clone());
    let archive_provider = ArchiveProvider {
        archives: parking_lot::Mutex::new(first_provider_archives),
        proof_checker: ProofChecker::new(storage.clone()),
    };
    let mut next_provider_archives = ArchivesSet::new();
    next_provider_archives.insert(1, first_archive);
    next_provider_archives.insert(101, next_archive.clone());
    let next_archive_provider = ArchiveProvider {
        archives: parking_lot::Mutex::new(next_provider_archives),
        proof_checker: ProofChecker::new(storage.clone()),
    };
    let mut last_provider_archives = ArchivesSet::new();
    last_provider_archives.insert(101, next_archive);
    last_provider_archives.insert(201, last_archive);
    let last_archive_provider = ArchiveProvider {
        archives: parking_lot::Mutex::new(last_provider_archives),
        proof_checker: ProofChecker::new(storage.clone()),
    };
    let strider_state = PersistentBlockStriderState::new(zerostate_id, storage.clone());
    let block_strider = BlockStrider::builder()
        .with_provider(
            archive_provider.chain(next_archive_provider).chain(last_archive_provider),
        )
        .with_state(strider_state)
        .with_block_subscriber(state_applier)
        .build();
    {
        __guard.end_section(313u32);
        let __result = block_strider.run().await;
        __guard.start_section(313u32);
        __result
    }?;
    let archive_id = storage.block_storage().get_archive_id(1);
    let ArchiveId::Found(archive_id) = archive_id else {
        anyhow::bail!("archive not found")
    };
    let archive_size = storage.block_storage().get_archive_size(archive_id)?.unwrap();
    assert!(archive_size > 0, "Archive should have content");
    let stored_archive_data = {
        __guard.end_section(332u32);
        let __result = storage
            .block_storage()
            .get_archive_compressed_full(archive_id)
            .await;
        __guard.start_section(332u32);
        __result
    }?
        .expect("archive should exist");
    let decompressed_stored = zstd_decompress_simple(&stored_archive_data)?;
    let original_decompressed = zstd_decompress_simple(&first_archive_data)?;
    assert_eq!(
        original_decompressed, decompressed_stored,
        "Archive content should match after decompression"
    );
    let mut archive_reader = storage
        .block_storage()
        .get_archive_reader(archive_id)?
        .expect("archive reader should exist");
    let mut read_data = Vec::new();
    archive_reader.read_to_end(&mut read_data)?;
    assert_eq!(read_data.as_slice(), stored_archive_data.as_ref());
    let all_blocks = {
        __guard.end_section(353u32);
        let __result = test_pagination(storage.clone()).await;
        __guard.start_section(353u32);
        __result
    }?;
    {
        __guard.end_section(354u32);
        let __result = test_orphaned_metadata_cleanup(
                storage,
                all_blocks,
                config,
                zerostate,
            )
            .await;
        __guard.start_section(354u32);
        __result
    }?;
    Ok(())
}
async fn test_pagination(storage: CoreStorage) -> Result<Vec<BlockId>> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(test_pagination)),
        file!(),
        359u32,
    );
    let storage = storage;
    use std::collections::HashSet;
    let mut all_blocks: Vec<BlockId> = Vec::new();
    let mut seen_blocks = HashSet::new();
    let mut continuation = None;
    let mut page_count = 0;
    loop {
        __guard.checkpoint(367u32);
        let (blocks, next_continuation) = {
            __guard.end_section(368u32);
            let __result = storage.block_storage().list_blocks(continuation).await;
            __guard.start_section(368u32);
            __result
        }?;
        if blocks.is_empty() {
            {
                __guard.end_section(371u32);
                __guard.start_section(371u32);
                break;
            };
        }
        page_count += 1;
        tracing::debug!("Page {} has {} blocks", page_count, blocks.len());
        for block in &blocks {
            __guard.checkpoint(378u32);
            assert!(seen_blocks.insert(* block), "Duplicate block found: {block:}",);
        }
        for block in &blocks {
            __guard.checkpoint(386u32);
            let block_handle = storage
                .block_handle_storage()
                .load_handle(block)
                .unwrap();
            let block_data = {
                __guard.end_section(392u32);
                let __result = storage
                    .block_storage()
                    .load_block_data(&block_handle)
                    .await;
                __guard.start_section(392u32);
                __result
            }?;
            let encoded_data = Boc::encode(block_data.root_cell());
            let expected_file_hash = Boc::file_hash(&encoded_data);
            assert_eq!(
                block.file_hash, expected_file_hash,
                "File hash mismatch for block {:?}: expected {:?}, got {:?}", block,
                expected_file_hash, block.file_hash
            );
        }
        for window in blocks.windows(2) {
            __guard.checkpoint(404u32);
            let (prev, curr) = (&window[0], &window[1]);
            if prev.shard == curr.shard {
                assert!(
                    prev.seqno <= curr.seqno, "Blocks not ordered by seqno within shard"
                );
            }
        }
        if let Some(last_prev) = all_blocks.last()
            && let Some(first_curr) = blocks.first()
            && last_prev.shard == first_curr.shard
        {
            assert!(
                last_prev.seqno <= first_curr.seqno, "Blocks not ordered between pages"
            );
        }
        all_blocks.extend(blocks);
        match next_continuation {
            Some(token) => continuation = Some(token),
            None => {
                __guard.end_section(428u32);
                __guard.start_section(428u32);
                break;
            }
        }
    }
    assert!(page_count > 0, "Should have at least one page");
    Ok(all_blocks)
}
async fn test_orphaned_metadata_cleanup(
    storage: CoreStorage,
    blocks: Vec<BlockId>,
    config: StorageConfig,
    zerostate: ShardStateStuff,
) -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(test_orphaned_metadata_cleanup)),
        file!(),
        442u32,
    );
    let storage = storage;
    let blocks = blocks;
    let config = config;
    let zerostate = zerostate;
    assert_eq!(
        storage.open_stats().orphaned_flags_count, 0, "fresh storage has orphans"
    );
    let meta_store = &storage.db().block_handles;
    let is_protected = |block_id: &BlockId| {
        if block_id.seqno == 0 {
            return true;
        }
        let meta = meta_store.get(block_id.root_hash).unwrap().unwrap();
        let meta = BlockMeta::deserialize(&mut &meta[..]);
        meta.flags().contains(BlockFlags::IS_KEY_BLOCK | BlockFlags::IS_PERSISTENT)
    };
    let mut test_blocks: Vec<_> = blocks
        .into_iter()
        .filter(|x| x.is_masterchain() && !is_protected(x))
        .collect();
    assert!(test_blocks.len() >= 55);
    let blocks_cas = storage.block_storage().blob_storage().blocks_for_test();
    let mut blocks_with_all_deleted = Vec::new();
    let mut blocks_with_block_deleted = Vec::new();
    let mut blocks_with_proof_deleted = Vec::new();
    for block_id in test_blocks.drain(..25) {
        __guard.checkpoint(477u32);
        for key_fn in [
            PackageEntryKey::block,
            PackageEntryKey::proof,
            PackageEntryKey::queue_diff,
        ] {
            __guard.checkpoint(478u32);
            assert!(blocks_cas.remove(& key_fn(& block_id)) ?);
        }
        blocks_with_all_deleted.push(block_id);
        tracing::info!(
            root_hash = ? block_id.root_hash, "Deleted all components from CAS"
        );
    }
    for block_id in test_blocks.drain(..10) {
        __guard.checkpoint(490u32);
        assert!(blocks_cas.remove(& PackageEntryKey::block(& block_id)) ?);
        blocks_with_block_deleted.push(block_id);
        tracing::info!(
            root_hash = ? block_id.root_hash, "Deleted only block data from CAS"
        );
    }
    for block_id in test_blocks.drain(..10) {
        __guard.checkpoint(497u32);
        assert!(blocks_cas.remove(& PackageEntryKey::proof(& block_id)) ?);
        blocks_with_proof_deleted.push(block_id);
        tracing::info!(root_hash = ? block_id.root_hash, "Deleted only proof from CAS");
    }
    let mut blocks_with_flags_removed = Vec::new();
    for block_id in test_blocks.drain(..10) {
        __guard.checkpoint(505u32);
        let meta_bytes = meta_store
            .get(block_id.root_hash)?
            .expect("Metadata should exist");
        let meta = BlockMeta::deserialize(&mut &meta_bytes[..]);
        meta.remove_flags(BlockFlags::HAS_ALL_BLOCK_PARTS);
        meta_store.insert(block_id.root_hash, meta.to_vec())?;
        blocks_with_flags_removed.push(block_id);
        tracing::info!(
            root_hash = ? block_id.root_hash, "Removed flags but kept data in CAS"
        );
    }
    let total_orphaned_cleaned = blocks_with_all_deleted.len()
        + blocks_with_block_deleted.len() + blocks_with_proof_deleted.len();
    let total_orphaned_restored = blocks_with_flags_removed.len();
    drop(storage);
    let storage = {
        __guard.end_section(529u32);
        let __result = prepare_storage(config, zerostate).await;
        __guard.start_section(529u32);
        __result
    }?;
    assert_eq!(
        storage.open_stats().orphaned_flags_count, total_orphaned_cleaned as u32,
        "orphaned metadata cleaned count mismatch"
    );
    assert_eq!(
        storage.open_stats().restored_flags_count, total_orphaned_restored as u32,
        "orphaned data restored count mismatch"
    );
    let meta_store = &storage.db().block_handles;
    for block_id in &blocks_with_all_deleted {
        __guard.checkpoint(545u32);
        let meta = meta_store
            .get(block_id.root_hash)?
            .expect("Metadata should still exist");
        let meta = BlockMeta::deserialize(&mut &meta[..]);
        let flags = meta.flags();
        assert!(! flags.contains(BlockFlags::HAS_DATA), "HAS_DATA should be cleared");
        assert!(! flags.contains(BlockFlags::HAS_PROOF), "HAS_PROOF should be cleared");
        assert!(
            ! flags.contains(BlockFlags::HAS_QUEUE_DIFF),
            "HAS_QUEUE_DIFF should be cleared"
        );
    }
    for block_id in &blocks_with_block_deleted {
        __guard.checkpoint(567u32);
        let meta = meta_store
            .get(block_id.root_hash)?
            .expect("Metadata should still exist");
        let meta = BlockMeta::deserialize(&mut &meta[..]);
        let flags = meta.flags();
        assert!(! flags.contains(BlockFlags::HAS_DATA), "HAS_DATA should be cleared");
        assert!(flags.contains(BlockFlags::HAS_PROOF), "HAS_PROOF should remain set");
        assert!(
            flags.contains(BlockFlags::HAS_QUEUE_DIFF),
            "HAS_QUEUE_DIFF should remain set"
        );
    }
    for block_id in &blocks_with_proof_deleted {
        __guard.checkpoint(589u32);
        let meta = meta_store
            .get(block_id.root_hash)?
            .expect("Metadata should still exist");
        let meta = BlockMeta::deserialize(&mut &meta[..]);
        let flags = meta.flags();
        assert!(flags.contains(BlockFlags::HAS_DATA), "HAS_DATA should remain set");
        assert!(! flags.contains(BlockFlags::HAS_PROOF), "HAS_PROOF should be cleared");
        assert!(
            flags.contains(BlockFlags::HAS_QUEUE_DIFF),
            "HAS_QUEUE_DIFF should remain set"
        );
    }
    for block_id in &blocks_with_flags_removed {
        __guard.checkpoint(611u32);
        let meta = meta_store
            .get(block_id.root_hash)?
            .expect("Metadata should still exist");
        let meta = BlockMeta::deserialize(&mut &meta[..]);
        let flags = meta.flags();
        assert!(
            flags.contains(BlockFlags::HAS_ALL_BLOCK_PARTS),
            "All parts should be restored {:?}", block_id
        );
    }
    Ok(())
}
#[test]
#[ignore = "must only be executed explicitly when `zstd-sys` version changes"]
fn repack_heavy_archives() -> Result<()> {
    let project_root = project_root()?.join(".scratch");
    let integration_test_path = project_root.join("integration_tests");
    for path in ["archive_1.bin", "archive_2.bin", "archive_3.bin"] {
        let path = integration_test_path.join(path);
        let data = std::fs::read(&path)?;
        let decompressed = zstd_decompress_simple(&data)?;
        drop(data);
        let chunk_size = ByteSize::kb(1024).as_u64() as usize;
        let mut stream = ZstdCompressStream::new(9, chunk_size)?;
        let workers = (std::thread::available_parallelism()?.get() / 4) as u8;
        stream.multithreaded(workers)?;
        let mut compressed = Vec::new();
        for chunk in decompressed.chunks(chunk_size) {
            stream.write(chunk, &mut compressed)?;
        }
        stream.finish(&mut compressed)?;
        drop(decompressed);
        std::fs::write(path, compressed)?;
    }
    Ok(())
}
#[tokio::test]
#[ignore]
async fn heavy_archives() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(heavy_archives)),
        file!(),
        663u32,
    );
    tycho_util::test::init_logger("heavy_archives", "debug");
    let project_root = project_root()?.join(".scratch");
    let integration_test_path = project_root.join("integration_tests");
    let current_test_path = integration_test_path.join("heavy_archives");
    std::fs::remove_dir_all(&current_test_path).ok();
    std::fs::create_dir_all(&current_test_path)?;
    let config = StorageConfig {
        root_dir: current_test_path.join("db"),
        rocksdb_enable_metrics: false,
        rocksdb_lru_capacity: ByteSize::mib(256),
    };
    let zerostate_path = integration_test_path.join("zerostate.boc");
    let zerostate_data = std::fs::read(zerostate_path)?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
    let zerostate_id = *zerostate.block_id();
    let storage = {
        __guard.end_section(684u32);
        let __result = prepare_storage(config, zerostate).await;
        __guard.start_section(684u32);
        __result
    }?;
    let state_subscriber = DummySubscriber;
    let state_applier = ShardStateApplier::new(storage.clone(), state_subscriber);
    let archive_handler = ArchiveHandler::new(
        storage.clone(),
        ArchiveHandlerSubscriber {},
    )?;
    let archive_path = integration_test_path.join("archive_1.bin");
    let first_archive_data = std::fs::read(archive_path)?;
    let first_archive = utils::parse_archive(&first_archive_data).map(Arc::new)?;
    let next_archive_path = integration_test_path.join("archive_2.bin");
    let next_archive_data = std::fs::read(next_archive_path)?;
    let next_archive = utils::parse_archive(&next_archive_data).map(Arc::new)?;
    let last_archive_path = integration_test_path.join("archive_3.bin");
    let last_archive_data = std::fs::read(last_archive_path)?;
    let last_archive = utils::parse_archive(&last_archive_data).map(Arc::new)?;
    let mut first_provider_archives = ArchivesSet::new();
    first_provider_archives.insert(1, first_archive.clone());
    let archive_provider = ArchiveProvider {
        archives: parking_lot::Mutex::new(first_provider_archives),
        proof_checker: ProofChecker::new(storage.clone()),
    };
    let mut next_provider_archives = ArchivesSet::new();
    next_provider_archives.insert(1, first_archive);
    next_provider_archives.insert(101, next_archive.clone());
    let next_archive_provider = ArchiveProvider {
        archives: parking_lot::Mutex::new(next_provider_archives),
        proof_checker: ProofChecker::new(storage.clone()),
    };
    let mut last_provider_archives = ArchivesSet::new();
    last_provider_archives.insert(101, next_archive);
    last_provider_archives.insert(201, last_archive);
    let last_archive_provider = ArchiveProvider {
        archives: parking_lot::Mutex::new(last_provider_archives),
        proof_checker: ProofChecker::new(storage.clone()),
    };
    let strider_state = PersistentBlockStriderState::new(zerostate_id, storage.clone());
    let block_strider = BlockStrider::builder()
        .with_provider(
            archive_provider.chain(next_archive_provider).chain(last_archive_provider),
        )
        .with_state(strider_state)
        .with_block_subscriber((state_applier, archive_handler))
        .build();
    {
        __guard.end_section(747u32);
        let __result = block_strider.run().await;
        __guard.start_section(747u32);
        __result
    }?;
    {
        __guard.end_section(748u32);
        let __result = storage.block_storage().wait_for_archive_commit().await;
        __guard.start_section(748u32);
        __result
    }?;
    {
        __guard.end_section(751u32);
        let __result = check_archive(&storage, &first_archive_data, 1).await;
        __guard.start_section(751u32);
        __result
    }?;
    {
        __guard.end_section(752u32);
        let __result = check_archive(&storage, &next_archive_data, 101).await;
        __guard.start_section(752u32);
        __result
    }?;
    let nodes = network::make_network(storage.clone(), 10);
    {
        __guard.end_section(756u32);
        let __result = network::discover(&nodes).await;
        __guard.start_section(756u32);
        __result
    }?;
    let peers = nodes.iter().map(|x| *x.network().peer_id()).collect::<Vec<PeerId>>();
    for node in &nodes {
        __guard.checkpoint(763u32);
        node.force_update_validators(peers.clone());
    }
    let node = nodes.first().unwrap();
    let client = BlockchainRpcClient::builder()
        .with_public_overlay_client(
            PublicOverlayClient::new(
                node.network().clone(),
                node.public_overlay().clone(),
                Default::default(),
            ),
        )
        .build();
    let archive_path = integration_test_path.join("archive_1.bin");
    let archive_data = std::fs::read(archive_path)?;
    let archive = utils::parse_archive(&archive_data)?;
    let mut sorted: Vec<_> = archive.blocks.into_iter().collect();
    sorted.sort_by_key(|(x, _)| *x);
    {
        for (block_id, data_entry) in &sorted {
            __guard.checkpoint(789u32);
            let result = {
                __guard.end_section(792u32);
                let __result = client
                    .get_block_full(block_id, DataRequirement::Required)
                    .await;
                __guard.start_section(792u32);
                __result
            }?;
            let block_full = result.data.unwrap();
            let block = BlockStuff::deserialize_checked(
                block_id,
                &block_full.block_data,
            )?;
            let proof = BlockProofStuff::deserialize(block_id, &block_full.proof_data)?;
            let queue_diff = QueueDiffStuff::deserialize(
                block_id,
                &block_full.queue_diff_data,
            )?;
            {
                let data = data_entry.block.as_ref().unwrap();
                let archive_block = BlockStuff::deserialize_checked(block_id, data)?;
                assert_eq!(block.block(), archive_block.block());
            }
            {
                let data = data_entry.proof.as_ref().unwrap();
                let archive_proof = BlockProofStuff::deserialize(block_id, data)?;
                assert_eq!(proof.proof().root, archive_proof.proof().root);
            }
            {
                let data = data_entry.queue_diff.as_ref().unwrap();
                let archive_queue_diff = QueueDiffStuff::deserialize(block_id, data)?;
                assert_eq!(queue_diff.diff(), archive_queue_diff.diff());
            }
        }
        for (block_id, _) in &sorted {
            __guard.checkpoint(823u32);
            let result = {
                __guard.end_section(826u32);
                let __result = client
                    .get_next_block_full(block_id, DataRequirement::Required)
                    .await;
                __guard.start_section(826u32);
                __result
            }?;
            let block_full = result.data.unwrap();
            let block = BlockStuff::deserialize_checked(
                &block_full.block_id,
                &block_full.block_data,
            );
            assert!(block.is_ok());
            let proof = BlockProofStuff::deserialize(
                &block_full.block_id,
                &block_full.proof_data,
            );
            assert!(proof.is_ok());
            let queue_diff = QueueDiffStuff::deserialize(
                &block_full.block_id,
                &block_full.queue_diff_data,
            );
            assert!(queue_diff.is_ok());
        }
    }
    let archive_path = integration_test_path.join("archive_1.bin");
    let archive_data = std::fs::read(archive_path)?;
    let archive = utils::parse_archive(&archive_data)?;
    {
        let archive_block_provider = ArchiveBlockProvider::new(
            client,
            storage,
            ArchiveBlockProviderConfig::default(),
        );
        for (_, mc_block_id) in archive.mc_block_ids.iter() {
            __guard.checkpoint(854u32);
            let result = {
                __guard.end_section(857u32);
                let __result = archive_block_provider
                    .get_block(&mc_block_id.relative_to_self())
                    .await;
                __guard.start_section(857u32);
                __result
            };
            let mc_block = result.unwrap()?;
            let data_entry = archive.blocks.get(mc_block_id).unwrap();
            let mc_block_data = data_entry.block.as_ref().unwrap();
            let archive_mc_block = BlockStuff::deserialize_checked(
                mc_block_id,
                mc_block_data,
            )?;
            assert_eq!(mc_block.block(), archive_mc_block.block());
            let custom = mc_block.load_custom()?;
            for entry in custom.shards.latest_blocks() {
                __guard.checkpoint(867u32);
                let shard_block_id = entry?;
                if shard_block_id.seqno != 0 {
                    let block = {
                        __guard.end_section(877u32);
                        let __result = archive_block_provider
                            .get_block(
                                &BlockIdRelation {
                                    mc_block_id: *mc_block.id(),
                                    block_id: shard_block_id,
                                },
                            )
                            .await;
                        __guard.start_section(877u32);
                        __result
                    }
                        .unwrap()?;
                    let data_entry = archive.blocks.get(&shard_block_id).unwrap();
                    let block_data = data_entry.block.as_ref().unwrap();
                    let archive_block = BlockStuff::deserialize_checked(
                        &shard_block_id,
                        block_data,
                    )?;
                    assert_eq!(block.block(), archive_block.block());
                }
            }
        }
        for (block_id, _) in &sorted {
            __guard.checkpoint(891u32);
            if block_id.is_masterchain() {
                let result = {
                    __guard.end_section(893u32);
                    let __result = archive_block_provider.get_next_block(block_id).await;
                    __guard.start_section(893u32);
                    __result
                };
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
    storage: &CoreStorage,
    original_archive: &[u8],
    seqno: u32,
) -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(check_archive)),
        file!(),
        906u32,
    );
    let storage = storage;
    let original_archive = original_archive;
    let seqno = seqno;
    tracing::info!("Checking archive {}", seqno);
    let archive_id = storage.block_storage().get_archive_id(seqno);
    let ArchiveId::Found(archive_id) = archive_id else {
        anyhow::bail!("archive {seqno} not found")
    };
    let archive_size = storage.block_storage().get_archive_size(archive_id)?.unwrap();
    let got_archive = {
        __guard.end_section(923u32);
        let __result = storage
            .block_storage()
            .get_archive_compressed_full(archive_id)
            .await;
        __guard.start_section(923u32);
        __result
    }?
        .expect("archive should exist");
    let got_archive = got_archive.as_ref();
    let original_decompressed = zstd_decompress_simple(original_archive)?;
    let got_decompressed = zstd_decompress_simple(got_archive)?;
    let original_len = original_decompressed.len();
    let got_len = got_decompressed.len();
    let old_parsed = Archive::new(original_decompressed.clone())?;
    let new_parsed = Archive::new(got_decompressed.clone())?;
    similar_asserts::assert_eq!(
        CheckArchive(& old_parsed), CheckArchive(& new_parsed),
        "Parsed archives should match"
    );
    assert_eq!(archive_size, original_archive.len(), "Size mismatch");
    assert_eq!(got_archive.len(), archive_size, "Retrieved size mismatch");
    assert_eq!(original_archive, got_archive, "Content mismatch");
    assert_eq!(original_decompressed, got_decompressed, "Decompressed mismatch");
    assert_eq!(original_len, got_len, "Decompressed size mismatch");
    Ok(())
}
struct CheckArchive<'a>(&'a Archive);
impl Eq for CheckArchive<'_> {}
impl PartialEq for CheckArchive<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.mc_block_ids == other.0.mc_block_ids
            && self.0.blocks.keys().collect::<BTreeSet<_>>()
                == other.0.blocks.keys().collect::<BTreeSet<_>>()
    }
}
impl std::fmt::Debug for CheckArchive<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let this = self.0;
        let mc_block_ids = this.mc_block_ids.keys().collect::<BTreeSet<_>>();
        let blocks = this
            .blocks
            .keys()
            .map(|x| x.as_short_id())
            .collect::<BTreeSet<_>>();
        f.debug_struct("Archive")
            .field("mc_block_ids", &mc_block_ids)
            .field("blocks", &blocks)
            .finish()
    }
}
