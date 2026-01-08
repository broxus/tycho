use std::fs::{self};
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta, QueueStateReader};
use tycho_storage::StorageContext;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::cell::CellBuilder;
use tycho_types::models::{Block, BlockId, ShardStateUnsplit};
use tycho_util::compression::zstd_decompress_simple;

use crate::internal_queue::queue::{QueueConfig, QueueFactory, QueueFactoryStdImpl};
use crate::internal_queue::state::storage::QueueStateImplFactory;
use crate::internal_queue::types::message::InternalMessageValue;
use crate::mempool::DumpedAnchor;
use crate::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};

pub fn try_init_test_tracing(level_filter: tracing_subscriber::filter::LevelFilter) {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(false).boxed())
        .try_init()
        .ok();
}

pub async fn prepare_test_storage() -> anyhow::Result<(CoreStorage, tempfile::TempDir)> {
    let (ctx, tmp_dir) = StorageContext::new_temp().await?;
    let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;
    let shard_states = storage.shard_state_storage();

    // master state
    let zerostate = Boc::decode(include_bytes!("../../test/data/zerostate.boc"))?;
    let master_zerostate = zerostate.parse::<Box<ShardStateUnsplit>>()?;

    let master_block_data = include_bytes!("../../test/data/first_block.bin");
    let master_block: Block = BocRepr::decode(master_block_data)?;

    let master_block_id = {
        let mc_block_id_str = include_str!("../../test/data/first_block_id.txt");
        let mc_block_id_str = mc_block_id_str.trim_end();
        BlockId::from_str(mc_block_id_str)?
    };

    let master_root = master_block.load_state_update()?.apply(&zerostate)?;
    let master_state = master_root.parse::<Box<ShardStateUnsplit>>()?;

    let mc_state_extra = master_state.load_custom()?;
    let mc_state_extra = mc_state_extra.unwrap();

    let handle = shard_states.min_ref_mc_state().insert(&master_state);
    let master_state_stuff =
        ShardStateStuff::from_state_and_root(&master_block_id, master_state, master_root, handle)?;

    let meta_data = NewBlockMeta {
        is_key_block: mc_state_extra.after_key_block,
        gen_utime: master_state_stuff.state().gen_utime,
        ref_by_mc_seqno: master_block_id.seqno,
    };
    let (handle, _) = storage
        .block_handle_storage()
        .create_or_load_handle(&master_block_id, meta_data);

    shard_states
        .store_state(&handle, &master_state_stuff, Default::default())
        .await?;

    // first master block
    let root = CellBuilder::build_from(&master_block)?;
    let data = tycho_types::boc::Boc::encode_rayon(&root);
    let block_stuff =
        BlockStuff::from_block_and_root(&master_block_id, master_block, root, data.len());
    let handle = storage
        .block_storage()
        .store_block_data(&block_stuff, &ArchiveData::New(data.into()), meta_data)
        .await?
        .handle;

    // first master block queue diff
    let queue_data = include_bytes!("../../test/data/first_block_queue_diff.bin");
    let stuff = QueueDiffStuff::deserialize(&master_block_id, queue_data)?;
    let stuff_aug = QueueDiffStuffAug::new(stuff, queue_data.to_vec());

    storage
        .block_storage()
        .store_queue_diff(&stuff_aug, handle.into())
        .await?;

    storage
        .node_state()
        .store_last_mc_block_id(&master_block_id);

    // initial shard states
    for entry in master_zerostate.load_custom()?.unwrap().shards.iter() {
        let (shard_ident, descr) = entry.unwrap();
        anyhow::ensure!(descr.seqno == 0, "invalid shard description {shard_ident}");

        let block_id = BlockId {
            shard: shard_ident,
            seqno: 0,
            root_hash: descr.root_hash,
            file_hash: descr.file_hash,
        };

        tracing::debug!(block_id = %block_id, "creating default zerostate");
        let state = ShardStateUnsplit {
            global_id: master_zerostate.global_id,
            shard_ident,
            gen_utime: master_zerostate.gen_utime,
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

        let shard_state_stuff = ShardStateStuff::from_root(
            &block_id,
            root,
            shard_states.min_ref_mc_state().insert_untracked(),
        )?;

        let (handle, _) =
            storage
                .block_handle_storage()
                .create_or_load_handle(&block_id, NewBlockMeta {
                    is_key_block: false,
                    gen_utime: shard_state_stuff.state().gen_utime,
                    ref_by_mc_seqno: master_block_id.seqno,
                });

        shard_states
            .store_state(&handle, &shard_state_stuff, Default::default())
            .await?;
    }

    Ok((storage, tmp_dir))
}

pub async fn create_test_queue_adapter<V: InternalMessageValue>()
-> Result<(Arc<dyn MessageQueueAdapter<V>>, tempfile::TempDir)> {
    let (ctx, tmp_dir) = StorageContext::new_temp().await?;
    let queue_state_factory = QueueStateImplFactory::new(ctx)?;
    let queue_factory = QueueFactoryStdImpl {
        state: queue_state_factory,
        config: Default::default(),
    };
    let queue = queue_factory.create()?;
    let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);
    Ok((Arc::new(message_queue_adapter), tmp_dir))
}

#[allow(clippy::disallowed_methods)]
async fn load_blocks_from_dump(storage: &CoreStorage, dump_path: &Path) -> Result<Option<BlockId>> {
    let mut latest_mc_block_id: Option<BlockId> = None;

    for entry in std::fs::read_dir(dump_path)? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) == Some("block") {
            let filename = path.file_stem().unwrap().to_string_lossy();
            let block_id_str = filename.replace('_', ":");
            let block_id = BlockId::from_str(&block_id_str)?;

            let block_boc = tokio::fs::read(&path).await?;
            let root = Boc::decode(block_boc.clone())?;
            let block: Block = root.parse()?;
            let block_stuff =
                BlockStuff::from_block_and_root(&block_id, block, root, block_boc.len());

            let info = block_stuff.load_info()?;
            let meta = tycho_core::storage::NewBlockMeta {
                is_key_block: info.key_block,
                gen_utime: info.gen_utime,
                ref_by_mc_seqno: info.min_ref_mc_seqno,
            };
            storage
                .block_storage()
                .store_block_data(&block_stuff, &ArchiveData::New(block_boc.into()), meta)
                .await?;

            if block_id.is_masterchain()
                && (latest_mc_block_id.is_none()
                    || block_id.seqno > latest_mc_block_id.unwrap().seqno)
            {
                latest_mc_block_id = Some(block_id);
            }
        }
    }

    Ok(latest_mc_block_id)
}

#[allow(clippy::disallowed_methods)]
async fn load_states_from_dump(storage: &CoreStorage, dump_path: &Path) -> Result<()> {
    let persistents_path = dump_path.join("persistents");
    if persistents_path.is_dir() {
        for entry in std::fs::read_dir(persistents_path)? {
            let path = entry?.path();
            let block_id_str = path.file_stem().unwrap().to_string_lossy();
            let block_id = BlockId::from_str(&block_id_str)?;

            // Read compressed file and decompress it
            let compressed_data = std::fs::read(&path)?;
            let decompressed_data = zstd_decompress_simple(&compressed_data)
                .context(format!("Failed to decompress state file for {}", block_id))?;

            let temp_path = path.with_extension("tempfile");
            let mut tempfile = std::fs::File::create(&temp_path)?;
            tempfile.write_all(&decompressed_data)?;
            drop(tempfile); // Close the write handle

            // Reopen the file for reading
            let tempfile = std::fs::File::open(&temp_path)?;
            storage
                .shard_state_storage()
                .store_state_file(&block_id, tempfile)
                .await?;

            if let Some(handle) = storage.block_handle_storage().load_handle(&block_id) {
                let handles = storage.block_handle_storage();
                handles.set_has_shard_state(&handle);
                if !block_id.is_masterchain() {
                    handles.set_block_committed(&handle);
                }
            }
            fs::remove_file(path.with_extension("tempfile"))?;
        }
    }
    Ok(())
}

#[allow(clippy::disallowed_methods)]
async fn load_queues_from_dump(
    storage: &CoreStorage,
    dump_path: &Path,
    queue_factory: &QueueFactoryStdImpl,
) -> Result<()> {
    // Process queue files after all blocks and states are loaded
    let queues_path = dump_path.join("queues");
    let queue_handler = &queue_factory.state.storage;
    if queues_path.is_dir() {
        // Collect queue files and sort them by sequence number to ensure proper order
        let mut queue_files = Vec::new();
        for entry in std::fs::read_dir(queues_path)? {
            let path = entry?.path();
            let block_id_str = path.file_stem().unwrap().to_string_lossy();
            if let Ok(block_id) = BlockId::from_str(&block_id_str) {
                queue_files.push((block_id, path));
            }
        }

        // Sort by sequence number to process in order
        queue_files.sort_by_key(|(block_id, _)| block_id.seqno);

        for (block_id, path) in queue_files {
            // Check if the block exists in storage first
            if let Some(handle) = storage.block_handle_storage().load_handle(&block_id) {
                let block = storage.block_storage().load_block_data(&handle).await?;
                let top_update = &block.block().out_msg_queue_updates;

                // Read compressed file and decompress it
                let compressed_data = std::fs::read(&path)?;
                let decompressed_data = zstd_decompress_simple(&compressed_data)
                    .context(format!("Failed to decompress queue file for {}", block_id))?;

                let temp_path = path.with_extension("tempfile");
                let mut tempfile = std::fs::File::create(&temp_path)?;
                tempfile.write_all(&decompressed_data)?;
                drop(tempfile); // Close the write handle

                // Read queue queue state from file
                let reader = QueueStateReader::begin_from_mapped(&decompressed_data, top_update)?;

                for queue_diff in &reader.state().header.queue_diffs {
                    let stuff_aug = QueueDiffStuff::builder(
                        block_id.shard,
                        block_id.seqno,
                        &queue_diff.prev_hash,
                    )
                    .with_processed_to(queue_diff.processed_to.clone())
                    .with_messages(
                        &queue_diff.min_message,
                        &queue_diff.max_message,
                        queue_diff.messages.iter(),
                    )
                    .with_router(
                        queue_diff.router_partitions_src.clone(),
                        queue_diff.router_partitions_dst.clone(),
                    )
                    .serialize()
                    .build(&block_id);
                    storage
                        .block_storage()
                        .store_queue_diff(&stuff_aug, handle.clone().into())
                        .await?;
                }

                let file = std::fs::File::open(temp_path)?;
                queue_handler
                    .import_from_file(top_update, file, block_id)
                    .await?;
                tracing::debug!("Imported queue data for block {}", block_id);

                fs::remove_file(path.with_extension("tempfile"))?;
            } else {
                tracing::debug!(
                    "Skipping queue data for block {} - block not found in storage",
                    block_id
                );
            }
        }
    }
    Ok(())
}

async fn load_mempool_anchors_from_dump(dump_path: &Path) -> Result<Vec<DumpedAnchor>> {
    let mut dumped_anchors = vec![];
    let dump_mempool_path = dump_path.join("mempool");
    if dump_mempool_path.is_dir() {
        for entry in std::fs::read_dir(dump_mempool_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let data = std::fs::read_to_string(&path).context("Failed to read mempool file")?;
                let decoded: DumpedAnchor =
                    serde_json::from_str(&data).context("Failed to deserialize mempool file")?;
                dumped_anchors.push(decoded);
            }
        }
    }
    Ok(dumped_anchors)
}

pub struct LoadedInfoFromDump<V: InternalMessageValue> {
    pub storage: CoreStorage,
    pub mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    pub mc_block_id: BlockId,
    pub dumped_anchors: Vec<DumpedAnchor>,
}

pub async fn load_info_from_dump<V: InternalMessageValue>(
    dump_path: &Path,
    ctx: StorageContext,
) -> Result<LoadedInfoFromDump<V>> {
    let storage = CoreStorage::open(
        ctx.clone(),
        tycho_core::storage::CoreStorageConfig::new_potato(),
    )
    .await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let mc_block_id = load_blocks_from_dump(&storage, dump_path)
        .await?
        .context("No master block found in dump")?;
    load_states_from_dump(&storage, dump_path).await?;
    load_queues_from_dump(&storage, dump_path, &queue_factory).await?;
    let dumped_anchors = load_mempool_anchors_from_dump(dump_path).await?;

    storage.node_state().store_last_mc_block_id(&mc_block_id);
    storage.node_state().store_init_mc_block_id(&mc_block_id);

    let queue = queue_factory.create()?;
    let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);
    Ok(LoadedInfoFromDump {
        storage,
        mq_adapter: Arc::new(message_queue_adapter),
        mc_block_id,
        dumped_anchors,
    })
}
