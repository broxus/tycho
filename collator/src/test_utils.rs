use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta};
use tycho_storage::StorageContext;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::cell::CellBuilder;
use tycho_types::models::{Block, BlockId, ShardStateUnsplit};

use crate::internal_queue::queue::{QueueFactory, QueueFactoryStdImpl};
use crate::internal_queue::state::storage::QueueStateImplFactory;
use crate::internal_queue::types::message::InternalMessageValue;
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
