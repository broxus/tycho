use everscale_types::boc::{Boc, BocRepr};
use everscale_types::cell::CellBuilder;
use everscale_types::models::{Block, BlockId, ShardStateUnsplit};
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::{NewBlockMeta, Storage};

pub fn try_init_test_tracing(level_filter: tracing_subscriber::filter::LevelFilter) {
    use std::io::IsTerminal;
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .from_env_lossy(),
        )
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        //.with_thread_ids(true)
        .compact()
        .try_init()
        .ok();
}

pub async fn prepare_test_storage() -> anyhow::Result<(Storage, tempfile::TempDir)> {
    let (storage, tmp_dir) = Storage::new_temp().await?;
    let tracker = MinRefMcStateTracker::default();

    // master state
    let zerostate = Boc::decode(include_bytes!("../../test/zerostate.boc"))?;

    let master_block_data = include_bytes!("../../test/first-block.boc");
    let master_block: Block = BocRepr::decode(master_block_data)?;

    let master_id: BlockId = "-1:8000000000000000:1:98853ae1fa6eb846610381f1a96320325c8557831152279755208435dc494eec:597a90796b17f9d1d55fd2a8ac046bbca7e2104e961778859ad935454dbf62c0".parse()?;

    let master_root = master_block.load_state_update()?.apply(&zerostate)?;
    let master_state = master_root.parse::<Box<ShardStateUnsplit>>()?;

    let mc_state_extra = master_state.load_custom()?;
    let mc_state_extra = mc_state_extra.unwrap();
    let shard_info_opt = mc_state_extra.shards.iter().next();
    let shard_info = shard_info_opt.unwrap().unwrap();

    let master_state_stuff =
        ShardStateStuff::from_state_and_root(&master_id, master_state, master_root, &tracker)?;

    let meta_data = NewBlockMeta {
        is_key_block: mc_state_extra.after_key_block,
        gen_utime: master_state_stuff.state().gen_utime,
        mc_ref_seqno: master_id.seqno,
    };
    let (handle, _) = storage
        .block_handle_storage()
        .create_or_load_handle(&master_id, meta_data);

    storage
        .shard_state_storage()
        .store_state(&handle, &master_state_stuff)
        .await?;

    let root = CellBuilder::build_from(&master_block)?;
    let data = everscale_types::boc::Boc::encode_rayon(&root);
    let block_stuff = BlockStuff::from_block_and_root(&master_id, master_block, root);
    let handle = storage
        .block_storage()
        .store_block_data(&block_stuff, &ArchiveData::New(data.into()), meta_data)
        .await?
        .handle;

    let queue_data = include_bytes!("../../test/first-block-queue-diff.boc");
    let stuff = QueueDiffStuff::deserialize(&master_id, queue_data)?;
    let stuff_aug = QueueDiffStuffAug::new(stuff, queue_data.to_vec());

    storage
        .block_storage()
        .store_queue_diff(&stuff_aug, handle.into())
        .await?;

    // shard state
    let shard_bytes = include_bytes!("../../test/test_state_2_0:80.boc");
    let shard_root = Boc::decode(shard_bytes)?;
    let shard_state = shard_root.parse::<Box<ShardStateUnsplit>>()?;
    let shard_id = BlockId {
        shard: shard_info.0,
        seqno: shard_info.1.seqno,
        root_hash: shard_info.1.root_hash,
        file_hash: shard_info.1.file_hash,
    };
    let shard_state_stuff =
        ShardStateStuff::from_state_and_root(&shard_id, shard_state, shard_root, &tracker)?;

    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(&shard_id, NewBlockMeta {
                is_key_block: false,
                gen_utime: shard_state_stuff.state().gen_utime,
                mc_ref_seqno: master_id.seqno,
            });

    storage
        .shard_state_storage()
        .store_state(&handle, &shard_state_stuff)
        .await?;

    storage.node_state().store_last_mc_block_id(&master_id);

    Ok((storage, tmp_dir))
}
