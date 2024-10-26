use std::str::FromStr;

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

    let master_state_stuff = ShardStateStuff::from_state_and_root(
        &master_block_id,
        master_state,
        master_root,
        &tracker,
    )?;

    let meta_data = NewBlockMeta {
        is_key_block: mc_state_extra.after_key_block,
        gen_utime: master_state_stuff.state().gen_utime,
        ref_by_mc_seqno: master_block_id.seqno,
    };
    let (handle, _) = storage
        .block_handle_storage()
        .create_or_load_handle(&master_block_id, meta_data);

    storage
        .shard_state_storage()
        .store_state(&handle, &master_state_stuff)
        .await?;

    // first master block
    let root = CellBuilder::build_from(&master_block)?;
    let data = everscale_types::boc::Boc::encode_rayon(&root);
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

        let shard_state_stuff = ShardStateStuff::from_root(&block_id, root, &tracker)?;

        let (handle, _) =
            storage
                .block_handle_storage()
                .create_or_load_handle(&block_id, NewBlockMeta {
                    is_key_block: false,
                    gen_utime: shard_state_stuff.state().gen_utime,
                    ref_by_mc_seqno: master_block_id.seqno,
                });

        storage
            .shard_state_storage()
            .store_state(&handle, &shard_state_stuff)
            .await?;
    }

    Ok((storage, tmp_dir))
}
