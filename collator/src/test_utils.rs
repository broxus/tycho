use everscale_types::boc::Boc;
use everscale_types::models::{BlockId, ShardStateUnsplit};
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

pub async fn prepare_test_storage() -> anyhow::Result<Storage> {
    let (storage, _tmp_dir) = Storage::new_temp()?;
    let tracker = MinRefMcStateTracker::default();

    // master state
    let master_bytes = include_bytes!("../../test/test_state_2_master.boc");
    let master_file_hash = Boc::file_hash_blake(master_bytes);
    let master_root = Boc::decode(master_bytes)?;
    let master_root_hash = *master_root.repr_hash();
    let master_state = master_root.parse::<Box<ShardStateUnsplit>>()?;

    let mc_state_extra = master_state.load_custom()?;
    let mc_state_extra = mc_state_extra.unwrap();
    let shard_info_opt = mc_state_extra.shards.iter().next();
    let shard_info = shard_info_opt.unwrap().unwrap();

    let master_id = BlockId {
        shard: master_state.shard_ident,
        seqno: master_state.seqno,
        root_hash: master_root_hash,
        file_hash: master_file_hash,
    };
    let master_state_stuff =
        ShardStateStuff::from_state_and_root(&master_id, master_state, master_root, &tracker)?;

    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(&master_id, NewBlockMeta {
                is_key_block: mc_state_extra.after_key_block,
                gen_utime: master_state_stuff.state().gen_utime,
                mc_ref_seqno: Some(master_id.seqno),
            });

    storage
        .shard_state_storage()
        .store_state(&handle, &master_state_stuff)
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
                mc_ref_seqno: Some(master_id.seqno),
            });

    storage
        .shard_state_storage()
        .store_state(&handle, &shard_state_stuff)
        .await?;

    storage.node_state().store_last_mc_block_id(&master_id);

    Ok(storage)
}
