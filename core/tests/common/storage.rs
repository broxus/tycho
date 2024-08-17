use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use tempfile::TempDir;
use tycho_block_util::archive::Archive;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::{BlockMetaData, Storage};

pub(crate) fn get_archive() -> Result<Archive> {
    let root_path = env!("CARGO_MANIFEST_DIR");
    let relative_path = "tests/data/archive.bin";
    let file_path = std::path::Path::new(root_path).join(relative_path);

    let data = std::fs::read(file_path)?;
    Archive::new(data)
}

pub(crate) fn get_zerostate() -> Result<ShardStateStuff> {
    let root_path = env!("CARGO_MANIFEST_DIR");
    let relative_path = "tests/data/zerostate.boc";
    let file_path = std::path::Path::new(root_path).join(relative_path);

    let data = std::fs::read(file_path)?;
    let file_hash = Boc::file_hash_blake(&data);

    let root = Boc::decode(&data).context("failed to decode BOC")?;
    let root_hash = *root.repr_hash();

    let state = root
        .parse::<ShardStateUnsplit>()
        .context("failed to parse state")?;

    anyhow::ensure!(state.seqno == 0, "not a zerostate");

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    let tracker = MinRefMcStateTracker::default();
    ShardStateStuff::from_root(&block_id, root, &tracker)
}

pub(crate) async fn init_storage() -> Result<(Storage, TempDir)> {
    let (storage, tmp_dir) = Storage::new_temp()?;

    // Init zerostate
    let zerostate = get_zerostate()?;

    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(zerostate.block_id(), BlockMetaData {
                is_key_block: zerostate.block_id().is_masterchain(),
                gen_utime: zerostate.state().gen_utime,
                mc_ref_seqno: Some(0),
            });

    storage
        .shard_state_storage()
        .store_state(&handle, &zerostate)
        .await?;

    // Init blocks
    let block_provider = get_archive()?;

    for (_, block_id) in &block_provider.mc_block_ids {
        let block = block_provider.get_block_by_id(block_id)?;
        let proof = block_provider.get_proof_by_id(block_id)?;

        let info = block.load_info().context("Failed to load block info")?;
        let meta = BlockMetaData {
            is_key_block: info.key_block,
            gen_utime: info.gen_utime,
            mc_ref_seqno: Some(block_id.seqno),
        };

        let block_result = storage
            .block_storage()
            .store_block_data(&block, &block.archive_data, meta)
            .await?;

        assert!(block_result.new);

        let handle = storage
            .block_handle_storage()
            .load_handle(block_id)
            .unwrap();

        assert_eq!(handle.id(), block.id());

        let bs = storage
            .block_storage()
            .load_block_data(&block_result.handle)
            .await?;

        assert_eq!(bs.id(), block_id);
        assert_eq!(bs.block(), block.as_ref());

        let handle = storage
            .block_storage()
            .store_block_proof(&proof, handle.into())
            .await?
            .handle;

        let bp = storage
            .block_storage()
            .load_block_proof(&handle, false)
            .await?;

        assert_eq!(bp.is_link(), proof.is_link());
        assert_eq!(bp.proof().root, proof.as_ref().root);
        assert_eq!(bp.proof().proof_for, proof.as_ref().proof_for);
    }

    Ok((storage, tmp_dir))
}
