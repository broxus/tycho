use std::sync::Arc;

use anyhow::{Context, Result};
use tempfile::TempDir;
use tycho_storage::{NewBlockMeta, Storage};

use crate::utils;

pub(crate) async fn init_storage() -> Result<(Storage, TempDir)> {
    let (storage, tmp_dir) = Storage::open_temp().await?;
    let handles = storage.block_handle_storage();
    let blocks = storage.block_storage();

    // Init zerostate
    let zerostate_data = utils::read_file("zerostate.boc")?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;

    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(zerostate.block_id(), NewBlockMeta {
                is_key_block: zerostate.block_id().is_masterchain(),
                gen_utime: zerostate.state().gen_utime,
                ref_by_mc_seqno: 0,
            });

    storage
        .shard_state_storage()
        .store_state(&handle, &zerostate, Default::default())
        .await?;

    // Init blocks
    let archive_data = utils::read_file("archive_1.bin")?;
    let block_provider = utils::parse_archive(&archive_data).map(Arc::new)?;

    for block_id in block_provider.mc_block_ids.values() {
        let (block, proof, diff) = block_provider.get_entry_by_id(block_id).await?;

        let info = block.load_info().context("Failed to load block info")?;
        let meta = NewBlockMeta {
            is_key_block: info.key_block,
            gen_utime: info.gen_utime,
            ref_by_mc_seqno: block_id.seqno,
        };

        let block_result = blocks
            .store_block_data(&block, &block.archive_data, meta)
            .await?;

        assert!(block_result.new);

        let handle = handles.load_handle(block_id).unwrap();

        assert_eq!(handle.id(), block.id());

        let bs = blocks.load_block_data(&block_result.handle).await?;
        assert_eq!(bs.id(), block_id);
        assert_eq!(bs.block(), block.as_ref());

        let handle = blocks
            .store_block_proof(&proof, handle.into())
            .await?
            .handle;
        let bp = blocks.load_block_proof(&handle).await?;

        assert_eq!(bp.is_link(), proof.is_link());
        assert_eq!(bp.proof().root, proof.as_ref().root);
        assert_eq!(bp.proof().proof_for, proof.as_ref().proof_for);

        let handle = storage
            .block_storage()
            .store_queue_diff(&diff, handle.into())
            .await?
            .handle;
        let df = blocks.load_queue_diff(&handle).await?;

        assert_eq!(df.block_id(), diff.block_id());
        assert_eq!(df.as_ref(), diff.as_ref());
    }

    Ok((storage, tmp_dir))
}
