use std::sync::Arc;
use anyhow::{Context, Result};
use tempfile::TempDir;
use tycho_core::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta};
use tycho_storage::StorageContext;
use crate::utils;
pub(crate) async fn init_storage() -> Result<(CoreStorage, TempDir)> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(init_storage)),
        file!(),
        10u32,
    );
    let (ctx, tmp_dir) = {
        __guard.end_section(11u32);
        let __result = StorageContext::new_temp().await;
        __guard.start_section(11u32);
        __result
    }?;
    let storage = {
        __guard.end_section(12u32);
        let __result = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await;
        __guard.start_section(12u32);
        __result
    }?;
    let handles = storage.block_handle_storage();
    let blocks = storage.block_storage();
    let zerostate_data = utils::read_file("zerostate.boc")?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
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
    {
        __guard.end_section(33u32);
        let __result = storage
            .shard_state_storage()
            .store_state(&handle, &zerostate, Default::default())
            .await;
        __guard.start_section(33u32);
        __result
    }?;
    let archive_data = utils::read_file("archive_1.bin")?;
    let block_provider = utils::parse_archive(&archive_data).map(Arc::new)?;
    for block_id in block_provider.mc_block_ids.values() {
        __guard.checkpoint(39u32);
        let (block, proof, diff) = {
            __guard.end_section(40u32);
            let __result = block_provider.get_entry_by_id(block_id).await;
            __guard.start_section(40u32);
            __result
        }?;
        let info = block.load_info().context("Failed to load block info")?;
        let meta = NewBlockMeta {
            is_key_block: info.key_block,
            gen_utime: info.gen_utime,
            ref_by_mc_seqno: block_id.seqno,
        };
        let block_result = {
            __guard.end_section(51u32);
            let __result = blocks
                .store_block_data(&block, &block.archive_data, meta)
                .await;
            __guard.start_section(51u32);
            __result
        }?;
        assert!(block_result.new);
        let handle = handles.load_handle(block_id).unwrap();
        assert_eq!(handle.id(), block.id());
        let bs = {
            __guard.end_section(59u32);
            let __result = blocks.load_block_data(&block_result.handle).await;
            __guard.start_section(59u32);
            __result
        }?;
        assert_eq!(bs.id(), block_id);
        assert_eq!(bs.block(), block.as_ref());
        let handle = {
            __guard.end_section(65u32);
            let __result = blocks.store_block_proof(&proof, handle.into()).await;
            __guard.start_section(65u32);
            __result
        }?
            .handle;
        let bp = {
            __guard.end_section(67u32);
            let __result = blocks.load_block_proof(&handle).await;
            __guard.start_section(67u32);
            __result
        }?;
        assert_eq!(bp.is_link(), proof.is_link());
        assert_eq!(bp.proof().root, proof.as_ref().root);
        assert_eq!(bp.proof().proof_for, proof.as_ref().proof_for);
        let handle = {
            __guard.end_section(76u32);
            let __result = storage
                .block_storage()
                .store_queue_diff(&diff, handle.into())
                .await;
            __guard.start_section(76u32);
            __result
        }?
            .handle;
        let df = {
            __guard.end_section(78u32);
            let __result = blocks.load_queue_diff(&handle).await;
            __guard.start_section(78u32);
            __result
        }?;
        assert_eq!(df.block_id(), diff.block_id());
        assert_eq!(df.as_ref(), diff.as_ref());
    }
    Ok((storage, tmp_dir))
}
