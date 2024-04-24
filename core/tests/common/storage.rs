use std::sync::Arc;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use everscale_types::models::BlockId;
use tempfile::TempDir;
use tycho_block_util::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug};
use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

use crate::common::*;

pub(crate) async fn init_empty_storage() -> Result<(Arc<Storage>, TempDir)> {
    let tmp_dir = tempfile::tempdir()?;
    let root_path = tmp_dir.path();

    // Init rocksdb
    let db_options = DbOptions {
        rocksdb_lru_capacity: ByteSize::kb(1024),
        cells_cache_size: ByteSize::kb(1024),
    };
    let db = Db::open(root_path.join("db_storage"), db_options)?;

    // Init storage
    let storage = Storage::new(
        db,
        root_path.join("file_storage"),
        db_options.cells_cache_size.as_u64(),
    )?;
    assert!(storage.node_state().load_init_mc_block_id().is_err());

    Ok((storage, tmp_dir))
}

pub(crate) fn get_block_ids() -> Result<Vec<BlockId>> {
    let data = include_bytes!("../../tests/data/00001");
    let archive = archive::Archive::new(data)?;

    let block_ids = archive
        .blocks
        .into_iter()
        .map(|(block_id, _)| block_id)
        .collect();

    Ok(block_ids)
}

pub(crate) fn get_archive() -> Result<archive::Archive> {
    let data = include_bytes!("../../tests/data/00001");
    let archive = archive::Archive::new(data)?;

    Ok(archive)
}

pub(crate) async fn init_storage() -> Result<(Arc<Storage>, TempDir)> {
    let (storage, tmp_dir) = init_empty_storage().await?;

    let data = include_bytes!("../../tests/data/00001");
    let provider = archive::Archive::new(data)?;

    for (block_id, archive) in provider.blocks {
        if block_id.shard.is_masterchain() {
            let block = archive.block.unwrap();
            let proof = archive.proof.unwrap();

            let info = block.info.load().context("Failed to load block info")?;

            let meta = BlockMetaData {
                is_key_block: info.key_block,
                gen_utime: info.gen_utime,
                mc_ref_seqno: info
                    .master_ref
                    .map(|r| {
                        r.load()
                            .context("Failed to load master ref")
                            .map(|mr| mr.seqno)
                    })
                    .transpose()
                    .context("Failed to process master ref")?,
            };

            let block_data = everscale_types::boc::BocRepr::encode(&block)?;
            let block_stuff =
                BlockStuffAug::new(BlockStuff::with_block(block_id, block.clone()), block_data);

            let block_result = storage
                .block_storage()
                .store_block_data(&block_stuff, meta)
                .await?;

            assert!(block_result.new);

            let handle = storage
                .block_handle_storage()
                .load_handle(&block_id)?
                .unwrap();

            assert_eq!(handle.id(), block_stuff.data.id());

            let bs = storage
                .block_storage()
                .load_block_data(&block_result.handle)
                .await?;

            assert_eq!(bs.id(), &block_id);
            assert_eq!(bs.block(), &block);

            let block_proof = BlockProofStuff::deserialize(
                block_id,
                everscale_types::boc::BocRepr::encode(&proof)?.as_slice(),
                false,
            )?;

            let block_proof_with_data = BlockProofStuffAug::new(
                block_proof.clone(),
                everscale_types::boc::BocRepr::encode(&proof)?,
            );

            let handle = storage
                .block_storage()
                .store_block_proof(&block_proof_with_data, handle.into())
                .await?
                .handle;

            let bp = storage
                .block_storage()
                .load_block_proof(&handle, false)
                .await?;

            assert_eq!(bp.is_link(), block_proof.is_link());
            assert_eq!(bp.proof().root, block_proof.proof().root);
            assert_eq!(bp.proof().proof_for, block_proof.proof().proof_for);
        }
    }

    Ok((storage, tmp_dir))
}
