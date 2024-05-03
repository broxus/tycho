use anyhow::{Context, Result};
use bytesize::ByteSize;
use tempfile::TempDir;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff};
use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

use crate::common::*;

pub(crate) async fn init_empty_storage() -> Result<(Storage, TempDir)> {
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
    assert!(storage.node_state().load_init_mc_block_id().is_none());

    Ok((storage, tmp_dir))
}

pub(crate) fn get_archive() -> Result<archive::Archive> {
    let data = include_bytes!("../../tests/data/00001");
    let archive = archive::Archive::new(data)?;

    Ok(archive)
}

pub(crate) async fn init_storage() -> Result<(Storage, TempDir)> {
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
                mc_ref_seqno: 0, // TODO: set mc ref seqno
            };

            let block_stuff = BlockStuff::with_block(block_id, block.data);

            let block_result = storage
                .block_storage()
                .store_block_data(&block_stuff, &block.archive_data, meta)
                .await?;

            assert!(block_result.new);

            let handle = storage
                .block_handle_storage()
                .load_handle(&block_id)
                .unwrap();

            assert_eq!(handle.id(), block_stuff.id());

            let bs = storage
                .block_storage()
                .load_block_data(&block_result.handle)
                .await?;

            assert_eq!(bs.id(), &block_id);
            assert_eq!(bs.block(), block_stuff.block());

            let proof_archive_data = match proof.archive_data {
                ArchiveData::New(archive_data) => archive_data,
                ArchiveData::Existing => anyhow::bail!("invalid proof archive data"),
            };

            let block_proof = BlockProofStuff::deserialize(
                block_id,
                everscale_types::boc::BocRepr::encode(&proof.data)?.as_slice(),
                false,
            )?;

            let block_proof_with_data =
                BlockProofStuffAug::new(block_proof.clone(), proof_archive_data);

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
