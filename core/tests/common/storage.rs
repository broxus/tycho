use anyhow::{Context, Result};
use tempfile::TempDir;
use tycho_block_util::archive::Archive;
use tycho_storage::{BlockMetaData, Storage};

pub(crate) fn get_archive() -> Result<Archive> {
    let data = include_bytes!("../../tests/data/00001");
    Archive::new(data.as_slice())
}

pub(crate) async fn init_storage() -> Result<(Storage, TempDir)> {
    let (storage, tmp_dir) = Storage::new_temp()?;

    let data = include_bytes!("../../tests/data/00001");
    let provider = Archive::new(data.as_slice())?;

    for (block_id, archive) in provider.blocks {
        if block_id.shard.is_masterchain() {
            let block = archive.block.unwrap();
            let proof = archive.proof.unwrap();

            let info = block.load_info().context("Failed to load block info")?;

            let meta = BlockMetaData {
                is_key_block: info.key_block,
                gen_utime: info.gen_utime,
                mc_ref_seqno: None, // TODO: set mc ref seqno
            };

            let block_result = storage
                .block_storage()
                .store_block_data(&block, &block.archive_data, meta)
                .await?;

            assert!(block_result.new);

            let handle = storage
                .block_handle_storage()
                .load_handle(&block_id)
                .unwrap();

            assert_eq!(handle.id(), block.id());

            let bs = storage
                .block_storage()
                .load_block_data(&block_result.handle)
                .await?;

            assert_eq!(bs.id(), &block_id);
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
    }

    Ok((storage, tmp_dir))
}
