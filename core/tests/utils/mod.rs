use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use tycho_block_util::archive::Archive;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_util::compression::ZstdDecompressStream;

const DATA_PATH: &str = "tests/data";

pub(crate) fn parse_zerostate(data: &Vec<u8>) -> Result<ShardStateStuff> {
    let file_hash = Boc::file_hash_blake(data);

    let root = Boc::decode(data).context("failed to decode BOC")?;
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

    let tracker = MinRefMcStateTracker::new();
    ShardStateStuff::from_root(&block_id, root, &tracker)
}

pub(crate) fn parse_archive(data: &[u8]) -> Result<Archive> {
    let mut decoder = ZstdDecompressStream::new(1024 * 1024)?;

    let mut decompressed = Vec::new();
    decoder.write(data, &mut decompressed)?;

    let archive = Archive::new(decompressed)?;

    Ok(archive)
}

pub(crate) fn read_file(filename: &str) -> Result<Vec<u8>> {
    let root_path = env!("CARGO_MANIFEST_DIR");
    let file_path = std::path::Path::new(root_path)
        .join(DATA_PATH)
        .join(filename);

    let data = std::fs::read(file_path)?;
    Ok(data)
}
