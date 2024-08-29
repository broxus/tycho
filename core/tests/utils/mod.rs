use std::io::BufRead;
use std::str::FromStr;

use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use sha2::Digest;
use tycho_block_util::archive::Archive;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_util::compression::ZstdDecompressStream;

const DATA_PATH: &str = "tests/data";
const BROXUS_URL: &str = "https://tycho-test.broxus.cc/";

pub(crate) async fn get_zerostate(
    filename: &str,
    require_download: bool,
) -> Result<ShardStateStuff> {
    let data = if require_download {
        let data = download(filename).await?;
        if !verify(filename, &data).await? {
            anyhow::bail!("Failed to verify downloaded file: {}", filename)
        }
        data
    } else {
        read(filename)?
    };

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

pub(crate) async fn get_archive_with_data(
    filename: &str,
    require_download: bool,
) -> Result<(Archive, Vec<u8>)> {
    let data = if require_download {
        let data = download(filename).await?;
        if !verify(filename, &data).await? {
            anyhow::bail!("Failed to verify downloaded file: {}", filename)
        }
        data
    } else {
        read(filename)?
    };

    let resize_by = if require_download {
        100 * 1024 * 1024
    } else {
        1024 * 1024
    };

    let mut decoder = ZstdDecompressStream::new(resize_by)?;

    let mut decompressed = Vec::new();
    decoder.write(&data, &mut decompressed)?;

    let archive = Archive::new(decompressed)?;

    Ok((archive, data))
}

fn read(filename: &str) -> Result<Vec<u8>> {
    let root_path = env!("CARGO_MANIFEST_DIR");
    let file_path = std::path::Path::new(root_path)
        .join(DATA_PATH)
        .join(filename);

    let data = std::fs::read(file_path)?;
    Ok(data)
}

async fn download(filename: &str) -> Result<Vec<u8>> {
    let url = reqwest::Url::from_str(BROXUS_URL)?.join(filename)?;

    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;

    let data = response.bytes().await?;
    Ok(data.to_vec())
}

async fn verify(filename: &str, data: &Vec<u8>) -> Result<bool> {
    // Download checksum
    let filename = format!("{}.sha256", filename);
    let checksum = download(&filename).await?;

    // Read checksum
    let cursor = std::io::Cursor::new(checksum);
    let reader = std::io::BufReader::new(cursor);
    let line = reader.lines().next().expect("should exist")?;
    let expected_hash = line.split_whitespace().next().expect("hash not found");

    // Compute hash
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let hash = hasher.finalize();

    // Check hashes
    Ok(hex::encode(hash) == expected_hash)
}
