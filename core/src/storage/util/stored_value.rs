use tycho_types::models::{BlockId, ShardIdent};

/// Writes `BlockIdExt` in little-endian format
pub fn write_block_id_le(block_id: &BlockId) -> [u8; 80] {
    let mut bytes = [0u8; 80];
    bytes[..4].copy_from_slice(&block_id.shard.workchain().to_le_bytes());
    bytes[4..12].copy_from_slice(&block_id.shard.prefix().to_le_bytes());
    bytes[12..16].copy_from_slice(&block_id.seqno.to_le_bytes());
    bytes[16..48].copy_from_slice(block_id.root_hash.as_slice());
    bytes[48..80].copy_from_slice(block_id.file_hash.as_slice());
    bytes
}

/// Reads `BlockId` in little-endian format
pub fn read_block_id_le(data: &[u8]) -> BlockId {
    assert!(data.len() >= 80);

    let mut workchain = [0; 4];
    workchain.copy_from_slice(&data[0..4]);
    let workchain = i32::from_le_bytes(workchain);

    let mut shard = [0; 8];
    shard.copy_from_slice(&data[4..12]);
    let shard = u64::from_le_bytes(shard);

    let mut seqno = [0; 4];
    seqno.copy_from_slice(&data[12..16]);
    let seqno = u32::from_le_bytes(seqno);

    let mut root_hash = [0; 32];
    root_hash.copy_from_slice(&data[16..48]);

    let mut file_hash = [0; 32];
    file_hash.copy_from_slice(&data[48..80]);

    let shard = unsafe { ShardIdent::new_unchecked(workchain, shard) };

    BlockId {
        shard,
        seqno,
        root_hash: root_hash.into(),
        file_hash: file_hash.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_block_id_le_serialization() {
        const SERIALIZED: [u8; 80] = [
            255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 128, 123, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        ];

        let block_id = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: 123,
            root_hash: [1u8; 32].into(),
            file_hash: [2u8; 32].into(),
        };

        let serialized = write_block_id_le(&block_id);
        assert_eq!(serialized, SERIALIZED);

        assert_eq!(read_block_id_le(&serialized), block_id);
    }
}
