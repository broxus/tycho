pub mod blockchain;
pub mod overlay;

mod tl_block_id {
    use everscale_types::models::{BlockId, ShardIdent};
    use everscale_types::prelude::HashBytes;
    use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

    pub const SIZE_HINT: usize = 80;

    pub const fn size_hint(_: &BlockId) -> usize {
        SIZE_HINT
    }

    pub fn write<P: TlPacket>(block_id: &BlockId, packet: &mut P) {
        block_id.shard.workchain().write_to(packet);
        block_id.shard.prefix().write_to(packet);
        block_id.seqno.write_to(packet);
        block_id.root_hash.0.write_to(packet);
        block_id.file_hash.0.write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<BlockId> {
        let workchain = i32::read_from(packet, offset)?;
        let prefix = u64::read_from(packet, offset)?;
        let seqno = u32::read_from(packet, offset)?;

        let shard = ShardIdent::new(workchain, prefix);

        let shard = match shard {
            None => return Err(tl_proto::TlError::InvalidData),
            Some(shard) => shard,
        };

        let root_hash = HashBytes(<[u8; 32]>::read_from(packet, offset)?);
        let file_hash = HashBytes(<[u8; 32]>::read_from(packet, offset)?);

        Ok(BlockId {
            shard,
            seqno,
            root_hash,
            file_hash,
        })
    }
}

mod tl_block_id_vec {
    use everscale_types::models::BlockId;
    use tl_proto::{TlError, TlPacket, TlRead, TlResult};

    use super::*;

    pub fn size_hint(ids: &[BlockId]) -> usize {
        4 + ids.len() * tl_block_id::SIZE_HINT
    }

    pub fn write<P: TlPacket>(blocks: &[BlockId], packet: &mut P) {
        packet.write_u32(blocks.len() as u32);
        for block in blocks {
            tl_block_id::write(block, packet);
        }
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Vec<BlockId>> {
        let len = u32::read_from(packet, offset)?;
        if *offset + len as usize * tl_block_id::SIZE_HINT > packet.len() {
            return Err(TlError::UnexpectedEof);
        }

        let mut ids = Vec::with_capacity(len as usize);
        for _ in 0..len {
            ids.push(tl_block_id::read(packet, offset)?);
        }
        Ok(ids)
    }
}

mod tl_big_bytes {
    use bytes::Bytes;
    use tl_proto::{TlPacket, TlRead, TlResult};

    pub const fn size_hint(bytes: &Bytes) -> usize {
        let len = bytes.len();
        4 + len + compute_padding(len)
    }

    pub fn write<P: TlPacket>(bytes: &Bytes, packet: &mut P) {
        const PADDING: [u8; 3] = [0; 3];

        let len = bytes.len();
        packet.write_u32(len as u32);
        packet.write_raw_slice(&bytes);
        if len % 4 != 0 {
            packet.write_raw_slice(&PADDING[0..4 - len % 4]);
        }
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Bytes> {
        const MAX_LEN: usize = 100 << 20; // 100 MB

        let len = u32::read_from(packet, offset)? as usize;
        if len > MAX_LEN {
            return Err(tl_proto::TlError::InvalidData);
        }
        let padding = compute_padding(len);

        if offset.saturating_add(len + padding) > packet.len() {
            return Err(tl_proto::TlError::UnexpectedEof);
        }

        let bytes = Bytes::copy_from_slice(&packet[*offset..*offset + len]);
        *offset += len + padding;

        Ok(bytes)
    }

    const fn compute_padding(len: usize) -> usize {
        (4 - len % 4) % 4
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn big_bytes() {
        // For each padding
        for i in 0..4 {
            let big_bytes = Bytes::from(vec![123; 1000 + i]);

            let mut serialized = Vec::new();
            tl_big_bytes::write(&big_bytes, &mut serialized);

            // Must be aligned by 4
            assert_eq!(serialized.len() % 4, 0);

            let mut offset = 0;
            let deserialized = tl_big_bytes::read(&serialized, &mut offset).unwrap();
            // Must be equal
            assert_eq!(big_bytes, deserialized);

            // Must consume all bytes
            assert_eq!(offset, serialized.len());
        }
    }
}
