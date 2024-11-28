use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

pub mod hash_bytes {
    use everscale_types::cell::HashBytes;

    use super::*;

    pub const SIZE_HINT: usize = 32;

    pub const fn size_hint(_: &HashBytes) -> usize {
        SIZE_HINT
    }

    #[inline]
    pub fn write<P: TlPacket>(hash_bytes: &HashBytes, packet: &mut P) {
        packet.write_raw_slice(hash_bytes.as_ref());
    }

    #[inline]
    pub fn read(data: &mut &[u8]) -> TlResult<HashBytes> {
        <&[u8; 32]>::read_from(data).map(|bytes| HashBytes(*bytes))
    }
}

pub mod shard_ident {
    use everscale_types::models::ShardIdent;

    use super::*;

    pub const SIZE_HINT: usize = 12;

    pub const fn size_hint(_: &ShardIdent) -> usize {
        SIZE_HINT
    }

    #[inline]
    pub fn write<P: TlPacket>(shard_ident: &ShardIdent, packet: &mut P) {
        shard_ident.workchain().write_to(packet);
        shard_ident.prefix().write_to(packet);
    }

    #[inline]
    pub fn read(data: &mut &[u8]) -> TlResult<ShardIdent> {
        let workchain = i32::read_from(data)?;
        let prefix = u64::read_from(data)?;
        ShardIdent::new(workchain, prefix).ok_or(TlError::InvalidData)
    }
}

pub mod block_id {
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

    pub fn read(packet: &mut &[u8]) -> TlResult<BlockId> {
        let workchain = i32::read_from(packet)?;
        let prefix = u64::read_from(packet)?;
        let seqno = u32::read_from(packet)?;

        let shard = ShardIdent::new(workchain, prefix);

        let shard = match shard {
            None => return Err(tl_proto::TlError::InvalidData),
            Some(shard) => shard,
        };

        let root_hash = HashBytes(<[u8; 32]>::read_from(packet)?);
        let file_hash = HashBytes(<[u8; 32]>::read_from(packet)?);

        Ok(BlockId {
            shard,
            seqno,
            root_hash,
            file_hash,
        })
    }
}

pub mod block_id_vec {
    use everscale_types::models::BlockId;
    use tl_proto::{TlError, TlPacket, TlRead, TlResult};

    use super::*;

    pub fn size_hint(ids: &[BlockId]) -> usize {
        4 + ids.len() * block_id::SIZE_HINT
    }

    pub fn write<P: TlPacket>(blocks: &[BlockId], packet: &mut P) {
        packet.write_u32(blocks.len() as u32);
        for block in blocks {
            block_id::write(block, packet);
        }
    }

    pub fn read(packet: &mut &[u8]) -> TlResult<Vec<BlockId>> {
        let len = u32::read_from(packet)?;
        if packet.len() < len as usize * block_id::SIZE_HINT {
            return Err(TlError::UnexpectedEof);
        }

        let mut ids = Vec::with_capacity(len as usize);
        for _ in 0..len {
            ids.push(block_id::read(packet)?);
        }
        Ok(ids)
    }
}
