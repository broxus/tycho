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
    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<HashBytes> {
        <&[u8; 32]>::read_from(data, offset).map(|bytes| HashBytes::from(*bytes))
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
    pub fn read(data: &[u8], offset: &mut usize) -> TlResult<ShardIdent> {
        let workchain = i32::read_from(data, offset)?;
        let prefix = u64::read_from(data, offset)?;
        ShardIdent::new(workchain, prefix).ok_or(TlError::InvalidData)
    }
}
