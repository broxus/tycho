use bytes::Buf;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use smallvec::SmallVec;

/// A trait for writing or reading data from a stack-allocated buffer
pub trait StoredValue {
    /// On-stack buffer size hint
    const SIZE_HINT: usize;

    /// On-stack buffer type (see [`smallvec::SmallVec`])
    type OnStackSlice: smallvec::Array<Item = u8>;

    /// Serializes the data to the buffer
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T);

    /// Deserializes the data from the buffer.
    ///
    /// In case of successful deserialization it is guaranteed that `reader` will be
    /// moved to the end of the deserialized data.
    ///
    /// NOTE: `reader` should not be used after this call in case of an error
    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized;

    /// Deserializes the data from the buffer.
    ///
    /// [`StoredValue::deserialize`]
    #[inline(always)]
    fn from_slice(mut data: &[u8]) -> Self
    where
        Self: Sized,
    {
        Self::deserialize(&mut data)
    }

    /// Constructs on-stack buffer with the serialized object
    fn to_vec(&self) -> SmallVec<Self::OnStackSlice> {
        let mut result = SmallVec::with_capacity(Self::SIZE_HINT);
        self.serialize(&mut result);
        result
    }
}

/// A trait for simple buffer-based serialization
pub trait StoredValueBuffer {
    fn write_raw_slice(&mut self, data: &[u8]);
}

impl StoredValueBuffer for Vec<u8> {
    #[inline(always)]
    fn write_raw_slice(&mut self, data: &[u8]) {
        self.extend_from_slice(data);
    }
}

impl<T> StoredValueBuffer for SmallVec<T>
where
    T: smallvec::Array<Item = u8>,
{
    #[inline(always)]
    fn write_raw_slice(&mut self, data: &[u8]) {
        self.extend_from_slice(data);
    }
}

impl StoredValue for BlockId {
    /// 4 bytes workchain,
    /// 8 bytes shard,
    /// 4 bytes seqno,
    /// 32 bytes root hash,
    /// 32 bytes file hash
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + 4 + 32 + 32;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard.serialize(buffer);
        buffer.write_raw_slice(&self.seqno.to_be_bytes());
        buffer.write_raw_slice(self.root_hash.as_slice());
        buffer.write_raw_slice(self.file_hash.as_slice());
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        debug_assert!(reader.remaining() >= Self::SIZE_HINT);

        let shard = ShardIdent::deserialize(reader);
        let seqno = reader.get_u32();

        let mut root_hash = HashBytes::default();
        root_hash.0.copy_from_slice(&reader[..32]);
        let mut file_hash = HashBytes::default();
        file_hash.0.copy_from_slice(&reader[32..]);

        Self {
            shard,
            seqno,
            root_hash,
            file_hash,
        }
    }
}

impl StoredValue for ShardIdent {
    /// 4 bytes workchain
    /// 8 bytes shard
    const SIZE_HINT: usize = 4 + 8;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    #[inline(always)]
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.workchain().to_be_bytes());
        buffer.write_raw_slice(&self.prefix().to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        debug_assert!(reader.remaining() >= ShardIdent::SIZE_HINT);

        let workchain = reader.get_u32() as i32;
        let prefix = reader.get_u64();
        unsafe { Self::new_unchecked(workchain, prefix) }
    }
}

impl StoredValue for BlockIdShort {
    /// 12 bytes shard ident
    /// 4 bytes seqno
    const SIZE_HINT: usize = ShardIdent::SIZE_HINT + 4;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    #[inline(always)]
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard.serialize(buffer);
        buffer.write_raw_slice(&self.seqno.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        debug_assert!(reader.remaining() >= BlockIdShort::SIZE_HINT);

        let shard = ShardIdent::deserialize(reader);
        let seqno = reader.get_u32();
        Self { shard, seqno }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_on_stack() {
        assert!(!BlockId::default().to_vec().spilled());
        assert!(!BlockId::default().to_vec().spilled());
    }
}
