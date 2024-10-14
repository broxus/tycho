use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Buf;

use crate::util::{StoredValue, StoredValueBuffer};

#[derive(Debug, Copy, Clone)]
pub struct NewBlockMeta {
    pub is_key_block: bool,
    pub gen_utime: u32,
    pub mc_ref_seqno: u32,
}

impl NewBlockMeta {
    pub fn zero_state(gen_utime: u32, is_key_block: bool) -> Self {
        Self {
            is_key_block,
            gen_utime,
            mc_ref_seqno: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct BlockMeta {
    flags: AtomicU64,
    gen_utime: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct LoadedBlockMeta {
    pub flags: BlockFlags,
    pub mc_ref_seqno: u32,
    pub gen_utime: u32,
}

impl BlockMeta {
    pub fn with_data(data: NewBlockMeta) -> Self {
        const IS_KEY_BLOCK_MASK: u64 =
            (BlockFlags::IS_KEY_BLOCK.bits() as u64) << BLOCK_FLAGS_OFFSET;

        Self {
            flags: AtomicU64::new(
                if data.is_key_block {
                    IS_KEY_BLOCK_MASK
                } else {
                    0
                } | data.mc_ref_seqno as u64,
            ),
            gen_utime: data.gen_utime,
        }
    }

    pub fn load(&self) -> LoadedBlockMeta {
        let flags = self.flags.load(Ordering::Acquire);
        LoadedBlockMeta {
            flags: BlockFlags::from_bits_retain((flags >> BLOCK_FLAGS_OFFSET) as u32),
            mc_ref_seqno: flags as u32,
            gen_utime: self.gen_utime,
        }
    }

    pub fn flags(&self) -> BlockFlags {
        let flags = self.flags.load(Ordering::Acquire) >> BLOCK_FLAGS_OFFSET;
        BlockFlags::from_bits_retain(flags as u32)
    }

    pub fn mc_ref_seqno(&self) -> u32 {
        self.flags.load(Ordering::Acquire) as u32
    }

    pub fn gen_utime(&self) -> u32 {
        self.gen_utime
    }

    pub(crate) fn add_flags(&self, flags: BlockFlags) -> bool {
        let flags = (flags.bits() as u64) << BLOCK_FLAGS_OFFSET;
        self.flags.fetch_or(flags, Ordering::Release) & flags != flags
    }
}

impl StoredValue for BlockMeta {
    /// 8 bytes flags
    /// 4 bytes `gen_utime`
    const SIZE_HINT: usize = 8 + 4;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let flags = self.flags.load(Ordering::Acquire);
        buffer.write_raw_slice(&flags.to_le_bytes());
        buffer.write_raw_slice(&self.gen_utime.to_le_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Self
    where
        Self: Sized,
    {
        let flags = reader.get_u64_le();
        let gen_utime = reader.get_u32_le();

        Self {
            flags: AtomicU64::new(flags),
            gen_utime,
        }
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BlockFlags: u32 {
        // Persistent flags
        const HAS_DATA = 1 << 0;
        const HAS_PROOF = 1 << 1;
        const HAS_QUEUE_DIFF = 1 << 2;

        const HAS_STATE = 1 << 3;
        const HAS_PERSISTENT_SHARD_STATE = 1 << 4;
        const HAS_PERSISTENT_QUEUE_STATE = 1 << 5;

        const HAS_NEXT_1 = 1 << 6;
        const HAS_NEXT_2 = 1 << 7;
        const HAS_PREV_1 = 1 << 8;
        const HAS_PREV_2 = 1 << 9;
        const IS_APPLIED = 1 << 10;
        const IS_KEY_BLOCK = 1 << 11;

        const IS_REMOVED = 1 << 15;

        // Composite flags
        const HAS_ALL_BLOCK_PARTS =
            Self::HAS_DATA.bits() | Self::HAS_PROOF.bits() | Self::HAS_QUEUE_DIFF.bits();
    }
}

const BLOCK_FLAGS_OFFSET: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_store_load() {
        let meta = BlockMeta::with_data(NewBlockMeta {
            is_key_block: true,
            gen_utime: 123456789,
            mc_ref_seqno: 4311231,
        });
        assert_eq!(meta.flags(), BlockFlags::IS_KEY_BLOCK);
        assert_eq!(meta.mc_ref_seqno(), 4311231);
        assert_eq!(meta.gen_utime(), 123456789);

        let stored = meta.to_vec();
        assert_eq!(stored.len(), BlockMeta::SIZE_HINT);

        let loaded = BlockMeta::from_slice(&stored);
        assert_eq!(loaded.flags(), BlockFlags::IS_KEY_BLOCK);
        assert_eq!(loaded.mc_ref_seqno(), 4311231);
        assert_eq!(loaded.gen_utime(), 123456789);

        let updated = meta.add_flags(BlockFlags::HAS_ALL_BLOCK_PARTS);
        assert!(updated);
        assert_eq!(
            meta.flags(),
            BlockFlags::HAS_ALL_BLOCK_PARTS | BlockFlags::IS_KEY_BLOCK
        );

        meta.add_flags(BlockFlags::IS_REMOVED);
        assert_eq!(
            meta.flags(),
            BlockFlags::IS_KEY_BLOCK | BlockFlags::HAS_ALL_BLOCK_PARTS | BlockFlags::IS_REMOVED
        );
    }
}
