use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Buf;
use everscale_types::models::BlockInfo;

use crate::util::{StoredValue, StoredValueBuffer};

#[derive(Debug, Copy, Clone)]
pub struct BlockMetaData {
    pub is_key_block: bool,
    pub gen_utime: u32,
    pub mc_ref_seqno: u32,
}

impl BlockMetaData {
    pub fn zero_state(gen_utime: u32, is_key_block: bool) -> Self {
        Self {
            is_key_block,
            gen_utime,
            mc_ref_seqno: 0,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BriefBlockInfo {
    pub is_key_block: bool,
    pub gen_utime: u32,
    pub after_split: bool,
}

impl From<&BlockInfo> for BriefBlockInfo {
    fn from(info: &BlockInfo) -> Self {
        Self {
            is_key_block: info.key_block,
            gen_utime: info.gen_utime,
            after_split: info.after_split,
        }
    }
}
#[derive(Debug, Default)]
pub struct BlockMeta {
    flags: AtomicU64,
    gen_utime: u32,
}

impl BlockMeta {
    pub fn with_data(data: BlockMetaData) -> Self {
        Self {
            flags: AtomicU64::new(
                if data.is_key_block {
                    BLOCK_META_FLAG_IS_KEY_BLOCK
                } else {
                    0
                } | data.mc_ref_seqno as u64,
            ),
            gen_utime: data.gen_utime,
        }
    }

    pub fn brief(&self) -> BriefBlockMeta {
        BriefBlockMeta {
            flags: self.flags.load(Ordering::Acquire),
            gen_utime: self.gen_utime,
        }
    }

    pub fn masterchain_ref_seqno(&self) -> u32 {
        self.flags.load(Ordering::Acquire) as u32
    }

    pub fn set_masterchain_ref_seqno(&self, seqno: u32) -> u32 {
        self.flags.fetch_or(seqno as u64, Ordering::Release) as u32
    }

    #[inline]
    pub fn gen_utime(&self) -> u32 {
        self.gen_utime
    }

    pub fn clear_data_and_proof(&self) {
        self.flags.fetch_and(CLEAR_DATA_MASK, Ordering::Release);
    }

    pub fn set_has_data(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_DATA)
    }

    pub fn has_data(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_DATA)
    }

    pub fn set_has_proof(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PROOF)
    }

    pub fn has_proof(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PROOF)
    }

    pub fn set_has_proof_link(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PROOF_LINK)
    }

    pub fn has_proof_link(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PROOF_LINK)
    }

    pub fn set_has_state(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_STATE)
    }

    pub fn has_state(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_STATE)
    }

    #[allow(unused)]
    pub fn set_has_persistent_state(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PERSISTENT_STATE)
    }

    #[allow(unused)]
    pub fn has_persistent_state(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PERSISTENT_STATE)
    }

    pub fn set_has_next1(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_NEXT_1)
    }

    pub fn has_next1(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_NEXT_1)
    }

    pub fn set_has_next2(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_NEXT_2)
    }

    pub fn has_next2(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_NEXT_2)
    }

    pub fn set_has_prev1(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PREV_1)
    }

    pub fn has_prev1(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PREV_1)
    }

    pub fn set_has_prev2(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PREV_2)
    }

    pub fn has_prev2(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PREV_2)
    }

    pub fn set_is_applied(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_IS_APPLIED)
    }

    pub fn is_applied(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_IS_APPLIED)
    }

    pub fn is_key_block(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_IS_KEY_BLOCK)
    }

    pub fn set_is_moving_to_archive(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_MOVING_TO_ARCHIVE)
    }

    pub fn set_is_archived(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_MOVED_TO_ARCHIVE)
    }

    pub fn is_archived(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_MOVED_TO_ARCHIVE)
    }

    fn test_flag(&self, flag: u64) -> bool {
        self.flags.load(Ordering::Acquire) & flag == flag
    }

    fn set_flag(&self, flag: u64) -> bool {
        self.flags.fetch_or(flag, Ordering::Release) & flag != flag
    }
}

impl StoredValue for BlockMeta {
    /// 8 bytes flags
    /// 4 bytes `gen_utime`
    const SIZE_HINT: usize = 8 + 4;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        const FLAGS_MASK: u64 = 0x0000_ffff_ffff_ffff;
        let flags = self.flags.load(Ordering::Acquire) & FLAGS_MASK;

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

#[derive(Debug, Default, Copy, Clone)]
pub struct BriefBlockMeta {
    flags: u64,
    gen_utime: u32,
}

impl BriefBlockMeta {
    #[inline]
    pub fn gen_utime(&self) -> u32 {
        self.gen_utime
    }

    #[inline]
    pub fn masterchain_ref_seqno(&self) -> u32 {
        self.flags as u32
    }

    #[inline]
    pub fn is_key_block(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_IS_KEY_BLOCK)
    }

    #[inline]
    fn test_flag(&self, flag: u64) -> bool {
        self.flags & flag == flag
    }
}

const BLOCK_META_FLAG_HAS_DATA: u64 = 1 << 32;
const BLOCK_META_FLAG_HAS_PROOF: u64 = 1 << (32 + 1);
const BLOCK_META_FLAG_HAS_PROOF_LINK: u64 = 1 << (32 + 2);
// skip flag 3 (processed by external listener)
const BLOCK_META_FLAG_HAS_STATE: u64 = 1 << (32 + 4);
const BLOCK_META_FLAG_HAS_PERSISTENT_STATE: u64 = 1 << (32 + 5);
const BLOCK_META_FLAG_HAS_NEXT_1: u64 = 1 << (32 + 6);
const BLOCK_META_FLAG_HAS_NEXT_2: u64 = 1 << (32 + 7);
const BLOCK_META_FLAG_HAS_PREV_1: u64 = 1 << (32 + 8);
const BLOCK_META_FLAG_HAS_PREV_2: u64 = 1 << (32 + 9);
const BLOCK_META_FLAG_IS_APPLIED: u64 = 1 << (32 + 10);
const BLOCK_META_FLAG_IS_KEY_BLOCK: u64 = 1 << (32 + 11);

const BLOCK_META_FLAG_MOVING_TO_ARCHIVE: u64 = 1 << (32 + 12);
const BLOCK_META_FLAG_MOVED_TO_ARCHIVE: u64 = 1 << (32 + 13);

const CLEAR_DATA_MASK: u64 =
    !(BLOCK_META_FLAG_HAS_DATA | BLOCK_META_FLAG_HAS_PROOF | BLOCK_META_FLAG_HAS_PROOF_LINK);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn fully_on_stack() {
        assert!(!BlockMeta::default().to_vec().spilled());
    }
}
