use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use tycho_types::prelude::*;

pub struct AtomicBitSet {
    data: NonNull<AtomicUsize>,
    length: usize,
}

unsafe impl Send for AtomicBitSet {}
unsafe impl Sync for AtomicBitSet {}

impl AtomicBitSet {
    pub const BLOCK_BITS: usize = std::mem::size_of::<Block>() * 8;

    pub fn with_capacity(bits: usize) -> Self {
        let data = vec![0; block_count(bits)]
            .into_iter()
            .map(Block::new)
            .collect::<Box<[Block]>>();

        Self {
            data: unsafe { NonNull::new_unchecked(Box::into_raw(data)).cast() },
            length: bits,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_zero(&self) -> bool {
        self.as_slice()
            .iter()
            .all(|item| item.load(Ordering::Acquire) == 0)
    }

    pub fn set(&self, bit: usize, enabled: bool) {
        assert!(
            bit < self.length,
            "set at index {bit} exceeds bitset size {}",
            self.length
        );

        // SAFETY: `bit` is whithin the range.
        unsafe { self.set_unchecked(bit, enabled) }
    }

    unsafe fn set_unchecked(&self, bit: usize, enabled: bool) {
        let block = bit / Self::BLOCK_BITS;
        let rem = bit % Self::BLOCK_BITS;

        let block = unsafe { &*self.data.as_ptr().add(block) };
        if enabled {
            block.fetch_or(1 << rem, Ordering::Release);
        } else {
            block.fetch_and(!(1 << rem), Ordering::Release);
        }
    }

    pub fn as_slice(&self) -> &[Block] {
        // SAFETY: Data was allocated for this exact block count.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), block_count(self.length)) }
    }
}

impl Drop for AtomicBitSet {
    fn drop(&mut self) {
        drop(unsafe {
            Box::<[Block]>::from_raw(std::ptr::slice_from_raw_parts_mut(
                self.data.as_ptr(),
                block_count(self.length),
            ))
        });
    }
}

impl Store for AtomicBitSet {
    fn store_into(
        &self,
        b: &mut CellBuilder,
        _: &dyn CellContext,
    ) -> Result<(), tycho_types::error::Error> {
        let Ok::<u16, _>(mut remaining_bits) = self.length.try_into() else {
            return Err(tycho_types::error::Error::CellOverflow);
        };

        for block in self.as_slice() {
            let bits = std::cmp::min(remaining_bits, Self::BLOCK_BITS as u16);
            remaining_bits -= bits;
            b.store_uint(block.load(Ordering::Acquire) as u64, bits)?;
        }

        Ok(())
    }
}

fn block_count(bits: usize) -> usize {
    bits.div_ceil(AtomicBitSet::BLOCK_BITS)
}

type Block = AtomicUsize;
