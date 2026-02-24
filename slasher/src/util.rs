use std::ptr::NonNull;

use parking_lot::RwLock;
use tl_proto::{TlError, TlRead, TlResult, TlWrite};
use tycho_slasher_traits::ValidationSessionId;
use tycho_types::prelude::*;

// === AtomicValidationSessionId ===

pub struct AtomicValidationSessionId(RwLock<ValidationSessionId>);

impl AtomicValidationSessionId {
    pub fn new(value: ValidationSessionId) -> Self {
        Self(RwLock::new(value))
    }

    pub fn set(&self, value: ValidationSessionId) {
        *self.0.write() = value;
    }

    pub fn load(&self) -> ValidationSessionId {
        *self.0.read()
    }
}

// === BitSet ===

pub struct BitSet {
    data: Option<NonNull<Block>>,
    length: usize,
}

unsafe impl Send for BitSet {}
unsafe impl Sync for BitSet {}

impl BitSet {
    pub const EMPTY: Self = Self {
        data: None,
        length: 0,
    };

    pub const BLOCK_BITS: usize = std::mem::size_of::<Block>() * 8;

    pub fn with_capacity(bits: usize) -> Self {
        if bits == 0 {
            return Self::EMPTY;
        }

        let data = Vec::<Block>::into_boxed_slice(vec![0; block_count(bits)]);

        Self {
            data: Some(unsafe { NonNull::new_unchecked(Box::into_raw(data)).cast() }),
            length: bits,
        }
    }

    pub fn load_from_cs(
        bits: usize,
        cs: &mut CellSlice<'_>,
    ) -> Result<Self, tycho_types::error::Error> {
        if bits == 0 {
            return Ok(Self::EMPTY);
        }
        if bits > tycho_types::cell::MAX_BIT_LEN as usize {
            return Err(tycho_types::error::Error::CellUnderflow);
        }
        let mut buffer = [0u8; 128];
        let bytes = cs.load_raw(&mut buffer, bits as u16)?;
        debug_assert_eq!(bytes.len(), bits.div_ceil(8));

        let (chunks, tail) = bytes.as_chunks::<{ Self::BLOCK_BITS / 8 }>();

        let mut data: Vec<Block> = vec![0; block_count(bits)];
        for (data, chunk) in std::iter::zip(&mut data, chunks) {
            *data = Block::from_be_bytes(*chunk).reverse_bits();
        }
        if let Some(data) = data.last_mut()
            && !tail.is_empty()
        {
            let mut buffer = [0u8; Self::BLOCK_BITS / 8];
            buffer[0..tail.len()].copy_from_slice(tail);
            *data = Block::from_be_bytes(buffer).reverse_bits();
        }

        Ok(Self {
            data: Some(unsafe {
                NonNull::new_unchecked(Box::into_raw(data.into_boxed_slice())).cast()
            }),
            length: bits,
        })
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_zero(&self) -> bool {
        self.as_slice().iter().all(|item| *item == 0)
    }

    pub fn set(&mut self, bit: usize, enabled: bool) {
        assert!(
            bit < self.length,
            "set at index {bit} exceeds bitset size {}",
            self.length
        );

        // SAFETY: `bit` is whithin the range.
        unsafe { self.set_unchecked(bit, enabled) }
    }

    unsafe fn set_unchecked(&mut self, bit: usize, enabled: bool) {
        let Some(data) = self.data else {
            return;
        };

        let block = bit / Self::BLOCK_BITS;
        let rem = bit % Self::BLOCK_BITS;

        let block = unsafe { &mut *data.as_ptr().add(block) };
        if enabled {
            *block |= 1 << rem;
        } else {
            *block &= !(1 << rem);
        }
    }

    pub fn as_slice(&self) -> &[Block] {
        match self.data {
            Some(data) => {
                // SAFETY: Data was allocated for this exact block count.
                unsafe { std::slice::from_raw_parts(data.as_ptr(), block_count(self.length)) }
            }
            None => &[],
        }
    }
}

impl std::fmt::Debug for BitSet {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut remaining_bits = self.length;
        for block in self.as_slice() {
            let bits = std::cmp::min(remaining_bits, Self::BLOCK_BITS);
            remaining_bits -= bits;

            let block = block.reverse_bits() >> (Self::BLOCK_BITS - bits);
            write!(f, "{block:0bits$b}")?;
        }
        Ok(())
    }
}

impl Eq for BitSet {}
impl PartialEq for BitSet {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.length == other.length && self.as_slice() == other.as_slice()
    }
}

impl Drop for BitSet {
    fn drop(&mut self) {
        if let Some(data) = self.data {
            drop(unsafe {
                Box::<[Block]>::from_raw(std::ptr::slice_from_raw_parts_mut(
                    data.as_ptr(),
                    block_count(self.length),
                ))
            });
        }
    }
}

impl Store for BitSet {
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

            let block = block.reverse_bits() >> (Self::BLOCK_BITS - bits as usize);
            b.store_uint(block, bits)?;
        }

        Ok(())
    }
}

impl TlWrite for BitSet {
    type Repr = tl_proto::Bare;

    fn max_size_hint(&self) -> usize {
        4 + tl_proto::bytes_max_size_hint(std::mem::size_of_val(self.as_slice()))
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        packet.write_u32(self.length as u32);

        let bytes = match self.data {
            Some(data) => unsafe {
                std::slice::from_raw_parts(
                    data.as_ptr().cast::<u8>(),
                    block_count(self.length) * std::mem::size_of::<Block>(),
                )
            },
            None => &[],
        };
        <&[u8] as TlWrite>::write_to(&bytes, packet);
    }
}

impl<'tl> TlRead<'tl> for BitSet {
    type Repr = tl_proto::Bare;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        let length = u32::read_from(packet)? as usize;
        let bytes = <&[u8]>::read_from(packet)?;

        let block_count = block_count(length);
        let Some(expected_byte_count) = block_count.checked_mul(std::mem::size_of::<Block>())
        else {
            return Err(TlError::InvalidData);
        };
        if expected_byte_count != bytes.len() {
            return Err(TlError::InvalidData);
        }

        if block_count == 0 {
            return Ok(Self::EMPTY);
        }

        let mut data = Box::<[Block]>::new_uninit_slice(block_count);
        debug_assert_eq!(
            data.len() * std::mem::size_of::<Block>(),
            expected_byte_count
        );

        // SAFETY: `data` has the exact same number of bytes allocated.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                data.as_mut_ptr().cast::<u8>(),
                expected_byte_count,
            );
        }

        Ok(Self {
            // SAFETY: We are constructing a non-null pointer right out of the `Box<T>`.
            data: Some(unsafe { NonNull::new_unchecked(Box::into_raw(data).cast::<Block>()) }),
            length,
        })
    }
}

fn block_count(bits: usize) -> usize {
    bits.div_ceil(BitSet::BLOCK_BITS)
}

type Block = u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitset_cell_repr() {
        // Empty bitset
        let cell = CellBuilder::build_from(BitSet::EMPTY).unwrap();
        assert!(cell.is_empty());
        let parsed = BitSet::load_from_cs(0, &mut cell.as_slice().unwrap()).unwrap();
        assert_eq!(BitSet::EMPTY, parsed);

        // Not-empty bitset
        let mut bitset = BitSet::with_capacity(100);
        for i in 0..bitset.len() {
            // Some random but distinct pattern to catch alignment bugs.
            if i % 7 == 0 || (50..=90).contains(&i) {
                bitset.set(i, true);
            }
        }

        let cell = CellBuilder::build_from(&bitset).unwrap();
        assert_eq!(cell.bit_len() as usize, bitset.len());

        let parsed = BitSet::load_from_cs(100, &mut cell.as_slice().unwrap()).unwrap();
        assert_eq!(bitset, parsed);
    }

    #[test]
    fn bitset_tl_repr() {
        // Empty bitset
        let stored = tl_proto::serialize(&BitSet::EMPTY);
        let parsed = tl_proto::deserialize::<BitSet>(&stored).unwrap();
        assert_eq!(BitSet::EMPTY, parsed);

        // Not-empty bitset
        let mut bitset = BitSet::with_capacity(100);
        for i in 0..bitset.len() {
            // Some random but distinct pattern to catch alignment bugs.
            if i % 7 == 0 || (50..=90).contains(&i) {
                bitset.set(i, true);
            }
        }

        println!("{bitset:?}");

        let stored = tl_proto::serialize(&bitset);
        let parsed = tl_proto::deserialize::<BitSet>(&stored).unwrap();
        assert_eq!(bitset, parsed);
    }
}
