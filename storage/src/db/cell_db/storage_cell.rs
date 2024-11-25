use std::cell::UnsafeCell;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use everscale_types::cell::{CellDescriptor, CellInner, CellTreeStats, VirtualCellWrapper};
use everscale_types::prelude::*;

use super::CellDb;

pub struct StorageCell {
    cell_db: CellDb,
    descriptor: CellDescriptor,
    bit_len: u16,
    data: Vec<u8>,
    hashes: Vec<(HashBytes, u16)>,

    reference_states: [AtomicU8; 4],
    reference_data: [UnsafeCell<StorageCellReferenceData>; 4],
}

impl StorageCell {
    const REF_EMPTY: u8 = 0x0;
    const REF_RUNNING: u8 = 0x1;
    const REF_STORAGE: u8 = 0x2;
    const REF_REPLACED: u8 = 0x3;

    pub fn deserialize(cell_db: CellDb, buffer: &[u8]) -> Option<Self> {
        if buffer.len() < 4 {
            return None;
        }

        let descriptor = CellDescriptor::new([buffer[0], buffer[1]]);
        let bit_len = u16::from_le_bytes([buffer[2], buffer[3]]);
        let byte_len = descriptor.byte_len() as usize;
        let hash_count = descriptor.hash_count() as usize;
        let ref_count = descriptor.reference_count() as usize;

        let total_len = 4usize + byte_len + (32 + 2) * hash_count + 32 * ref_count;
        if buffer.len() < total_len {
            return None;
        }

        let data = buffer[4..4 + byte_len].to_vec();

        let mut hashes = Vec::with_capacity(hash_count);
        let mut offset = 4 + byte_len;
        for _ in 0..hash_count {
            hashes.push((
                HashBytes::from_slice(&buffer[offset..offset + 32]),
                u16::from_le_bytes([buffer[offset + 32], buffer[offset + 33]]),
            ));
            offset += 32 + 2;
        }

        let reference_states = Default::default();
        let reference_data = unsafe {
            MaybeUninit::<[UnsafeCell<StorageCellReferenceData>; 4]>::uninit().assume_init()
        };

        for slot in reference_data.iter().take(ref_count) {
            let slot = slot.get().cast::<u8>();
            unsafe { std::ptr::copy_nonoverlapping(buffer.as_ptr().add(offset), slot, 32) };
            offset += 32;
        }

        Some(Self {
            cell_db,
            bit_len,
            descriptor,
            data,
            hashes,
            reference_states,
            reference_data,
        })
    }

    pub fn deserialize_references(data: &[u8], target: &mut Vec<HashBytes>) -> bool {
        if data.len() < 4 {
            return false;
        }

        let descriptor = CellDescriptor::new([data[0], data[1]]);
        let hash_count = descriptor.hash_count();
        let ref_count = descriptor.reference_count() as usize;

        let mut offset = 4usize + descriptor.byte_len() as usize + (32 + 2) * hash_count as usize;
        if data.len() < offset + 32 * ref_count {
            return false;
        }

        target.reserve(ref_count);
        for _ in 0..ref_count {
            target.push(HashBytes::from_slice(&data[offset..offset + 32]));
            offset += 32;
        }

        true
    }

    pub fn serialize_to(cell: &DynCell, target: &mut Vec<u8>) {
        let descriptor = cell.descriptor();
        let hash_count = descriptor.hash_count();
        let ref_count = descriptor.reference_count();

        target.reserve(
            4usize
                + descriptor.byte_len() as usize
                + (32 + 2) * hash_count as usize
                + 32 * ref_count as usize,
        );

        target.extend_from_slice(&[descriptor.d1, descriptor.d2]);
        target.extend_from_slice(&cell.bit_len().to_le_bytes());
        target.extend_from_slice(cell.data());
        assert_eq!(cell.data().len(), descriptor.byte_len() as usize);

        for i in 0..descriptor.hash_count() {
            target.extend_from_slice(cell.hash(i).as_array());
            target.extend_from_slice(&cell.depth(i).to_le_bytes());
        }

        for i in 0..descriptor.reference_count() {
            let cell = cell.reference(i).expect("child not found");
            target.extend_from_slice(cell.repr_hash().as_array());
        }
    }

    pub fn reference_raw(&self, index: u8) -> Option<&Arc<StorageCell>> {
        if index > 3 || index >= self.descriptor.reference_count() {
            return None;
        }

        let state = &self.reference_states[index as usize];
        let slot = self.reference_data[index as usize].get();

        let current_state = state.load(Ordering::Acquire);
        if current_state == Self::REF_STORAGE {
            return Some(unsafe { &(*slot).storage_cell });
        }

        let mut res = Ok(());
        Self::initialize_inner(
            state,
            &mut || match self.cell_db.load_cell(unsafe { &(*slot).hash }) {
                Ok(cell) => unsafe {
                    *slot = StorageCellReferenceData {
                        storage_cell: ManuallyDrop::new(cell),
                    };
                    true
                },
                Err(err) => {
                    res = Err(err);
                    false
                }
            },
        );

        // TODO: just return none?
        res.unwrap();

        Some(unsafe { &(*slot).storage_cell })
    }

    fn initialize_inner(state: &AtomicU8, init: &mut impl FnMut() -> bool) {
        struct Guard<'a> {
            state: &'a AtomicU8,
            new_state: u8,
        }

        impl<'a> Drop for Guard<'a> {
            fn drop(&mut self) {
                self.state.store(self.new_state, Ordering::Release);
                unsafe {
                    let key = self.state as *const AtomicU8 as usize;
                    parking_lot_core::unpark_all(key, parking_lot_core::DEFAULT_UNPARK_TOKEN);
                }
            }
        }

        loop {
            let exchange = state.compare_exchange_weak(
                Self::REF_EMPTY,
                Self::REF_RUNNING,
                Ordering::Acquire,
                Ordering::Acquire,
            );
            match exchange {
                Ok(_) => {
                    let mut guard = Guard {
                        state,
                        new_state: Self::REF_EMPTY,
                    };
                    if init() {
                        guard.new_state = Self::REF_STORAGE;
                    }
                    return;
                }
                Err(Self::REF_STORAGE) => return,
                Err(Self::REF_RUNNING) => unsafe {
                    let key = state as *const AtomicU8 as usize;
                    parking_lot_core::park(
                        key,
                        || state.load(Ordering::Relaxed) == Self::REF_RUNNING,
                        || (),
                        |_, _| (),
                        parking_lot_core::DEFAULT_PARK_TOKEN,
                        None,
                    );
                },
                Err(Self::REF_EMPTY) => (),
                Err(_) => debug_assert!(false),
            }
        }
    }
}

impl CellImpl for StorageCell {
    #[inline]
    fn untrack(self: CellInner<Self>) -> Cell {
        Cell::from(self)
    }

    fn descriptor(&self) -> CellDescriptor {
        self.descriptor
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn bit_len(&self) -> u16 {
        self.bit_len
    }

    fn reference(&self, index: u8) -> Option<&DynCell> {
        Some(self.reference_raw(index)?.as_ref())
    }

    fn reference_cloned(&self, index: u8) -> Option<Cell> {
        Some(Cell::from(self.reference_raw(index)?.clone() as Arc<_>))
    }

    fn virtualize(&self) -> &DynCell {
        VirtualCellWrapper::wrap(self)
    }

    fn hash(&self, level: u8) -> &HashBytes {
        let i = self.descriptor.level_mask().hash_index(level);
        &self.hashes[i as usize].0
    }

    fn depth(&self, level: u8) -> u16 {
        let i = self.descriptor.level_mask().hash_index(level);
        self.hashes[i as usize].1
    }

    fn take_first_child(&mut self) -> Option<Cell> {
        let state = self.reference_states[0].swap(Self::REF_EMPTY, Ordering::AcqRel);
        let data = self.reference_data[0].get_mut();
        match state {
            Self::REF_STORAGE => Some(unsafe { data.take_storage_cell() }),
            Self::REF_REPLACED => Some(unsafe { data.take_replaced_cell() }),
            _ => None,
        }
    }

    fn replace_first_child(&mut self, parent: Cell) -> std::result::Result<Cell, Cell> {
        let state = self.reference_states[0].load(Ordering::Acquire);
        if state < Self::REF_STORAGE {
            return Err(parent);
        }

        self.reference_states[0].store(Self::REF_REPLACED, Ordering::Release);
        let data = self.reference_data[0].get_mut();

        let cell = match state {
            Self::REF_STORAGE => unsafe { data.take_storage_cell() },
            Self::REF_REPLACED => unsafe { data.take_replaced_cell() },
            _ => return Err(parent),
        };
        data.replaced_cell = ManuallyDrop::new(parent);
        Ok(cell)
    }

    fn take_next_child(&mut self) -> Option<Cell> {
        while self.descriptor.reference_count() > 1 {
            self.descriptor.d1 -= 1;
            let idx = (self.descriptor.d1 & CellDescriptor::REF_COUNT_MASK) as usize;

            let state = self.reference_states[idx].swap(Self::REF_EMPTY, Ordering::AcqRel);
            let data = self.reference_data[idx].get_mut();

            return Some(match state {
                Self::REF_STORAGE => unsafe { data.take_storage_cell() },
                Self::REF_REPLACED => unsafe { data.take_replaced_cell() },
                _ => continue,
            });
        }

        None
    }

    fn stats(&self) -> CellTreeStats {
        // TODO: make real implementation

        // STUB: just return default value
        Default::default()
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.cell_db.drop_cell(DynCell::repr_hash(self));
        for i in 0..4 {
            let state = self.reference_states[i].load(Ordering::Acquire);
            let data = self.reference_data[i].get_mut();

            unsafe {
                match state {
                    Self::REF_STORAGE => ManuallyDrop::drop(&mut data.storage_cell),
                    Self::REF_REPLACED => ManuallyDrop::drop(&mut data.replaced_cell),
                    _ => {}
                }
            }
        }
    }
}

unsafe impl Send for StorageCell {}
unsafe impl Sync for StorageCell {}

union StorageCellReferenceData {
    /// Incplmete state.
    hash: HashBytes,
    /// Complete state.
    storage_cell: ManuallyDrop<Arc<StorageCell>>,
    /// Replaced state.
    replaced_cell: ManuallyDrop<Cell>,
}

impl StorageCellReferenceData {
    unsafe fn take_storage_cell(&mut self) -> Cell {
        Cell::from(ManuallyDrop::take(&mut self.storage_cell) as Arc<_>)
    }

    unsafe fn take_replaced_cell(&mut self) -> Cell {
        ManuallyDrop::take(&mut self.replaced_cell)
    }
}
