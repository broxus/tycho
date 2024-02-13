use everscale_types::cell::{CellType, LevelMask};

pub struct EntriesBuffer(Box<[[u8; HashesEntry::LEN]; 5]>);

impl EntriesBuffer {
    pub fn new() -> Self {
        Self(Box::new([[0; HashesEntry::LEN]; 5]))
    }

    pub fn current_entry_buffer(&mut self) -> &mut [u8; HashesEntry::LEN] {
        &mut self.0[0]
    }

    pub fn iter_child_buffers(
        &mut self,
    ) -> impl Iterator<Item = &mut [u8; HashesEntry::<'static>::LEN]> {
        self.0.iter_mut().skip(1)
    }

    pub fn split_children<'a, 'b>(
        &'a mut self,
        references: &'b [u32],
    ) -> (HashesEntryWriter<'a>, EntriesBufferChildren<'b>)
        where
            'a: 'b,
    {
        let [first, tail @ ..] = &mut *self.0;
        (
            HashesEntryWriter(first),
            EntriesBufferChildren(references, tail),
        )
    }

    pub fn repr_hash(&self) -> &[u8; 32] {
        let [first, ..] = &*self.0;
        HashesEntry(first).hash(3)
    }
}

pub struct EntriesBufferChildren<'a>(&'a [u32], &'a [[u8; HashesEntry::LEN]]);

impl EntriesBufferChildren<'_> {
    pub fn iter(&self) -> impl Iterator<Item = (&u32, HashesEntry)> {
        self.0
            .iter()
            .zip(self.1)
            .map(|(index, item)| (index, HashesEntry(item)))
    }
}

pub struct HashesEntryWriter<'a>(&'a mut [u8; HashesEntry::LEN]);

impl HashesEntryWriter<'_> {
    pub fn as_reader(&self) -> HashesEntry {
        HashesEntry(self.0)
    }

    pub fn clear(&mut self) {
        for byte in &mut *self.0 {
            *byte = 0;
        }
    }

    pub fn set_level_mask(&mut self, level_mask: LevelMask) {
        self.0[0] = level_mask.into();
    }

    pub fn set_cell_type(&mut self, cell_type: CellType) {
        self.0[1] = cell_type.into();
    }

    pub fn set_tree_bits_count(&mut self, count: u64) {
        self.0[4..12].copy_from_slice(&count.to_le_bytes());
    }

    pub fn set_tree_cell_count(&mut self, count: u64) {
        self.0[12..20].copy_from_slice(&count.to_le_bytes());
    }

    pub fn get_tree_counters(&mut self) -> &[u8] {
        &self.0[4..20]
    }

    pub fn set_hash(&mut self, i: u8, hash: &[u8]) {
        self.get_hash_slice(i).copy_from_slice(hash);
    }

    pub fn get_hash_slice(&mut self, i: u8) -> &mut [u8; 32] {
        let offset = HashesEntry::HASHES_OFFSET + 32 * i as usize;
        unsafe { &mut *(self.0.as_mut_ptr().add(offset) as *mut _) }
    }

    pub fn set_depth(&mut self, i: u8, depth: u16) {
        self.get_depth_slice(i)
            .copy_from_slice(&depth.to_le_bytes());
    }

    pub fn get_depth_slice(&mut self, i: u8) -> &mut [u8; 2] {
        let offset = HashesEntry::DEPTHS_OFFSET + 2 * i as usize;
        unsafe { &mut *(self.0.as_mut_ptr().add(offset) as *mut _) }
    }
}

pub struct HashesEntry<'a>(&'a [u8; HashesEntry::LEN]);

impl<'a> HashesEntry<'a> {
    // 4 bytes - info (1 byte level mask, 1 byte cell type, 2 bytes padding)
    // 8 bytes - tree bits count
    // 8 bytes - cell count
    // 32 * 4 bytes - hashes
    // 2 * 4 bytes - depths
    pub const LEN: usize = 4 + 8 + 8 + 32 * 4 + 2 * 4;
    pub const HASHES_OFFSET: usize = 4 + 8 + 8;
    pub const DEPTHS_OFFSET: usize = 4 + 8 + 8 + 32 * 4;

    pub fn level_mask(&self) -> LevelMask {
        // SAFETY: loaded from `set_level_mask`
        unsafe { LevelMask::new_unchecked(self.0[0]) }
    }

    pub fn cell_type(&self) -> CellType {
        match self.0[1] {
            1 => CellType::PrunedBranch,
            2 => CellType::LibraryReference,
            3 => CellType::MerkleProof,
            4 => CellType::MerkleUpdate,
            _ => CellType::Ordinary,
        }
    }

    pub fn tree_bits_count(&self) -> u64 {
        u64::from_le_bytes(self.0[4..12].try_into().unwrap())
    }

    pub fn tree_cell_count(&self) -> u64 {
        u64::from_le_bytes(self.0[12..20].try_into().unwrap())
    }

    pub fn hash(&self, n: u8) -> &'a [u8; 32] {
        let offset = Self::HASHES_OFFSET + 32 * self.level_mask().hash_index(n) as usize;
        unsafe { &*(self.0.as_ptr().add(offset) as *const _) }
    }

    pub fn depth(&self, n: u8) -> u16 {
        let offset = Self::DEPTHS_OFFSET + 2 * self.level_mask().hash_index(n) as usize;
        u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
    }

    pub fn pruned_branch_hash<'b>(&self, n: u8, data: &'b [u8]) -> Option<&'b [u8; 32]>
        where
            'a: 'b,
    {
        let level_mask = self.level_mask();
        let index = level_mask.hash_index(n) as usize;
        let level = level_mask.level() as usize;

        Some(if index == level {
            let offset = Self::HASHES_OFFSET;
            unsafe { &*(self.0.as_ptr().add(offset) as *const _) }
        } else {
            let offset = 1 + 1 + index * 32;
            if data.len() < offset + 32 {
                return None;
            }
            unsafe { &*(data.as_ptr().add(offset) as *const _) }
        })
    }

    pub fn pruned_branch_depth(&self, n: u8, data: &[u8]) -> u16 {
        let level_mask = self.level_mask();
        let index = level_mask.hash_index(n) as usize;
        let level = level_mask.level() as usize;

        if index == level {
            let offset = Self::DEPTHS_OFFSET;
            u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
        } else {
            let offset = 1 + 1 + level * 32 + index * 2;
            u16::from_be_bytes([data[offset], data[offset + 1]])
        }
    }
}