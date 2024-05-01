use std::sync::Arc;

use everscale_types::models::BlockId;
use tycho_block_util::block::TopBlocks;
use tycho_block_util::state::is_persistent_state;
use tycho_util::FastDashMap;

use crate::db::*;
use crate::models::*;
use crate::util::*;

pub struct BlockHandleStorage {
    db: Arc<Db>,
    cache: Arc<FastDashMap<BlockId, WeakBlockHandle>>,
}

impl BlockHandleStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            cache: Arc::new(Default::default()),
        }
    }

    pub fn store_block_applied(&self, handle: &BlockHandle) -> bool {
        let updated = handle.meta().set_is_applied();
        if updated {
            self.store_handle(handle);
        }
        updated
    }

    pub fn create_or_load_handle(
        &self,
        block_id: &BlockId,
        meta_data: BlockMetaData,
    ) -> (BlockHandle, HandleCreationStatus) {
        use dashmap::mapref::entry::Entry;

        let block_handles = &self.db.block_handles;

        // Fast path - lookup in cache
        if let Some(weak) = self.cache.get(block_id) {
            if let Some(handle) = weak.upgrade() {
                return (handle, HandleCreationStatus::Fetched);
            }
        }

        match block_handles.get(block_id.root_hash.as_slice()).unwrap() {
            // Try to load block handle from an existing data
            Some(data) => {
                let meta = BlockMeta::from_slice(data.as_ref());

                // Fill the cache with a new handle
                let handle = self.fill_cache(block_id, meta);

                // Done
                (handle, HandleCreationStatus::Fetched)
            }
            None => {
                // Create a new handle
                let handle = BlockHandle::new(
                    block_id,
                    BlockMeta::with_data(meta_data),
                    self.cache.clone(),
                );

                // Fill the cache with the new handle
                match self.cache.entry(*block_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(handle.downgrade());
                    }
                    Entry::Occupied(mut entry) => match entry.get().upgrade() {
                        // Another thread has created the handle
                        Some(handle) => return (handle, HandleCreationStatus::Fetched),
                        None => {
                            entry.insert(handle.downgrade());
                        }
                    },
                };

                // Store the handle in the storage
                self.store_handle(&handle);

                // Done
                (handle, HandleCreationStatus::Created)
            }
        }
    }

    pub fn load_handle(&self, block_id: &BlockId) -> Option<BlockHandle> {
        let block_handles = &self.db.block_handles;

        // Fast path - lookup in cache
        if let Some(weak) = self.cache.get(block_id) {
            if let Some(handle) = weak.upgrade() {
                return Some(handle);
            }
        }

        // Load meta from storage
        let meta = match block_handles.get(block_id.root_hash.as_slice()).unwrap() {
            Some(data) => BlockMeta::from_slice(data.as_ref()),
            None => return None,
        };

        // Fill the cache with a new handle
        Some(self.fill_cache(block_id, meta))
    }

    pub fn store_handle(&self, handle: &BlockHandle) {
        let id = handle.id();

        self.db
            .block_handles
            .insert(id.root_hash.as_slice(), handle.meta().to_vec())
            .unwrap();

        if handle.is_key_block() {
            self.db
                .key_blocks
                .insert(id.seqno.to_be_bytes(), id.to_vec())
                .unwrap();
        }
    }

    pub fn load_key_block_handle(&self, seqno: u32) -> Option<BlockHandle> {
        let key_blocks = &self.db.key_blocks;
        let key_block_id = match key_blocks.get(seqno.to_be_bytes()).unwrap() {
            Some(data) => BlockId::from_slice(data.as_ref()),
            None => return None,
        };
        self.load_handle(&key_block_id)
    }

    pub fn find_last_key_block(&self) -> Option<BlockHandle> {
        let mut iter = self.db.key_blocks.raw_iterator();
        iter.seek_to_last();

        // Load key block from the current iterator value
        let key_block_id = BlockId::from_slice(iter.value()?);
        self.load_handle(&key_block_id)
    }

    pub fn find_prev_key_block(&self, seqno: u32) -> Option<BlockHandle> {
        if seqno == 0 {
            return None;
        }

        // Create iterator and move it to the previous key block before the specified
        let mut iter = self.db.key_blocks.raw_iterator();
        iter.seek_for_prev((seqno - 1u32).to_be_bytes());

        // Load key block from current iterator value
        let key_block_id = BlockId::from_slice(iter.value()?);
        self.load_handle(&key_block_id)
    }

    pub fn find_prev_persistent_key_block(&self, seqno: u32) -> Option<BlockHandle> {
        if seqno == 0 {
            return None;
        }

        // Create iterator and move it to the previous key block before the specified
        let mut iter = self.db.key_blocks.raw_iterator();
        iter.seek_for_prev((seqno - 1u32).to_be_bytes());

        // Loads key block from current iterator value and moves it backward
        let mut get_key_block = move || -> Option<BlockHandle> {
            // Load key block id
            let key_block_id = BlockId::from_slice(iter.value()?);

            // Load block handle for this id
            let handle = self.load_handle(&key_block_id)?;

            // Move iterator backward
            iter.prev();

            // Done
            Some(handle)
        };

        // Load previous key block
        let mut key_block = get_key_block()?;

        // Load previous key blocks and check if the `key_block` is for persistent state
        while let Some(prev_key_block) = get_key_block() {
            if is_persistent_state(
                key_block.meta().gen_utime(),
                prev_key_block.meta().gen_utime(),
            ) {
                // Found
                return Some(key_block);
            }
            key_block = prev_key_block;
        }

        // Not found
        None
    }

    pub fn key_blocks_iterator(
        &self,
        direction: KeyBlocksDirection,
    ) -> impl Iterator<Item = BlockId> + '_ {
        let mut raw_iterator = self.db.key_blocks.raw_iterator();
        let reverse = match direction {
            KeyBlocksDirection::ForwardFrom(seqno) => {
                raw_iterator.seek(seqno.to_be_bytes());
                false
            }
            KeyBlocksDirection::Backward => {
                raw_iterator.seek_to_last();
                true
            }
        };

        KeyBlocksIterator {
            raw_iterator,
            reverse,
        }
    }

    pub fn gc_handles_cache(&self, top_blocks: &TopBlocks) -> usize {
        let mut total_removed = 0;

        self.cache.retain(|block_id, value| {
            let value = match value.upgrade() {
                Some(value) => value,
                None => {
                    total_removed += 1;
                    return false;
                }
            };

            if block_id.seqno == 0
                || block_id.is_masterchain() && value.is_key_block()
                || top_blocks.contains(block_id)
            {
                // Keep zero state, key blocks and latest blocks
                true
            } else {
                // Remove all outdated
                total_removed += 1;
                value.meta().clear_data_and_proof();
                false
            }
        });

        total_removed
    }

    fn fill_cache(&self, block_id: &BlockId, meta: BlockMeta) -> BlockHandle {
        use dashmap::mapref::entry::Entry;

        match self.cache.entry(*block_id) {
            Entry::Vacant(entry) => {
                let handle = BlockHandle::new(block_id, meta, self.cache.clone());
                entry.insert(handle.downgrade());
                handle
            }
            Entry::Occupied(mut entry) => match entry.get().upgrade() {
                Some(handle) => handle,
                None => {
                    let handle = BlockHandle::new(block_id, meta, self.cache.clone());
                    entry.insert(handle.downgrade());
                    handle
                }
            },
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HandleCreationStatus {
    Created,
    Fetched,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum KeyBlocksDirection {
    ForwardFrom(u32),
    Backward,
}

struct KeyBlocksIterator<'a> {
    raw_iterator: weedb::rocksdb::DBRawIterator<'a>,
    reverse: bool,
}

impl Iterator for KeyBlocksIterator<'_> {
    type Item = BlockId;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.raw_iterator.value().map(BlockId::from_slice)?;
        if self.reverse {
            self.raw_iterator.prev();
        } else {
            self.raw_iterator.next();
        }
        Some(value)
    }
}
