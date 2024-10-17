use std::sync::Arc;

use everscale_types::models::BlockId;
use tycho_block_util::block::ShardHeights;
use tycho_block_util::state::is_persistent_state;
use tycho_util::FastDashMap;

pub(crate) use self::handle::BlockDataGuard;
pub use self::handle::{BlockHandle, WeakBlockHandle};
pub use self::meta::{BlockFlags, BlockMeta, LoadedBlockMeta, NewBlockMeta};
use crate::db::*;
use crate::util::*;

mod handle;
mod meta;

type BlockHandleCache = FastDashMap<BlockId, WeakBlockHandle>;

pub struct BlockHandleStorage {
    db: BaseDb,
    cache: Arc<BlockHandleCache>,
}

impl BlockHandleStorage {
    pub fn new(db: BaseDb) -> Self {
        Self {
            db,
            cache: Arc::new(Default::default()),
        }
    }

    pub fn set_block_applied(&self, handle: &BlockHandle) -> bool {
        let updated = handle.meta().add_flags(BlockFlags::IS_APPLIED);
        if updated {
            self.store_handle(handle);
        }
        updated
    }

    pub fn set_has_persistent_shard_state(&self, handle: &BlockHandle) -> bool {
        let updated = handle
            .meta()
            .add_flags(BlockFlags::HAS_PERSISTENT_SHARD_STATE);
        if updated {
            self.store_handle(handle);
        }
        updated
    }

    pub fn set_has_persistent_queue_state(&self, handle: &BlockHandle) -> bool {
        let updated = handle
            .meta()
            .add_flags(BlockFlags::HAS_PERSISTENT_QUEUE_STATE);
        if updated {
            self.store_handle(handle);
        }
        updated
    }

    pub fn create_or_load_handle(
        &self,
        block_id: &BlockId,
        meta_data: NewBlockMeta,
    ) -> (BlockHandle, HandleCreationStatus) {
        use dashmap::mapref::entry::Entry;

        let block_handles = &self.db.block_handles;

        // Fast path - lookup in cache
        if let Some(handle) = self.cache.get(block_id) {
            if let Some(handle) = handle.upgrade() {
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
        if let Some(handle) = self.cache.get(block_id) {
            if let Some(handle) = handle.upgrade() {
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
            .rocksdb()
            .merge_cf_opt(
                &self.db.block_handles.cf(),
                id.root_hash.as_slice(),
                handle.meta().to_vec(),
                self.db.block_handles.write_config(),
            )
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

    pub fn gc_handles_cache(&self, mc_seqno: u32, shard_heights: &ShardHeights) -> usize {
        let mut total_removed = 0;

        self.cache.retain(|block_id, value| {
            let value = match value.upgrade() {
                Some(value) => value,
                None => {
                    total_removed += 1;
                    return false;
                }
            };

            let is_masterchain = block_id.is_masterchain();

            if block_id.seqno == 0
                || is_masterchain && (block_id.seqno >= mc_seqno || value.is_key_block())
                || !is_masterchain
                    && shard_heights.contains_shard_seqno(&block_id.shard, block_id.seqno)
            {
                // Keep zero state, key blocks and latest blocks
                true
            } else {
                // Remove all outdated
                total_removed += 1;
                value.meta().add_flags(BlockFlags::IS_REMOVED);
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

#[cfg(test)]
mod tests {
    use everscale_types::models::ShardIdent;

    use super::*;
    use crate::Storage;

    #[tokio::test]
    async fn merge_operator_works() -> anyhow::Result<()> {
        let (storage, _tmp_dir) = Storage::new_temp().await?;

        let block_handles = storage.block_handle_storage();

        let block_id = BlockId {
            shard: ShardIdent::BASECHAIN,
            seqno: 100,
            ..Default::default()
        };

        let meta = NewBlockMeta {
            is_key_block: false,
            gen_utime: 123,
            ref_by_mc_seqno: 456,
        };

        {
            let (handle, status) = block_handles.create_or_load_handle(&block_id, meta);
            assert_eq!(status, HandleCreationStatus::Created);

            assert_eq!(handle.mc_ref_seqno(), 456);
            assert!(!handle.is_key_block());
            assert!(!handle.is_applied());

            let updated = block_handles.set_block_applied(&handle);
            assert!(updated);
            assert!(handle.is_applied());

            // Ensure that handles are reused
            let (handle2, status) = block_handles.create_or_load_handle(&block_id, meta);
            assert_eq!(status, HandleCreationStatus::Fetched);

            assert_eq!(
                arc_swap::RefCnt::as_ptr(&handle),
                arc_swap::RefCnt::as_ptr(&handle2),
            );
        }

        // Ensure that the handle is dropped
        assert!(!block_handles.cache.contains_key(&block_id));

        // Ensure that storage is properly updated
        {
            let (handle, status) = block_handles.create_or_load_handle(&block_id, meta);
            assert_eq!(status, HandleCreationStatus::Fetched);

            assert_eq!(handle.mc_ref_seqno(), 456);
            assert!(!handle.is_key_block());
            assert!(handle.is_applied());
        }

        // Ensure that the handle is dropped
        assert!(!block_handles.cache.contains_key(&block_id));

        Ok(())
    }
}
