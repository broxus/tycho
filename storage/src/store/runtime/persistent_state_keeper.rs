use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapAny;
use tokio::sync::Notify;

use tycho_block_util::state::*;

use crate::models::{BlockHandle, BriefBlockMeta};
use crate::BlockHandleStorage;

pub struct PersistentStateKeeper {
    block_handle_storage: Arc<BlockHandleStorage>,
    initialized: AtomicBool,
    persistent_state_changed: Notify,
    current_persistent_state: ArcSwapAny<Option<BlockHandle>>,
    last_utime: AtomicU32,
}

impl PersistentStateKeeper {
    pub fn new(block_handle_storage: Arc<BlockHandleStorage>) -> Self {
        Self {
            block_handle_storage,
            initialized: Default::default(),
            persistent_state_changed: Default::default(),
            current_persistent_state: Default::default(),
            last_utime: Default::default(),
        }
    }

    pub fn update(&self, block_handle: &BlockHandle) -> Result<()> {
        if !self.initialized.load(Ordering::Acquire) {
            let prev_persistent_key_block = self
                .block_handle_storage
                .find_prev_persistent_key_block(block_handle.id().seqno);

            if let Some(handle) = &prev_persistent_key_block {
                self.last_utime
                    .store(handle.meta().gen_utime(), Ordering::Release);
            }
            self.current_persistent_state
                .store(prev_persistent_key_block);

            self.initialized.store(true, Ordering::Release);

            self.persistent_state_changed.notify_waiters();
        }

        if !block_handle.is_key_block() {
            return Ok(());
        }

        let block_utime = block_handle.meta().gen_utime();
        let prev_utime = self.last_utime();

        if prev_utime > block_utime {
            return Ok(());
        }

        if is_persistent_state(block_utime, prev_utime) {
            self.last_utime.store(block_utime, Ordering::Release);
            self.current_persistent_state
                .store(Some(block_handle.clone()));
            self.persistent_state_changed.notify_waiters();
        }

        Ok(())
    }

    pub fn last_utime(&self) -> u32 {
        self.last_utime.load(Ordering::Acquire)
    }

    pub fn current(&self) -> Option<BlockHandle> {
        self.current_persistent_state.load_full()
    }

    pub fn current_meta(&self) -> Option<(u32, BriefBlockMeta)> {
        self.current_persistent_state
            .load()
            .as_ref()
            .map(|handle| (handle.id().seqno, handle.meta().brief()))
    }

    pub fn new_state_found(&self) -> tokio::sync::futures::Notified<'_> {
        self.persistent_state_changed.notified()
    }
}
