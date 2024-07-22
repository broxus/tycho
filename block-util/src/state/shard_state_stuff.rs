use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::state::{MinRefMcStateTracker, RefMcStateHandle};

/// Parsed shard state.
#[derive(Clone)]
#[repr(transparent)]
pub struct ShardStateStuff {
    inner: Arc<Inner>,
}

impl ShardStateStuff {
    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        CellBuilder::build_from(ShardStateSplit {
            left: Lazy::from_raw(left),
            right: Lazy::from_raw(right),
        })
        .map_err(From::from)
    }

    pub fn from_root(
        block_id: &BlockId,
        root: Cell,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
        Self::from_state_and_root(block_id, shard_state, root, tracker)
    }

    pub fn from_state(
        block_id: &BlockId,
        shard_state: Box<ShardStateUnsplit>,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
        let root = CellBuilder::build_from(&shard_state)?;
        ShardStateStuff::from_state_and_root(block_id, shard_state, root, tracker)
    }

    pub fn from_state_and_root(
        block_id: &BlockId,
        shard_state: Box<ShardStateUnsplit>,
        root: Cell,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
        anyhow::ensure!(
            shard_state.shard_ident == block_id.shard,
            "shard state shard_ident mismatch"
        );

        anyhow::ensure!(shard_state.seqno == block_id.seqno, "state seqno mismatch");

        let handle = tracker.insert(shard_state.min_ref_mc_seqno);
        Ok(Self {
            inner: Arc::new(Inner {
                block_id: *block_id,
                shard_state_extra: shard_state.load_custom()?,
                shard_state,
                root,
                handle,
            }),
        })
    }

    pub fn deserialize_zerostate(zerostate_id: &BlockId, bytes: &[u8]) -> Result<Self> {
        anyhow::ensure!(zerostate_id.seqno == 0, "given id has a non-zero seqno");

        let file_hash = Boc::file_hash_blake(bytes);
        anyhow::ensure!(
            zerostate_id.file_hash.as_slice() == file_hash.as_slice(),
            "file_hash mismatch. Expected: {}, got: {}",
            hex::encode(file_hash),
            zerostate_id.file_hash,
        );

        let root = Boc::decode(bytes)?;
        anyhow::ensure!(
            &zerostate_id.root_hash == root.repr_hash(),
            "root_hash mismatch for {zerostate_id}. Expected: {expected}, got: {got}",
            expected = zerostate_id.root_hash,
            got = root.repr_hash(),
        );

        Self::from_root(
            zerostate_id,
            root,
            ZEROSTATE_REFS.get_or_init(MinRefMcStateTracker::new),
        )
    }

    pub fn block_id(&self) -> &BlockId {
        &self.inner.block_id
    }

    pub fn state(&self) -> &ShardStateUnsplit {
        &self.inner.shard_state
    }

    pub fn state_extra(&self) -> Result<&McStateExtra> {
        let Some(extra) = self.inner.shard_state_extra.as_ref() else {
            anyhow::bail!("given state is not a masterchain state");
        };
        Ok(extra)
    }

    pub fn ref_mc_state_handle(&self) -> &RefMcStateHandle {
        &self.inner.handle
    }

    pub fn root_cell(&self) -> &Cell {
        &self.inner.root
    }

    pub fn shards(&self) -> Result<&ShardHashes> {
        Ok(&self.state_extra()?.shards)
    }

    pub fn config_params(&self) -> Result<&BlockchainConfig> {
        Ok(&self.state_extra()?.config)
    }
}

impl AsRef<ShardStateUnsplit> for ShardStateStuff {
    #[inline]
    fn as_ref(&self) -> &ShardStateUnsplit {
        &self.inner.shard_state
    }
}

unsafe impl arc_swap::RefCnt for ShardStateStuff {
    type Base = Inner;

    fn into_ptr(me: Self) -> *mut Self::Base {
        arc_swap::RefCnt::into_ptr(me.inner)
    }

    fn as_ptr(me: &Self) -> *mut Self::Base {
        arc_swap::RefCnt::as_ptr(&me.inner)
    }

    unsafe fn from_ptr(ptr: *const Self::Base) -> Self {
        Self {
            inner: arc_swap::RefCnt::from_ptr(ptr),
        }
    }
}

#[doc(hidden)]
pub struct Inner {
    block_id: BlockId,
    shard_state: Box<ShardStateUnsplit>,
    shard_state_extra: Option<McStateExtra>,
    handle: RefMcStateHandle,
    root: Cell,
}

pub fn is_persistent_state(block_utime: u32, prev_utime: u32) -> bool {
    block_utime >> 17 != prev_utime >> 17
}

static ZEROSTATE_REFS: std::sync::OnceLock<MinRefMcStateTracker> = std::sync::OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_ref_mc_state() {
        let state = MinRefMcStateTracker::default();

        {
            let _handle = state.insert(10);
            assert_eq!(state.seqno(), Some(10));
        }
        assert_eq!(state.seqno(), None);

        {
            let handle1 = state.insert(10);
            assert_eq!(state.seqno(), Some(10));
            let _handle2 = state.insert(15);
            assert_eq!(state.seqno(), Some(10));
            let handle3 = state.insert(10);
            assert_eq!(state.seqno(), Some(10));
            drop(handle3);
            assert_eq!(state.seqno(), Some(10));
            drop(handle1);
            assert_eq!(state.seqno(), Some(15));
        }
        assert_eq!(state.seqno(), None);
    }
}
