use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::Lazy;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tycho_util::FastHashMap;

use crate::state::shard_state_data::ShardStateData;
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
            left: Lazy::from_raw(left)?,
            right: Lazy::from_raw(right)?,
        })
        .map_err(From::from)
    }

    pub fn from_root(
        block_id: &BlockId,
        root: Cell,
        data_roots: FastHashMap<u8, Cell>,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;

        let shard_state_data = data_roots
            .into_iter()
            .map(|(k, cell)| ShardStateData::from_root(cell).map(|v| (k, v)))
            .collect::<Result<FastHashMap<u8, ShardStateData>>>()?;

        Self::from_state_and_root(block_id, root, shard_state, shard_state_data, tracker)
    }

    pub fn from_state_and_root(
        block_id: &BlockId,
        root: Cell,
        shard_state: Box<ShardStateUnsplit>,
        shard_state_data: FastHashMap<u8, ShardStateData>,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
        anyhow::ensure!(
            shard_state.shard_ident == block_id.shard,
            "shard state shard_ident mismatch"
        );

        anyhow::ensure!(shard_state.seqno == block_id.seqno, "state seqno mismatch");

        let handle = if block_id.seqno == 0 {
            // Insert zerostates as untracked states to prevent their cache
            // to hold back the global archives GC. This handle will still
            // point to a shared tracker, but will have not touch any ref.
            tracker.insert_untracked()
        } else {
            tracker.insert(shard_state.min_ref_mc_seqno)
        };

        Ok(Self {
            inner: Arc::new(Inner {
                block_id: *block_id,
                shard_state_extra: shard_state.load_custom()?,
                shard_state,
                shard_state_data,
                root,
                handle,
            }),
        })
    }

    pub fn deserialize_zerostate(
        zerostate_id: &BlockId,
        bytes: &[u8],
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
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

        let data_roots = FastHashMap::default(); // TODO
        Self::from_root(zerostate_id, root, data_roots, tracker)
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

    pub fn data_root_cells(&self) -> FastHashMap<u8, Cell> {
        self.inner
            .shard_state_data
            .iter()
            .map(|(k, v)| (*k, v.root_cell().clone()))
            .collect()
    }

    pub fn load_accounts(&self) -> FastHashMap<u8, ShardAccounts> {
        self.inner
            .shard_state_data
            .iter()
            .map(|(k, v)| (*k, v.accounts().clone()))
            .collect()
    }

    pub fn shards(&self) -> Result<&ShardHashes> {
        Ok(&self.state_extra()?.shards)
    }

    pub fn config_params(&self) -> Result<&BlockchainConfig> {
        Ok(&self.state_extra()?.config)
    }

    pub fn get_gen_chain_time(&self) -> u64 {
        let state = self.state();
        debug_assert!(state.gen_utime_ms < 1000);
        state.gen_utime as u64 * 1000 + state.gen_utime_ms as u64
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
    shard_state_data: FastHashMap<u8, ShardStateData>,
    shard_state_extra: Option<McStateExtra>,
    handle: RefMcStateHandle,
    root: Cell,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_ref_mc_state() {
        let state = MinRefMcStateTracker::new();

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
