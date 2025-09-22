use std::mem::ManuallyDrop;
use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_types::cell::Lazy;
use tycho_types::merkle::MerkleUpdate;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::FastHashSet;
use tycho_util::mem::Reclaimer;

use crate::dict::split_aug_dict_raw;
use crate::state::RefMcStateHandle;

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

    pub fn from_root(block_id: &BlockId, root: Cell, handle: RefMcStateHandle) -> Result<Self> {
        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;
        Self::from_state_and_root(block_id, shard_state, root, handle)
    }

    pub fn from_state_and_root(
        block_id: &BlockId,
        shard_state: Box<ShardStateUnsplit>,
        root: Cell,
        handle: RefMcStateHandle,
    ) -> Result<Self> {
        anyhow::ensure!(
            shard_state.shard_ident == block_id.shard,
            "shard state shard_ident mismatch"
        );

        anyhow::ensure!(shard_state.seqno == block_id.seqno, "state seqno mismatch");

        Ok(Self {
            inner: Arc::new(Inner {
                block_id: *block_id,
                parts: ManuallyDrop::new(InnerParts {
                    shard_state_extra: shard_state.load_custom()?,
                    shard_state,
                    root,
                    handle,
                }),
            }),
        })
    }

    pub fn deserialize_zerostate(
        zerostate_id: &BlockId,
        bytes: &[u8],
        handle: RefMcStateHandle,
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

        Self::from_root(zerostate_id, root, handle)
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

    pub fn get_gen_chain_time(&self) -> u64 {
        let state = self.state();
        debug_assert!(state.gen_utime_ms < 1000);
        state.gen_utime as u64 * 1000 + state.gen_utime_ms as u64
    }

    pub fn get_top_shards(&self) -> Result<Vec<ShardIdent>> {
        let mut res = vec![self.block_id().shard];

        for item in self.shards()?.latest_blocks() {
            let block_id = item?;
            res.push(block_id.shard);
        }

        Ok(res)
    }

    /// Creates a derived state which tracks access to cells data and references.
    #[must_use = "this new state must be used to track cell usage"]
    pub fn track_usage(&self, usage_mode: UsageTreeMode) -> Result<(UsageTree, Self)> {
        let usage_tree = UsageTree::new(usage_mode);
        let root = usage_tree.track(&Cell::untrack(self.inner.root.clone()));

        // NOTE: Reload parsed object from a tracked cell to fill the tracker
        // with the tree root.
        let shard_state = root.parse::<Box<ShardStateUnsplit>>()?;

        let shard_state = Self {
            inner: Arc::new(Inner {
                block_id: self.inner.block_id,
                parts: ManuallyDrop::new(InnerParts {
                    shard_state_extra: shard_state.load_custom()?,
                    shard_state,
                    root,
                    handle: self.inner.handle.clone(),
                }),
            }),
        };

        Ok((usage_tree, shard_state))
    }

    /// Applies merkle update of the specified block and preserves
    /// the `tracker` from the initial state.
    ///
    /// NOTE: Call from inside `rayon`.
    pub fn par_make_next_state(
        &self,
        next_block_id: &BlockId,
        merkle_update: &MerkleUpdate,
        split_at_depth: Option<u8>,
    ) -> Result<Self> {
        let old_split_at = if let Some(depth) = split_at_depth {
            let shard_accounts = self
                .root_cell()
                .reference_cloned(1)
                .context("invalid shard state")?
                .parse::<ShardAccounts>()
                .context("failed to load shard accounts")?;

            split_aug_dict_raw(shard_accounts, depth)
                .context("failed to split shard accounts")?
                .into_keys()
                .collect::<FastHashSet<_>>()
        } else {
            Default::default()
        };

        let new_root = merkle_update
            .par_apply(&self.inner.root, &old_split_at)
            .context("failed to apply merkle update")?;

        let shard_state = new_root.parse::<Box<ShardStateUnsplit>>()?;
        anyhow::ensure!(
            shard_state.shard_ident == next_block_id.shard,
            "shard state shard_ident mismatch"
        );
        anyhow::ensure!(
            shard_state.seqno == next_block_id.seqno,
            "state seqno mismatch"
        );

        Ok(Self {
            inner: Arc::new(Inner {
                block_id: *next_block_id,
                parts: ManuallyDrop::new(InnerParts {
                    shard_state_extra: shard_state.load_custom()?,
                    shard_state,
                    root: new_root,
                    handle: self.inner.handle.clone(),
                }),
            }),
        })
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
            inner: unsafe { arc_swap::RefCnt::from_ptr(ptr) },
        }
    }
}

#[doc(hidden)]
pub struct Inner {
    block_id: BlockId,
    parts: ManuallyDrop<InnerParts>,
}

impl std::ops::Deref for Inner {
    type Target = InnerParts;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // SAFETY: Inner is dropped only once.
        let parts = unsafe { ManuallyDrop::take(&mut self.parts) };
        Reclaimer::instance().drop(parts);
    }
}

#[doc(hidden)]
pub struct InnerParts {
    shard_state: Box<ShardStateUnsplit>,
    shard_state_extra: Option<McStateExtra>,
    root: Cell,
    // The fields of a struct are dropped in declaration order. So we need the
    // `root` field to drop BEFORE the handle.
    handle: RefMcStateHandle,
}
