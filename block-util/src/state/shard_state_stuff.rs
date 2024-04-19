use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;
use sha2::Digest;

use crate::state::{MinRefMcStateTracker, RefMcStateHandle};

/// Parsed shard state.
#[derive(Clone)]
pub struct ShardStateStuff {
    block_id: BlockId,
    shard_state: ShardStateUnsplit,
    shard_state_extra: Option<McStateExtra>,
    handle: RefMcStateHandle,
    root: Cell,
}

impl ShardStateStuff {
    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        CellBuilder::build_from(ShardStateSplit {
            left: Lazy::from_raw(left),
            right: Lazy::from_raw(right),
        })
        .map_err(From::from)
    }

    pub fn new(block_id: BlockId, root: Cell, tracker: &MinRefMcStateTracker) -> Result<Self> {
        let shard_state = root.parse::<ShardStateUnsplit>()?;

        Self::from_state_and_root(block_id, shard_state, root, tracker)
    }

    pub fn from_state_and_root(
        block_id: BlockId,
        shard_state: ShardStateUnsplit,
        root: Cell,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Self> {
        anyhow::ensure!(
            shard_state.shard_ident == block_id.shard,
            "shard state shard_ident mismatch"
        );

        anyhow::ensure!(
            shard_state.seqno == block_id.seqno,
            "state state seqno mismatch"
        );

        let handle = tracker.insert(shard_state.min_ref_mc_seqno);
        Ok(Self {
            block_id,
            shard_state_extra: shard_state.load_custom()?,
            shard_state,
            root,
            handle,
        })
    }

    pub fn deserialize_zerostate(id: BlockId, bytes: &[u8]) -> Result<Self> {
        anyhow::ensure!(id.seqno == 0, "given id has a non-zero seqno");

        let file_hash = sha2::Sha256::digest(bytes);
        anyhow::ensure!(
            id.file_hash.as_slice() == file_hash.as_slice(),
            "file_hash mismatch. Expected: {}, got: {}",
            hex::encode(file_hash),
            id.file_hash,
        );

        let root = Boc::decode(bytes)?;
        anyhow::ensure!(
            &id.root_hash == root.repr_hash(),
            "root_hash mismatch for {id}. Expected: {expected}, got: {got}",
            id = id,
            expected = id.root_hash,
            got = root.repr_hash(),
        );

        Self::new(
            id,
            root,
            ZEROSTATE_REFS.get_or_init(MinRefMcStateTracker::new),
        )
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn state(&self) -> &ShardStateUnsplit {
        &self.shard_state
    }

    pub fn state_extra(&self) -> Result<&McStateExtra> {
        let Some(extra) = self.shard_state_extra.as_ref() else {
            anyhow::bail!("given state is not a masterchain state");
        };
        Ok(extra)
    }

    pub fn ref_mc_state_handle(&self) -> &RefMcStateHandle {
        &self.handle
    }

    pub fn root_cell(&self) -> &Cell {
        &self.root
    }

    pub fn shards(&self) -> Result<&ShardHashes> {
        Ok(&self.state_extra()?.shards)
    }

    pub fn config_params(&self) -> Result<&BlockchainConfig> {
        Ok(&self.state_extra()?.config)
    }
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
