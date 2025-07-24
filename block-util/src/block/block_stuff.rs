use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::FastHashMap;

use crate::archive::WithArchiveData;

pub type BlockStuffAug = WithArchiveData<BlockStuff>;

/// Deserialized block.
#[derive(Clone)]
#[repr(transparent)]
pub struct BlockStuff {
    inner: Arc<Inner>,
}

impl BlockStuff {
    /// Time until the block is considered "trusted". We use it to force
    /// all new nodes to download at least this amount of history.
    pub const BOOT_OFFSET: Duration = Duration::from_secs(12 * 3600);

    pub fn compute_is_persistent(block_utime: u32, prev_utime: u32) -> bool {
        let hack_enabled = cfg!(tycho_unstable)
            && std::env::var("HACK_EACH_KEY_BLOCK_IS_PERSISTENT").unwrap_or_default() == "1";

        if hack_enabled {
            true
        } else {
            block_utime >> 17 != prev_utime >> 17
        }
    }

    pub fn can_use_for_boot(block_utime: u32, now_utime: u32) -> bool {
        now_utime.saturating_sub(block_utime) as u64 >= Self::BOOT_OFFSET.as_secs()
    }

    pub fn time_until_can_use_for_boot(block_utime: u32, now_utime: u32) -> Duration {
        let time_since_collated = Duration::from_secs(now_utime.saturating_sub(block_utime) as _);
        Self::BOOT_OFFSET.saturating_sub(time_since_collated)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn new_empty(shard: ShardIdent, seqno: u32) -> Self {
        Self::new_with(shard, seqno, |_| {})
    }

    #[cfg(any(test, feature = "test"))]
    pub fn new_with<F>(shard: ShardIdent, seqno: u32, modify: F) -> Self
    where
        F: FnOnce(&mut Block),
    {
        use tycho_types::cell::Lazy;
        use tycho_types::merkle::MerkleUpdate;

        const DATA_SIZE: usize = 1024; // ~1 KB of data for an empty block.

        let block_info = BlockInfo {
            shard,
            seqno,
            ..Default::default()
        };

        let mut block = Block {
            global_id: 0,
            info: Lazy::new(&block_info).unwrap(),
            value_flow: Lazy::new(&ValueFlow::default()).unwrap(),
            state_update: Lazy::new(&MerkleUpdate::default()).unwrap(),
            out_msg_queue_updates: OutMsgQueueUpdates {
                diff_hash: Default::default(),
                tail_len: 0,
            },
            extra: Lazy::new(&BlockExtra::default()).unwrap(),
        };

        // Apply modifications
        modify(&mut block);

        let root = CellBuilder::build_from(&block).unwrap();
        let root_hash = *root.repr_hash();
        let file_hash = Boc::file_hash_blake(Boc::encode(&root));

        let block_id = BlockId {
            shard: block_info.shard,
            seqno: block_info.seqno,
            root_hash,
            file_hash,
        };

        Self::from_block_and_root(&block_id, block, root, DATA_SIZE)
    }

    pub fn from_block_and_root(id: &BlockId, block: Block, root: Cell, data_size: usize) -> Self {
        debug_assert_eq!(&id.root_hash, root.repr_hash());

        Self {
            inner: Arc::new(Inner {
                id: *id,
                block,
                root,
                block_info: Default::default(),
                block_extra: Default::default(),
                block_mc_extra: Default::default(),
                data_size,
            }),
        }
    }

    pub fn deserialize_checked(id: &BlockId, data: &[u8]) -> Result<Self> {
        let file_hash = Boc::file_hash_blake(data);
        anyhow::ensure!(
            id.file_hash.as_slice() == file_hash.as_slice(),
            "file_hash mismatch for {id}"
        );

        Self::deserialize(id, data)
    }

    pub fn deserialize(id: &BlockId, data: &[u8]) -> Result<Self> {
        let root = Boc::decode(data)?;
        anyhow::ensure!(
            &id.root_hash == root.repr_hash(),
            "root_hash mismatch for {id}"
        );

        let block = root.parse::<Block>()?;
        Ok(Self {
            inner: Arc::new(Inner {
                id: *id,
                block,
                root,
                block_info: Default::default(),
                block_extra: Default::default(),
                block_mc_extra: Default::default(),
                data_size: data.len(),
            }),
        })
    }

    pub fn root_cell(&self) -> &Cell {
        &self.inner.root
    }

    pub fn data_size(&self) -> usize {
        self.inner.data_size
    }

    pub fn with_archive_data<A>(self, data: A) -> WithArchiveData<Self>
    where
        Bytes: From<A>,
    {
        WithArchiveData::new(self, data)
    }

    pub fn id(&self) -> &BlockId {
        &self.inner.id
    }

    pub fn block(&self) -> &Block {
        &self.inner.block
    }

    pub fn into_block(self) -> Block {
        self.inner.block.clone()
    }

    pub fn construct_prev_id(&self) -> Result<(BlockId, Option<BlockId>)> {
        let header = self.load_info()?;
        match header.load_prev_ref()? {
            PrevBlockRef::Single(prev) => {
                let shard = if header.after_split {
                    let Some(shard) = header.shard.merge() else {
                        anyhow::bail!("failed to merge shard");
                    };
                    shard
                } else {
                    header.shard
                };

                let id = BlockId {
                    shard,
                    seqno: prev.seqno,
                    root_hash: prev.root_hash,
                    file_hash: prev.file_hash,
                };

                Ok((id, None))
            }
            PrevBlockRef::AfterMerge { left, right } => {
                let Some((left_shard, right_shard)) = header.shard.split() else {
                    anyhow::bail!("failed to split shard");
                };

                let id1 = BlockId {
                    shard: left_shard,
                    seqno: left.seqno,
                    root_hash: left.root_hash,
                    file_hash: left.file_hash,
                };

                let id2 = BlockId {
                    shard: right_shard,
                    seqno: right.seqno,
                    root_hash: right.root_hash,
                    file_hash: right.file_hash,
                };

                Ok((id1, Some(id2)))
            }
        }
    }

    pub fn load_info(&self) -> Result<&BlockInfo, tycho_types::error::Error> {
        #[expect(
            clippy::disallowed_methods,
            reason = "We are implementing that load_info getter here"
        )]
        self.inner
            .block_info
            .get_or_init(|| self.inner.block.load_info())
            .as_ref()
            .map_err(|e| e.clone())
    }

    pub fn load_extra(&self) -> Result<&BlockExtra, tycho_types::error::Error> {
        #[expect(
            clippy::disallowed_methods,
            reason = "We are implementing that load_extra getter here"
        )]
        self.inner
            .block_extra
            .get_or_init(|| self.inner.block.load_extra())
            .as_ref()
            .map_err(|e| e.clone())
    }

    pub fn load_custom(&self) -> Result<&McBlockExtra, tycho_types::error::Error> {
        let extra = self.load_extra()?;

        #[expect(
            clippy::disallowed_methods,
            reason = "We are implementing that load_custom getter here"
        )]
        self.inner
            .block_mc_extra
            .get_or_init(|| {
                extra
                    .load_custom()
                    .and_then(|c| c.ok_or(tycho_types::error::Error::InvalidData))
            })
            .as_ref()
            .map_err(|e| e.clone())
    }

    pub fn shard_blocks(&self) -> Result<FastHashMap<ShardIdent, BlockId>> {
        self.load_custom()?
            .shards
            .latest_blocks()
            .map(|id| id.map(|id| (id.shard, id)).map_err(From::from))
            .collect()
    }

    pub fn shard_blocks_seqno(&self) -> Result<FastHashMap<ShardIdent, u32>> {
        self.load_custom()?
            .shards
            .latest_blocks()
            .map(|id| id.map(|id| (id.shard, id.seqno)).map_err(From::from))
            .collect()
    }
}

impl AsRef<Block> for BlockStuff {
    #[inline]
    fn as_ref(&self) -> &Block {
        &self.inner.block
    }
}

unsafe impl arc_swap::RefCnt for BlockStuff {
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
    id: BlockId,
    block: Block,
    root: Cell,
    block_info: OnceLock<Result<BlockInfo, tycho_types::error::Error>>,
    block_extra: OnceLock<Result<BlockExtra, tycho_types::error::Error>>,
    block_mc_extra: OnceLock<Result<McBlockExtra, tycho_types::error::Error>>,
    data_size: usize,
}
