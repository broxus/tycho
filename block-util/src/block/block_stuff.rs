use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use everscale_types::models::*;
use everscale_types::prelude::*;
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
    #[cfg(any(test, feature = "test"))]
    pub fn new_empty(shard: ShardIdent, seqno: u32) -> Self {
        use everscale_types::merkle::MerkleUpdate;

        let block_info = BlockInfo {
            shard,
            seqno,
            ..Default::default()
        };

        let block = Block {
            global_id: 0,
            info: Lazy::new(&BlockInfo::default()).unwrap(),
            value_flow: Lazy::new(&ValueFlow::default()).unwrap(),
            state_update: Lazy::new(&MerkleUpdate::default()).unwrap(),
            out_msg_queue_updates: None,
            extra: Lazy::new(&BlockExtra::default()).unwrap(),
        };

        let root = CellBuilder::build_from(&block).unwrap();
        let root_hash = *root.repr_hash();
        let file_hash = Boc::file_hash_blake(Boc::encode(&root));

        let block_id = BlockId {
            shard: block_info.shard,
            seqno: block_info.seqno,
            root_hash,
            file_hash,
        };

        Self::from_block_and_root(&block_id, block, root)
    }

    pub fn from_block_and_root(id: &BlockId, block: Block, root: Cell) -> Self {
        debug_assert_eq!(&id.root_hash, root.repr_hash());

        Self {
            inner: Arc::new(Inner {
                id: *id,
                block,
                root,
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
            }),
        })
    }

    pub fn root_cell(&self) -> &Cell {
        &self.inner.root
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
        let header = self.inner.block.load_info()?;
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

    pub fn load_info(&self) -> Result<BlockInfo> {
        self.inner.block.load_info().map_err(Into::into)
    }

    pub fn load_custom(&self) -> Result<McBlockExtra> {
        let Some(data) = self.inner.block.load_extra()?.load_custom()? else {
            anyhow::bail!("given block is not a master block");
        };
        Ok(data)
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
            inner: arc_swap::RefCnt::from_ptr(ptr),
        }
    }
}

#[doc(hidden)]
pub struct Inner {
    id: BlockId,
    block: Block,
    root: Cell,
}
