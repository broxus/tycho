use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;
use sha2::Digest;
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
    pub fn with_block(id: BlockId, block: Block) -> Self {
        Self {
            inner: Arc::new(Inner { id, block }),
        }
    }

    pub fn deserialize_checked(id: BlockId, data: &[u8]) -> Result<Self> {
        let file_hash = sha2::Sha256::digest(data);
        anyhow::ensure!(
            id.file_hash.as_slice() == file_hash.as_slice(),
            "file_hash mismatch for {id}"
        );

        Self::deserialize(id, data)
    }

    pub fn deserialize(id: BlockId, data: &[u8]) -> Result<Self> {
        let root = Boc::decode(data)?;
        anyhow::ensure!(
            &id.root_hash == root.repr_hash(),
            "root_hash mismatch for {id}"
        );

        let block = root.parse::<Block>()?;
        Ok(Self {
            inner: Arc::new(Inner { id, block }),
        })
    }

    pub fn with_archive_data(self, data: &[u8]) -> WithArchiveData<Self> {
        WithArchiveData::new(self, data.to_vec())
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

struct Inner {
    id: BlockId,
    block: Block,
}
