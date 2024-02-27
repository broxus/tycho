use anyhow::Result;
use everscale_types::models::*;
use tycho_util::FastHashMap;

use crate::block::BlockStuff;

/// Stores last blocks for each workchain and shard.
#[derive(Debug, Clone)]
pub struct TopBlocks {
    pub mc_block: BlockIdShort,
    pub shard_heights: FastHashMap<ShardIdent, u32>,
}

impl TopBlocks {
    /// Constructs this structure for the zerostate.
    pub fn zerostate() -> Self {
        Self {
            mc_block: BlockIdShort::from((ShardIdent::MASTERCHAIN, 0)),
            shard_heights: FastHashMap::from_iter([(ShardIdent::BASECHAIN, 0u32)]),
        }
    }

    /// Extracts last blocks for each workchain and shard from the given masterchain block.
    pub fn from_mc_block(mc_block_data: &BlockStuff) -> Result<Self> {
        let block_id = mc_block_data.id();
        debug_assert!(block_id.shard.is_masterchain());

        Ok(Self {
            mc_block: block_id.as_short_id(),
            shard_heights: mc_block_data.shard_blocks_seqno()?,
        })
    }

    /// Masterchain block seqno
    pub fn mc_seqno(&self) -> u32 {
        self.mc_block.seqno
    }

    /// Returns block count (including masterchain).
    pub fn count(&self) -> usize {
        1 + self.shard_heights.len()
    }

    /// Checks whether the given block is equal to or greater than
    /// the last block for the given shard.
    pub fn contains(&self, block_id: &BlockId) -> bool {
        self.contains_shard_seqno(&block_id.shard, block_id.seqno)
    }

    /// Checks whether the given pair of [`ton_block::ShardIdent`] and seqno
    /// is equal to or greater than the last block for the given shard.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn contains_shard_seqno(&self, shard_ident: &ShardIdent, seqno: u32) -> bool {
        if shard_ident.is_masterchain() {
            seqno >= self.mc_block.seqno
        } else {
            match self.shard_heights.get(shard_ident) {
                Some(&top_seqno) => seqno >= top_seqno,
                None => self
                    .shard_heights
                    .iter()
                    .find(|&(shard, _)| shard_ident.intersects(shard))
                    .map(|(_, &top_seqno)| seqno >= top_seqno)
                    .unwrap_or_default(),
            }
        }
    }

    /// Returns an iterator over the short ids of the latest blocks.
    pub fn short_ids(&self) -> TopBlocksShortIdsIter<'_> {
        TopBlocksShortIdsIter {
            top_blocks: self,
            shards_iter: None,
        }
    }
}

/// Iterator over the short ids of the latest blocks.
pub struct TopBlocksShortIdsIter<'a> {
    top_blocks: &'a TopBlocks,
    shards_iter: Option<std::collections::hash_map::Iter<'a, ShardIdent, u32>>,
}

impl<'a> Iterator for TopBlocksShortIdsIter<'a> {
    type Item = BlockIdShort;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.shards_iter {
            None => {
                self.shards_iter = Some(self.top_blocks.shard_heights.iter());
                Some(self.top_blocks.mc_block)
            }
            Some(iter) => {
                let (shard_ident, seqno) = iter.next()?;
                Some(BlockIdShort::from((*shard_ident, *seqno)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_shards() {
        let mut shard_heights = FastHashMap::default();

        let main_shard = ShardIdent::new_full(0);

        let (left_shard, right_shard) = main_shard.split().unwrap();
        shard_heights.insert(left_shard, 1000);
        shard_heights.insert(right_shard, 1001);

        let top_blocks = TopBlocks {
            mc_block: (ShardIdent::MASTERCHAIN, 100).into(),
            shard_heights,
        };

        assert!(!top_blocks.contains(&BlockId {
            shard: right_shard,
            seqno: 100,
            ..Default::default()
        }));

        // Merged shard test
        assert!(!top_blocks.contains(&BlockId {
            shard: main_shard,
            seqno: 100,
            ..Default::default()
        }));
        assert!(top_blocks.contains(&BlockId {
            shard: main_shard,
            seqno: 10000,
            ..Default::default()
        }));

        // Split shard test
        let (right_left_shard, _) = right_shard.split().unwrap();
        assert!(!top_blocks.contains(&BlockId {
            shard: right_left_shard,
            seqno: 100,
            ..Default::default()
        }));
        assert!(top_blocks.contains(&BlockId {
            shard: right_left_shard,
            seqno: 10000,
            ..Default::default()
        }));
    }
}
