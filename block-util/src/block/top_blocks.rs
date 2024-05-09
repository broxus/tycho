use anyhow::Result;
use everscale_types::models::*;
use tycho_util::FastHashMap;

use crate::block::BlockStuff;

/// A map from shard identifiers to the last block seqno.
#[derive(Debug, Default, Clone)]
pub struct ShardHeights(FastHashMap<ShardIdent, u32>);

impl ShardHeights {
    /// Checks whether the given block is equal to or greater than
    /// the last block for the given shard.
    pub fn contains(&self, block_id: &BlockId) -> bool {
        self.contains_shard_seqno(&block_id.shard, block_id.seqno)
    }

    /// Checks whether the given block satisfies the comparator.
    ///
    /// Comparator: `f(top_seqno, block_id.seqno)`.
    pub fn contains_ext<F>(&self, block_id: &BlockId, f: F) -> bool
    where
        F: Fn(u32, u32) -> bool,
    {
        self.contains_shard_seqno_ext(&block_id.shard, block_id.seqno, f)
    }

    /// Checks whether the given pair of [`ShardIdent`] and seqno
    /// is equal to or greater than the last block for the given shard.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn contains_shard_seqno(&self, shard_ident: &ShardIdent, seqno: u32) -> bool {
        self.contains_shard_seqno_ext(shard_ident, seqno, |top_seqno, seqno| top_seqno <= seqno)
    }

    /// Checks whether the given pair of [`ShardIdent`] and seqno
    /// satisfies the comparator.
    ///
    /// Comparator: `f(top_seqno, seqno)`.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn contains_shard_seqno_ext<F>(&self, shard_ident: &ShardIdent, seqno: u32, f: F) -> bool
    where
        F: Fn(u32, u32) -> bool,
    {
        match self.0.get(shard_ident) {
            Some(&top_seqno) => f(top_seqno, seqno),
            None => self
                .0
                .iter()
                .find(|&(shard, _)| shard_ident.intersects(shard))
                .map(|(_, &top_seqno)| f(top_seqno, seqno))
                .unwrap_or_default(),
        }
    }

    /// Returns block count.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = BlockIdShort> + Clone + '_ {
        self.0
            .iter()
            .map(|(shard, seqno)| BlockIdShort::from((*shard, *seqno)))
    }
}

impl FromIterator<(ShardIdent, u32)> for ShardHeights {
    #[inline]
    fn from_iter<T: IntoIterator<Item = (ShardIdent, u32)>>(iter: T) -> Self {
        Self(FastHashMap::from_iter(iter))
    }
}

impl FromIterator<BlockIdShort> for ShardHeights {
    fn from_iter<T: IntoIterator<Item = BlockIdShort>>(iter: T) -> Self {
        Self(
            iter.into_iter()
                .map(|block_id| (block_id.shard, block_id.seqno))
                .collect(),
        )
    }
}

impl From<FastHashMap<ShardIdent, u32>> for ShardHeights {
    #[inline]
    fn from(map: FastHashMap<ShardIdent, u32>) -> Self {
        Self(map)
    }
}

/// Stores last blocks for each workchain and shard.
#[derive(Debug, Clone)]
pub struct TopBlocks {
    pub mc_block: BlockIdShort,
    pub shard_heights: ShardHeights,
}

impl TopBlocks {
    /// Constructs this structure for the zerostate.
    pub fn zerostate() -> Self {
        Self {
            mc_block: BlockIdShort::from((ShardIdent::MASTERCHAIN, 0)),
            shard_heights: ShardHeights::from_iter([(ShardIdent::BASECHAIN, 0u32)]),
        }
    }

    /// Extracts last blocks for each workchain and shard from the given masterchain block.
    pub fn from_mc_block(mc_block_data: &BlockStuff) -> Result<Self> {
        let block_id = mc_block_data.id();
        debug_assert!(block_id.shard.is_masterchain());

        Ok(Self {
            mc_block: block_id.as_short_id(),
            shard_heights: ShardHeights(mc_block_data.shard_blocks_seqno()?),
        })
    }

    /// Masterchain block seqno
    pub fn mc_seqno(&self) -> u32 {
        self.mc_block.seqno
    }

    pub fn shard_heights(&self) -> &ShardHeights {
        &self.shard_heights
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

    /// Checks whether the given pair of [`ShardIdent`] and seqno
    /// is equal to or greater than the last block for the given shard.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn contains_shard_seqno(&self, shard_ident: &ShardIdent, seqno: u32) -> bool {
        if shard_ident.is_masterchain() {
            seqno >= self.mc_block.seqno
        } else {
            self.shard_heights.contains_shard_seqno(shard_ident, seqno)
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
                self.shards_iter = Some(self.top_blocks.shard_heights.0.iter());
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
            shard_heights: shard_heights.into(),
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
