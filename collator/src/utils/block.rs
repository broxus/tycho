use crate::mempool::MempoolAnchorId;
use crate::types::ShardDescriptionShort;

pub fn detect_top_processed_to_anchor<I>(
    mc_top_shards: I,
    mc_processed_to_anchor_id: MempoolAnchorId,
) -> MempoolAnchorId
where
    I: Iterator<Item = ShardDescriptionShort>,
{
    // get top processed to anchor id for master block
    let mut min_top = mc_processed_to_anchor_id;

    // read from top shards to get min
    for ShardDescriptionShort {
        top_sc_block_updated,
        ext_processed_to_anchor_id,
        ..
    } in mc_top_shards
    {
        // only if top shard was updated in master
        if top_sc_block_updated {
            min_top = min_top.min(ext_processed_to_anchor_id);
        }
    }

    min_top
}
