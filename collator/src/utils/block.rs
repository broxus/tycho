use everscale_types::models::ExternalsProcessedUpto;

use crate::mempool::MempoolAnchorId;
use crate::types::ShardDescriptionShort;

pub fn detect_top_processed_to_anchor<I>(
    mc_top_shards: I,
    mc_ext_processed_upto: Option<&ExternalsProcessedUpto>,
) -> MempoolAnchorId
where
    I: Iterator<Item = ShardDescriptionShort>,
{
    let mut min_top_processed_to_anchor_id = 0;

    if let Some(upto) = mc_ext_processed_upto {
        // get top processed to anchor id for master block
        min_top_processed_to_anchor_id = upto.processed_to.0;

        // read from shard descriptions to get min
        for ShardDescriptionShort {
            top_sc_block_updated,
            ext_processed_to_anchor_id,
            ..
        } in mc_top_shards
        {
            if top_sc_block_updated {
                min_top_processed_to_anchor_id =
                    min_top_processed_to_anchor_id.min(ext_processed_to_anchor_id);
            }
        }
    }

    min_top_processed_to_anchor_id
}
