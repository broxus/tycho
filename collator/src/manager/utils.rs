use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::{BlockId, ShardIdent, ValidatorDescription};
use tycho_util::FastHashMap;

use super::blocks_cache::BlocksCache;
use super::BlockCacheKey;
use crate::state_node::StateNodeAdapter;
use crate::types::processed_upto::ProcessedUptoInfoStuff;
use crate::types::ProcessedToByPartitions;

pub fn find_us_in_collators_set(
    keypair: &KeyPair,
    set: &FastHashMap<[u8; 32], ValidatorDescription>,
) -> Option<PublicKey> {
    let local_pubkey = keypair.public_key;
    if set.contains_key(local_pubkey.as_bytes()) {
        Some(local_pubkey)
    } else {
        None
    }
}

pub async fn get_all_shards_processed_to_by_partitions_for_mc_block_without_cache(
    mc_block_id: &BlockId,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
) -> Result<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>> {
    let empty_cache = BlocksCache::new();

    get_all_shards_processed_to_by_partitions_for_mc_block_impl(
        &mc_block_id.as_short_id(),
        &empty_cache,
        state_node_adapter,
        Some(mc_block_id),
    )
    .await
}

pub(super) async fn get_all_shards_processed_to_by_partitions_for_mc_block_with_cache(
    mc_block_key: &BlockCacheKey,
    blocks_cache: &BlocksCache,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
) -> Result<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>> {
    get_all_shards_processed_to_by_partitions_for_mc_block_impl(
        mc_block_key,
        blocks_cache,
        state_node_adapter,
        None,
    )
    .await
}

async fn get_all_shards_processed_to_by_partitions_for_mc_block_impl(
    mc_block_key: &BlockCacheKey,
    blocks_cache: &BlocksCache,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    backoff_mc_block_id: Option<&BlockId>,
) -> Result<FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>> {
    let mut result = FastHashMap::default();

    if mc_block_key.seqno == 0 {
        return Ok(result);
    }

    // try preload from cache
    let mut preloaded = blocks_cache.preload_top_blocks_processed_to_by_partitions(mc_block_key)?;

    if preloaded.is_empty() {
        // try preload from state if master block not exists in cache
        if let Some(mc_block_id) = backoff_mc_block_id {
            preloaded = preload_top_blocks_processed_to_by_partitions_from_state(
                mc_block_id,
                state_node_adapter.as_ref(),
            )
            .await?;
        } else {
            bail!(
                "get_all_shards_processed_to_by_partitions_for_mc_block: Master block not found in cache! ({})",
                mc_block_key,
            )
        }
    }

    for (top_block_id, (updated, processed_to_opt)) in preloaded {
        // load processed_to for shard blocks if was not loaded from cache
        let processed_to = match processed_to_opt {
            Some(processed_to) => processed_to,
            None => {
                if top_block_id.seqno == 0 {
                    FastHashMap::default()
                } else {
                    // get from state
                    let state = state_node_adapter.load_state(&top_block_id).await?;
                    let processed_upto = state.state().processed_upto.load()?;
                    let processed_upto = ProcessedUptoInfoStuff::try_from(processed_upto)?;
                    processed_upto.get_internals_processed_to_by_partitions()
                }
            }
        };

        result.insert(top_block_id.shard, (updated, processed_to));
    }

    Ok(result)
}

async fn preload_top_blocks_processed_to_by_partitions_from_state(
    mc_block_id: &BlockId,
    state_node_adapter: &dyn StateNodeAdapter,
) -> Result<FastHashMap<BlockId, (bool, Option<ProcessedToByPartitions>)>> {
    let mut result = FastHashMap::default();

    // get master block processed to
    let mc_state = state_node_adapter.load_state(mc_block_id).await?;
    let processed_upto = mc_state.state().processed_upto.load()?;
    let processed_upto = ProcessedUptoInfoStuff::try_from(processed_upto)?;
    let processed_to = processed_upto.get_internals_processed_to_by_partitions();

    result.insert(*mc_block_id, (true, Some(processed_to)));

    // preload top shard blocks info
    let top_shard_blocks_info = mc_state.get_top_shard_blocks_info()?;
    for (top_shard_block_id, updated) in top_shard_blocks_info {
        result.insert(top_shard_block_id, (updated, None));
    }

    Ok(result)
}
