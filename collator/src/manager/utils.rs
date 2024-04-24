use anyhow::Result;
use everscale_crypto::ed25519::PublicKey;
use everscale_types::boc::BocRepr;
use everscale_types::models::ValidatorDescription;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};

use crate::types::{BlockStuffForSync, CollationConfig};

use super::types::BlockCandidateEntry;

pub fn build_block_stuff_for_sync(
    block_candidate: &BlockCandidateEntry,
) -> Result<BlockStuffForSync> {
    let block_data = block_candidate.candidate.data().to_vec();
    let block = BocRepr::decode(&block_data)?;
    let block_stuff = BlockStuff::with_block(*block_candidate.candidate.block_id(), block);

    let block_stuff_aug = BlockStuffAug::new(block_stuff, block_data);

    let res = BlockStuffForSync {
        block_id: *block_candidate.candidate.block_id(),
        block_stuff_aug,
        signatures: block_candidate.signatures.clone(),
        prev_blocks_ids: block_candidate.candidate.prev_blocks_ids().into(),
        top_shard_blocks_ids: block_candidate.candidate.top_shard_blocks_ids().into(),
    };

    Ok(res)
}

pub fn find_us_in_collators_set(
    config: &CollationConfig,
    collators_set: &[ValidatorDescription],
) -> Option<PublicKey> {
    let local_pubkey = config.key_pair.public_key;
    let local_pubkey_hash = local_pubkey.as_bytes();
    for node in collators_set {
        if local_pubkey_hash == &node.public_key {
            return Some(local_pubkey);
        }
    }
    None
}
