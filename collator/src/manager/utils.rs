use anyhow::Result;
use everscale_crypto::ed25519::PublicKey;
use everscale_types::models::ValidatorDescription;

use crate::types::{BlockStuffForSync, CollationConfig};

use super::types::BlockCandidateEntry;

pub fn build_block_stuff_for_sync(
    block_candidate: &BlockCandidateEntry,
) -> Result<BlockStuffForSync> {
    todo!()
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
