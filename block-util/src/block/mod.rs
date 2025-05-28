use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;

pub use self::block_id_ext::{calc_next_block_id_short, BlockIdExt, BlockIdRelation};
pub use self::block_proof_stuff::{
    check_with_master_state, check_with_prev_key_block_proof, AlwaysInclude, BlockProofStuff,
    BlockProofStuffAug, ValidatorSubsetInfo,
};
pub use self::block_stuff::{BlockStuff, BlockStuffAug};
pub use self::top_blocks::{ShardHeights, TopBlocks, TopBlocksShortIdsIter};

mod block_id_ext;
mod block_proof_stuff;
mod block_stuff;
mod top_blocks;

pub fn shard_ident_at_depth(workchain: i32, account: &HashBytes, depth: u8) -> ShardIdent {
    assert!(
        depth <= ShardIdent::MAX_SPLIT_DEPTH,
        "split depth is too big",
    );

    if depth == 0 {
        return ShardIdent::new_full(workchain);
    }

    let prefix = u64::from_be_bytes(*account.first_chunk());
    let mask = u64::MAX << (64 - depth);
    let tag = (mask | (mask >> 1)) ^ mask;

    ShardIdent::new(workchain, (prefix & mask) | tag).expect("computed prefix should be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_shard_ident() {
        let account = HashBytes([0xac; 32]);
        let mut shard = ShardIdent::new_full(0);
        for depth in 0..ShardIdent::MAX_SPLIT_DEPTH {
            assert_eq!(shard_ident_at_depth(0, &account, depth), shard);

            let (left, right) = shard.split().unwrap();
            if left.contains_account(&account) {
                shard = left;
            } else if right.contains_account(&account) {
                shard = right;
            } else {
                unreachable!()
            }
        }
    }
}
