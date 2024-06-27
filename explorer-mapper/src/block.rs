use anyhow::Result;
use everscale_types::models::{Block as BCBlock, BlockId, BlockInfo as BCBlockInfo, PrevBlockRef};
use explorer_models::{Block, BlockInfo, ExtBlkRef, GlobalVersion, HashInsert, ProcessingContext};

pub fn fill_block(ctx: &mut ProcessingContext, block: &BCBlock, block_id: BlockId) -> Result<()> {
    let block_info = block.load_info()?;
    let prev_info = match block_info.load_prev_ref()? {
        PrevBlockRef::Single(rf) => (rf, None),
        PrevBlockRef::AfterMerge { left, right } => (left, Some(right)),
    };

    let block = Block {
        workchain: block_id.shard.workchain() as i8,
        shard: block_id.shard.prefix(),
        seqno: block_id.seqno,
        root_hash: HashInsert {
            hash: block_id.root_hash.0,
        },
        file_hash: HashInsert {
            hash: block_id.file_hash.0,
        },
        is_key_block: block_info.key_block,
        transaction_count: 0,
        gen_utime: block_info.gen_utime,
        gen_software_version: block_info.gen_software.version,
        prev1: HashInsert {
            hash: prev_info.0.root_hash.0,
        },
        prev1_seqno: prev_info.0.seqno,
        prev2: prev_info.1.map(|rf| HashInsert {
            hash: rf.root_hash.0,
        }),
        prev2_seqno: prev_info.1.map(|rf| rf.seqno),
        prev_key_block: block_info.prev_key_block_seqno,
        block_info: "".to_string(),
        value_flow: "".to_string(),
        account_blocks: "".to_string(),
        shards_info: None,
        additional_info: None,
    };
}

fn fill_block_info(
    ctx: &mut ProcessingContext,
    block: &BCBlock,
    block_info: &BCBlockInfo,
) -> Result<()> {
    let block_info = BlockInfo {
        version: block_info.version,
        after_merge: block_info.after_merge,
        before_split: block_info.before_split,
        after_split: block_info.after_split,
        want_split: block_info.want_split,
        want_merge: block_info.want_merge,
        key_block: block_info.key_block,
        vert_seq_no: block_info.vert_seqno,
        vert_seq_no_incr: 0, // todo: what is this?
        flags: block_info.flags,
        start_lt: block_info.start_lt,
        end_lt: block_info.end_lt,
        gen_validator_list_hash_short: block_info.gen_validator_list_hash_short,
        gen_catchain_seqno: block_info.gen_catchain_seqno,
        min_ref_mc_seqno: block_info.min_ref_mc_seqno,
        prev_key_block_seqno: block_info.prev_key_block_seqno,
        gen_software: Some(GlobalVersion {
            version: block_info.gen_software.version,
            capabilities: block_info.gen_software.capabilities.into_inner(),
        }),

        master_ref: Some(ExtBlkRef {
            end_lt: block_info.master_ref,
            seq_no: 0,
            root_hash: HashInsert {},
        }),
        prev_ref: (),
        prev_vert_ref: None,
    };
}
