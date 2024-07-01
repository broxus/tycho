use anyhow::Result;
use everscale_types::models::{
    Block as BCBlock, BlockExtra, BlockId, BlockInfo as BCBlockInfo, PrevBlockRef,
};
use explorer_models::schema::sql_types::State;
use explorer_models::{
    to_json_value, AccountUpdate, BlkPrevInfo, Block, BlockInfo, ExtBlkRef, GlobalVersion,
    HashInsert, JsonValue, ProcessingContext, ValueFlow,
};

use crate::transaction::process_transaction;

pub fn fill_block(ctx: &mut ProcessingContext, block: &BCBlock, block_id: BlockId) -> Result<()> {
    let block_info = block.load_info()?;
    let extra = block.load_extra()?;
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
        prev2: prev_info.1.as_ref().map(|rf| HashInsert {
            hash: rf.root_hash.0,
        }),
        prev2_seqno: prev_info.1.as_ref().map(|rf| rf.seqno),
        prev_key_block: block_info.prev_key_block_seqno,
        block_info: map_block_info(&block_info)?,
        value_flow: map_value_flow(block)?,
        account_blocks: map_account_blocks(&extra, block_id)?,
        shards_info: None,
        additional_info: None,
    };

    Ok(())
}

fn map_block_info(block_info: &BCBlockInfo) -> Result<JsonValue> {
    let master_ref = match block_info.load_master_ref()? {
        Some(rf) => Some(ExtBlkRef {
            end_lt: rf.end_lt,
            seq_no: rf.seqno,
            root_hash: rf.root_hash.0.into(),
        }),
        None => None,
    };
    let prev_ref = block_info.load_prev_ref()?;
    let prev_ref = match prev_ref {
        PrevBlockRef::Single(s) => BlkPrevInfo::Block {
            prev: ExtBlkRef {
                end_lt: s.end_lt,
                seq_no: s.seqno,
                root_hash: s.root_hash.0.into(),
            },
        },
        PrevBlockRef::AfterMerge { left, right } => BlkPrevInfo::Blocks {
            prev1: ExtBlkRef {
                end_lt: left.end_lt,
                seq_no: left.seqno,
                root_hash: left.root_hash.0.into(),
            },
            prev2: ExtBlkRef {
                end_lt: right.end_lt,
                seq_no: right.seqno,
                root_hash: right.root_hash.0.into(),
            },
        },
    };

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

        master_ref,
        prev_ref,
        prev_vert_ref: None, // doesn't exist in real world
    };

    Ok(to_json_value(&block_info))
}

fn map_value_flow(block: &BCBlock) -> Result<JsonValue> {
    let value_flow = block.load_value_flow()?;
    let value_flow = ValueFlow {
        from_prev_blk: value_flow.from_prev_block.tokens.into_inner() as u64,
        to_next_blk: value_flow.to_next_block.tokens.into_inner() as u64,
        imported: value_flow.imported.tokens.into_inner() as u64,
        exported: value_flow.exported.tokens.into_inner() as u64,
        fees_collected: value_flow.fees_collected.tokens.into_inner() as u64,
        fees_imported: value_flow.fees_imported.tokens.into_inner() as u64,
        recovered: value_flow.recovered.tokens.into_inner() as u64,
        created: value_flow.created.tokens.into_inner() as u64,
        minted: value_flow.minted.tokens.into_inner() as u64,
    };

    Ok(to_json_value(&value_flow))
}

fn map_account_blocks(block: &BlockExtra, block_id: BlockId) -> Result<JsonValue> {
    let account_blocks = block.account_blocks.load()?;

    for account in account_blocks.values() {
        let (cc, block) = account?;
        let transactions = block.transactions;

        let mut update = AccountUpdate {
            address: block.account.0.into(),
            wc: block_id.shard.workchain() as i8,
            last_transaction_time: 0,
            last_transaction_lt: 0,
            creator: None,
            state: State::NonExist,
            deleted: false,
        };
        for tx in transactions.values() {
            let tx = tx?;
            let tx = tx.1.load()?;
            process_transaction(todo!(), &mut update, &block_id, &tx)?;
        }
    }

    todo!()
}
