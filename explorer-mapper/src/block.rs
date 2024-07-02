use std::sync::OnceLock;

use anyhow::{Context, Result};
use everscale_types::cell::{CellBuilder, HashBytes};
use everscale_types::models::{
    AccountBlock, Block as BCBlock, BlockExtra, BlockId, BlockInfo as BCBlockInfo, OptionalAccount,
    PrevBlockRef,
};
use explorer_models::schema::sql_types::State;
use explorer_models::{
    to_json_value, AccountBlockInfo, AccountUpdate, BlkPrevInfo, Block, BlockInfo, ExtBlkRef,
    FutureSplitMerge, GlobalVersion, HashInsert, JsonValue, ProcessingContext, ProcessingType,
    ShardDescrInfo, ShardDescrInfoItem, ShardId, ValueFlow,
};

use crate::transaction::process_transaction;

pub fn fill_block(ctx: &mut ProcessingContext, block: &BCBlock, block_id: BlockId) -> Result<()> {
    let block_info = block.load_info()?;
    let extra = block.load_extra()?;
    let prev_info = match block_info.load_prev_ref()? {
        PrevBlockRef::Single(rf) => (rf, None),
        PrevBlockRef::AfterMerge { left, right } => (left, Some(right)),
    };

    let account_blocks = map_account_blocks(ctx, &extra, block_id)?;

    let shards_info = if block_info.shard.is_masterchain() {
        Some(block_extra(&extra).context("Failed to process block extra")?)
    } else {
        None
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
        transaction_count: account_blocks.1,
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
        account_blocks: account_blocks.0,
        shards_info,
        additional_info: None, // legacy
    };

    ctx.blocks.push(block);

    Ok(())
}

fn map_block_info(block_info: &BCBlockInfo) -> Result<JsonValue> {
    let master_ref = block_info.load_master_ref()?.map(|rf| ExtBlkRef {
        end_lt: rf.end_lt,
        seq_no: rf.seqno,
        root_hash: rf.root_hash.0.into(),
    });
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

fn map_account_blocks(
    ctx: &mut ProcessingContext,
    block: &BlockExtra,
    block_id: BlockId,
) -> Result<(JsonValue, u16)> {
    let account_blocks = block.account_blocks.load()?;
    let mut processed_blocks = Vec::new();

    for account in account_blocks.values() {
        let (_, block) = account?;
        let proccessed_block = process_account_block(ctx, &block_id, &block, ProcessingType::Full)?;
        processed_blocks.push(proccessed_block);
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
            process_transaction(ctx, &mut update, &block_id, &tx)?;
        }
    }

    let total_transaction_count = processed_blocks.iter().map(|b| b.transaction_count).sum();
    Ok((to_json_value(&processed_blocks), total_transaction_count))
}

fn process_account_block(
    ctx: &mut ProcessingContext,
    block_id: &BlockId,
    block: &AccountBlock,
    processing_type: ProcessingType,
) -> Result<AccountBlockInfo> {
    let mut transaction_count = 0;
    let state_update = block.state_update.load()?;

    if processing_type == ProcessingType::Full {
        let mut account_update = AccountUpdate {
            address: block.account.0.into(),
            wc: block_id.shard.workchain() as i8,
            last_transaction_time: 0,
            last_transaction_lt: 0,
            creator: None,
            state: State::Uninit,
            deleted: &state_update.new == default_account_hash(),
        };

        for item in block.transactions.values() {
            let item = item?.1.load()?;
            process_transaction(ctx, &mut account_update, block_id, &item)?;
            transaction_count += 1;
        }
        if transaction_count > 0 {
            ctx.account_updates.push(account_update);
        }
    } else {
        transaction_count = block.transactions.values().count();
    }

    Ok(AccountBlockInfo {
        wc: block_id.shard.workchain(),
        address: block.account.0.into(),
        old_hash: state_update.old.0.into(),
        new_hash: state_update.new.0.into(),
        transaction_count: transaction_count
            .try_into()
            .context("Transaction count overflow")?,
    })
}

fn default_account_hash() -> &'static HashBytes {
    static HASH: OnceLock<HashBytes> = OnceLock::new();
    HASH.get_or_init(|| {
        *CellBuilder::build_from(OptionalAccount::EMPTY)
            .unwrap()
            .repr_hash()
    })
}

fn block_extra(extra: &BlockExtra) -> Result<JsonValue> {
    let custom = extra.load_custom()?.context("not a masterchain block")?;
    let res: Vec<_> = custom
        .shards
        .iter()
        .filter_map(|s| {
            let (ident, descr) = s.ok()?;
            Some(ShardDescrInfoItem {
                shard_ident: ShardId {
                    wc: ident.workchain(),
                    shard_prefix: ident.prefix(),
                },
                info: ShardDescrInfo {
                    seq_no: descr.seqno,
                    reg_mc_seqno: descr.reg_mc_seqno,
                    start_lt: descr.start_lt,
                    end_lt: descr.end_lt,
                    root_hash: descr.root_hash.0.into(),
                    file_hash: descr.file_hash.0.into(),
                    before_split: descr.before_split,
                    before_merge: descr.before_merge,
                    want_split: descr.want_split,
                    want_merge: descr.want_merge,
                    nx_cc_updated: descr.nx_cc_updated,
                    flags: 0, // todo: is it needed?
                    next_catchain_seqno: descr.next_catchain_seqno,
                    next_validator_shard: descr.next_validator_shard,
                    min_ref_mc_seqno: descr.min_ref_mc_seqno,
                    gen_utime: descr.gen_utime,
                    split_merge_at: match descr.split_merge_at {
                        Some(everscale_types::models::FutureSplitMerge::Split {
                            split_utime,
                            interval,
                        }) => FutureSplitMerge::Split {
                            split_utime,
                            interval,
                        },
                        Some(everscale_types::models::FutureSplitMerge::Merge {
                            merge_utime,
                            interval,
                        }) => FutureSplitMerge::Merge {
                            merge_utime,
                            interval,
                        },
                        None => FutureSplitMerge::None,
                    },
                    fees_collected: descr.fees_collected.tokens.into_inner() as u64,
                    funds_created: descr.funds_created.tokens.into_inner() as u64,
                },
            })
        })
        .collect();

    Ok(to_json_value(&res))
}
