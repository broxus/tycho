use super::BlockProvider;
use crate::block_strider::provider::OptionalBlockStuff;
use everscale_types::cell::{Cell, CellFamily, Store};
use everscale_types::dict::{AugDict, Dict};
use everscale_types::merkle::MerkleUpdate;
use everscale_types::models::{
    Block, BlockExtra, BlockId, BlockInfo, BlockRef, CurrencyCollection, Lazy, McBlockExtra,
    PrevBlockRef, ShardDescription, ShardFees, ShardHashes, ShardIdent, ValueFlow,
};
use everscale_types::prelude::HashBytes;
use std::collections::HashMap;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};

pub mod archive_provider;

const ZERO_HASH: HashBytes = HashBytes([0; 32]);

impl BlockProvider for TestBlockProvider {
    // type GetNextBlockFut<'a>: Future<Output = OptionalBlockStuff> + Send + 'a;
    type GetNextBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
    type GetBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        let next_id = self
            .master_blocks
            .iter()
            .find(|id| id.seqno == prev_block_id.seqno + 1);
        futures_util::future::ready(next_id.and_then(|id| {
            self.blocks.get(id).map(|b| {
                Ok(BlockStuffAug::new(
                    BlockStuff::with_block(*id, b.clone()),
                    everscale_types::boc::BocRepr::encode(b).unwrap(),
                ))
            })
        }))
    }

    fn get_block(&self, id: &BlockId) -> Self::GetBlockFut<'_> {
        futures_util::future::ready(self.blocks.get(id).map(|b| {
            Ok(BlockStuffAug::new(
                BlockStuff::with_block(*id, b.clone()),
                everscale_types::boc::BocRepr::encode(b).unwrap(),
            ))
        }))
    }
}

pub struct TestBlockProvider {
    master_blocks: Vec<BlockId>,
    blocks: HashMap<BlockId, Block>,
}

impl TestBlockProvider {
    pub fn new(num_master_blocks: u32) -> Self {
        let (blocks, master_blocks) = create_block_chain(num_master_blocks);
        Self {
            blocks,
            master_blocks,
        }
    }

    pub fn first_master_block(&self) -> BlockId {
        *self.master_blocks.first().unwrap()
    }

    pub fn validate(&self) {
        for master in &self.master_blocks {
            tracing::info!("Loading extra for block {:?}", master);
            let block = self.blocks.get(master).unwrap();
            let extra = block.load_extra().unwrap();
            extra.load_custom().unwrap().expect("validation failed");
        }
    }
}

fn create_block_chain(num_blocks: u32) -> (HashMap<BlockId, Block>, Vec<BlockId>) {
    let mut blocks = HashMap::new();
    let mut master_block_ids = Vec::new();
    let mut prev_shard_block_ref = zero_ref();
    let mut prev_block_ref = zero_ref();

    for seqno in 1..=num_blocks {
        master_block(
            seqno,
            &mut prev_block_ref,
            &mut prev_shard_block_ref,
            &mut blocks,
            &mut master_block_ids,
        );
    }
    (blocks, master_block_ids)
}

fn master_block(
    seqno: u32,
    prev_block_ref: &mut PrevBlockRef,
    prev_shard_block_ref: &mut PrevBlockRef,
    blocks: &mut HashMap<BlockId, Block>,
    master_ids: &mut Vec<BlockId>,
) {
    let (block_ref, block_info) = block_info(prev_block_ref, seqno, true);
    *prev_block_ref = PrevBlockRef::Single(block_ref.clone());

    let shard_block_ids = link_shard_blocks(prev_shard_block_ref, 2, blocks);
    let block_extra = McBlockExtra {
        shards: ShardHashes::from_shards(shard_block_ids.iter().map(|x| (&x.0, &x.1))).unwrap(),
        fees: ShardFees {
            root: None,
            fees: Default::default(),
            create: Default::default(),
        },
        prev_block_signatures: Default::default(),
        recover_create_msg: None,
        mint_msg: None,
        copyleft_msgs: Default::default(),
        config: None,
    };
    let master_extra = Some(Lazy::new(&block_extra).unwrap());
    insert_block(
        seqno,
        block_info,
        block_ref,
        blocks,
        Some(master_ids),
        master_extra,
    );
}

fn insert_block(
    seqno: u32,
    block_info: BlockInfo,
    block_ref: BlockRef,
    blocks: &mut HashMap<BlockId, Block>,
    master_ids: Option<&mut Vec<BlockId>>,
    mc_extra: Option<Lazy<McBlockExtra>>,
) {
    let block = Block {
        global_id: 0,
        info: Lazy::new(&block_info).unwrap(),
        value_flow: default_cc(),
        state_update: Lazy::new(&MerkleUpdate::default()).unwrap(),
        out_msg_queue_updates: None,
        extra: extra(mc_extra.clone()),
    };
    let id = BlockId {
        shard: block_info.shard,
        seqno,
        root_hash: block_ref.root_hash,
        file_hash: block_ref.file_hash,
    };
    blocks.insert(id, block);
    if let Some(master_ids) = master_ids {
        master_ids.push(id);
    }
}

fn extra(custom: Option<Lazy<McBlockExtra>>) -> Lazy<BlockExtra> {
    Lazy::new(&BlockExtra {
        in_msg_description: Default::default(),
        out_msg_description: Default::default(),
        account_blocks: Lazy::new(&AugDict::new()).unwrap(),
        rand_seed: Default::default(),
        created_by: Default::default(),
        custom,
    })
    .unwrap()
}

fn link_shard_blocks(
    prev_block_ref: &mut PrevBlockRef,
    chain_len: u32,
    blocks: &mut HashMap<BlockId, Block>,
) -> Vec<(ShardIdent, ShardDescription)> {
    let starting_seqno = match &prev_block_ref {
        PrevBlockRef::Single(s) => s.seqno,
        PrevBlockRef::AfterMerge { .. } => {
            unreachable!()
        }
    };
    let mut last_ref = None;
    for seqno in starting_seqno + 1..starting_seqno + chain_len {
        let (block_ref, info) = block_info(prev_block_ref, seqno, false);
        last_ref = Some((
            ShardIdent::BASECHAIN,
            ShardDescription {
                seqno,
                reg_mc_seqno: 0,
                start_lt: 0,
                end_lt: 0,
                root_hash: block_ref.root_hash,
                file_hash: block_ref.file_hash,
                before_split: false,
                before_merge: false,
                want_split: false,
                want_merge: false,
                nx_cc_updated: false,
                next_catchain_seqno: 0,
                next_validator_shard: 0,
                min_ref_mc_seqno: 0,
                gen_utime: 0,
                split_merge_at: None,
                fees_collected: Default::default(),
                funds_created: Default::default(),
                copyleft_rewards: Default::default(),
                proof_chain: None,
            },
        ));
        insert_block(seqno, info, block_ref.clone(), blocks, None, None);
        *prev_block_ref = PrevBlockRef::Single(block_ref);
    }
    vec![last_ref.unwrap()]
}

fn default_cc() -> Lazy<ValueFlow> {
    let def_cc = CurrencyCollection::default();
    Lazy::new(&ValueFlow {
        from_prev_block: def_cc.clone(),
        to_next_block: def_cc.clone(),
        imported: def_cc.clone(),
        exported: def_cc.clone(),
        fees_collected: def_cc.clone(),
        fees_imported: def_cc.clone(),
        recovered: def_cc.clone(),
        created: def_cc.clone(),
        minted: def_cc.clone(),
        copyleft_rewards: Dict::new(),
    })
    .unwrap()
}

fn block_info(prev_block_ref: &PrevBlockRef, seqno: u32, is_mc: bool) -> (BlockRef, BlockInfo) {
    let shard = if is_mc {
        ShardIdent::MASTERCHAIN
    } else {
        ShardIdent::BASECHAIN
    };

    let prev_block_ref = encode_ref(prev_block_ref.clone());
    let block_info = BlockInfo {
        version: 0,
        after_merge: false,
        before_split: false,
        after_split: false,
        want_split: false,
        want_merge: false,
        key_block: false,
        flags: 0,
        seqno,
        vert_seqno: 0,
        shard,
        gen_utime: 0,
        start_lt: 0,
        end_lt: 0,
        gen_validator_list_hash_short: 0,
        gen_catchain_seqno: 0,
        min_ref_mc_seqno: 0,
        prev_key_block_seqno: 0,
        gen_software: Default::default(),
        master_ref: None,
        prev_ref: prev_block_ref,
        prev_vert_ref: None,
    };
    (
        BlockRef {
            end_lt: 0,
            seqno,
            root_hash: seqno_to_hash(seqno),
            file_hash: seqno_to_hash(seqno),
        },
        block_info,
    )
}

fn seqno_to_hash(i: u32) -> HashBytes {
    let mut bytes = [0; 32];
    bytes[0] = i as u8;
    HashBytes::from(bytes)
}

fn zero_ref() -> PrevBlockRef {
    PrevBlockRef::Single(BlockRef {
        end_lt: 0,
        seqno: 0,
        root_hash: ZERO_HASH,
        file_hash: ZERO_HASH,
    })
}

fn encode_ref(prev_block_ref: PrevBlockRef) -> Cell {
    let mut builder = everscale_types::cell::CellBuilder::new();
    match prev_block_ref {
        PrevBlockRef::Single(r) => {
            r.store_into(&mut builder, &mut Cell::empty_context())
                .unwrap();
        }
        PrevBlockRef::AfterMerge { left, right } => {
            let context = &mut Cell::empty_context();
            left.store_into(&mut builder, context).unwrap();
            right.store_into(&mut builder, context).unwrap();
        }
    }
    builder.build().unwrap()
}
