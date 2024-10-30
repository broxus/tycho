use everscale_types::merkle::MerkleUpdate;
use everscale_types::models::{
    BlockExtra, BlockId, BlockInfo, BlockRef, BlockchainConfig, GlobalVersion, McBlockExtra,
    McStateExtra, PrevBlockRef, ShardFeeCreated, ShardStateUnsplit,
};

use crate::types::{DebugDisplay, ProcessedUptoInfoStuff};

pub struct BlockDebugInfo<'a> {
    pub block_id: &'a BlockId,
    pub block_info: &'a BlockInfo,
    pub prev_ref: &'a PrevBlockRef,
    pub state: &'a ShardStateUnsplit,
    pub processed_upto: &'a ProcessedUptoInfoStuff,
    pub mc_state_extra: Option<&'a McStateExtra>,
    pub merkle_update: &'a MerkleUpdate,
    pub block_extra: &'a BlockExtra,
    pub mc_block_extra: Option<&'a McBlockExtra>,
}

impl std::fmt::Debug for BlockDebugInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("BlockDebugInfo");

        s.field("block_id", &DebugDisplay(self.block_id))
            .field("info", &DebugBlockInfo(self.block_info))
            .field("prev_ref", &DebugPrevRef(self.prev_ref))
            .field("state", &DebugShardStateUnslit(self.state));

        if let Some(mc_state_extra) = self.mc_state_extra {
            s.field("mc_state_extra", &DebugMcStateExtra(mc_state_extra));
        }

        s.field("processed_upto", self.processed_upto)
            .field("merkle_update", &DebugMerkleUpdate(self.merkle_update))
            .field("block_extra", &DebugBlockExtra(self.block_extra));

        if let Some(mc_block_extra) = self.mc_block_extra {
            s.field("mc_block_extra", &DebugMcBlockExtra(mc_block_extra));
        }

        s.finish()
    }
}

pub struct DebugBlockInfo<'a>(pub &'a BlockInfo);
impl std::fmt::Debug for DebugBlockInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockInfo")
            .field("version", &self.0.version)
            .field("flags", &self.0.flags)
            .field("start_lt", &self.0.start_lt)
            .field("end_lt", &self.0.end_lt)
            .field(
                "gen_validator_list_hash_short",
                &self.0.gen_validator_list_hash_short,
            )
            .field("gen_catchain_seqno", &self.0.gen_catchain_seqno)
            .field("min_ref_mc_seqno", &self.0.min_ref_mc_seqno)
            .field("prev_key_block_seqno", &self.0.prev_key_block_seqno)
            .field("gen_software", &self.0.gen_software)
            .finish()
    }
}

pub struct DisplayGlobalVersion<'a>(pub &'a GlobalVersion);
impl std::fmt::Debug for DisplayGlobalVersion<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DisplayGlobalVersion<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ver: {}, capabilities: {:?}",
            self.0.version, self.0.capabilities
        )
    }
}

pub struct DebugPrevRef<'a>(pub &'a PrevBlockRef);
impl std::fmt::Debug for DebugPrevRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            PrevBlockRef::Single(block_ref) => write!(f, "Single({})", DisplayBlockRef(block_ref)),
            PrevBlockRef::AfterMerge { left, right } => f
                .debug_struct("AfterMerge")
                .field("left", &DisplayBlockRef(left))
                .field("right", &DisplayBlockRef(right))
                .finish(),
        }
    }
}

pub struct DisplayBlockRef<'a>(pub &'a BlockRef);
impl std::fmt::Debug for DisplayBlockRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DisplayBlockRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "end_lt: {}, ref: {}:{}:{}",
            self.0.end_lt, self.0.seqno, self.0.root_hash, self.0.file_hash
        )
    }
}

pub struct DebugShardStateUnslit<'a>(pub &'a ShardStateUnsplit);
impl std::fmt::Debug for DebugShardStateUnslit<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardStateUnsplit")
            .field("global_id", &self.0.global_id)
            .field(
                "processed_upto.hash",
                self.0.processed_upto.inner().repr_hash(),
            )
            .field("accounts.hash", self.0.accounts.inner().repr_hash())
            .field("wu_used_from_last_anchor", &self.0.overload_history)
            .field(
                "total_balance.tokens",
                &DebugDisplay(self.0.total_balance.tokens),
            )
            .field(
                "total_validator_fees.tokens",
                &DebugDisplay(self.0.total_validator_fees.tokens),
            )
            .field(
                "libraries.hash",
                &self
                    .0
                    .libraries
                    .root()
                    .as_ref()
                    .map(|cell| cell.repr_hash()),
            )
            .field(
                "master_ref",
                &self.0.master_ref.as_ref().map(DisplayBlockRef),
            )
            .finish()
    }
}

pub struct DebugMcStateExtra<'a>(pub &'a McStateExtra);
impl std::fmt::Debug for DebugMcStateExtra<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McStateExtra")
            //.field("shards", &self.0.shards)
            .field("config", &DebugBlockchainConfig(&self.0.config))
            .field(
                "prev_blocks.hash",
                &self
                    .0
                    .prev_blocks
                    .dict()
                    .root()
                    .as_ref()
                    .map(|cell| cell.repr_hash()),
            )
            .field("after_key_block", &self.0.after_key_block)
            .field(
                "last_key_block",
                &self.0.last_key_block.as_ref().map(DisplayBlockRef),
            )
            .field(
                "global_balance.tokens",
                &DebugDisplay(self.0.global_balance.tokens),
            )
            .finish()
    }
}

pub struct DebugBlockchainConfig<'a>(pub &'a BlockchainConfig);
impl std::fmt::Debug for DebugBlockchainConfig<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "params.hash: {:?}, address: {}",
            self.0
                .params
                .as_dict()
                .root()
                .as_ref()
                .map(|cell| cell.repr_hash()),
            self.0.address,
        )
    }
}

pub struct DebugMerkleUpdate<'a>(pub &'a MerkleUpdate);
impl std::fmt::Debug for DebugMerkleUpdate<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MerkleUpdate")
            .field("old_hash", &self.0.old_hash)
            .field("new_hash", &self.0.new_hash)
            .field("old_depth", &self.0.old_depth)
            .field("new_depth", &self.0.new_depth)
            .finish()
    }
}

pub struct DebugBlockExtra<'a>(pub &'a BlockExtra);
impl std::fmt::Debug for DebugBlockExtra<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockExtra")
            .field(
                "in_msg_descr.hash",
                self.0.in_msg_description.inner().repr_hash(),
            )
            .field(
                "out_msg_descr.hash",
                self.0.out_msg_description.inner().repr_hash(),
            )
            .field(
                "account_blocks.hash",
                self.0.account_blocks.inner().repr_hash(),
            )
            .field("rand_seed", &self.0.rand_seed)
            .field("created_by", &self.0.created_by)
            .finish()
    }
}

pub struct DebugMcBlockExtra<'a>(pub &'a McBlockExtra);
impl std::fmt::Debug for DebugMcBlockExtra<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McBlockExtra")
            .field(
                "fees.extra",
                &DebugShardFeeCreated(self.0.fees.root_extra()),
            )
            .field("recover_create_msg", &self.0.recover_create_msg)
            .field("mint_msg", &self.0.mint_msg)
            .finish()
    }
}

pub struct DebugShardFeeCreated<'a>(pub &'a ShardFeeCreated);
impl std::fmt::Debug for DebugShardFeeCreated<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardFeeCreated")
            .field("fees.tokens", &DebugDisplay(self.0.fees.tokens))
            .field("create.tokens", &DebugDisplay(self.0.create.tokens))
            .finish()
    }
}
