use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ahash::HashMapExt;
use anyhow::{anyhow, bail, Context, Result};
use everscale_types::boc;
use everscale_types::cell::{Cell, CellFamily, HashBytes, Lazy, UsageTree, UsageTreeMode};
use everscale_types::dict::{self, Dict};
use everscale_types::models::{
    AccountBlocks, AccountState, BlockId, BlockIdShort, BlockInfo, BlockLimits, BlockParamLimits,
    BlockRef, BlockchainConfig, CollationConfig, CurrencyCollection, HashUpdate, ImportFees, InMsg,
    InMsgDescr, IntAddr, LibDescr, MsgInfo, MsgsExecutionParams, OptionalAccount, OutMsg,
    OutMsgDescr, OwnedMessage, PrevBlockRef, ShardAccount, ShardAccounts, ShardDescription,
    ShardFeeCreated, ShardFees, ShardIdent, ShardIdentFull, ShardStateUnsplit, SpecialFlags,
    StateInit, StdAddr, Transaction, ValueFlow,
};
use everscale_types::num::Tokens;
use parking_lot::lock_api::RwLockReadGuard;
use parking_lot::{MappedRwLockReadGuard, Mutex, RwLock};
use tl_proto::TlWrite;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, SerializedQueueDiff};
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_executor::{AccountMeta, PublicLibraryChange, TransactionMeta};
use tycho_network::PeerId;
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap, FastHashSet};

use super::do_collate::work_units::PrepareMsgGroupsWu;
use super::messages_reader::{MessagesReaderMetrics, ReaderState};
use crate::collator::do_collate::work_units::{ExecuteWu, FinalizeWu};
use crate::collator::messages_reader::MetricsTimer;
use crate::internal_queue::types::{
    AccountStatistics, Bound, DiffStatistics, InternalMessageValue, QueueShardBoundedRange,
    QueueStatistics, SeparatedStatisticsByPartitions,
};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::{
    find_min_processed_to_by_shards, BlockSeqno, Lt, ProcessedUptoInfoStuff,
};
use crate::types::{BlockCandidate, McData, ProcessedToByPartitions, TopShardBlockInfo};

pub(super) struct WorkingState {
    pub next_block_id_short: BlockIdShort,
    pub mc_data: Arc<McData>,
    pub collation_config: Arc<CollationConfig>,
    pub wu_used_from_last_anchor: u64,
    pub prev_shard_data: Option<PrevData>,
    pub usage_tree: Option<UsageTree>,
    pub has_unprocessed_messages: Option<bool>,
    pub reader_state: ReaderState,
}

impl WorkingState {
    pub fn prev_shard_data_ref(&self) -> &PrevData {
        self.prev_shard_data.as_ref().unwrap()
    }
}

pub(super) struct PrevData {
    observable_states: Vec<ShardStateStuff>,
    observable_accounts: ShardAccounts,

    blocks_ids: Vec<BlockId>,

    pure_states: Vec<ShardStateStuff>,
    pure_state_root: Cell,

    gen_chain_time: u64,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    wu_used_from_last_anchor: u64,

    processed_upto: ProcessedUptoInfoStuff,

    prev_queue_diff_hashes: Vec<HashBytes>,
}

impl PrevData {
    pub fn build(
        prev_states: Vec<ShardStateStuff>,
        prev_queue_diff_hashes: Vec<HashBytes>,
    ) -> Result<(Self, UsageTree)> {
        // TODO: make real implementation
        // consider split/merge logic
        //  Collator::prepare_data()
        //  Collator::unpack_last_state()

        let prev_blocks_ids: Vec<_> = prev_states.iter().map(|s| *s.block_id()).collect();
        let pure_prev_state_root = prev_states[0].root_cell().clone();
        let pure_prev_states = prev_states;

        let usage_tree = UsageTree::new(UsageTreeMode::OnLoad);
        let observable_root = usage_tree.track(&pure_prev_state_root);
        let observable_states = vec![ShardStateStuff::from_root(
            pure_prev_states[0].block_id(),
            observable_root,
            pure_prev_states[0].ref_mc_state_handle().tracker(),
        )?];

        let gen_chain_time = observable_states[0].get_gen_chain_time();
        let gen_lt = observable_states[0].state().gen_lt;
        let observable_accounts = observable_states[0].state().load_accounts()?;
        let total_validator_fees = observable_states[0].state().total_validator_fees.clone();
        let wu_used_from_last_anchor = observable_states[0].state().overload_history;

        let processed_upto_info = pure_prev_states[0].state().processed_upto.load()?;

        let prev_data = Self {
            observable_states,
            observable_accounts,

            blocks_ids: prev_blocks_ids,

            pure_states: pure_prev_states,
            pure_state_root: pure_prev_state_root,

            gen_chain_time,
            gen_lt,
            total_validator_fees,
            wu_used_from_last_anchor,

            processed_upto: processed_upto_info.try_into()?,

            prev_queue_diff_hashes,
        };

        Ok((prev_data, usage_tree))
    }

    pub fn pure_states(&self) -> &Vec<ShardStateStuff> {
        &self.pure_states
    }

    pub fn observable_states(&self) -> &Vec<ShardStateStuff> {
        &self.observable_states
    }

    pub fn observable_accounts(&self) -> &ShardAccounts {
        &self.observable_accounts
    }

    pub fn blocks_ids(&self) -> &Vec<BlockId> {
        &self.blocks_ids
    }

    pub fn get_blocks_ref(&self) -> Result<PrevBlockRef> {
        if self.pure_states.is_empty() || self.pure_states.len() > 2 {
            bail!(
                "There should be 1 or 2 prev states. Actual count is {}",
                self.pure_states.len()
            )
        }

        let mut block_refs = vec![];
        for state in self.pure_states.iter() {
            block_refs.push(BlockRef {
                end_lt: state.state().gen_lt,
                seqno: state.block_id().seqno,
                root_hash: state.block_id().root_hash,
                file_hash: state.block_id().file_hash,
            });
        }

        let prev_ref = if block_refs.len() == 2 {
            PrevBlockRef::AfterMerge {
                left: block_refs.remove(0),
                right: block_refs.remove(0),
            }
        } else {
            PrevBlockRef::Single(block_refs.remove(0))
        };

        Ok(prev_ref)
    }

    pub fn ref_mc_state_handle(&self) -> &RefMcStateHandle {
        self.observable_states[0].ref_mc_state_handle()
    }

    pub fn pure_state_root(&self) -> &Cell {
        &self.pure_state_root
    }

    pub fn gen_chain_time(&self) -> u64 {
        self.gen_chain_time
    }

    pub fn gen_lt(&self) -> u64 {
        self.gen_lt
    }

    pub fn wu_used_from_last_anchor(&self) -> u64 {
        self.wu_used_from_last_anchor
    }

    pub fn total_validator_fees(&self) -> &CurrencyCollection {
        &self.total_validator_fees
    }

    pub fn processed_upto(&self) -> &ProcessedUptoInfoStuff {
        &self.processed_upto
    }

    pub fn prev_queue_diff_hashes(&self) -> &Vec<HashBytes> {
        &self.prev_queue_diff_hashes
    }
}

#[derive(Debug)]
pub(super) struct BlockCollationDataBuilder {
    pub block_id_short: BlockIdShort,
    pub gen_utime: u32,
    pub gen_utime_ms: u16,
    shards: Option<FastHashMap<ShardIdent, Box<ShardDescription>>>,
    pub shards_max_end_lt: u64,
    pub shard_fees: ShardFees,
    pub min_ref_mc_seqno: u32,
    pub rand_seed: HashBytes,
    #[cfg(feature = "block-creator-stats")]
    pub block_create_count: FastHashMap<HashBytes, u64>,
    pub created_by: HashBytes,
    pub top_shard_blocks: Vec<TopShardBlockInfo>,

    pub mc_shards_processed_to_by_partitions:
        FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,

    /// Mempool config override for a new genesis
    pub mempool_config_override: Option<MempoolGlobalConfig>,
}

impl BlockCollationDataBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_id_short: BlockIdShort,
        rand_seed: HashBytes,
        min_ref_mc_seqno: u32,
        next_chain_time: u64,
        created_by: HashBytes,
        mempool_config_override: Option<MempoolGlobalConfig>,
    ) -> Self {
        let gen_utime = (next_chain_time / 1000) as u32;
        let gen_utime_ms = (next_chain_time % 1000) as u16;
        Self {
            block_id_short,
            gen_utime,
            gen_utime_ms,
            shards_max_end_lt: 0,
            shard_fees: Default::default(),
            min_ref_mc_seqno,
            rand_seed,
            #[cfg(feature = "block-creator-stats")]
            block_create_count: Default::default(),
            created_by,
            shards: None,
            top_shard_blocks: vec![],
            mc_shards_processed_to_by_partitions: Default::default(),
            mempool_config_override,
        }
    }

    pub fn set_shards(&mut self, shards: FastHashMap<ShardIdent, Box<ShardDescription>>) {
        self.shards = Some(shards);
    }

    pub fn shards_mut(&mut self) -> Result<&mut FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_mut()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }

    pub fn shards(&self) -> Result<&FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_ref()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }

    pub fn update_shards_max_end_lt(&mut self, val: u64) {
        if val > self.shards_max_end_lt {
            self.shards_max_end_lt = val;
        }
    }

    pub fn store_shard_fees(
        &mut self,
        shard_id: ShardIdent,
        proof_funds: ShardFeeCreated,
    ) -> Result<()> {
        self.shard_fees.set(
            ShardIdentFull::from(shard_id),
            proof_funds.clone(),
            proof_funds,
        )?;
        Ok(())
    }

    #[cfg(feature = "block-creator-stats")]
    pub fn register_shard_block_creators(&mut self, creators: Vec<HashBytes>) -> Result<()> {
        for creator in creators {
            self.block_create_count
                .entry(creator)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        Ok(())
    }

    pub fn build(self, start_lt: u64, block_limits: BlockLimits) -> BlockCollationData {
        let block_limit = BlockLimitStats::new(block_limits, start_lt);
        BlockCollationData {
            block_id_short: self.block_id_short,
            gen_utime: self.gen_utime,
            gen_utime_ms: self.gen_utime_ms,
            min_ref_mc_seqno: self.min_ref_mc_seqno,
            rand_seed: self.rand_seed,
            created_by: self.created_by,
            shards: self.shards,
            top_shard_blocks: self.top_shard_blocks,
            shard_fees: self.shard_fees,
            value_flow: Default::default(),
            block_limit,
            start_lt,
            next_lt: start_lt + 1,
            tx_count: 0,
            shard_accounts_count: 0,
            updated_accounts_count: 0,
            added_accounts_count: 0,
            removed_accounts_count: 0,
            total_execute_msgs_time_mc: 0,
            execute_count_all: 0,
            execute_count_ext: 0,
            ext_msgs_error_count: 0,
            ext_msgs_skipped_count: 0,
            execute_count_int: 0,
            execute_count_new_int: 0,
            int_enqueue_count: 0,
            int_dequeue_count: 0,
            // read_ext_msgs_count: 0,
            // read_int_msgs_from_iterator_count: 0,
            new_msgs_created_count: 0,
            inserted_new_msgs_count: 0,
            // read_new_msgs_count: 0,
            in_msgs: Default::default(),
            out_msgs: Default::default(),
            mint_msg: None,
            recover_create_msg: None,
            mempool_config_override: self.mempool_config_override,
            consensus_config_changed: None,
            #[cfg(feature = "block-creator-stats")]
            block_create_count: self.block_create_count,
            diff_tail_len: 0,
            mc_shards_processed_to_by_partitions: self.mc_shards_processed_to_by_partitions,
        }
    }
}

#[derive(Debug)]
pub(super) struct BlockCollationData {
    pub block_id_short: BlockIdShort,
    pub gen_utime: u32,
    pub gen_utime_ms: u16,

    pub tx_count: u64,

    pub shard_accounts_count: u64,
    pub updated_accounts_count: u64,
    pub added_accounts_count: u64,
    pub removed_accounts_count: u64,

    pub block_limit: BlockLimitStats,

    pub total_execute_msgs_time_mc: u128,

    pub execute_count_all: u64,
    pub execute_count_ext: u64,
    pub execute_count_int: u64,
    pub execute_count_new_int: u64,

    pub ext_msgs_error_count: u64,
    pub ext_msgs_skipped_count: u64,

    pub int_enqueue_count: u64,
    pub int_dequeue_count: u64,

    // pub read_ext_msgs_count: u64,
    // pub read_int_msgs_from_iterator_count: u64,
    pub new_msgs_created_count: u64,
    pub inserted_new_msgs_count: u64,
    // pub read_new_msgs_count: u64,
    pub start_lt: u64,
    // Should be updated on each tx finalization from MessagesPreparer.max_lt
    // which is updating during tx execution
    pub next_lt: u64,

    pub in_msgs: BTreeMap<HashBytes, PreparedInMsg>,
    pub out_msgs: BTreeMap<HashBytes, PreparedOutMsg>,

    /// Ids of top blocks from shards that were included in the master block
    pub top_shard_blocks: Vec<TopShardBlockInfo>,

    /// Actual processed to info by each shard from referenced master block:
    /// * will contain copy of data from `McData` when collating shard blocks
    /// * will contain updated info by top shard blocks info when collating master blocks
    ///
    /// Stores a tuple for each shard:
    /// (referenced shard is updated in master block, processed to info by partitions)
    pub mc_shards_processed_to_by_partitions:
        FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,

    shards: Option<FastHashMap<ShardIdent, Box<ShardDescription>>>,

    // TODO: setup update logic when ShardFees would be implemented
    pub shard_fees: ShardFees,

    pub mint_msg: Option<InMsg>,
    pub recover_create_msg: Option<InMsg>,

    pub value_flow: PartialValueFlow,
    pub min_ref_mc_seqno: u32,

    pub rand_seed: HashBytes,

    pub created_by: HashBytes,

    /// Mempool config override for a new genesis
    pub mempool_config_override: Option<MempoolGlobalConfig>,

    /// If current block is a key master block and `ConsensusConfig` was changed.
    /// `None` - if it is a shard block or not a key master block.
    pub consensus_config_changed: Option<bool>,

    #[cfg(feature = "block-creator-stats")]
    pub block_create_count: FastHashMap<HashBytes, u64>,
    // TODO: try remove
    pub diff_tail_len: u32,
}

impl BlockCollationData {
    pub fn get_gen_chain_time(&self) -> u64 {
        self.gen_utime as u64 * 1000 + self.gen_utime_ms as u64
    }

    /// Updates [`Self::value_flow`] and returns its full version.
    pub fn finalize_value_flow(
        &mut self,
        account_blocks: &AccountBlocks,
        shard_accounts: &ShardAccounts,
        in_msgs: &InMsgDescr,
        out_msgs: &OutMsgDescr,
        config: &BlockchainConfig,
    ) -> Result<ValueFlow> {
        let is_masterchain = self.block_id_short.is_masterchain();
        let burning = config.get_burning_config().unwrap_or_default();

        // Add message and transaction fees collected in this block.
        let mut new_fees = account_blocks.root_extra().clone();
        new_fees.try_add_assign_tokens(in_msgs.root_extra().fees_collected)?;

        // Burn a fraction of fees in masterchain.
        if is_masterchain {
            let burned = burning.compute_burned_fees(new_fees.tokens)?;

            self.value_flow.burned.try_add_assign_tokens(burned)?;
            new_fees.try_sub_assign_tokens(burned)?;
        }

        self.value_flow.fees_collected.try_add_assign(&new_fees)?;

        // Finalize value flow.
        Ok(ValueFlow {
            from_prev_block: self.value_flow.from_prev_block.clone(),
            to_next_block: shard_accounts.root_extra().balance.clone(),
            imported: in_msgs.root_extra().value_imported.clone(),
            exported: out_msgs.root_extra().clone(),
            fees_collected: self.value_flow.fees_collected.clone(),
            burned: self.value_flow.burned.clone(),
            fees_imported: self.shard_fees.root_extra().fees.clone(),
            recovered: self.value_flow.recovered.clone(),
            created: self.value_flow.created.clone(),
            minted: self.value_flow.minted.clone(),
        })
    }
}

#[derive(Default, Debug)]
pub struct PartialValueFlow {
    /// Total amount transferred from the previous block.
    pub from_prev_block: CurrencyCollection,
    /// Fee recovery.
    pub recovered: CurrencyCollection,
    /// Block creation fees.
    pub created: CurrencyCollection,
    /// Minted native tokens and extra currencies.
    pub minted: CurrencyCollection,
    /// Burned native tokens and extra currencies.
    pub burned: CurrencyCollection,
    /// Fees collected on init.
    pub fees_collected: CurrencyCollection,
}

#[derive(Debug)]
pub struct BlockLimitStats {
    pub gas_used: u64,
    pub lt_current: u64,
    pub lt_start: u64,
    pub cells_bits: u32,
    pub block_limits: BlockLimits,
}

impl BlockLimitStats {
    pub fn new(block_limits: BlockLimits, lt_start: u64) -> Self {
        Self {
            gas_used: 0,
            lt_current: lt_start,
            lt_start,
            cells_bits: 0,
            block_limits,
        }
    }

    pub fn reached(&self, level: BlockLimitsLevel) -> bool {
        let BlockLimits {
            bytes,
            gas,
            lt_delta,
        } = &self.block_limits;

        let BlockParamLimits {
            soft_limit,
            hard_limit,
            ..
        } = bytes;

        let cells_bytes = self.cells_bits / 8;
        if cells_bytes >= *hard_limit {
            return true;
        }
        if cells_bytes >= *soft_limit && level == BlockLimitsLevel::Soft {
            return true;
        }

        let BlockParamLimits {
            soft_limit,
            hard_limit,
            ..
        } = gas;

        if self.gas_used >= *hard_limit as u64 {
            return true;
        }
        if self.gas_used >= *soft_limit as u64 && level == BlockLimitsLevel::Soft {
            return true;
        }

        let BlockParamLimits {
            soft_limit,
            hard_limit,
            ..
        } = lt_delta;

        let delta_lt = u32::try_from(self.lt_current - self.lt_start).unwrap_or(u32::MAX);
        if delta_lt >= *hard_limit {
            return true;
        }
        if delta_lt >= *soft_limit && level == BlockLimitsLevel::Soft {
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum BlockLimitsLevel {
    Soft,
    Hard,
}

#[derive(Debug)]
pub struct PreparedInMsg {
    pub in_msg: Lazy<InMsg>,
    pub import_fees: ImportFees,
}

#[derive(Debug)]
pub struct PreparedOutMsg {
    pub out_msg: Lazy<OutMsg>,
    pub exported_value: CurrencyCollection,
    pub new_tx: Option<Lazy<Transaction>>,
}

impl BlockCollationData {
    pub fn shards(&self) -> Option<&FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards.as_ref()
    }

    pub fn get_shards(&self) -> Result<&FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_ref()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }

    pub fn get_shards_mut(
        &mut self,
    ) -> Result<&mut FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_mut()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }

    pub fn update_ref_min_mc_seqno(&mut self, mc_seqno: u32) -> u32 {
        self.min_ref_mc_seqno = std::cmp::min(self.min_ref_mc_seqno, mc_seqno);
        self.min_ref_mc_seqno
    }
}

#[derive(Debug, Default)]
pub(super) struct CollatorStats {
    pub total_execute_msgs_time_mc: u128,
    pub avg_exec_msgs_per_1000_ms: u128,

    pub total_execute_count_all: u64,
    pub total_execute_count_ext: u64,
    pub total_execute_count_int: u64,
    pub total_execute_count_new_int: u64,
    pub int_queue_length: u64,
    pub tps_block: u32,
    pub tps_timer: Option<std::time::Instant>,
    pub tps_execute_count: u64,
    pub tps: u128,
}

#[derive(Debug, Clone)]
pub(super) struct AnchorInfo {
    pub id: MempoolAnchorId,
    pub ct: u64,
    pub all_exts_count: usize,
    #[allow(dead_code)]
    pub our_exts_count: usize,
    pub author: PeerId,
}

impl AnchorInfo {
    pub fn from_anchor(anchor: Arc<MempoolAnchor>, our_exts_count: usize) -> AnchorInfo {
        Self {
            id: anchor.id,
            ct: anchor.chain_time,
            all_exts_count: anchor.externals.len(),
            our_exts_count,
            author: anchor.author,
        }
    }
}

pub(super) type AccountId = HashBytes;

#[derive(Clone)]
pub(super) struct ShardAccountStuff {
    pub workchain_id: i8,
    pub account_addr: AccountId,
    pub shard_account: ShardAccount,
    pub special: SpecialFlags,
    pub initial_state_hash: HashBytes,
    pub balance: CurrencyCollection,
    pub public_libs_diff: AccountPublicLibsDiff,
    pub exists: AccountExistence,
    pub transactions: BTreeMap<u64, (CurrencyCollection, Lazy<Transaction>)>,
    pub min_next_lt: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum AccountExistence {
    Exists,
    NotExists,
    Created,
    Removed,
}

pub type AccountPublicLibsDiff = FastHashMap<HashBytes, Option<Cell>>;

impl ShardAccountStuff {
    pub fn new(
        workchain_id: i8,
        account_addr: &AccountId,
        shard_account: ShardAccount,
    ) -> Result<Self> {
        let initial_state_hash = *shard_account.account.inner().repr_hash();

        let mut special = SpecialFlags::default();
        let balance;
        let exists;

        if let Some(account) = shard_account.load_account()? {
            if let AccountState::Active(StateInit {
                special: acc_flags, ..
            }) = account.state
            {
                special = acc_flags.unwrap_or_default();
            }
            balance = account.balance;
            exists = AccountExistence::Exists;
        } else {
            balance = CurrencyCollection::ZERO;
            exists = AccountExistence::NotExists;
        }

        Ok(Self {
            workchain_id,
            account_addr: *account_addr,
            shard_account,
            special,
            initial_state_hash,
            balance,
            public_libs_diff: FastHashMap::new(),
            exists,
            transactions: Default::default(),
            // NOTE: Since there were no transactions on this account yet we don't
            //       need to additionaly align the execution LT.
            //       (considering that the block LT is also aligned)
            min_next_lt: 0,
        })
    }

    pub fn new_empty(workchain_id: i8, account_addr: &AccountId) -> Self {
        static EMPTY_SHARD_ACCOUNT: OnceLock<ShardAccount> = OnceLock::new();

        let shard_account = EMPTY_SHARD_ACCOUNT
            .get_or_init(|| ShardAccount {
                account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
                last_trans_hash: Default::default(),
                last_trans_lt: 0,
            })
            .clone();

        let initial_state_hash = *shard_account.account.inner().repr_hash();

        Self {
            workchain_id,
            account_addr: *account_addr,
            shard_account,
            special: Default::default(),
            initial_state_hash,
            balance: CurrencyCollection::ZERO,
            public_libs_diff: FastHashMap::new(),
            exists: AccountExistence::NotExists,
            transactions: Default::default(),
            // NOTE: No need to align logical time for non-existing accounts.
            min_next_lt: 0,
        }
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.shard_account.load_account()?.is_none())
    }

    /// Aligns the provided `lt` with the logical time of the last transaction on account.
    /// Prevents reusing the same lt for uninit accounts in a single execution group.
    pub fn align_min_lt(&self, lt: u64) -> u64 {
        std::cmp::max(lt, self.min_next_lt)
    }

    pub fn make_std_addr(&self) -> StdAddr {
        StdAddr::new(self.workchain_id, self.account_addr)
    }

    pub fn build_hash_update(&self) -> Lazy<HashUpdate> {
        Lazy::new(&HashUpdate {
            old: self.initial_state_hash,
            new: *self.shard_account.account.inner().repr_hash(),
        })
        .unwrap()
    }

    pub fn apply_transaction(
        &mut self,
        new_state: ShardAccount,
        account_meta: AccountMeta,
        tx: Lazy<Transaction>,
        tx_meta: &TransactionMeta,
        mut public_libs_diff: Vec<PublicLibraryChange>,
    ) {
        use std::collections::hash_map;

        let is_masterchain = self.workchain_id as i32 == ShardIdent::MASTERCHAIN.workchain();
        debug_assert!(
            is_masterchain || public_libs_diff.is_empty(),
            "non-empty public libs diff for a non-masterchain account"
        );

        debug_assert!(new_state.last_trans_lt >= self.min_next_lt);

        let lt = new_state.last_trans_lt;

        self.shard_account = new_state;
        self.transactions
            .insert(lt, (tx_meta.total_fees.into(), tx));
        self.balance = account_meta.balance;
        self.min_next_lt = tx_meta.next_lt;

        // update account exists flag
        self.exists = match (self.exists, account_meta.exists) {
            (AccountExistence::NotExists, true) => AccountExistence::Created,
            (AccountExistence::Created, false) => AccountExistence::NotExists,
            (AccountExistence::Exists, false) => AccountExistence::Removed,
            (AccountExistence::Removed, true) => AccountExistence::Exists,
            (state, _) => state,
        };

        if is_masterchain {
            // Sort diff in reverse order (sort must be stable here).
            public_libs_diff.sort_by(|a, b| a.lib_hash().cmp(b.lib_hash()).reverse());

            // Merge diff with the previous.
            let mut prev_hash = None::<HashBytes>;
            for op in public_libs_diff {
                let hash = op.lib_hash();
                if matches!(&prev_hash, Some(prev_hash) if prev_hash == hash) {
                    // Skip duplicates (only the last change will be used because of reverse order).
                    continue;
                }
                prev_hash = Some(*hash);

                match self.public_libs_diff.entry(*hash) {
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(match op {
                            PublicLibraryChange::Add(cell) => Some(cell),
                            PublicLibraryChange::Remove(_) => None,
                        });
                    }
                    hash_map::Entry::Occupied(entry) => match (entry.get(), op) {
                        // Removed before, added later -> no diff.
                        // Added before, removed later -> no diff.
                        (None, PublicLibraryChange::Add(_))
                        | (Some(_), PublicLibraryChange::Remove(_)) => {
                            entry.remove();
                        }
                        // Removed before, removed now -> diff persists.
                        // Added before, added now -> diff persists.
                        (None, PublicLibraryChange::Remove(_))
                        | (Some(_), PublicLibraryChange::Add(_)) => {}
                    },
                }
            }
        }
    }
}

pub struct PublicLibsDiff {
    pub original: Dict<HashBytes, LibDescr>,
    pub changes: BTreeMap<HashBytes, PublicLibrariesDiffItem>,
}

impl PublicLibsDiff {
    pub fn new(original: Dict<HashBytes, LibDescr>) -> Self {
        Self {
            original,
            changes: BTreeMap::new(),
        }
    }

    pub fn finalize(mut self) -> Result<Dict<HashBytes, LibDescr>> {
        const SMALL_CHANGES: usize = 3;

        match self.changes.len() {
            0 => {}
            // Simple set is faster on a small number of changes.
            1..=SMALL_CHANGES => {
                for (lib_hash, change) in self.changes {
                    match change.finalize()? {
                        None => {
                            self.original.remove(lib_hash)?;
                        }
                        Some(new_descr) => {
                            self.original.set(lib_hash, new_descr)?;
                        }
                    }
                }
            }
            // Otherwise apply a full modify.
            _ => {
                self.original.modify_with_sorted_iter_ext(
                    self.changes.into_iter(),
                    |(key, _)| *key,
                    |(_, value)| value.finalize(),
                    Cell::empty_context(),
                )?;
            }
        }

        Ok(self.original)
    }

    pub fn merge(&mut self, account: &HashBytes, diff: AccountPublicLibsDiff) -> Result<()> {
        use std::collections::btree_map;

        for (lib_hash, new_lib) in diff {
            match self.changes.entry(lib_hash) {
                // A first change of this library in the block.
                btree_map::Entry::Vacant(entry) => {
                    // Find an existing published library first.
                    let existing = self.original.get(lib_hash)?;

                    let (origin, add) = match (existing, new_lib) {
                        // Trying to remove a non-existing library does nothing.
                        // TODO: Check if this even possible, maybe panic/error instead.
                        (None, None) => {
                            tracing::warn!(
                                target: tracing_targets::COLLATOR,
                                %account, %lib_hash,
                                "removing a nonexisting library",
                            );
                            continue;
                        }
                        // Publishing a new library.
                        (None, Some(new_lib)) => (PublicLibraryOrigin::New(new_lib), true),
                        // Library exists, only update publishers.
                        (Some(existing), new_lib) => {
                            (PublicLibraryOrigin::Existing(existing), new_lib.is_some())
                        }
                    };

                    entry.insert(PublicLibrariesDiffItem {
                        origin,
                        publishers: vec![(*account, add)],
                    });
                }
                // Changes to this library by multiple accounts.
                btree_map::Entry::Occupied(mut entry) => {
                    let entry = entry.get_mut();
                    if let PublicLibraryOrigin::New(_) = &entry.origin {
                        // Trying to remove a non-existing library does nothing.
                        // TODO: Check if this even possible, maybe panic/error instead.
                        if new_lib.is_none() {
                            tracing::warn!(
                                target: tracing_targets::COLLATOR,
                                %account, %lib_hash,
                                "removing a nonexisting library",
                            );
                            continue;
                        }
                    }

                    entry.publishers.push((*account, new_lib.is_some()));
                }
            }
        }

        Ok(())
    }
}

pub struct PublicLibrariesDiffItem {
    pub origin: PublicLibraryOrigin,
    pub publishers: Vec<(HashBytes, bool)>,
}

impl PublicLibrariesDiffItem {
    pub fn finalize(mut self) -> Result<Option<LibDescr>, everscale_types::error::Error> {
        match self.origin {
            // New `LibDescr` can be simply built from a list of publishers.
            PublicLibraryOrigin::New(lib) => {
                if self.publishers.is_empty() {
                    // Might be unreachable but handle just in case.
                    return Ok(None);
                }

                self.publishers.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
                let root = dict::build_dict_from_sorted_iter(
                    self.publishers.into_iter().map(|(key, add)| {
                        debug_assert!(add, "must not remove publishers from nonexisting libraries");
                        (key, ())
                    }),
                    Cell::empty_context(),
                )?;
                debug_assert!(root.is_some());

                Ok(Some(LibDescr {
                    lib,
                    publishers: Dict::from_raw(root),
                }))
            }
            // Existing `LibDescr` must be modified.
            PublicLibraryOrigin::Existing(mut descr) => {
                if self.publishers.is_empty() {
                    return Ok(Some(descr));
                }

                self.publishers.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
                descr.publishers.modify_with_sorted_iter_ext(
                    self.publishers,
                    |(key, _)| *key,
                    |(_, add)| Ok(add.then_some(())),
                    Cell::empty_context(),
                )?;
                if descr.publishers.is_empty() {
                    // All publishers were removed so the library should also be removed.
                    return Ok(None);
                }

                Ok(Some(descr))
            }
        }
    }
}

pub enum PublicLibraryOrigin {
    Existing(LibDescr),
    New(Cell),
}

pub trait ShardDescriptionExt {
    fn from_block_info(
        block_id: BlockId,
        block_info: &BlockInfo,
        ext_processed_to_anchor_id: u32,
        value_flow: &ValueFlow,
    ) -> ShardDescription;
}

impl ShardDescriptionExt for ShardDescription {
    fn from_block_info(
        block_id: BlockId,
        block_info: &BlockInfo,
        ext_processed_to_anchor_id: u32,
        value_flow: &ValueFlow,
    ) -> ShardDescription {
        ShardDescription {
            seqno: block_id.seqno,
            reg_mc_seqno: 0,
            start_lt: block_info.start_lt,
            end_lt: block_info.end_lt,
            root_hash: block_id.root_hash,
            file_hash: block_id.file_hash,
            before_split: block_info.before_split,
            before_merge: false, // TODO: by t-node, needs to review
            want_split: block_info.want_split,
            want_merge: block_info.want_merge,
            nx_cc_updated: false, // TODO: by t-node, needs to review
            next_catchain_seqno: block_info.gen_catchain_seqno,
            ext_processed_to_anchor_id,
            top_sc_block_updated: false,
            min_ref_mc_seqno: block_info.min_ref_mc_seqno,
            gen_utime: block_info.gen_utime,
            split_merge_at: None, // TODO: check if we really should not use it here
            fees_collected: value_flow.fees_collected.clone(),
            funds_created: value_flow.created.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BlockSerializerCache {
    inner: Arc<Mutex<BlockSerializerCacheInner>>,
}

impl BlockSerializerCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BlockSerializerCacheInner {
                boc_header: BocHeaderCache::with_capacity(capacity),
            })),
        }
    }

    pub fn take_boc_header_cache(&self) -> BocHeaderCache {
        std::mem::take(&mut self.inner.lock().boc_header)
    }

    pub fn set_boc_header_cache(&self, boc_header: BocHeaderCache) {
        self.inner.lock().boc_header = boc_header;
    }
}

struct BlockSerializerCacheInner {
    boc_header: BocHeaderCache,
}

type BocHeaderCache = boc::ser::BocHeaderCache<ahash::RandomState>;

#[derive(Clone, Debug)]
pub struct ExecutedTransaction {
    pub transaction: Lazy<Transaction>,
    pub out_msgs: Vec<Lazy<OwnedMessage>>,
    pub gas_used: u64,
    pub next_lt: u64,
    pub burned: Tokens,
}

#[derive(Clone, Debug)]
pub struct SkippedTransaction {
    pub gas_used: u64,
}

pub struct ParsedMessage {
    pub info: MsgInfo,
    pub dst_in_current_shard: bool,
    pub cell: Cell,
    pub special_origin: Option<SpecialOrigin>,
    pub block_seqno: Option<BlockSeqno>,
    pub from_same_shard: Option<bool>,
    pub ext_msg_chain_time: Option<u64>,
}

impl ParsedMessage {
    pub fn kind(&self) -> ParsedMessageKind {
        match (&self.info, self.special_origin) {
            (_, Some(SpecialOrigin::Recover)) => ParsedMessageKind::Recover,
            (_, Some(SpecialOrigin::Mint)) => ParsedMessageKind::Mint,
            (MsgInfo::ExtIn(_), _) => ParsedMessageKind::ExtIn,
            (MsgInfo::Int(_), _) => ParsedMessageKind::Int,
            (MsgInfo::ExtOut(_), _) => ParsedMessageKind::ExtOut,
        }
    }

    pub fn is_external(&self) -> bool {
        matches!(self.info, MsgInfo::ExtIn(_) | MsgInfo::ExtOut(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedMessageKind {
    Recover,
    Mint,
    ExtIn,
    Int,
    ExtOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialOrigin {
    Recover,
    Mint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dequeued {
    pub same_shard: bool,
}

#[derive(Default, Clone)]
pub struct AnchorsCache {
    /// The cache of imported from mempool anchors that were not processed yet.
    /// Anchor is removed from the cache when all its externals are processed.
    cache: VecDeque<(MempoolAnchorId, Arc<MempoolAnchor>)>,

    last_imported_anchor: Option<AnchorInfo>,

    has_pending_externals: bool,
}

impl AnchorsCache {
    pub fn set_last_imported_anchor_info(&mut self, anchor_info: AnchorInfo) {
        self.last_imported_anchor = Some(anchor_info);
    }

    pub fn last_imported_anchor(&self) -> Option<&AnchorInfo> {
        self.last_imported_anchor.as_ref()
    }

    pub fn get_last_imported_anchor_id_and_ct(&self) -> Option<(u32, u64)> {
        self.last_imported_anchor
            .as_ref()
            .map(|anchor| (anchor.id, anchor.ct))
    }

    pub fn insert(&mut self, anchor: Arc<MempoolAnchor>, our_exts_count: usize) {
        if our_exts_count > 0 {
            self.has_pending_externals = true;
            self.cache.push_back((anchor.id, anchor.clone()));
        }
        self.last_imported_anchor = Some(AnchorInfo::from_anchor(anchor, our_exts_count));
    }

    pub fn remove(&mut self, index: usize) -> Option<(MempoolAnchorId, Arc<MempoolAnchor>)> {
        if index == 0 {
            self.cache.pop_front()
        } else {
            self.cache.remove(index)
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.last_imported_anchor = None;
        self.has_pending_externals = false;
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&self, index: usize) -> Option<(MempoolAnchorId, Arc<MempoolAnchor>)> {
        self.cache.get(index).cloned()
    }

    pub fn has_pending_externals(&self) -> bool {
        self.has_pending_externals
    }

    pub fn set_has_pending_externals(&mut self, has_pending_externals: bool) {
        self.has_pending_externals = has_pending_externals;
    }
}

pub struct FinalizeMessagesReaderResult {
    pub queue_diff: SerializedQueueDiff,
    pub has_unprocessed_messages: bool,
    pub reader_state: ReaderState,
    pub processed_upto: ProcessedUptoInfoStuff,
    pub anchors_cache: AnchorsCache,
}

pub struct FinalizeCollationResult {
    pub handle_block_candidate_elapsed: Duration,
}

pub struct ExecuteResult {
    pub prepare_msg_groups_wu: PrepareMsgGroupsWu,
    /// Accumulated messages reader metrics across all partitions
    pub msgs_reader_metrics: MessagesReaderMetrics,

    pub execute_wu: ExecuteWu,
    pub execute_metrics: ExecuteMetrics,
}

pub struct FinalizeBlockResult {
    pub collation_data: Box<BlockCollationData>,
    pub block_candidate: Box<BlockCandidate>,
    pub mc_data: Option<Arc<McData>>,
    pub old_mc_data: Arc<McData>,
    pub new_state_root: Cell,
    pub new_observable_state: Box<ShardStateUnsplit>,
    pub finalize_wu: FinalizeWu,
    pub finalize_metrics: FinalizeMetrics,
    pub collation_config: Arc<CollationConfig>,
}

pub struct CollationResult {
    pub final_result: FinalResult,
    pub finalized: FinalizeBlockResult,
    pub reader_state: ReaderState,
    pub anchors_cache: AnchorsCache,
    pub execute_result: ExecuteResult,
}

pub struct FinalResult {
    pub prepare_elapsed: Duration,
    pub has_unprocessed_messages: bool,
}

#[derive(Debug)]
pub enum ForceMasterCollation {
    No,
    ByUncommittedChain,
    ByAnchorImportSkipped,
    ByUprocessedMessages,
}
impl ForceMasterCollation {
    pub fn is_forced(&self) -> bool {
        !matches!(self, Self::No)
    }
}

/// Rand seed for block source data.
#[derive(Debug, Clone, Hash, PartialEq, Eq, TlWrite)]
#[tl(boxed, id = "collator.randSeed", scheme = "proto.tl")]
pub struct RandSeed {
    #[tl(with = "tycho_block_util::tl::shard_ident")]
    pub shard: ShardIdent,
    pub seqno: u32,
    pub next_chain_time: u64,
}

pub trait MsgsExecutionParamsExtension {
    fn group_slots_fractions(&self) -> Result<BTreeMap<QueuePartitionIdx, u8>>;
    fn open_ranges_limit(&self) -> usize;
}

impl MsgsExecutionParamsExtension for MsgsExecutionParams {
    fn group_slots_fractions(&self) -> Result<BTreeMap<QueuePartitionIdx, u8>> {
        let mut res = BTreeMap::new();
        for item in self.group_slots_fractions.iter() {
            let (par_id, fraction) = item?;
            res.insert(QueuePartitionIdx(par_id), fraction);
        }
        Ok(res)
    }

    fn open_ranges_limit(&self) -> usize {
        self.open_ranges_limit.max(2) as usize
    }
}

type DiffMaxMessage = QueueKey;

#[derive(Debug)]
pub struct QueueStatisticsWithRemaning {
    /// Statistics shows all messages count
    pub initial_stats: QueueStatistics,
    /// Statistics shows remaining not read messages.
    /// We reduce initial statistics by the number of messages that were read.
    pub remaning_stats: ConcurrentQueueStatistics,
}

pub struct CumulativeStatistics {
    /// Cumulative statistics created for this shard. When reader reads messages, it decrements `remaining messages`
    /// Another shard stats can be decremented only by calling `update_processed_to_by_partitions`
    for_shard: ShardIdent,
    /// Actual processed to info for master and all shards
    all_shards_processed_to_by_partitions: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,

    /// Stores per-shard statistics, keyed by `ShardIdent`.
    /// Each shard has a `FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>`
    shards_stats_by_partitions: FastHashMap<ShardIdent, SeparatedStatisticsByPartitions>,

    /// The final aggregated statistics (across all shards) by partitions.
    result: FastHashMap<QueuePartitionIdx, QueueStatisticsWithRemaning>,
}

impl CumulativeStatistics {
    pub fn new(
        for_shard: ShardIdent,
        all_shards_processed_to_by_partitions: FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
    ) -> Self {
        Self {
            for_shard,
            all_shards_processed_to_by_partitions,
            shards_stats_by_partitions: Default::default(),
            result: Default::default(),
        }
    }

    /// Create range and full load statistics and store it
    pub fn load<V: InternalMessageValue>(
        &mut self,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
        partitions: &FastHashSet<QueuePartitionIdx>,
        prev_state_gen_lt: Lt,
        mc_state_gen_lt: Lt,
        mc_top_shards_end_lts: &FastHashMap<ShardIdent, Lt>,
    ) -> Result<()> {
        let ranges = Self::compute_cumulative_stats_ranges(
            &self.for_shard,
            &self.all_shards_processed_to_by_partitions,
            prev_state_gen_lt,
            mc_state_gen_lt,
            mc_top_shards_end_lts,
        );

        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "cumulative_stats_ranges: {:?}",
            ranges
        );

        self.load_internal(mq_adapter, partitions, ranges)
    }

    /// Partially loads diff statistics for the given ranges.
    /// Automatically applies `processed_to` filtering and updates internal state.
    pub fn load_partial<V: InternalMessageValue>(
        &mut self,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: Vec<QueueShardBoundedRange>,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "cumulative_stats_partial_ranges: {:?}",
            ranges
        );

        self.load_internal(mq_adapter, partitions, ranges)
    }

    /// Internal helper to load and apply diff statistics.
    fn load_internal<V: InternalMessageValue>(
        &mut self,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: Vec<QueueShardBoundedRange>,
    ) -> Result<()> {
        for range in ranges {
            let stats_by_partitions = mq_adapter
                .load_separated_diff_statistics(partitions, &range)
                .with_context(|| format!("partitions: {partitions:?}; range: {range:?}"))?;

            for (partition, partition_stats) in stats_by_partitions {
                for (diff_max_message, diff_partition_stats) in partition_stats {
                    self.apply(
                        partition,
                        range.shard_ident,
                        diff_max_message,
                        diff_partition_stats,
                    );
                }
            }
        }

        Ok(())
    }

    /// Update `all_shards_processed_to_by_partitions` and remove
    /// processed data
    pub fn update_processed_to_by_partitions(
        &mut self,
        new_pt: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
    ) {
        if self.all_shards_processed_to_by_partitions == new_pt {
            return;
        }
        for (&dst_shard, (_, ref processed_to)) in &new_pt {
            let changed = match self.all_shards_processed_to_by_partitions.get(&dst_shard) {
                Some((_, old_pt)) => old_pt != processed_to,
                None => true,
            };

            if changed {
                self.handle_processed_to_update(dst_shard, processed_to.clone());
            }
        }

        self.shards_stats_by_partitions
            .retain(|_src_shard, by_partitions| {
                by_partitions.retain(|_part, diffs| !diffs.is_empty());
                !by_partitions.is_empty()
            });

        self.all_shards_processed_to_by_partitions = new_pt;
    }

    pub(crate) fn compute_cumulative_stats_ranges(
        current_shard: &ShardIdent,
        all_shards_processed_to_by_partitions: &FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
        prev_state_gen_lt: Lt,
        mc_state_gen_lt: Lt,
        mc_top_shards_end_lts: &FastHashMap<ShardIdent, Lt>,
    ) -> Vec<QueueShardBoundedRange> {
        let mut ranges = vec![];

        let from_ranges = find_min_processed_to_by_shards(all_shards_processed_to_by_partitions);

        for (shard_ident, from) in from_ranges {
            let to_lt = if shard_ident.is_masterchain() {
                mc_state_gen_lt
            } else if shard_ident == *current_shard {
                prev_state_gen_lt
            } else {
                *mc_top_shards_end_lts.get(&shard_ident).unwrap()
            };

            let to = Bound::Included(QueueKey::max_for_lt(to_lt));

            ranges.push(QueueShardBoundedRange {
                shard_ident,
                from: Bound::Excluded(from),
                to,
            });
        }

        ranges
    }

    /// Adds diff stats weeding processed accounts according to `processed_to` info
    fn apply(
        &mut self,
        partition: QueuePartitionIdx,
        diff_shard: ShardIdent,
        diff_max_message: DiffMaxMessage,
        mut diff_partition_stats: AccountStatistics,
    ) {
        for (dst_shard, (_, shard_processed_to_by_partitions)) in
            &self.all_shards_processed_to_by_partitions
        {
            if let Some(partition_processed_to) = shard_processed_to_by_partitions.get(&partition) {
                // get processed_to border for diff's shard in the destination shard
                if let Some(to_key) = partition_processed_to.get(&diff_shard) {
                    // if diff is below processed_to border
                    // then remove accounts of destination shard from stats
                    if diff_max_message <= *to_key {
                        diff_partition_stats
                            .retain(|dst_acc, _| !dst_shard.contains_address(dst_acc));
                    }
                }
            }
        }

        let entry = self
            .result
            .entry(partition)
            .or_insert_with(|| QueueStatisticsWithRemaning {
                initial_stats: QueueStatistics::default(),
                remaning_stats: ConcurrentQueueStatistics::new(self.for_shard),
            });
        entry.initial_stats.append(&diff_partition_stats);
        entry.remaning_stats.append(&diff_partition_stats);

        // finally add weeded stats
        self.add_diff_partition_stats(
            partition,
            diff_shard,
            diff_max_message,
            diff_partition_stats,
        );
    }

    fn add_diff_partition_stats(
        &mut self,
        partition: QueuePartitionIdx,
        diff_shard: ShardIdent,
        diff_max_message: DiffMaxMessage,
        diff_partition_stats: AccountStatistics,
    ) {
        // update diffs stats collection
        self.shards_stats_by_partitions
            .entry(diff_shard)
            .or_default()
            .entry(partition)
            .or_default()
            .insert(diff_max_message, diff_partition_stats);
    }

    /// Adds diff stats for a particular shard, split by partitions.
    /// Overwrites any existing data for the same `shard_id`.
    pub fn add_diff_stats(
        &mut self,
        diff_shard: ShardIdent,
        diff_max_message: DiffMaxMessage,
        diff_stats: DiffStatistics,
    ) {
        for (&partition, diff_partition_stats) in diff_stats.iter() {
            // append to cumulative stats
            let partition_stats =
                self.result
                    .entry(partition)
                    .or_insert_with(|| QueueStatisticsWithRemaning {
                        initial_stats: QueueStatistics::default(),
                        remaning_stats: ConcurrentQueueStatistics::new(self.for_shard),
                    });
            partition_stats.initial_stats.append(diff_partition_stats);
            partition_stats.remaning_stats.append(diff_partition_stats);

            self.add_diff_partition_stats(
                partition,
                diff_shard,
                diff_max_message,
                diff_partition_stats.clone(),
            );
        }
    }

    /// Remove stats for accounts from processed diffs
    pub fn handle_processed_to_update(
        &mut self,
        dst_shard: ShardIdent,
        shard_processed_to_by_partitions: ProcessedToByPartitions,
    ) {
        for (src_shard, shard_stats_by_partitions) in self.shards_stats_by_partitions.iter_mut() {
            for (partition, diffs) in shard_stats_by_partitions.iter_mut() {
                if let Some(partition_processed_to) =
                    shard_processed_to_by_partitions.get(partition)
                {
                    let cumulative_stats = self.result.entry(*partition).or_insert_with(|| {
                        QueueStatisticsWithRemaning {
                            initial_stats: QueueStatistics::default(),
                            remaning_stats: ConcurrentQueueStatistics::new(self.for_shard),
                        }
                    });
                    if let Some(to_key) = partition_processed_to.get(src_shard) {
                        let mut to_remove_diffs = vec![];
                        // find diffs that below processed_to border and remove destination accounts from stats
                        for (diff_max_message, diff_stats) in diffs.iter_mut() {
                            if diff_max_message <= to_key {
                                diff_stats.retain(|dst_acc, count| {
                                    if dst_shard.contains_address(dst_acc) {
                                        cumulative_stats
                                            .initial_stats
                                            .decrement_for_account(dst_acc.clone(), *count);

                                        if self.for_shard != dst_shard {
                                            cumulative_stats
                                                .remaning_stats
                                                .decrement_for_account(dst_acc.clone(), *count);
                                        }
                                        false
                                    } else {
                                        true
                                    }
                                });
                                if diff_stats.is_empty() {
                                    to_remove_diffs.push(*diff_max_message);
                                }
                            } else {
                                // do not need to process diffs above processed_to border
                                break;
                            }
                        }
                        // remove drained diffs
                        for key in to_remove_diffs {
                            diffs.remove(&key);
                        }
                    }
                }
            }
        }

        // update all processed_to state
        self.all_shards_processed_to_by_partitions
            .insert(dst_shard, (true, shard_processed_to_by_partitions));
    }

    /// Returns  a reference to the aggregated stats by partitions.
    /// If the data is marked as dirty, it triggers a lazy recalculation first.
    pub fn result(&mut self) -> &FastHashMap<QueuePartitionIdx, QueueStatisticsWithRemaning> {
        &self.result
    }

    /// Calc aggregated stats among all partitions.
    /// If the data is marked as dirty, it triggers a lazy recalculation first.
    pub fn get_aggregated_result(&mut self) -> QueueStatistics {
        let mut res: Option<QueueStatistics> = None;
        for stats in self.result.values() {
            if let Some(aggregated) = res.as_mut() {
                aggregated.append(stats.initial_stats.statistics());
            } else {
                res.replace(stats.initial_stats.clone());
            }
        }
        res.unwrap_or_default()
    }

    pub fn remaining_total_for_own_shard(&self) -> u64 {
        self.result
            .values()
            .map(|stats| stats.remaning_stats.tracked_total())
            .sum()
    }
}

impl fmt::Debug for CumulativeStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shards_summary: Vec<String> = self
            .shards_stats_by_partitions
            .iter()
            .map(|(shard_id, by_partitions)| {
                let parts: Vec<String> = by_partitions
                    .iter()
                    .map(|(part, diffs)| format!("p{}:{}", part, diffs.len()))
                    .collect();
                format!("{} -> {}", shard_id, parts.join(", "))
            })
            .collect();

        f.debug_struct("CumulativeStatistics")
            .field(
                "processed_to",
                &format!(
                    "{} shards",
                    self.all_shards_processed_to_by_partitions.len()
                ),
            )
            .field("shards_stats_by_partitions", &shards_summary)
            .field("result", &format!("{} partitions", self.result.len()))
            .field("for_shard", &self.for_shard)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentQueueStatistics {
    statistics: Arc<FastDashMap<IntAddr, u64>>,
    track_shard: ShardIdent,
    tracked_total: Arc<AtomicU64>,
}

impl ConcurrentQueueStatistics {
    pub fn new(track_shard: ShardIdent) -> Self {
        Self {
            statistics: Arc::new(Default::default()),
            track_shard,
            tracked_total: Arc::new(Default::default()),
        }
    }

    pub fn tracked_total(&self) -> u64 {
        self.tracked_total.load(Ordering::Relaxed)
    }

    pub fn statistics(&self) -> &FastDashMap<IntAddr, u64> {
        &self.statistics
    }

    pub fn contains(&self, account_addr: &IntAddr) -> bool {
        self.statistics.contains_key(account_addr)
    }

    pub fn decrement_for_account(&self, account_addr: IntAddr, count: u64) {
        if let DashMapEntry::Occupied(mut occupied) = self.statistics.entry(account_addr.clone()) {
            let value = occupied.get_mut();
            *value -= count;
            if *value == 0 {
                occupied.remove();
            }
            if self.track_shard.contains_address(&account_addr) {
                self.tracked_total.fetch_sub(count, Ordering::Relaxed);
            }
        } else {
            panic!("attempt to decrement non-existing account");
        }
    }

    pub fn append(&self, other: &AccountStatistics) {
        let mut delta_tracked = 0;

        for (account_addr, &msgs_count) in other {
            self.statistics
                .entry(account_addr.clone())
                .and_modify(|count| *count += msgs_count)
                .or_insert(msgs_count);

            if self.track_shard.contains_address(account_addr) {
                delta_tracked += msgs_count;
            }
        }

        if delta_tracked != 0 {
            self.tracked_total
                .fetch_add(delta_tracked, Ordering::Relaxed);
        }
    }
}

#[derive(Default, Clone)]
struct MsgsExecutionParamsStuffInner {
    current: MsgsExecutionParams,
    new: Option<MsgsExecutionParams>,
}

#[derive(Default, Clone)]
pub(super) struct MsgsExecutionParamsStuff {
    inner: Arc<RwLock<MsgsExecutionParamsStuffInner>>,
}

impl MsgsExecutionParamsStuff {
    pub fn create(
        prev_params: Option<MsgsExecutionParams>,
        mc_data_params: MsgsExecutionParams,
    ) -> Self {
        match prev_params {
            Some(prev_params) => {
                let new = (prev_params != mc_data_params).then_some(mc_data_params);
                Self {
                    inner: Arc::new(RwLock::new(MsgsExecutionParamsStuffInner {
                        current: prev_params,
                        new,
                    })),
                }
            }
            _ => Self {
                inner: Arc::new(RwLock::new(MsgsExecutionParamsStuffInner {
                    current: mc_data_params,
                    new: None,
                })),
            },
        }
    }

    pub fn new_is_some(&self) -> bool {
        self.inner.read().new.is_some()
    }

    pub fn update(&self) {
        let mut guard = self.inner.write();

        let new = guard.new.take();

        if let Some(new) = new {
            guard.current = new;
        }
    }

    #[allow(clippy::wrong_self_convention, clippy::new_ret_no_self)]
    pub fn new(&self) -> MappedRwLockReadGuard<'_, Option<MsgsExecutionParams>> {
        RwLockReadGuard::map(self.inner.read(), |x| &x.new)
    }

    pub fn current(&self) -> MappedRwLockReadGuard<'_, MsgsExecutionParams> {
        RwLockReadGuard::map(self.inner.read(), |x| &x.current)
    }
}

#[derive(Debug, Default)]
pub(super) struct ExecuteMetrics {
    pub execute_groups_vm_only_timer: MetricsTimer,
    pub process_txs_timer: MetricsTimer,

    pub execute_tick_elapsed: Duration,
    pub execute_special_elapsed: Duration,
    pub execute_incoming_msgs_elapsed: Duration,
    pub execute_tock_elapsed: Duration,
}

impl ExecuteMetrics {
    pub fn execute_groups_total_elapsed(&self) -> Duration {
        self.execute_groups_vm_only_timer
            .total_elapsed
            .saturating_add(self.process_txs_timer.total_elapsed)
    }
}

#[derive(Debug, Default)]
pub(super) struct FinalizeMetrics {
    pub create_queue_diff_elapsed: Duration,
    pub apply_queue_diff_elapsed: Duration,

    pub update_shard_accounts_elapsed: Duration,
    pub build_accounts_blocks_elapsed: Duration,

    pub build_accounts_elapsed: Duration,

    pub build_in_msgs_elapsed: Duration,
    pub build_out_msgs_elapsed: Duration,

    pub build_accounts_and_messages_in_parallel_elased: Duration,

    pub build_mc_state_extra_elapsed: Duration,

    pub build_state_update_elapsed: Duration,
    pub build_block_elapsed: Duration,

    pub finalize_block_elapsed: Duration,

    pub total_timer: MetricsTimer,
}
