use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::{Cell, CellBuilder, CellSlice, HashBytes, UsageTree, UsageTreeMode};
use everscale_types::dict::Dict;
use everscale_types::models::{
    Account, AccountState, BaseMessage, BlockId, BlockIdShort, BlockInfo, BlockLimits,
    BlockParamLimits, BlockRef, BlockchainConfig, CollationConfig, CurrencyCollection,
    ExtInMsgInfo, GlobalVersion, HashUpdate, ImportFees, InMsg, InMsgExternal, InMsgFinal, IntAddr,
    IntMsgInfo, IntermediateAddr, Lazy, LibDescr, MsgEnvelope, MsgInfo, OptionalAccount, OutMsg,
    OutMsgDequeueImmediate, OutMsgExternal, OutMsgImmediate, OutMsgNew, PrevBlockRef, ShardAccount,
    ShardAccounts, ShardDescription, ShardFeeCreated, ShardFees, ShardIdent, ShardIdentFull,
    SimpleLib, SpecialFlags, StateInit, TickTock, Transaction, ValueFlow, WorkUnitsParamsExecute,
    WorkUnitsParamsPrepare,
};
use ton_executor::{ExecuteParams, ExecutorOutput, PreloadedBlockchainConfig};
use tycho_block_util::queue::{QueueDiffStuff, QueueKey, SerializedQueueDiff};
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_network::PeerId;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use super::execution_manager::{MessagesExecutor, MessagesReader};
use super::mq_iterator_adapter::QueueIteratorAdapter;
use super::CollatorStdImpl;
use crate::collator::execution_manager::GetNextMessageGroupMode;
use crate::collator::mq_iterator_adapter::InitIteratorMode;
use crate::internal_queue::types::EnqueuedMessage;
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::{McData, ProcessedUptoInfoStuff, ProofFunds};

pub(super) struct WorkingState {
    pub next_block_id_short: BlockIdShort,
    pub mc_data: Arc<McData>,
    pub collation_config: Arc<CollationConfig>,
    pub wu_used_from_last_anchor: u64,
    pub prev_shard_data: Option<PrevData>,
    pub usage_tree: Option<UsageTree>,
    pub has_unprocessed_messages: Option<bool>,
    pub msgs_buffer: MessagesBuffer,
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
    pub processed_upto: ProcessedUptoInfoStuff,
    shards: Option<FastHashMap<ShardIdent, Box<ShardDescription>>>,
    pub shards_max_end_lt: u64,
    pub shard_fees: ShardFees,
    pub value_flow: ValueFlow,
    pub min_ref_mc_seqno: u32,
    pub rand_seed: HashBytes,
    #[cfg(feature = "block-creator-stats")]
    pub block_create_count: FastHashMap<HashBytes, u64>,
    pub created_by: HashBytes,
    pub global_version: GlobalVersion,
    pub top_shard_blocks_ids: Vec<BlockId>,

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
        processed_upto: ProcessedUptoInfoStuff,
        created_by: HashBytes,
        global_version: GlobalVersion,
        mempool_config_override: Option<MempoolGlobalConfig>,
    ) -> Self {
        let gen_utime = (next_chain_time / 1000) as u32;
        let gen_utime_ms = (next_chain_time % 1000) as u16;
        Self {
            block_id_short,
            gen_utime,
            gen_utime_ms,
            processed_upto,
            shards_max_end_lt: 0,
            shard_fees: Default::default(),
            value_flow: Default::default(),
            min_ref_mc_seqno,
            rand_seed,
            #[cfg(feature = "block-creator-stats")]
            block_create_count: Default::default(),
            created_by,
            global_version,
            shards: None,
            top_shard_blocks_ids: vec![],
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

    pub fn update_shards_max_end_lt(&mut self, val: u64) {
        if val > self.shards_max_end_lt {
            self.shards_max_end_lt = val;
        }
    }

    pub fn store_shard_fees(
        &mut self,
        shard_id: ShardIdent,
        proof_funds: ProofFunds,
    ) -> Result<()> {
        let shard_fee_created = ShardFeeCreated {
            fees: proof_funds.fees_collected.clone(),
            create: proof_funds.funds_created.clone(),
        };
        self.shard_fees.set(
            ShardIdentFull::from(shard_id),
            shard_fee_created.clone(),
            shard_fee_created,
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
            processed_upto: self.processed_upto,
            min_ref_mc_seqno: self.min_ref_mc_seqno,
            rand_seed: self.rand_seed,
            created_by: self.created_by,
            global_version: self.global_version,
            shards: self.shards,
            top_shard_blocks_ids: self.top_shard_blocks_ids,
            shard_fees: self.shard_fees,
            value_flow: self.value_flow,
            block_limit,
            start_lt,
            next_lt: start_lt + 1,
            tx_count: 0,
            accounts_count: 0,
            total_execute_msgs_time_mc: 0,
            execute_count_all: 0,
            execute_count_ext: 0,
            ext_msgs_error_count: 0,
            ext_msgs_skipped: 0,
            execute_count_int: 0,
            execute_count_new_int: 0,
            int_enqueue_count: 0,
            int_dequeue_count: 0,
            read_ext_msgs: 0,
            read_int_msgs_from_iterator: 0,
            new_msgs_created: 0,
            inserted_new_msgs_to_iterator: 0,
            read_new_msgs_from_iterator: 0,
            in_msgs: Default::default(),
            out_msgs: Default::default(),
            mint_msg: None,
            recover_create_msg: None,
            mempool_config_override: self.mempool_config_override,
            #[cfg(feature = "block-creator-stats")]
            block_create_count: self.block_create_count,
        }
    }
}

#[derive(Debug)]
pub(super) struct BlockCollationData {
    pub block_id_short: BlockIdShort,
    pub gen_utime: u32,
    pub gen_utime_ms: u16,

    pub tx_count: u64,
    pub accounts_count: u64,

    pub block_limit: BlockLimitStats,

    pub total_execute_msgs_time_mc: u128,

    pub execute_count_all: u64,
    pub execute_count_ext: u64,
    pub execute_count_int: u64,
    pub execute_count_new_int: u64,

    pub ext_msgs_error_count: u64,
    pub ext_msgs_skipped: u64,

    pub int_enqueue_count: u64,
    pub int_dequeue_count: u64,

    pub read_ext_msgs: u64,
    pub read_int_msgs_from_iterator: u64,
    pub new_msgs_created: u64,
    pub inserted_new_msgs_to_iterator: u64,
    pub read_new_msgs_from_iterator: u64,

    pub start_lt: u64,
    // Should be updated on each tx finalization from MessagesPreparer.max_lt
    // which is updating during tx execution
    pub next_lt: u64,

    pub in_msgs: BTreeMap<HashBytes, PreparedInMsg>,
    pub out_msgs: BTreeMap<HashBytes, PreparedOutMsg>,

    pub processed_upto: ProcessedUptoInfoStuff,

    /// Ids of top blocks from shards that be included in the master block
    pub top_shard_blocks_ids: Vec<BlockId>,

    shards: Option<FastHashMap<ShardIdent, Box<ShardDescription>>>,

    // TODO: setup update logic when ShardFees would be implemented
    pub shard_fees: ShardFees,

    pub mint_msg: Option<InMsg>,
    pub recover_create_msg: Option<InMsg>,

    pub value_flow: ValueFlow,

    pub min_ref_mc_seqno: u32,

    pub rand_seed: HashBytes,

    pub created_by: HashBytes,

    pub global_version: GlobalVersion,

    /// Mempool config override for a new genesis
    pub mempool_config_override: Option<MempoolGlobalConfig>,

    #[cfg(feature = "block-creator-stats")]
    pub block_create_count: FastHashMap<HashBytes, u64>,
}

impl BlockCollationData {
    pub fn get_gen_chain_time(&self) -> u64 {
        self.gen_utime as u64 * 1000 + self.gen_utime_ms as u64
    }
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
    pub fn shards(&self) -> Result<&FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_ref()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }

    pub fn shards_mut(&mut self) -> Result<&mut FastHashMap<ShardIdent, Box<ShardDescription>>> {
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

pub(super) struct ShardAccountStuff {
    pub account_addr: AccountId,
    pub shard_account: ShardAccount,
    pub special: SpecialFlags,
    pub initial_state_hash: HashBytes,
    pub libraries: Dict<HashBytes, SimpleLib>,
    pub transactions: BTreeMap<u64, (CurrencyCollection, Lazy<Transaction>)>,
}

impl ShardAccountStuff {
    pub fn new(account_addr: &AccountId, shard_account: ShardAccount) -> Result<Self> {
        let initial_state_hash = *shard_account.account.inner().repr_hash();

        // TODO: Add intrinsic to everscale_types for a more optimal way to get libraries
        let (libraries, special) = shard_account
            .load_account()?
            .and_then(|account| {
                if let AccountState::Active(StateInit {
                    libraries, special, ..
                }) = account.state
                {
                    Some((libraries, special.unwrap_or_default()))
                } else {
                    None
                }
            })
            .unwrap_or_default();

        Ok(Self {
            account_addr: *account_addr,
            shard_account,
            special,
            initial_state_hash,
            libraries,
            transactions: Default::default(),
        })
    }

    pub fn new_empty(account_addr: &AccountId) -> Self {
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
            account_addr: *account_addr,
            shard_account,
            special: Default::default(),
            initial_state_hash,
            libraries: Dict::new(),
            transactions: Default::default(),
        }
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.shard_account.load_account()?.is_none())
    }

    pub fn build_hash_update(&self) -> Lazy<HashUpdate> {
        Lazy::new(&HashUpdate {
            old: self.initial_state_hash,
            new: *self.shard_account.account.inner().repr_hash(),
        })
        .unwrap()
    }

    pub fn add_transaction(
        &mut self,
        lt: u64,
        total_fees: CurrencyCollection,
        transaction: Lazy<Transaction>,
    ) {
        self.transactions.insert(lt, (total_fees, transaction));
    }

    pub fn update_public_libraries(
        &self,
        loaded_account: &Option<Account>,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        static EMPTY_LIBS: Dict<HashBytes, SimpleLib> = Dict::new();

        let new_libraries = match loaded_account {
            Some(Account {
                state: AccountState::Active(s),
                ..
            }) => &s.libraries,
            _ => &EMPTY_LIBS,
        };

        if new_libraries.root() == self.libraries.root() {
            return Ok(());
        }

        for entry in new_libraries.iter_union(&self.libraries) {
            let (ref key, new_value, old_value) = entry?;
            match (new_value, old_value) {
                (Some(new), Some(old)) => {
                    if new.public && !old.public {
                        self.add_public_library(key, &new.root, global_libraries)?;
                    } else if !new.public && old.public {
                        self.remove_public_library(key, global_libraries)?;
                    }
                }
                (Some(new), None) if new.public => {
                    self.add_public_library(key, &new.root, global_libraries)?;
                }
                (None, Some(old)) if old.public => {
                    self.remove_public_library(key, global_libraries)?;
                }
                _ => continue,
            }
        }
        Ok(())
    }

    pub fn remove_public_library(
        &self,
        key: &HashBytes,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        tracing::trace!(
            account_addr = %self.account_addr,
            library = %key,
            "removing public library",
        );

        let Some(mut lib_descr) = global_libraries.get(key)? else {
            anyhow::bail!(
                "cannot remove public library {key} of account {} because this public \
                library did not exist",
                self.account_addr
            )
        };

        anyhow::ensure!(
            lib_descr.lib.repr_hash() == key,
            "cannot remove public library {key} of account {} because this public library \
            LibDescr record does not contain a library root cell with required hash",
            self.account_addr
        );

        anyhow::ensure!(
            lib_descr.publishers.remove(self.account_addr)?.is_some(),
            "cannot remove public library {key} of account {} because this public library \
            LibDescr record does not list this account as one of publishers",
            self.account_addr
        );

        if lib_descr.publishers.is_empty() {
            tracing::debug!(
                account_addr = %self.account_addr,
                library = %key,
                "library has no publishers left, removing altogether",
            );
            global_libraries.remove(key)?;
        } else {
            global_libraries.set(key, &lib_descr)?;
        }

        Ok(())
    }

    pub fn add_public_library(
        &self,
        key: &HashBytes,
        library: &Cell,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        tracing::trace!(
            account_addr = %self.account_addr,
            library = %key,
            "adding public library",
        );

        anyhow::ensure!(
            library.repr_hash() == key,
            "cannot add library {key} because its root has a different hash",
        );

        let lib_descr = if let Some(mut old_lib_descr) = global_libraries.get(key)? {
            anyhow::ensure!(
                old_lib_descr.lib.repr_hash() == library.repr_hash(),
                "cannot add public library {key} of account {} because existing LibDescr \
                data has a different root cell hash",
                self.account_addr,
            );

            anyhow::ensure!(
                old_lib_descr.publishers.get(self.account_addr)?.is_none(),
                "cannot add public library {key} of account {} because this public library's \
                LibDescr record already lists this account as a publisher",
                self.account_addr,
            );

            old_lib_descr.publishers.set(self.account_addr, ())?;
            old_lib_descr
        } else {
            let mut dict = Dict::new();
            dict.set(self.account_addr, ())?;
            LibDescr {
                lib: library.clone(),
                publishers: dict,
            }
        };

        global_libraries.set(key, &lib_descr)?;

        Ok(())
    }
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
            copyleft_rewards: Default::default(),
            proof_chain: None,
        }
    }
}

pub struct ParsedMessage {
    pub info: MsgInfo,
    pub dst_in_current_shard: bool,
    pub cell: Cell,
    pub special_origin: Option<SpecialOrigin>,
    pub dequeued: Option<Dequeued>,
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

#[derive(Default)]
pub(super) struct MessageGroups {
    shard_id: ShardIdent,

    offset: u32,
    max_message_key: QueueKey,
    groups: FastHashMap<u32, MessageGroup>,

    int_messages_count: usize,
    ext_messages_count: usize,

    group_limit: usize,
    group_vert_size: usize,
}

impl MessageGroups {
    pub fn new(shard_id: ShardIdent, group_limit: usize, group_vert_size: usize) -> Self {
        Self {
            shard_id,
            group_limit,
            group_vert_size,
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.offset = 0;
        self.max_message_key = QueueKey::MIN;
        self.groups.clear();
        self.int_messages_count = 0;
        self.ext_messages_count = 0;
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn max_message_key(&self) -> &QueueKey {
        &self.max_message_key
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn messages_count(&self) -> usize {
        self.int_messages_count + self.ext_messages_count
    }

    pub fn int_messages_count(&self) -> usize {
        self.int_messages_count
    }

    pub fn ext_messages_count(&self) -> usize {
        self.ext_messages_count
    }

    fn incriment_counters(&mut self, is_int: bool) {
        if is_int {
            self.int_messages_count += 1;
        } else {
            self.ext_messages_count += 1;
        }
    }

    /// add message adjusting groups,
    pub fn add_message(&mut self, msg: Box<ParsedMessage>) {
        assert_eq!(
            msg.special_origin, None,
            "unexpected special origin in ordinary messages set"
        );

        let (account_id, is_int) = match &msg.info {
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) => {
                (dst.as_std().map(|a| a.address).unwrap_or_default(), false)
            }
            MsgInfo::Int(IntMsgInfo {
                dst, created_lt, ..
            }) => {
                self.max_message_key = self.max_message_key.max(QueueKey {
                    lt: *created_lt,
                    hash: *msg.cell.repr_hash(),
                });
                (dst.as_std().map(|a| a.address).unwrap_or_default(), true)
            }
            MsgInfo::ExtOut(info) => {
                unreachable!("ext out message in ordinary messages set: {info:?}")
            }
        };

        self.incriment_counters(is_int);

        let mut offset = self.offset;
        loop {
            let group_entry = self.groups.entry(offset).or_default();

            if group_entry.is_full {
                offset += 1;
                continue;
            }

            let group_len = group_entry.inner.len();
            match group_entry.inner.entry(account_id) {
                Entry::Vacant(entry) => {
                    if group_len < self.group_limit {
                        entry.insert(vec![msg]);
                        group_entry.incriment_counters(is_int);
                        break;
                    }

                    offset += 1;
                }
                Entry::Occupied(mut entry) => {
                    let msgs = entry.get_mut();
                    if msgs.len() < self.group_vert_size {
                        msgs.push(msg);

                        if msgs.len() == self.group_vert_size {
                            group_entry.filling += 1;
                            if group_entry.filling == self.group_limit {
                                group_entry.is_full = true;
                            }
                        }

                        group_entry.incriment_counters(is_int);

                        break;
                    }

                    offset += 1;
                }
            }
        }

        let labels = [("workchain", self.shard_id.workchain().to_string())];
        metrics::gauge!("tycho_do_collate_msgs_exec_buffer_messages_count", &labels)
            .set(self.messages_count() as f64);
    }

    pub fn first_group_is_full(&self) -> bool {
        if let Some(first_group) = self.groups.get(&self.offset) {
            // FIXME: check if first group is full by stats on adding message
            // let first_group_is_full = first_group.len() >= self.group_limit
            //     && first_group
            //         .inner
            //         .values()
            //         .all(|account_msgs| account_msgs.len() >= self.group_vert_size);
            // first_group_is_full

            first_group.is_full
        } else {
            false
        }
    }

    pub fn extract_first_group(&mut self) -> Option<MessageGroup> {
        let first_group_opt = self.extract_first_group_inner();
        if first_group_opt.is_some() {
            self.offset += 1;
        }
        if let Some(first_group) = first_group_opt.as_ref() {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "extracted first message group from message_groups buffer: offset={}, buffer int={}, ext={}, group {}",
                self.offset(), self.int_messages_count(), self.ext_messages_count(),
                DisplayMessageGroup(first_group),
            );
        }
        first_group_opt
    }

    fn extract_first_group_inner(&mut self) -> Option<MessageGroup> {
        if let Some(first_group) = self.groups.remove(&self.offset) {
            self.int_messages_count -= first_group.int_messages_count;
            self.ext_messages_count -= first_group.ext_messages_count;

            Some(first_group)
        } else {
            None
        }
    }

    pub fn extract_merged_group(&mut self) -> Option<MessageGroup> {
        let mut merged_group_opt: Option<MessageGroup> = None;
        while let Some(next_group) = self.extract_first_group_inner() {
            if let Some(merged_group) = merged_group_opt.as_mut() {
                merged_group.int_messages_count += next_group.int_messages_count;
                merged_group.ext_messages_count += next_group.ext_messages_count;
                for (account_id, mut account_msgs) in next_group.inner {
                    if let Some(existing_account_msgs) = merged_group.inner.get_mut(&account_id) {
                        existing_account_msgs.append(&mut account_msgs);
                    } else {
                        merged_group.inner.insert(account_id, account_msgs);
                    }
                }
            } else {
                self.offset += 1;
                merged_group_opt = Some(next_group);
            }
        }
        if let Some(merged_group) = merged_group_opt.as_ref() {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "extracted merged message group of new messages from message_groups buffer: buffer int={}, ext={}, group {}",
                self.int_messages_count(), self.ext_messages_count(),
                DisplayMessageGroup(merged_group),
            );
        }
        merged_group_opt
    }
}

// pub(super) type MessageGroup = FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>;
#[derive(Default)]
pub(super) struct MessageGroup {
    #[allow(clippy::vec_box)]
    inner: FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>,
    int_messages_count: usize,
    ext_messages_count: usize,
    filling: usize,
    is_full: bool,
}

impl MessageGroup {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn messages_count(&self) -> usize {
        self.int_messages_count + self.ext_messages_count
    }

    fn incriment_counters(&mut self, is_int: bool) {
        if is_int {
            self.int_messages_count += 1;
        } else {
            self.ext_messages_count += 1;
        }
    }
}

impl IntoIterator for MessageGroup {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type IntoIter = std::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

pub(super) struct DisplayMessageGroup<'a>(pub &'a MessageGroup);

impl std::fmt::Debug for DisplayMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "int={}, ext={}, ",
            self.0.int_messages_count, self.0.ext_messages_count
        )?;
        let mut l = f.debug_list();
        for messages in self.0.inner.values() {
            l.entry(&messages.len());
        }
        l.finish()
    }
}

#[allow(dead_code)]
pub(super) struct DisplayMessageGroups<'a>(pub &'a MessageGroups);

impl std::fmt::Debug for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in self.0.groups.iter() {
            m.entry(k, &DisplayMessageGroup(v));
        }
        m.finish()
    }
}

pub(super) struct MessagesBuffer {
    /// messages groups
    pub message_groups: MessageGroups,
    /// current read positions of internals mq iterator
    /// when it is not finished
    pub current_iterator_positions: Option<FastHashMap<ShardIdent, QueueKey>>,
    /// current read position for externals
    pub current_ext_reader_position: Option<(u32, u64)>,
}

impl MessagesBuffer {
    pub fn new(shard_id: ShardIdent, group_limit: usize, group_vert_size: usize) -> Self {
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_limit").set(group_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_vert_size")
            .set(group_vert_size as f64);
        Self {
            message_groups: MessageGroups::new(shard_id, group_limit, group_vert_size),
            current_iterator_positions: Some(FastHashMap::default()),
            current_ext_reader_position: None,
        }
    }

    pub fn message_groups_offset(&self) -> u32 {
        self.message_groups.offset()
    }

    pub fn has_pending_messages(&self) -> bool {
        !self.message_groups.is_empty()
    }
}

#[derive(Default)]
pub struct AnchorsCache {
    /// The cache of imported from mempool anchors that were not processed yet.
    /// Anchor is removed from the cache when all its externals are processed.
    cache: VecDeque<(MempoolAnchorId, Arc<MempoolAnchor>)>,

    last_imported_anchor: Option<AnchorInfo>,

    has_pending_externals: bool,
}

impl AnchorsCache {
    pub fn set_last_imported_anchor_info(
        &mut self,
        anchor_id: MempoolAnchorId,
        anchor_ct: u64,
        created_by: HashBytes,
    ) {
        let anchor_info = AnchorInfo {
            id: anchor_id,
            ct: anchor_ct,
            all_exts_count: 0,
            our_exts_count: 0,
            author: PeerId(created_by.0),
        };
        self.last_imported_anchor = Some(anchor_info);
    }

    pub fn get_last_imported_anchor_ct(&self) -> Option<u64> {
        self.last_imported_anchor.as_ref().map(|anchor| anchor.ct)
    }

    pub fn get_last_imported_anchor_author(&self) -> Option<HashBytes> {
        self.last_imported_anchor
            .as_ref()
            .map(|anchor| anchor.author.0.into())
    }

    pub fn get_last_imported_anchor_id_and_ct(&self) -> Option<(u32, u64)> {
        self.last_imported_anchor
            .as_ref()
            .map(|anchor| (anchor.id, anchor.ct))
    }

    pub fn get_last_imported_anchor_id_and_all_exts_counts(&self) -> Option<(u32, u64)> {
        self.last_imported_anchor
            .as_ref()
            .map(|anchor| (anchor.id, anchor.all_exts_count as _))
    }

    pub fn insert(&mut self, anchor: Arc<MempoolAnchor>, our_exts_count: usize) {
        if our_exts_count > 0 {
            self.has_pending_externals = true;
            self.cache.push_back((anchor.id, anchor.clone()));
        }
        self.last_imported_anchor = Some(AnchorInfo::from_anchor(anchor, our_exts_count));
    }

    pub fn remove(&mut self, index: usize) {
        if index == 0 {
            self.cache.pop_front();
        } else {
            self.cache.remove(index);
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
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

    pub fn iter_from_index(&self, index: usize) -> impl Iterator<Item = Arc<MempoolAnchor>> + '_ {
        self.cache
            .iter()
            .skip(index)
            .map(|(_, anchor)| anchor)
            .cloned()
    }
}

pub struct ParsedExternals {
    #[allow(clippy::vec_box)]
    pub ext_messages: Vec<Box<ParsedMessage>>,
    pub current_reader_position: Option<(u32, u64)>,
    pub last_read_to_anchor_chain_time: Option<u64>,
    pub was_stopped_on_prev_read_to_reached: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadNextExternalsMode {
    ToTheEnd,
    ToPreviuosReadTo,
}

pub struct PreparedCollation {
    messages_reader: MessagesReader,
    collation_config: Arc<CollationConfig>,
    collation_data: Box<BlockCollationData>,
    msgs_buffer: MessagesBuffer,
    mc_data: Arc<McData>,
    prev_shard_data: PrevData,
}

impl PreparedCollation {
    pub async fn prepare_collation(
        collator: &mut CollatorStdImpl,
        collation_config: Arc<CollationConfig>,
        mut collation_data: Box<BlockCollationData>,
        mut msgs_buffer: MessagesBuffer,
        mc_data: Arc<McData>,
        prev_shard_data: PrevData,
    ) -> Result<(PreparedCollation, ExecutorWrapper)> {
        // init executor
        let executor = MessagesExecutor::new(
            collator.shard_id,
            collation_data.next_lt,
            Arc::new(PreloadedBlockchainConfig::with_config(
                mc_data.config.clone(),
                mc_data.global_id,
            )?),
            Arc::new(ExecuteParams {
                state_libs: mc_data.libraries.clone(),
                // generated unix time
                block_unixtime: collation_data.gen_utime,
                // block's start logical time
                block_lt: collation_data.start_lt,
                // block random seed
                seed_block: collation_data.rand_seed,
                block_version: collator.config.supported_block_version,
                ..ExecuteParams::default()
            }),
            prev_shard_data.observable_accounts().clone(),
            collation_config.work_units_params.execute.clone(),
        );

        // if this is a masterchain, we must take top shard blocks end lt
        let mc_top_shards_end_lts: Vec<_> = if collator.shard_id.is_masterchain() {
            collation_data
                .shards()?
                .iter()
                .map(|(k, v)| (*k, v.end_lt))
                .collect()
        } else {
            mc_data.shards.iter().map(|(k, v)| (*k, v.end_lt)).collect()
        };

        // create iterator adapter
        let mut mq_iterator_adapter = QueueIteratorAdapter::new(
            collator.shard_id,
            collator.mq_adapter.clone(),
            msgs_buffer.current_iterator_positions.take().unwrap(),
            mc_data.gen_lt,
            prev_shard_data.gen_lt(),
        );

        // we need to init iterator anyway because we need it to add new messages to queue
        tracing::debug!(target: tracing_targets::COLLATOR,
            "init iterator for current ranges"
        );
        mq_iterator_adapter
            .try_init_next_range_iterator(
                &mut collation_data.processed_upto,
                mc_top_shards_end_lts.iter().copied(),
                // We always init first iterator during block collation
                // with current ranges from processed_upto info
                // and do not touch next range before we read all existing messages buffer.
                // In this case the initial iterator range will be equal both
                // on Refill and on Continue.
                InitIteratorMode::OmitNextRange,
            )
            .await?;

        // create messages reader
        let mut messages_reader = MessagesReader::new(
            collator.shard_id,
            collation_config.msgs_exec_params.buffer_limit as _,
            mc_top_shards_end_lts,
        );

        // refill messages buffer and skip groups upto offset (on node restart)
        let prev_processed_offset = collation_data.processed_upto.processed_offset;
        if !msgs_buffer.has_pending_messages() && prev_processed_offset > 0 {
            tracing::debug!(target: tracing_targets::COLLATOR,
                prev_processed_offset,
                "refill messages buffer and skip groups upto",
            );

            while msgs_buffer.message_groups_offset() < prev_processed_offset {
                let msg_group = messages_reader
                    .get_next_message_group(
                        &mut msgs_buffer,
                        &mut collator.anchors_cache,
                        &mut collation_data,
                        &mut mq_iterator_adapter,
                        &QueueKey::MIN,
                        GetNextMessageGroupMode::Refill,
                    )
                    .await?;
                if msg_group.is_none() {
                    // on restart from a new genesis we will not be able to refill buffer with externals
                    // so we stop refilling when there is no more groups in buffer
                    break;
                }
            }

            // next time we should read next message group like we did not make refill before
            // so we need to reset flags that control from where to read messages
            messages_reader.reset_read_flags();
        }

        // holds the max LT_HASH of a new created messages to current shard
        // it needs to define the read range for new messages when we get next message group
        let max_new_message_key_to_current_shard = QueueKey::MIN;

        Ok((
            PreparedCollation {
                messages_reader,
                collation_config,
                collation_data,
                msgs_buffer,
                mc_data,
                prev_shard_data,
            },
            ExecutorWrapper::new(
                executor,
                max_new_message_key_to_current_shard,
                mq_iterator_adapter,
                collator.shard_id,
                collator.mq_adapter.clone(),
            ),
        ))
    }

    pub async fn execute_special_transactions(
        &mut self,
        executor_wrapper: &mut ExecutorWrapper,
    ) -> Result<()> {
        executor_wrapper
            .create_ticktock_transactions(
                &self.mc_data.config,
                TickTock::Tick,
                &mut self.collation_data,
            )
            .await?;

        executor_wrapper
            .create_special_transactions(&self.mc_data.config, &mut self.collation_data)
            .await?;

        Ok(())
    }

    pub async fn execute(
        mut self,
        anchors_cache: &mut AnchorsCache,
        executor_wrapper: &mut ExecutorWrapper,
    ) -> Result<ExecuteCollation> {
        let labels = [(
            "workchain",
            executor_wrapper.shard_id.workchain().to_string(),
        )];

        let mut fill_msgs_total_elapsed = Duration::ZERO;
        let mut execute_msgs_total_elapsed = Duration::ZERO;
        let mut process_txs_total_elapsed = Duration::ZERO;
        let mut execute_groups_wu_vm_only = 0u64;

        let mut executed_groups_count = 0;
        loop {
            let mut timer = std::time::Instant::now();
            let msgs_group_opt = self
                .messages_reader
                .get_next_message_group(
                    &mut self.msgs_buffer,
                    anchors_cache,
                    &mut self.collation_data,
                    &mut executor_wrapper.mq_iterator_adapter,
                    &executor_wrapper.max_new_message_key_to_current_shard,
                    GetNextMessageGroupMode::Continue,
                )
                .await?;
            fill_msgs_total_elapsed += timer.elapsed();

            if let Some(msgs_group) = msgs_group_opt {
                // Execute messages group
                timer = std::time::Instant::now();
                let group_result = executor_wrapper.executor.execute_group(msgs_group).await?;
                execute_msgs_total_elapsed += timer.elapsed();
                executed_groups_count += 1;
                self.collation_data.tx_count += group_result.items.len() as u64;
                self.collation_data.ext_msgs_error_count += group_result.ext_msgs_error_count;
                self.collation_data.ext_msgs_skipped += group_result.ext_msgs_skipped;
                execute_groups_wu_vm_only = execute_groups_wu_vm_only
                    .saturating_add(group_result.total_exec_wu)
                    .saturating_add(group_result.ext_msgs_error_count.saturating_mul(
                        self.collation_config.work_units_params.execute.execute_err as u64,
                    ));

                // Process transactions
                timer = std::time::Instant::now();
                for item in group_result.items {
                    executor_wrapper.process_transaction(
                        item.executor_output,
                        Some(item.in_message),
                        &mut self.collation_data,
                    )?;
                }
                process_txs_total_elapsed += timer.elapsed();

                if self
                    .collation_data
                    .block_limit
                    .reached(BlockLimitsLevel::Hard)
                {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "block limits reached: {:?}", self.collation_data.block_limit,
                    );
                    metrics::counter!("tycho_do_collate_blocks_with_limits_reached_count", &labels)
                        .increment(1);
                    break;
                }
            } else if executor_wrapper
                .mq_iterator_adapter
                .no_pending_existing_internals()
                && executor_wrapper
                    .mq_iterator_adapter
                    .no_pending_new_messages()
            {
                break;
            }
        }

        self.collation_data.total_execute_msgs_time_mc = execute_msgs_total_elapsed.as_millis();

        metrics::gauge!("tycho_do_collate_exec_msgs_groups_per_block", &labels)
            .set(executed_groups_count as f64);

        metrics::histogram!("tycho_do_collate_fill_msgs_total_time", &labels)
            .record(fill_msgs_total_elapsed);

        let init_iterator_elapsed = executor_wrapper
            .mq_iterator_adapter
            .init_iterator_total_elapsed();
        metrics::histogram!("tycho_do_collate_init_iterator_time", &labels)
            .record(init_iterator_elapsed);
        let read_existing_messages_elapsed =
            self.messages_reader.read_existing_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_int_msgs_time", &labels)
            .record(read_existing_messages_elapsed);
        let read_new_messages_elapsed = self.messages_reader.read_new_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_new_msgs_time", &labels)
            .record(read_new_messages_elapsed);
        let read_ext_messages_elapsed = self.messages_reader.read_ext_messages_total_elapsed();
        metrics::histogram!("tycho_do_collate_read_ext_msgs_time", &labels)
            .record(read_ext_messages_elapsed);
        let add_to_message_groups_elapsed =
            self.messages_reader.add_to_message_groups_total_elapsed();
        metrics::histogram!("tycho_do_collate_add_to_msg_groups_time", &labels)
            .record(add_to_message_groups_elapsed);

        metrics::histogram!("tycho_do_collate_exec_msgs_total_time", &labels)
            .record(execute_msgs_total_elapsed);
        metrics::histogram!("tycho_do_collate_process_txs_total_time", &labels)
            .record(process_txs_total_elapsed);

        let last_read_to_anchor_chain_time = self.messages_reader.last_read_to_anchor_chain_time();

        let process_txs_wu = calc_process_txs_wu(
            &self.collation_data,
            &self.collation_config.work_units_params.execute,
        );
        let execute_groups_wu_total = execute_groups_wu_vm_only.saturating_add(process_txs_wu);

        let prepare_groups_wu_total = calc_prepare_groups_wu_total(
            &self.collation_data,
            &self.collation_config.work_units_params.prepare,
        );

        Ok(ExecuteCollation {
            collation_config: self.collation_config,
            collation_data: self.collation_data,
            msgs_buffer: self.msgs_buffer,
            mc_data: self.mc_data,
            prev_shard_data: self.prev_shard_data,
            execute_result: ExecuteResult {
                execute_groups_wu_vm_only,
                process_txs_wu,
                execute_groups_wu_total,
                prepare_groups_wu_total,
                fill_msgs_total_elapsed,
                execute_msgs_total_elapsed,
                process_txs_total_elapsed,
                init_iterator_elapsed,
                read_existing_messages_elapsed,
                read_ext_messages_elapsed,
                read_new_messages_elapsed,
                add_to_message_groups_elapsed,
                last_read_to_anchor_chain_time,
            },
        })
    }
}

pub struct ExecutorWrapper {
    executor: MessagesExecutor,
    max_new_message_key_to_current_shard: QueueKey,
    mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
    shard_id: ShardIdent,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
}

impl ExecutorWrapper {
    pub fn new(
        executor: MessagesExecutor,
        max_new_message_key_to_current_shard: QueueKey,
        mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Self {
        Self {
            executor,
            max_new_message_key_to_current_shard,
            mq_iterator_adapter,
            shard_id,
            mq_adapter,
        }
    }

    pub fn destruct(
        self,
    ) -> (
        MessagesExecutor,
        QueueIteratorAdapter<EnqueuedMessage>,
        ShardIdent,
        Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) {
        (
            self.executor,
            self.mq_iterator_adapter,
            self.shard_id,
            self.mq_adapter,
        )
    }

    pub fn process_transaction(
        &mut self,
        executor_output: ExecutorOutput,
        in_message: Option<Box<ParsedMessage>>,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        let new_messages =
            new_transaction(collation_data, &self.shard_id, executor_output, in_message)?;

        collation_data.new_msgs_created += new_messages.len() as u64;
        for new_message in new_messages {
            let MsgInfo::Int(int_msg_info) = new_message.info else {
                continue;
            };

            if new_message.dst_in_current_shard {
                let new_message_key = QueueKey {
                    lt: int_msg_info.created_lt,
                    hash: *new_message.cell.repr_hash(),
                };
                self.max_new_message_key_to_current_shard =
                    std::cmp::max(self.max_new_message_key_to_current_shard, new_message_key);
            }

            collation_data.inserted_new_msgs_to_iterator += 1;

            let enqueued_message = EnqueuedMessage::from((int_msg_info, new_message.cell));

            self.mq_adapter
                .add_message_to_iterator(self.mq_iterator_adapter.iterator(), enqueued_message)?;
        }

        collation_data.next_lt = self.executor.min_next_lt();
        collation_data.block_limit.lt_current = collation_data.next_lt;

        Ok(())
    }

    /// Create special transactions for the collator
    pub async fn create_special_transactions(
        &mut self,
        config: &BlockchainConfig,
        collator_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "create_special_transactions");

        // TODO: Execute in parallel if addresses are distinct?

        if !collator_data.value_flow.recovered.tokens.is_zero() {
            self.create_special_transaction(
                &config.get_fee_collector_address()?,
                collator_data.value_flow.recovered.clone(),
                SpecialOrigin::Recover,
                collator_data,
            )
            .await?;
        }

        if !collator_data.value_flow.minted.other.is_empty() {
            self.create_special_transaction(
                &config.get_minter_address()?,
                collator_data.value_flow.minted.clone(),
                SpecialOrigin::Mint,
                collator_data,
            )
            .await?;
        }

        Ok(())
    }

    async fn create_special_transaction(
        &mut self,
        account_id: &HashBytes,
        amount: CurrencyCollection,
        special_origin: SpecialOrigin,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            amount = %amount.tokens,
            ?special_origin,
            "create_special_transaction",
        );

        let Some(account_stuff) = self.executor.take_account_stuff_if(account_id, |_| true)? else {
            return Ok(());
        };

        let in_message = {
            let info = MsgInfo::Int(IntMsgInfo {
                ihr_disabled: false,
                bounce: true,
                bounced: false,
                src: IntAddr::from((-1, HashBytes::ZERO)),
                dst: IntAddr::from((-1, *account_id)),
                value: amount,
                ihr_fee: Default::default(),
                fwd_fee: Default::default(),
                created_lt: collation_data.start_lt,
                created_at: collation_data.gen_utime,
            });
            let cell = CellBuilder::build_from(BaseMessage {
                info: info.clone(),
                init: None,
                body: CellSlice::default(),
                layout: None,
            })?;

            Box::new(ParsedMessage {
                info,
                dst_in_current_shard: true,
                cell,
                special_origin: Some(special_origin),
                dequeued: None,
            })
        };

        let executed = self
            .executor
            .execute_ordinary_transaction(account_stuff, in_message)
            .await?;

        let executor_output = executed.result?;

        self.process_transaction(executor_output, Some(executed.in_message), collation_data)?;

        Ok(())
    }

    pub async fn create_ticktock_transactions(
        &mut self,
        config: &BlockchainConfig,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            kind = ?tick_tock,
            "create_ticktock_transactions"
        );

        // TODO: Execute in parallel since these are unique accounts

        for account_id in config.get_fundamental_addresses()?.keys() {
            self.create_ticktock_transaction(&account_id?, tick_tock, collation_data)
                .await?;
        }

        self.create_ticktock_transaction(&config.address, tick_tock, collation_data)
            .await?;
        Ok(())
    }

    async fn create_ticktock_transaction(
        &mut self,
        account_id: &HashBytes,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            kind = ?tick_tock,
            "create_ticktock_transaction",
        );

        let Some(account_stuff) =
            self.executor
                .take_account_stuff_if(account_id, |stuff| match tick_tock {
                    TickTock::Tick => stuff.special.tick,
                    TickTock::Tock => stuff.special.tock,
                })?
        else {
            return Ok(());
        };

        let executor_output = self
            .executor
            .execute_ticktock_transaction(account_stuff, tick_tock)
            .await?;

        self.process_transaction(executor_output, None, collation_data)?;

        Ok(())
    }
}

pub struct ExecuteCollation {
    pub collation_config: Arc<CollationConfig>,
    pub collation_data: Box<BlockCollationData>,
    pub msgs_buffer: MessagesBuffer,
    pub mc_data: Arc<McData>,
    pub prev_shard_data: PrevData,
    pub execute_result: ExecuteResult,
}

impl ExecuteCollation {
    pub async fn execute_special_transactions(
        &mut self,
        executor_wrapper: &mut ExecutorWrapper,
    ) -> Result<()> {
        executor_wrapper
            .create_ticktock_transactions(
                &self.mc_data.config,
                TickTock::Tock,
                &mut self.collation_data,
            )
            .await?;

        Ok(())
    }

    pub async fn update_queue_diff(
        &mut self,
        mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Result<UpdateQueueDiffResult> {
        let labels = [("workchain", shard_id.workchain().to_string())];

        let prev_hash = self
            .prev_shard_data
            .prev_queue_diff_hashes()
            .first()
            .cloned()
            .unwrap_or_default();

        // get queue diff and check for pending internals
        let histogram_create_queue_diff =
            HistogramGuard::begin_with_labels("tycho_do_collate_create_queue_diff_time", &labels);

        let (has_pending_internals_in_iterator, diff_with_messages) = mq_iterator_adapter.release(
            !self.msgs_buffer.has_pending_messages(),
            &mut self.msgs_buffer.current_iterator_positions,
        )?;

        let create_queue_diff_elapsed = histogram_create_queue_diff.finish();

        let diff_messages_len = diff_with_messages.messages.len();
        let has_unprocessed_messages =
            self.msgs_buffer.has_pending_messages() || has_pending_internals_in_iterator;

        let (min_message, max_message) = {
            let messages = &diff_with_messages.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (
                    QueueKey::min_for_lt(self.collation_data.start_lt),
                    QueueKey::max_for_lt(self.collation_data.next_lt),
                ),
            }
        };

        let queue_diff = QueueDiffStuff::builder(
            shard_id,
            self.collation_data.block_id_short.seqno,
            &prev_hash,
        )
        .with_processed_upto(
            diff_with_messages
                .processed_upto
                .iter()
                .map(|(k, v)| (*k, v.lt, &v.hash)),
        )
        .with_messages(
            &min_message,
            &max_message,
            diff_with_messages.messages.keys().map(|k| &k.hash),
        )
        .serialize();

        let queue_diff_hash = *queue_diff.hash();
        tracing::debug!(target: tracing_targets::COLLATOR, queue_diff_hash = %queue_diff_hash);

        // start async update queue task
        let update_queue_task: JoinTask<std::result::Result<Duration, anyhow::Error>> =
            JoinTask::<Result<_>>::new({
                let block_id_short = self.collation_data.block_id_short;
                let labels = labels.clone();
                async move {
                    // apply queue diff
                    let histogram = HistogramGuard::begin_with_labels(
                        "tycho_do_collate_apply_queue_diff_time",
                        &labels,
                    );

                    mq_adapter
                        .apply_diff(diff_with_messages, block_id_short, &queue_diff_hash)
                        .await?;
                    let apply_queue_diff_elapsed = histogram.finish();

                    Ok(apply_queue_diff_elapsed)
                }
            });

        Ok(UpdateQueueDiffResult {
            queue_diff,
            update_queue_task,
            has_unprocessed_messages,
            diff_messages_len,
            create_queue_diff_elapsed,
        })
    }
}

pub struct ExecuteResult {
    pub execute_groups_wu_vm_only: u64,
    pub process_txs_wu: u64,
    pub execute_groups_wu_total: u64,
    pub prepare_groups_wu_total: u64,
    pub fill_msgs_total_elapsed: Duration,
    pub execute_msgs_total_elapsed: Duration,
    pub process_txs_total_elapsed: Duration,
    pub init_iterator_elapsed: Duration,
    pub read_existing_messages_elapsed: Duration,
    pub read_ext_messages_elapsed: Duration,
    pub read_new_messages_elapsed: Duration,
    pub add_to_message_groups_elapsed: Duration,

    pub last_read_to_anchor_chain_time: Option<u64>,
}

pub struct UpdateQueueDiffResult {
    pub queue_diff: SerializedQueueDiff,
    pub update_queue_task: JoinTask<std::result::Result<Duration, anyhow::Error>>,
    pub has_unprocessed_messages: bool,
    pub diff_messages_len: usize,
    pub create_queue_diff_elapsed: Duration,
}

pub struct CollationResult {
    pub handle_block_candidate_elapsed: Duration,
    pub collation_data: Box<BlockCollationData>,
}

fn calc_process_txs_wu(
    collation_data: &BlockCollationData,
    wu_params_execute: &WorkUnitsParamsExecute,
) -> u64 {
    let &WorkUnitsParamsExecute {
        serialize_enqueue,
        serialize_dequeue,
        insert_new_msgs_to_iterator,
        ..
    } = wu_params_execute;

    (collation_data.int_enqueue_count)
        .saturating_mul(serialize_enqueue as u64)
        .saturating_add((collation_data.int_dequeue_count).saturating_mul(serialize_dequeue as u64))
        .saturating_add(
            (collation_data.inserted_new_msgs_to_iterator)
                .saturating_mul(insert_new_msgs_to_iterator as u64),
        )
}

fn calc_prepare_groups_wu_total(
    collation_data: &BlockCollationData,
    wu_params_prepare: &WorkUnitsParamsPrepare,
) -> u64 {
    let &WorkUnitsParamsPrepare {
        fixed_part,
        read_ext_msgs,
        read_int_msgs,
        read_new_msgs,
    } = wu_params_prepare;

    (fixed_part as u64)
        .saturating_add(
            collation_data
                .read_ext_msgs
                .saturating_mul(read_ext_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_int_msgs_from_iterator
                .saturating_mul(read_int_msgs as u64),
        )
        .saturating_add(
            collation_data
                .read_new_msgs_from_iterator
                .saturating_mul(read_new_msgs as u64),
        )
}

/// add in and out messages from to block
#[allow(clippy::vec_box)]
fn new_transaction(
    collation_data: &mut BlockCollationData,
    shard_id: &ShardIdent,
    executor_output: ExecutorOutput,
    in_msg: Option<Box<ParsedMessage>>,
) -> Result<Vec<Box<ParsedMessage>>> {
    tracing::trace!(
        target: tracing_targets::COLLATOR,
        message_hash = ?in_msg.as_ref().map(|m| m.cell.repr_hash()),
        transaction_hash = %executor_output.transaction.inner().repr_hash(),
        "process new transaction from message",
    );

    collation_data.execute_count_all += 1;

    let gas_used = &mut collation_data.block_limit.gas_used;
    *gas_used = gas_used.saturating_add(executor_output.gas_used);

    if let Some(in_msg) = in_msg {
        process_in_message(collation_data, executor_output.transaction.clone(), in_msg)?;
    }

    let mut out_messages = vec![];

    for out_msg_cell in executor_output.out_msgs.values() {
        let out_msg_cell = out_msg_cell?;
        let out_msg_hash = *out_msg_cell.repr_hash();
        let out_msg_info = out_msg_cell.parse::<MsgInfo>()?;

        tracing::trace!(
            target: tracing_targets::COLLATOR,
            message_hash = %out_msg_hash,
            info = ?out_msg_info,
            "adding out message to out_msgs",
        );
        match &out_msg_info {
            MsgInfo::Int(IntMsgInfo { fwd_fee, dst, .. }) => {
                collation_data.int_enqueue_count += 1;

                let dst_prefix = dst.prefix();
                let dst_workchain = dst.workchain();
                let dst_in_current_shard = contains_prefix(shard_id, dst_workchain, dst_prefix);

                let out_msg = OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                        // NOTE: `next_addr` is not used in current routing between shards logic
                        next_addr: if dst_in_current_shard {
                            IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                        } else {
                            IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                        },
                        fwd_fee_remaining: *fwd_fee,
                        message: Lazy::from_raw(out_msg_cell.clone()),
                    })?,
                    transaction: executor_output.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: Some(executor_output.transaction.clone()),
                    });

                out_messages.push(Box::new(ParsedMessage {
                    info: out_msg_info,
                    dst_in_current_shard,
                    cell: out_msg_cell,
                    special_origin: None,
                    dequeued: None,
                }));
            }
            MsgInfo::ExtOut(_) => {
                let out_msg = OutMsg::External(OutMsgExternal {
                    out_msg: Lazy::from_raw(out_msg_cell),
                    transaction: executor_output.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: None,
                    });
            }
            MsgInfo::ExtIn(_) => bail!("External inbound message cannot be an output"),
        }
    }

    Ok(out_messages)
}

fn process_in_message(
    collation_data: &mut BlockCollationData,
    transaction: Lazy<Transaction>,
    in_msg: Box<ParsedMessage>,
) -> Result<()> {
    let import_fees;
    let in_msg_hash = *in_msg.cell.repr_hash();
    let in_msg = match (in_msg.info, in_msg.special_origin) {
        // Messages with special origin are always immediate
        (_, Some(_)) => {
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&MsgEnvelope {
                    cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    fwd_fee_remaining: Default::default(),
                    message: Lazy::from_raw(in_msg.cell),
                })?,
                transaction,
                fwd_fee: Default::default(),
            });

            import_fees = in_msg.compute_fees()?;
            Lazy::new(&in_msg)?
        }
        // External messages are added as is
        (MsgInfo::ExtIn(_), _) => {
            collation_data.execute_count_ext += 1;

            import_fees = ImportFees::default();
            Lazy::new(&InMsg::External(InMsgExternal {
                in_msg: Lazy::from_raw(in_msg.cell),
                transaction,
            }))?
        }
        // Dequeued messages have a dedicated `InMsg` type
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _) if in_msg.dequeued.is_some() => {
            collation_data.execute_count_int += 1;

            let same_shard = in_msg.dequeued.map(|d| d.same_shard).unwrap_or_default();

            let envelope = Lazy::new(&MsgEnvelope {
                // NOTE: `cur_addr` is not used in current routing between shards logic
                cur_addr: if same_shard {
                    IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                } else {
                    IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                },
                next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell),
            })?;

            let in_msg = InMsg::Final(InMsgFinal {
                in_msg_envelope: envelope.clone(),
                transaction,
                fwd_fee,
            });
            import_fees = in_msg.compute_fees()?;

            let in_msg = Lazy::new(&in_msg)?;

            if same_shard {
                let out_msg = OutMsg::DequeueImmediate(OutMsgDequeueImmediate {
                    out_msg_envelope: envelope.clone(),
                    reimport: in_msg.clone(),
                });
                let exported_value = out_msg.compute_exported_value()?;

                collation_data.out_msgs.insert(in_msg_hash, PreparedOutMsg {
                    out_msg: Lazy::new(&out_msg)?,
                    exported_value,
                    new_tx: None,
                });
            }
            collation_data.int_dequeue_count += 1;

            in_msg
        }
        // New messages are added as is
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _) => {
            collation_data.execute_count_new_int += 1;

            let msg_envelope = MsgEnvelope {
                cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell),
            };
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction,
                fwd_fee,
            });

            import_fees = in_msg.compute_fees()?;
            let in_msg = Lazy::new(&in_msg)?;

            let prev_transaction = match collation_data.out_msgs.get(&in_msg_hash) {
                Some(prepared) => match &prepared.new_tx {
                    Some(tx) => tx.clone(),
                    None => anyhow::bail!("invalid out message state for in_msg {in_msg_hash}"),
                },
                None => anyhow::bail!("immediate in_msg {in_msg_hash} not found in out_msgs"),
            };

            let out_msg = OutMsg::Immediate(OutMsgImmediate {
                out_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: prev_transaction,
                reimport: in_msg.clone(),
            });
            let exported_value = out_msg.compute_exported_value()?;

            collation_data.out_msgs.insert(in_msg_hash, PreparedOutMsg {
                out_msg: Lazy::new(&out_msg)?,
                exported_value,
                new_tx: None,
            });
            collation_data.int_enqueue_count -= 1;

            in_msg
        }
        (msg_info, special_origin) => {
            unreachable!(
                "unexpected message. info: {msg_info:?}, \
                special_origin: {special_origin:?}"
            )
        }
    };

    collation_data.in_msgs.insert(in_msg_hash, PreparedInMsg {
        in_msg,
        import_fees,
    });

    Ok(())
}

pub fn contains_prefix(shard_id: &ShardIdent, workchain_id: i32, prefix_without_tag: u64) -> bool {
    if shard_id.workchain() == workchain_id {
        if shard_id.prefix() == 0x8000_0000_0000_0000u64 {
            return true;
        }
        let shift = 64 - shard_id.prefix_len();
        return (shard_id.prefix() >> shift) == (prefix_without_tag >> shift);
    }
    false
}
