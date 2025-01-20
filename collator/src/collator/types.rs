use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::{Cell, HashBytes, UsageTree, UsageTreeMode};
use everscale_types::dict::Dict;
use everscale_types::models::{
    AccountState, BlockId, BlockIdShort, BlockInfo, BlockLimits, BlockParamLimits, BlockRef,
    CollationConfig, CurrencyCollection, GlobalVersion, HashUpdate, ImportFees, InMsg, Lazy,
    LibDescr, MsgInfo, OptionalAccount, OutMsg, PrevBlockRef, ShardAccount, ShardAccounts,
    ShardDescription, ShardFeeCreated, ShardFees, ShardIdent, ShardIdentFull, ShardStateUnsplit,
    SimpleLib, SpecialFlags, StateInit, Transaction, ValueFlow,
};
use tl_proto::TlWrite;
use ton_executor::{AccountMeta, ExecutedTransaction};
use tycho_block_util::queue::SerializedQueueDiff;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use super::messages_reader::ReaderState;
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::types::processed_upto::ProcessedUptoInfoStuff;
use crate::types::{BlockCandidate, McData, ProofFunds, TopShardBlockInfo};

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
    pub value_flow: ValueFlow,
    pub min_ref_mc_seqno: u32,
    pub rand_seed: HashBytes,
    #[cfg(feature = "block-creator-stats")]
    pub block_create_count: FastHashMap<HashBytes, u64>,
    pub created_by: HashBytes,
    pub global_version: GlobalVersion,
    pub top_shard_blocks: Vec<TopShardBlockInfo>,

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
        global_version: GlobalVersion,
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
            value_flow: Default::default(),
            min_ref_mc_seqno,
            rand_seed,
            #[cfg(feature = "block-creator-stats")]
            block_create_count: Default::default(),
            created_by,
            global_version,
            shards: None,
            top_shard_blocks: vec![],
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
            min_ref_mc_seqno: self.min_ref_mc_seqno,
            rand_seed: self.rand_seed,
            created_by: self.created_by,
            global_version: self.global_version,
            shards: self.shards,
            top_shard_blocks: self.top_shard_blocks,
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
            ext_msgs_skipped_count: 0,
            execute_count_int: 0,
            execute_count_new_int: 0,
            int_enqueue_count: 0,
            int_dequeue_count: 0,
            read_ext_msgs_count: 0,
            read_int_msgs_from_iterator_count: 0,
            new_msgs_created_count: 0,
            inserted_new_msgs_to_iterator_count: 0,
            read_new_msgs_from_iterator_count: 0,
            in_msgs: Default::default(),
            out_msgs: Default::default(),
            mint_msg: None,
            recover_create_msg: None,
            mempool_config_override: self.mempool_config_override,
            #[cfg(feature = "block-creator-stats")]
            block_create_count: self.block_create_count,
            diff_tail_len: 0,
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
    pub ext_msgs_skipped_count: u64,

    pub int_enqueue_count: u64,
    pub int_dequeue_count: u64,

    pub read_ext_msgs_count: u64,
    pub read_int_msgs_from_iterator_count: u64,
    pub new_msgs_created_count: u64,
    pub inserted_new_msgs_to_iterator_count: u64,
    pub read_new_msgs_from_iterator_count: u64,

    pub start_lt: u64,
    // Should be updated on each tx finalization from MessagesPreparer.max_lt
    // which is updating during tx execution
    pub next_lt: u64,

    pub in_msgs: BTreeMap<HashBytes, PreparedInMsg>,
    pub out_msgs: BTreeMap<HashBytes, PreparedOutMsg>,

    /// Ids of top blocks from shards that be included in the master block
    pub top_shard_blocks: Vec<TopShardBlockInfo>,

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
    pub diff_tail_len: u32,
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
    pub account_addr: AccountId,
    pub shard_account: ShardAccount,
    pub special: SpecialFlags,
    pub initial_state_hash: HashBytes,
    pub balance: CurrencyCollection,
    pub initial_libraries: Dict<HashBytes, SimpleLib>,
    pub libraries: Dict<HashBytes, SimpleLib>,
    pub exists: bool,
    pub transactions: BTreeMap<u64, (CurrencyCollection, Lazy<Transaction>)>,
}

impl ShardAccountStuff {
    pub fn new(account_addr: &AccountId, shard_account: ShardAccount) -> Result<Self> {
        let initial_state_hash = *shard_account.account.inner().repr_hash();

        let mut libraries = Dict::new();
        let mut special = SpecialFlags::default();
        let balance;
        let exists;

        if let Some(account) = shard_account.load_account()? {
            if let AccountState::Active(StateInit {
                libraries: acc_libs,
                special: acc_flags,
                ..
            }) = account.state
            {
                libraries = acc_libs;
                special = acc_flags.unwrap_or_default();
            }
            balance = account.balance;
            exists = true;
        } else {
            balance = CurrencyCollection::ZERO;
            exists = false;
        }

        Ok(Self {
            account_addr: *account_addr,
            shard_account,
            special,
            initial_state_hash,
            balance,
            initial_libraries: libraries.clone(),
            libraries,
            exists,
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
            balance: CurrencyCollection::ZERO,
            initial_libraries: Dict::new(),
            libraries: Dict::new(),
            exists: false,
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

    pub fn apply_transaction(
        &mut self,
        lt: u64,
        total_fees: CurrencyCollection,
        account_meta: AccountMeta,
        tx: &ExecutedTransaction,
    ) {
        self.transactions
            .insert(lt, (total_fees, tx.transaction.clone()));
        self.balance = account_meta.balance;
        self.libraries = account_meta.libraries;
        self.exists = account_meta.exists;
    }

    pub fn update_public_libraries(
        &self,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        if self.libraries.root() == self.initial_libraries.root() {
            return Ok(());
        }

        for entry in self.libraries.iter_union(&self.initial_libraries) {
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
    pub queue_diff_messages_count: usize,
    pub has_unprocessed_messages: bool,
    pub reader_state: ReaderState,
    pub anchors_cache: AnchorsCache,

    pub create_queue_diff_elapsed: Duration,
}

pub struct FinalizeCollationResult {
    pub handle_block_candidate_elapsed: Duration,
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

pub struct FinalizeBlockResult {
    pub collation_data: Box<BlockCollationData>,
    pub block_candidate: Box<BlockCandidate>,
    pub mc_data: Option<Arc<McData>>,
    pub old_mc_data: Arc<McData>,
    pub new_state_root: Cell,
    pub new_observable_state: Box<ShardStateUnsplit>,
    pub finalize_wu_total: u64,
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
    pub finalize_block_elapsed: Duration,
    pub has_unprocessed_messages: bool,
    pub queue_diff_messages_count: usize,
    pub execute_elapsed: Duration,
    pub execute_tick_elapsed: Duration,
    pub execute_tock_elapsed: Duration,
    pub create_queue_diff_elapsed: Duration,
    pub apply_queue_diff_elapsed: Duration,
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
