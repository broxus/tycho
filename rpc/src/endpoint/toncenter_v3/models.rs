use std::num::NonZeroUsize;

use everscale_types::models::{
    BlockId, BlockIdShort, IntAddr, MsgInfo, ShardIdent, StateInit, StdAddr, StdAddrBase64Repr,
};
use everscale_types::num::{Tokens, VarUint24, VarUint56};
use everscale_types::prelude::*;
use serde::ser::{SerializeMap, SerializeStruct};
use serde::{Deserialize, Serialize};
use tycho_block_util::message::build_normalized_external_message;
use tycho_util::serde_helpers::BorrowedStr;
use tycho_util::FastHashSet;

use crate::state::{BriefBlockInfo, TransactionInfo};
use crate::util::serde_helpers;
use crate::util::tonlib_helpers::load_bytes_rope;

// === Requests ===

#[derive(Debug, Deserialize)]
pub struct BlocksRequest {
    #[serde(default)]
    pub workchain: Option<i32>,
    #[serde(default)]
    pub shard: Option<ShardPrefix>,
    #[serde(default)]
    pub seqno: Option<u32>,
    #[serde(default)]
    pub mc_seqno: Option<u32>,
    #[serde(default)]
    pub start_utime: Option<u32>,
    #[serde(default)]
    pub end_utime: Option<u32>,
    #[serde(default)]
    pub start_lt: Option<u64>,
    #[serde(default)]
    pub end_lt: Option<u64>,
    #[serde(default = "default_blocks_limit")]
    pub limit: NonZeroUsize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_sort_direction")]
    pub sort: SortDirection,
}

const fn default_blocks_limit() -> NonZeroUsize {
    NonZeroUsize::new(10).unwrap()
}

#[derive(Debug, Deserialize)]
pub struct TransactionsRequest {
    #[serde(default)]
    pub workchain: Option<i32>,
    #[serde(default)]
    pub shard: Option<ShardPrefix>,
    #[serde(default)]
    pub seqno: Option<u32>,
    #[serde(default)]
    pub mc_seqno: Option<u32>,
    #[serde(
        default,
        deserialize_with = "TransactionsRequest::deserialize_address_list"
    )]
    pub account: FastHashSet<StdAddr>,
    #[serde(
        default,
        deserialize_with = "TransactionsRequest::deserialize_address_list"
    )]
    pub exclude_account: FastHashSet<StdAddr>,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub hash: Option<HashBytes>,
    #[serde(default)]
    pub lt: Option<u64>,
    #[serde(default)]
    pub start_utime: Option<u32>,
    #[serde(default)]
    pub end_utime: Option<u32>,
    #[serde(default)]
    pub start_lt: Option<u64>,
    #[serde(default)]
    pub end_lt: Option<u64>,
    #[serde(default = "default_tx_limit")]
    pub limit: NonZeroUsize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_sort_direction")]
    pub sort: SortDirection,
}

impl TransactionsRequest {
    fn deserialize_address_list<'de, D>(deserializer: D) -> Result<FastHashSet<StdAddr>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Item(#[serde(with = "serde_helpers::tonlib_address")] StdAddr);

        Ok(Vec::<_>::deserialize(deserializer)?
            .into_iter()
            .map(|Item(addr)| addr)
            .collect())
    }
}

#[derive(Debug, Deserialize)]
pub struct TransactionsByMcBlockRequest {
    pub seqno: u32,
    #[serde(default = "default_tx_limit")]
    pub limit: NonZeroUsize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_sort_direction")]
    pub sort: SortDirection,
}

#[derive(Debug, Deserialize)]
pub struct AdjacentTransactionsRequest {
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
    #[serde(default)]
    pub direction: Option<MessageDirection>,
}

#[derive(Debug, Deserialize)]
pub struct TransactionsByMessageRequest {
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub msg_hash: HashBytes,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub body_hash: Option<HashBytes>,
    #[serde(default)]
    pub opcode: Option<i32>,
    #[serde(default)]
    pub direction: Option<MessageDirection>,
    #[serde(default = "default_tx_limit")]
    pub limit: NonZeroUsize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_sort_direction")]
    pub sort: SortDirection,
}

const fn default_tx_limit() -> NonZeroUsize {
    NonZeroUsize::new(10).unwrap()
}

const fn default_sort_direction() -> SortDirection {
    SortDirection::Desc
}

// === Responses ===

#[derive(Debug, Serialize)]
pub struct MasterchainInfoResponse {
    pub last: Block,
    pub first: Block,
}

#[derive(Debug, Serialize)]
pub struct BlocksResponse {
    pub blocks: Vec<Block>,
}

#[derive(Default, Serialize)]
pub struct TransactionsResponse {
    pub transactions: Vec<Transaction>,
    pub address_book: AddressBook,
}

impl TransactionsResponse {
    pub fn new(transactions: Vec<Transaction>) -> Self {
        let mut address_book = AddressBook::default();
        address_book.fill_from_transactions(&transactions);
        Self {
            transactions,
            address_book,
        }
    }
}

// === Stuff ===

#[derive(Debug, Serialize)]
pub struct Block {
    pub workchain: i32,
    pub shard: ShardPrefix,
    pub seqno: u32,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub root_hash: HashBytes,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub file_hash: HashBytes,
    pub global_id: i32,
    pub version: u32,
    pub after_merge: bool,
    pub before_split: bool,
    pub after_split: bool,
    pub want_merge: bool,
    pub want_split: bool,
    pub key_block: bool,
    pub vert_seqno_incr: bool,
    pub flags: u8,
    #[serde(with = "serde_helpers::string")]
    pub gen_utime: u32,
    #[serde(with = "serde_helpers::string")]
    pub start_lt: u64,
    #[serde(with = "serde_helpers::string")]
    pub end_lt: u64,
    pub validator_list_hash_short: u32,
    pub gen_catchain_seqno: u32,
    pub min_ref_mc_seqno: u32,
    pub prev_key_block_seqno: u32,
    pub vert_seqno: u32,
    pub master_ref_seqno: u32,
    pub rand_seed: HashBytes,
    pub created_by: HashBytes,
    pub tx_count: u32,
    pub masterchain_block_ref: BlockRef,
    pub prev_blocks: Vec<BlockRef>,
}

impl Block {
    pub fn from_stored(block_id: &BlockId, info: BriefBlockInfo) -> Self {
        let master_ref_seqno = match &info.master_ref {
            None => block_id.seqno,
            Some(id) => id.seqno,
        };

        Self {
            workchain: block_id.shard.workchain(),
            shard: ShardPrefix(block_id.shard.prefix()),
            seqno: block_id.seqno,
            root_hash: block_id.root_hash,
            file_hash: block_id.file_hash,
            global_id: info.global_id,
            version: info.version,
            after_merge: info.after_merge,
            before_split: info.before_split,
            after_split: info.after_split,
            want_merge: info.want_merge,
            want_split: info.want_split,
            key_block: info.is_key_block,
            vert_seqno_incr: false, // TODO (if really needed)
            flags: info.flags,
            gen_utime: info.gen_utime,
            start_lt: info.start_lt,
            end_lt: info.end_lt,
            validator_list_hash_short: info.validator_list_hash_short,
            gen_catchain_seqno: info.catchain_seqno,
            min_ref_mc_seqno: info.min_ref_mc_seqno,
            prev_key_block_seqno: info.prev_key_block_seqno,
            vert_seqno: info.vert_seqno,
            master_ref_seqno,
            rand_seed: info.rand_seed,
            created_by: HashBytes::ZERO,
            tx_count: info.tx_count,
            masterchain_block_ref: BlockRef(BlockIdShort {
                shard: ShardIdent::MASTERCHAIN,
                seqno: master_ref_seqno,
            }),
            prev_blocks: info
                .prev_blocks
                .iter()
                .map(|block_id| BlockRef(block_id.as_short_id()))
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub struct Transaction {
    pub account: StdAddr,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
    #[serde(with = "serde_helpers::string")]
    pub lt: u64,
    pub now: u32,
    pub mc_block_seqno: u32,
    // TODO: Set some hash other than zero here?
    pub trace_id: HashBytes,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub prev_trans_hash: HashBytes,
    #[serde(with = "serde_helpers::string")]
    pub prev_trans_lt: u64,
    pub orig_status: AccountStatus,
    pub end_status: AccountStatus,
    pub total_fees: Tokens,
    pub total_fees_extra_currencies: ExtraCurrenciesStub,
    pub description: TxDescription,
    pub block_ref: BlockRef,
    pub in_msg: Option<Message>,
    pub out_msgs: Vec<Message>,
    pub account_state_before: BriefAccountState,
    pub account_state_after: BriefAccountState,
}

impl Transaction {
    pub fn load_raw(
        info: &TransactionInfo,
        cell: &DynCell,
    ) -> Result<Self, everscale_types::error::Error> {
        let tx = cell.parse::<everscale_types::models::Transaction>()?;

        let state_update = tx.state_update.load()?;

        Ok(Self {
            account: info.account.clone(),
            hash: *cell.repr_hash(),
            lt: tx.lt,
            now: tx.now,
            mc_block_seqno: info.mc_seqno,
            trace_id: HashBytes::ZERO,
            prev_trans_hash: tx.prev_trans_hash,
            prev_trans_lt: tx.prev_trans_lt,
            orig_status: tx.orig_status.into(),
            end_status: tx.end_status.into(),
            total_fees: tx.total_fees.tokens,
            total_fees_extra_currencies: ExtraCurrenciesStub {},
            description: tx.load_info()?.into(),
            block_ref: BlockRef(info.block_id.as_short_id()),
            in_msg: tx.in_msg.as_deref().map(Message::load_raw).transpose()?,
            out_msgs: {
                let mut res = Vec::with_capacity(tx.out_msg_count.into_inner() as usize);
                for item in tx.out_msgs.values() {
                    let cell = item?;
                    res.push(Message::load_raw(cell.as_ref())?);
                }
                res
            },
            // TODO: Fill state update.
            account_state_before: BriefAccountState {
                hash: state_update.old,
                balance: None,
                extra_currencies: None,
                account_status: AccountStatus::new_only_existing(tx.orig_status),
                frozen_hash: None,
                data_hash: None,
                code_hash: None,
            },
            // TODO: Fill state update.
            account_state_after: BriefAccountState {
                hash: state_update.new,
                balance: None,
                extra_currencies: None,
                account_status: AccountStatus::new_only_existing(tx.end_status),
                frozen_hash: None,
                data_hash: None,
                code_hash: None,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BriefAccountState {
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
    pub balance: Option<Tokens>,
    pub extra_currencies: Option<ExtraCurrenciesStub>,
    pub account_status: Option<AccountStatus>,
    #[serde(with = "serde_helpers::option_tonlib_hash")]
    pub frozen_hash: Option<HashBytes>,
    #[serde(with = "serde_helpers::option_tonlib_hash")]
    pub data_hash: Option<HashBytes>,
    #[serde(with = "serde_helpers::option_tonlib_hash")]
    pub code_hash: Option<HashBytes>,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct BlockRef(pub BlockIdShort);

impl Serialize for BlockRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("BlockRef", 3)?;
        s.serialize_field("workchain", &self.0.shard.workchain())?;
        s.serialize_field("shard", &ShardPrefix(self.0.shard.prefix()))?;
        s.serialize_field("seqno", &self.0.seqno)?;
        s.end()
    }
}

impl From<BlockIdShort> for BlockRef {
    #[inline]
    fn from(value: BlockIdShort) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ShardPrefix(pub u64);

impl Serialize for ShardPrefix {
    #[inline]
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ShardPrefix {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let BorrowedStr(s) = <_>::deserialize(deserializer)?;
        if s.len() != 16 {
            return Err(Error::custom("invalid shard prefix"));
        }
        u64::from_str_radix(s.trim_start_matches('0'), 16)
            .map(Self)
            .map_err(Error::custom)
    }
}

impl std::fmt::Display for ShardPrefix {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum TxDescription {
    #[serde(rename = "ord")]
    Ordinary(TxDescriptionOrdinary),
    #[serde(rename = "tick_tock")]
    TickTock(TxDescriptionTickTock),
}

impl From<everscale_types::models::TxInfo> for TxDescription {
    fn from(value: everscale_types::models::TxInfo) -> Self {
        use everscale_types::models::TxInfo;

        match value {
            TxInfo::Ordinary(info) => Self::Ordinary(info.into()),
            TxInfo::TickTock(info) => Self::TickTock(info.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionTickTock {
    pub aborted: bool,
    pub destroyed: bool,
    pub is_tock: bool,
    pub storage_ph: TxDescriptionStoragePhase,
    pub compute_ph: TxDescriptionComputePhase,
    pub action: Option<TxDescriptionActionPhase>,
}

impl From<everscale_types::models::TickTockTxInfo> for TxDescriptionTickTock {
    fn from(value: everscale_types::models::TickTockTxInfo) -> Self {
        use everscale_types::models::TickTock;

        Self {
            aborted: value.aborted,
            destroyed: value.destroyed,
            is_tock: matches!(value.kind, TickTock::Tock),
            storage_ph: value.storage_phase.into(),
            compute_ph: value.compute_phase.into(),
            action: value.action_phase.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionOrdinary {
    pub aborted: bool,
    pub destroyed: bool,
    pub credit_first: bool,
    pub storage_ph: Option<TxDescriptionStoragePhase>,
    pub credit_ph: Option<TxDescriptionCreditPhase>,
    pub compute_ph: TxDescriptionComputePhase,
    pub action: Option<TxDescriptionActionPhase>,
    pub bounce: Option<TxDescriptionBouncePhase>,
}

impl From<everscale_types::models::OrdinaryTxInfo> for TxDescriptionOrdinary {
    fn from(value: everscale_types::models::OrdinaryTxInfo) -> Self {
        Self {
            aborted: value.aborted,
            destroyed: value.destroyed,
            credit_first: value.credit_first,
            storage_ph: value.storage_phase.map(Into::into),
            credit_ph: value.credit_phase.map(Into::into),
            compute_ph: value.compute_phase.into(),
            action: value.action_phase.map(Into::into),
            bounce: value.bounce_phase.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionStoragePhase {
    pub storage_fees_collected: Tokens,
    pub status_change: AccountStatusChange,
}

impl From<everscale_types::models::StoragePhase> for TxDescriptionStoragePhase {
    #[inline]
    fn from(value: everscale_types::models::StoragePhase) -> Self {
        Self {
            storage_fees_collected: value.storage_fees_collected,
            status_change: value.status_change.into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct AccountStatus(everscale_types::models::AccountStatus);

impl AccountStatus {
    fn new_only_existing(status: everscale_types::models::AccountStatus) -> Option<Self> {
        if status == everscale_types::models::AccountStatus::NotExists {
            None
        } else {
            Some(Self(status))
        }
    }
}

impl Serialize for AccountStatus {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self.0 {
            everscale_types::models::AccountStatus::Uninit => "uninit",
            everscale_types::models::AccountStatus::Frozen => "frozen",
            everscale_types::models::AccountStatus::Active => "active",
            everscale_types::models::AccountStatus::NotExists => "nonexist",
        })
    }
}

impl From<everscale_types::models::AccountStatus> for AccountStatus {
    #[inline]
    fn from(value: everscale_types::models::AccountStatus) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct AccountStatusChange(everscale_types::models::AccountStatusChange);

impl Serialize for AccountStatusChange {
    #[inline]
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self.0 {
            everscale_types::models::AccountStatusChange::Unchanged => "unchanged",
            everscale_types::models::AccountStatusChange::Frozen => "frozen",
            everscale_types::models::AccountStatusChange::Deleted => "deleted",
        })
    }
}

impl From<everscale_types::models::AccountStatusChange> for AccountStatusChange {
    #[inline]
    fn from(value: everscale_types::models::AccountStatusChange) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionCreditPhase {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_fees_collected: Option<Tokens>,
    pub credit: Tokens,
}

impl From<everscale_types::models::CreditPhase> for TxDescriptionCreditPhase {
    fn from(value: everscale_types::models::CreditPhase) -> Self {
        Self {
            due_fees_collected: value.due_fees_collected,
            credit: value.credit.tokens,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum TxDescriptionComputePhase {
    Skipped(TxDescriptionComputePhaseSkipped),
    Executed(TxDescriptionComputePhaseExecuted),
}

impl From<everscale_types::models::ComputePhase> for TxDescriptionComputePhase {
    fn from(value: everscale_types::models::ComputePhase) -> Self {
        use everscale_types::models::ComputePhase;

        match value {
            ComputePhase::Skipped(phase) => Self::Skipped(phase.into()),
            ComputePhase::Executed(phase) => Self::Executed(phase.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionComputePhaseSkipped {
    pub skipped: bool,
    pub reason: &'static str,
}

impl From<everscale_types::models::SkippedComputePhase> for TxDescriptionComputePhaseSkipped {
    fn from(value: everscale_types::models::SkippedComputePhase) -> Self {
        use everscale_types::models::ComputePhaseSkipReason;

        Self {
            skipped: true,
            reason: match value.reason {
                ComputePhaseSkipReason::NoState => "no_state",
                ComputePhaseSkipReason::BadState => "bad_state",
                ComputePhaseSkipReason::NoGas => "no_gas",
                ComputePhaseSkipReason::Suspended => "suspended",
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionComputePhaseExecuted {
    pub skipped: bool,
    pub success: bool,
    pub msg_state_used: bool,
    pub account_activated: bool,
    pub gas_fees: Tokens,
    #[serde(with = "serde_helpers::string")]
    pub gas_used: VarUint56,
    #[serde(with = "serde_helpers::string")]
    pub gas_limit: VarUint56,
    #[serde(
        with = "serde_helpers::option_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub gas_credit: Option<VarUint24>,
    pub mode: i8,
    pub exit_code: i32,
    pub vm_steps: u32,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub vm_init_state_hash: HashBytes,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub vm_final_state_hash: HashBytes,
}

impl From<everscale_types::models::ExecutedComputePhase> for TxDescriptionComputePhaseExecuted {
    fn from(value: everscale_types::models::ExecutedComputePhase) -> Self {
        Self {
            skipped: false,
            success: value.success,
            msg_state_used: value.msg_state_used,
            account_activated: value.account_activated,
            gas_fees: value.gas_fees,
            gas_used: value.gas_used,
            gas_limit: value.gas_limit,
            gas_credit: value.gas_credit,
            mode: value.mode,
            exit_code: value.exit_code,
            vm_steps: value.vm_steps,
            vm_init_state_hash: value.vm_init_state_hash,
            vm_final_state_hash: value.vm_final_state_hash,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionActionPhase {
    pub success: bool,
    pub valid: bool,
    pub no_funds: bool,
    pub status_change: AccountStatusChange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_fwd_fees: Option<Tokens>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_action_fees: Option<Tokens>,
    pub result_code: i32,
    pub tot_actions: u16,
    pub spec_actions: u16,
    pub skipped_actions: u16,
    pub msgs_created: u16,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub action_list_hash: HashBytes,
    pub tot_msg_size: MessageSize,
}

impl From<everscale_types::models::ActionPhase> for TxDescriptionActionPhase {
    fn from(value: everscale_types::models::ActionPhase) -> Self {
        Self {
            success: value.success,
            valid: value.valid,
            no_funds: value.no_funds,
            status_change: value.status_change.into(),
            total_fwd_fees: value.total_fwd_fees,
            total_action_fees: value.total_action_fees,
            result_code: value.result_code,
            tot_actions: value.total_actions,
            spec_actions: value.special_actions,
            skipped_actions: value.skipped_actions,
            msgs_created: value.messages_created,
            action_list_hash: value.action_list_hash,
            tot_msg_size: MessageSize {
                cells: value.total_message_size.cells,
                bits: value.total_message_size.bits,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDescriptionBouncePhase {
    #[serde(rename = "type")]
    pub ty: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_size: Option<MessageSize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_fwd_fees: Option<Tokens>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_fees: Option<Tokens>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fwd_fees: Option<Tokens>,
}

impl From<everscale_types::models::BouncePhase> for TxDescriptionBouncePhase {
    fn from(value: everscale_types::models::BouncePhase) -> Self {
        use everscale_types::models::BouncePhase;

        let mut res = Self {
            ty: "",
            msg_size: None,
            req_fwd_fees: None,
            msg_fees: None,
            fwd_fees: None,
        };

        match value {
            BouncePhase::NegativeFunds => res.ty = "negfunds",
            BouncePhase::NoFunds(phase) => {
                res.ty = "nofunds";
                res.msg_size = Some(MessageSize {
                    cells: phase.msg_size.cells,
                    bits: phase.msg_size.bits,
                });
                res.req_fwd_fees = Some(phase.req_fwd_fees);
            }
            BouncePhase::Executed(phase) => {
                res.ty = "ok";
                res.msg_size = Some(MessageSize {
                    cells: phase.msg_size.cells,
                    bits: phase.msg_size.bits,
                });
                res.msg_fees = Some(phase.msg_fees);
                res.fwd_fees = Some(phase.fwd_fees);
            }
        }

        res
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageSize {
    #[serde(with = "serde_helpers::string")]
    pub cells: VarUint56,
    #[serde(with = "serde_helpers::string")]
    pub bits: VarUint56,
}

#[derive(Debug, Clone, Serialize)]
pub struct Message {
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
    pub source: Option<StdAddr>,
    pub destination: Option<StdAddr>,
    pub value: Option<Tokens>,
    pub value_extra_currencies: Option<ExtraCurrenciesStub>,
    pub fwd_fee: Option<Tokens>,
    pub ihr_fee: Option<Tokens>,
    #[serde(with = "serde_helpers::option_string")]
    pub created_lt: Option<u64>,
    #[serde(with = "serde_helpers::option_string")]
    pub created_at: Option<u32>,
    pub ihr_disabled: Option<bool>,
    pub bounce: Option<bool>,
    pub bounced: Option<bool>,
    pub import_fee: Option<Tokens>,
    pub message_content: MessageContent,
    pub init_state: Option<MessageContent>,
    #[serde(with = "serde_helpers::option_tonlib_hash")]
    pub hash_norm: Option<HashBytes>,
}

impl Message {
    pub fn load_raw(cell: &DynCell) -> Result<Self, everscale_types::error::Error> {
        let hash = cell.repr_hash();

        let mut cs = cell.as_slice()?;
        let info = MsgInfo::load_from(&mut cs)?;

        let init_state = if cs.load_bit()? {
            let cell = if cs.load_bit()? {
                cs.load_reference_cloned()?
            } else {
                let mut slice = cs;
                StateInit::load_from(&mut cs)?;
                slice.skip_last(cs.size_bits(), cs.size_refs())?;
                CellBuilder::build_from(slice)?
            };

            Some(MessageContent {
                hash: *cell.repr_hash(),
                body: cell,
                decoded: None,
            })
        } else {
            None
        };

        let body = if cs.load_bit()? {
            cs.load_reference_cloned()?
        } else {
            CellBuilder::build_from(cs)?
        };

        let message_content = MessageContent {
            decoded: DecodedContent::try_load(body.as_ref()).ok(),
            hash: *body.repr_hash(),
            body,
        };

        let mut res = Self {
            hash: *hash,
            source: None,
            destination: None,
            value: None,
            value_extra_currencies: None,
            fwd_fee: None,
            ihr_fee: None,
            created_lt: None,
            created_at: None,
            ihr_disabled: None,
            bounce: None,
            bounced: None,
            import_fee: None,
            message_content,
            init_state,
            hash_norm: None,
        };
        match &info {
            MsgInfo::Int(info) => {
                res.ihr_disabled = Some(info.ihr_disabled);
                res.bounce = Some(info.bounce);
                res.bounced = Some(info.bounced);
                res.source = to_std_addr(&info.src);
                res.destination = to_std_addr(&info.dst);
                res.value = Some(info.value.tokens);
                res.value_extra_currencies = Some(ExtraCurrenciesStub {});
                res.ihr_fee = Some(info.ihr_fee);
                res.fwd_fee = Some(info.fwd_fee);
                res.created_lt = Some(info.created_lt);
                res.created_at = Some(info.created_at);
            }
            MsgInfo::ExtIn(info) => {
                res.destination = to_std_addr(&info.dst);
                res.import_fee = Some(info.import_fee);
                res.hash_norm = Some(
                    *build_normalized_external_message(
                        &info.dst,
                        res.message_content.body.clone(),
                    )?
                    .repr_hash(),
                );
            }
            MsgInfo::ExtOut(info) => {
                res.source = to_std_addr(&info.src);
                res.created_lt = Some(info.created_lt);
                res.created_at = Some(info.created_at);
            }
        }

        Ok(res)
    }
}

fn to_std_addr(addr: &IntAddr) -> Option<StdAddr> {
    match addr {
        IntAddr::Std(addr) => Some(addr.clone()),
        IntAddr::Var(_) => None,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageContent {
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
    #[serde(with = "Boc")]
    pub body: Cell,
    pub decoded: Option<DecodedContent>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum DecodedContent {
    #[serde(rename = "text_comment")]
    TextComment { comment: String },
}

impl DecodedContent {
    pub fn try_load(body: &DynCell) -> Result<Self, everscale_types::error::Error> {
        let mut cs = body.as_slice()?;
        let tag = cs.load_u32()?;
        match tag {
            0x00000000 => {
                let bytes = load_bytes_rope(cs, true)?;
                Ok(Self::TextComment {
                    comment: String::from_utf8_lossy(&bytes).into_owned(),
                })
            }
            _ => Err(everscale_types::error::Error::InvalidTag),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageDirection {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExtraCurrenciesStub {}

#[derive(Default)]
pub struct AddressBook {
    pub items: FastHashSet<StdAddr>,
}

impl AddressBook {
    pub fn fill_from_transactions(&mut self, transactions: &[Transaction]) {
        for tx in transactions {
            self.items.insert(tx.account.clone());

            if let Some(msg) = &tx.in_msg {
                if let Some(src) = msg.source.clone() {
                    self.items.insert(src);
                }
            }

            for msg in &tx.out_msgs {
                if let Some(dst) = msg.destination.clone() {
                    self.items.insert(dst);
                }
            }
        }
    }
}

impl serde::Serialize for AddressBook {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Item<'a> {
            #[serde(with = "StdAddrBase64Repr::<true>")]
            user_friendly: &'a StdAddr,
            domain: (),
        }

        let mut s = serializer.serialize_map(Some(self.items.len()))?;
        for addr in &self.items {
            s.serialize_entry(addr, &Item {
                user_friendly: addr,
                domain: (),
            })?;
        }
        s.end()
    }
}
