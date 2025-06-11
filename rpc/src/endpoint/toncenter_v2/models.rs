use std::cell::RefCell;
use std::num::NonZeroU8;
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::Context;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::error::Error;
use everscale_types::models::{
    Base64StdAddrFlags, BlockId, DisplayBase64StdAddr, IntAddr, Message, MsgInfo, StdAddr,
    StdAddrFormat, Transaction, TxInfo,
};
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use num_bigint::BigInt;
use rand::Rng;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tycho_block_util::message::ExtMsgRepr;
use tycho_util::FastHashMap;

use crate::state::{
    BlockTransactionIdsIter, BlockTransactionsIterBuilder, BriefShardDescr, TransactionsIterBuilder,
};
use crate::util::jrpc_extractor::{
    JrpcError, JrpcErrorResponse, JrpcOkResponse, JSONRPC_FIELD, JSONRPC_VERSION,
};
use crate::util::serde_helpers::{
    self, normalize_base64, should_normalize_base64, Base64BytesWithLimit,
};
use crate::util::tonlib_helpers::{load_bytes_rope, StackParser};

// === ID ===

#[derive(Debug, Clone)]
pub enum JrpcId {
    Skip,
    Set(StringOrNumber),
}

impl<'de> Deserialize<'de> for JrpcId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StringOrNumber::deserialize(deserializer).map(Self::Set)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrNumber {
    Number(i64),
    String(String),
}

// Strange JRPC OK response.
impl<T: Serialize> Serialize for JrpcOkResponse<JrpcId, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let field_count = match &self.id {
            JrpcId::Skip => 2,
            JrpcId::Set(_) => 4,
        };

        let mut ser = serializer.serialize_struct("JrpcResponse", field_count)?;

        if let JrpcId::Set(id) = &self.id {
            ser.serialize_field(JSONRPC_FIELD, JSONRPC_VERSION)?;
            ser.serialize_field("id", id)?;
        }

        ser.serialize_field("result", &self.result)?;
        ser.serialize_field("ok", &true)?;
        ser.end()
    }
}

impl Serialize for JrpcErrorResponse<JrpcId> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let id;
        let field_count;
        match &self.id {
            None => {
                id = Some(None);
                field_count = 4;
            }
            Some(JrpcId::Skip) => {
                id = None;
                field_count = 2;
            }
            Some(JrpcId::Set(x)) => {
                id = Some(Some(x));
                field_count = 4;
            }
        };

        let mut ser = serializer.serialize_struct("JrpcResponse", field_count)?;

        if let Some(id) = id {
            ser.serialize_field(JSONRPC_FIELD, JSONRPC_VERSION)?;
            ser.serialize_field("id", &id)?;
        }

        ser.serialize_field("error", &JrpcError {
            code: self.code,
            message: &self.message,
        })?;
        ser.serialize_field("ok", &false)?;
        ser.end()
    }
}

// === Requests ===

#[derive(Debug, Deserialize)]
pub struct EmptyParams;

#[derive(Debug, Deserialize)]
pub struct GetShardsParams {
    pub seqno: u32,
}

#[derive(Debug)]
pub struct DetectAddressParams {
    pub address: StdAddr,
    pub base64_flags: Option<Base64StdAddrFlags>,
}

impl<'de> Deserialize<'de> for DetectAddressParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        struct Helper {
            address: String,
        }

        let Helper { mut address } = <_>::deserialize(deserializer)?;
        if address.len() == 64 {
            return Ok(Self {
                address: StdAddr::new(-1, HashBytes::from_str(&address).map_err(Error::custom)?),
                base64_flags: None,
            });
        }

        let is_base64 = address.len() == 48;

        // Try to normalize a URL-safe base64.
        if is_base64 && should_normalize_base64(&address) && !normalize_base64(&mut address) {
            return Err(Error::custom("invalid character"));
        }

        let (address, flags) =
            StdAddr::from_str_ext(&address, StdAddrFormat::any()).map_err(Error::custom)?;

        Ok(Self {
            address,
            base64_flags: is_base64.then_some(flags),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct AccountParams {
    #[serde(with = "serde_helpers::tonlib_address")]
    pub address: StdAddr,
}

#[derive(Debug, Deserialize)]
pub struct BlockHeaderParams {
    pub workchain: i8,
    pub shard: i64,
    pub seqno: u32,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub root_hash: Option<HashBytes>,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub file_hash: Option<HashBytes>,
}

#[derive(Debug, Deserialize)]
pub struct TransactionsParams {
    #[serde(with = "serde_helpers::tonlib_address")]
    pub address: StdAddr,
    #[serde(default = "default_tx_limit")]
    pub limit: u8,
    #[serde(
        default,
        deserialize_with = "serde_helpers::string_or_u64::deserialize_option"
    )]
    pub lt: Option<u64>,
    #[expect(unused)]
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub hash: Option<HashBytes>,
    #[serde(default, with = "serde_helpers::string_or_u64")]
    pub to_lt: u64,
}

const fn default_tx_limit() -> u8 {
    10
}

#[derive(Debug, Deserialize)]
pub struct BlockTransactionsParams {
    pub workchain: i8,
    pub shard: i64,
    pub seqno: u32,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub root_hash: Option<HashBytes>,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub file_hash: Option<HashBytes>,
    #[serde(
        default,
        deserialize_with = "serde_helpers::string_or_u64::deserialize_option"
    )]
    pub after_lt: Option<u64>,
    #[serde(default, with = "serde_helpers::option_tonlib_hash")]
    pub after_hash: Option<HashBytes>,
    #[serde(default = "default_block_tx_limit")]
    pub count: NonZeroU8,
}

const fn default_block_tx_limit() -> NonZeroU8 {
    NonZeroU8::new(10).unwrap()
}

#[derive(Debug, Deserialize)]
pub struct SendBocParams {
    #[serde(with = "Base64BytesWithLimit::<{ ExtMsgRepr::MAX_BOC_SIZE }>")]
    pub boc: bytes::Bytes,
}

#[derive(Debug, Deserialize)]
pub struct RunGetMethodParams {
    #[serde(with = "serde_helpers::tonlib_address")]
    pub address: StdAddr,
    #[serde(with = "serde_helpers::method_id")]
    pub method: i64,
    pub stack: Vec<TonlibInputStackItem>,
}

// === Responses ===

#[derive(Serialize)]
pub struct MasterchainInfoResponse {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub last: TonlibBlockId,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub state_root_hash: HashBytes,
    pub init: TonlibBlockId,
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
}

impl MasterchainInfoResponse {
    pub const TY: &str = "blocks.masterchainInfo";
}

#[derive(Serialize)]
pub struct ShardsResponse<'a> {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(serialize_with = "ShardsResponse::serialize_shards")]
    pub shards: &'a [BriefShardDescr],
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
}

impl<'a> ShardsResponse<'a> {
    pub const TY: &'static str = "blocks.shards";

    fn serialize_shards<S: serde::Serializer>(
        shards: &'a [BriefShardDescr],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_seq(Some(shards.len()))?;
        for descr in shards {
            s.serialize_element(&TonlibBlockId {
                ty: TonlibBlockId::TY,
                workchain: descr.shard_ident.workchain(),
                shard: descr.shard_ident.prefix() as i64,
                seqno: descr.seqno,
                root_hash: descr.root_hash,
                file_hash: descr.file_hash,
            })?;
        }
        s.end()
    }
}

pub struct AddressFormsResponse {
    pub raw_form: StdAddr,
    pub given_type: AddressType,
    pub test_only: bool,
}

impl Serialize for AddressFormsResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Base64Form<'a> {
            #[serde(with = "serde_helpers::string")]
            b64: DisplayBase64StdAddr<'a>,
            #[serde(with = "serde_helpers::string")]
            b64url: DisplayBase64StdAddr<'a>,
        }

        impl<'a> Base64Form<'a> {
            fn new(addr: &'a StdAddr, bounceable: bool, testnet: bool) -> Self {
                let flags = Base64StdAddrFlags {
                    testnet,
                    base64_url: false,
                    bounceable,
                };
                Self {
                    b64: DisplayBase64StdAddr { addr, flags },
                    b64url: DisplayBase64StdAddr {
                        addr,
                        flags: Base64StdAddrFlags {
                            base64_url: true,
                            ..flags
                        },
                    },
                }
            }
        }

        let mut s = serializer.serialize_struct("AddressFormsResponse", 5)?;
        s.serialize_field("raw_form", &self.raw_form)?;
        s.serialize_field(
            "bounceable",
            &Base64Form::new(&self.raw_form, true, self.test_only),
        )?;
        s.serialize_field(
            "non_bounceable",
            &Base64Form::new(&self.raw_form, false, self.test_only),
        )?;
        s.serialize_field("given_type", &self.given_type)?;
        s.serialize_field("test_only", &self.test_only)?;
        s.end()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressType {
    RawForm,
    FriendlyBounceable,
    FriendlyNonBounceable,
}

#[derive(Serialize)]
pub struct AddressInformationResponse {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_helpers::string")]
    pub balance: Tokens,
    pub extra_currencies: [(); 0],
    #[serde(with = "serde_helpers::boc_or_empty")]
    pub code: Option<Cell>,
    #[serde(with = "serde_helpers::boc_or_empty")]
    pub data: Option<Cell>,
    pub last_transaction_id: TonlibTransactionId,
    pub block_id: TonlibBlockId,
    #[serde(serialize_with = "serde_helpers::tonlib_hash::serialize_or_empty")]
    pub frozen_hash: Option<HashBytes>,
    pub sync_utime: u32,
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
    pub state: TonlibAccountStatus,
}

impl AddressInformationResponse {
    pub const TY: &str = "raw.fullAccountState";
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TonlibAccountStatus {
    Uninitialized,
    Frozen,
    Active,
}

#[derive(Serialize)]
pub struct ExtendedAddressInformationResponse<'a> {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub address: TonlibAddress<'a>,
    #[serde(with = "serde_helpers::string")]
    pub balance: Tokens,
    pub extra_currencies: [(); 0],
    pub last_transaction_id: TonlibTransactionId,
    pub block_id: TonlibBlockId,
    pub sync_utime: u32,
    pub account_state: ParsedAccountState,
    pub revision: i32,
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
}

impl ExtendedAddressInformationResponse<'_> {
    pub const TY: &'static str = "fullAccountState";
}

#[derive(Serialize)]
pub struct WalletInformationResponse {
    pub wallet: bool,
    #[serde(with = "serde_helpers::string")]
    pub balance: Tokens,
    pub extra_currencies: [(); 0],
    pub account_state: TonlibAccountStatus,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub fields: Option<WalletFields>,
    pub last_transaction_id: TonlibTransactionId,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "contract_type")]
pub enum TokenData {
    JettonMaster(JettonMasterData),
    JettonWallet(JettonWalletData),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenDataType {
    JettonMaster,
    JettonWallet,
}

impl TokenDataType {
    pub const fn getter_name(&self) -> &'static str {
        match self {
            Self::JettonMaster => "get_jetton_data",
            Self::JettonWallet => "get_wallet_data",
        }
    }
}

pub trait GetTokenData: Sized {
    fn from_stack(stack: RcStack) -> anyhow::Result<Self>;
}

type RcStack = tycho_vm::SafeRc<tycho_vm::Stack>;

#[derive(Serialize)]
pub struct JettonMasterData {
    // NOTE: For some unknown reason the original
    // implementationo returns this as `int`,
    // but this definitely will not work for large values.
    #[serde(with = "serde_helpers::string")]
    pub total_supply: num_bigint::BigInt,
    pub mintable: bool,
    #[serde(with = "serde_helpers::option_tonlib_address")]
    pub admin_address: Option<StdAddr>,
    pub jetton_content: TokenDataAttributes,
    #[serde(with = "Boc")]
    pub jetton_wallet_code: Cell,
}

impl GetTokenData for JettonMasterData {
    fn from_stack(stack: RcStack) -> anyhow::Result<Self> {
        let mut parser = StackParser::begin_from_bottom(stack);
        Ok(Self {
            total_supply: parser.pop_int()?,
            mintable: parser.pop_bool()?,
            admin_address: parser.pop_address_or_none()?,
            jetton_content: parser
                .pop_cell()?
                .parse::<TokenDataAttributes>()
                .context("invalid token data attributes")?,
            jetton_wallet_code: parser.pop_cell()?,
        })
    }
}

#[derive(Serialize)]
pub struct JettonWalletData {
    #[serde(with = "serde_helpers::string")]
    pub balance: num_bigint::BigInt,
    #[serde(with = "serde_helpers::tonlib_address")]
    pub owner: StdAddr,
    #[serde(with = "serde_helpers::tonlib_address")]
    pub jetton: StdAddr,
    #[serde(with = "Boc")]
    pub jetton_wallet_code: Cell,
}

impl GetTokenData for JettonWalletData {
    fn from_stack(stack: RcStack) -> anyhow::Result<Self> {
        let mut parser = StackParser::begin_from_bottom(stack);
        Ok(Self {
            balance: parser.pop_int()?,
            owner: parser.pop_address()?,
            jetton: parser.pop_address()?,
            jetton_wallet_code: parser.pop_cell()?,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenDataAttributes {
    Onchain {
        data: FastHashMap<TokenDataAttribute, String>,
    },
    Offchain {
        data: String,
    },
}

impl TokenDataAttributes {
    pub fn parse_value(mut value: CellSlice<'_>) -> Result<Vec<u8>, everscale_types::error::Error> {
        if value.is_data_empty() {
            value = value.load_reference_as_slice()?;
        }

        match value.load_u8()? {
            0x00 => load_bytes_rope(value, false),
            0x01 => {
                let dict = Dict::<u32, Cell>::load_from(&mut value)?;
                let mut data = Vec::new();
                for item in dict.values() {
                    let item = item?;
                    data.extend_from_slice(&load_bytes_rope(item.as_slice()?, false)?);
                }
                Ok(data)
            }
            _ => Err(everscale_types::error::Error::InvalidTag),
        }
    }
}

impl<'a> Load<'a> for TokenDataAttributes {
    fn load_from(cs: &mut CellSlice<'a>) -> Result<Self, Error> {
        match cs.load_u8()? {
            0x00 => {
                let mut data = FastHashMap::default();
                let dict = Dict::<HashBytes, CellSlice<'_>>::load_from(cs)?;
                for item in dict.iter() {
                    let (name, value) = item?;
                    let value = Self::parse_value(value)?;
                    data.insert(
                        TokenDataAttribute::resolve(&name),
                        String::from_utf8_lossy(&value).into_owned(),
                    );
                }
                Ok(Self::Onchain { data })
            }
            0x01 => {
                let data = load_bytes_rope(*cs, false)?;
                Ok(Self::Offchain {
                    data: String::from_utf8_lossy(&data).into_owned(),
                })
            }
            _ => Err(Error::InvalidTag),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TokenDataAttribute {
    Known(&'static str),
    Unknown(HashBytes),
}

impl TokenDataAttribute {
    pub const KNOWN_ATTRIBUTES: [&str; 9] = [
        "uri",
        "name",
        "description",
        "image",
        "image_data",
        "symbol",
        "decimals",
        "amount_style",
        "render_type",
    ];

    pub fn resolve(hash: &HashBytes) -> Self {
        static KNOWN: OnceLock<FastHashMap<HashBytes, &'static str>> = OnceLock::new();
        let known = KNOWN.get_or_init(|| {
            let mut result = FastHashMap::with_capacity_and_hasher(
                Self::KNOWN_ATTRIBUTES.len(),
                Default::default(),
            );
            for name in Self::KNOWN_ATTRIBUTES {
                let hash = sha2::Sha256::digest(name);
                result.insert(HashBytes(hash.into()), name);
            }
            result
        });
        match known.get(hash).copied() {
            Some(name) => Self::Known(name),
            None => Self::Unknown(*hash),
        }
    }
}

impl std::fmt::Display for TokenDataAttribute {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Known(s) => f.write_str(s),
            Self::Unknown(s) => std::fmt::Display::fmt(s, f),
        }
    }
}

impl serde::Serialize for TokenDataAttribute {
    #[inline]
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[derive(Serialize)]
pub struct BlockHeaderResponse {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub id: TonlibBlockId,
    pub global_id: i32,
    pub version: u32,
    pub flags: u8,
    pub after_merge: bool,
    pub after_split: bool,
    pub before_split: bool,
    pub want_merge: bool,
    pub want_split: bool,
    pub validator_list_hash_short: u32,
    pub catchain_seqno: u32,
    pub min_ref_mc_seqno: u32,
    pub is_key_block: bool,
    pub prev_key_block_seqno: u32,
    #[serde(with = "serde_helpers::string")]
    pub start_lt: u64,
    #[serde(with = "serde_helpers::string")]
    pub end_lt: u64,
    pub gen_utime: u32,
    pub vert_seqno: u32,
    pub prev_blocks: Vec<TonlibBlockId>,
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
}

impl BlockHeaderResponse {
    pub const TY: &'static str = "blocks.header";
}

// === Transactions Response ===

pub struct GetTransactionsResponse<'a> {
    pub address: &'a StdAddr,
    pub list: RefCell<Option<TransactionsIterBuilder>>,
    pub limit: u8,
    pub to_lt: u64,
}

impl Serialize for GetTransactionsResponse<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let list = self.list.borrow_mut().take().unwrap();

        let mut seq = serializer.serialize_seq(None)?;

        let mut buffer = String::new();

        // NOTE: We use a `.map` from a separate impl thus we cannot use `.try_for_each`.
        #[allow(clippy::map_collect_result_unit)]
        list.map(|item| {
            TonlibTransaction::handle_serde::<S, _, _, _, _>(
                item,
                &mut buffer,
                |tx| tx.lt > self.to_lt,
                |_| self.address.clone(),
                |tx| seq.serialize_element(tx),
            )
        })
        .take(self.limit as _)
        .collect::<Result<(), _>>()?;

        seq.end()
    }
}

pub struct GetBlockTransactionsResponse {
    pub req_count: u8,
    pub transactions: RefCell<Option<BlockTransactionIdsIter>>,
}

impl GetBlockTransactionsResponse {
    const TY: &'static str = "blocks.transactions";
}

impl Serialize for GetBlockTransactionsResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct TransactionsList {
            req_count: usize,
            incomplete: std::cell::Cell<bool>,
            transactions: RefCell<BlockTransactionIdsIter>,
        }

        impl Serialize for TransactionsList {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(None)?;

                let mut iter = self.transactions.borrow_mut();
                let mut n = 0usize;
                for item in iter.by_ref().take(self.req_count) {
                    n += 1;
                    seq.serialize_element(&TonlibBlockTransactionId {
                        ty: TonlibBlockTransactionId::TY,
                        mode: 135,
                        account: &item.account,
                        lt: item.lt,
                        hash: &item.hash,
                    })?;
                }

                if n >= self.req_count {
                    self.incomplete.set(iter.next().is_some());
                }

                seq.end()
            }
        }

        let transactions = self
            .transactions
            .borrow_mut()
            .take()
            .expect("transactions response must not be serialized twise");

        let mut s = serializer.serialize_struct("GetBlockTransactionsResponse", 6)?;
        s.serialize_field("@type", Self::TY)?;
        s.serialize_field("id", &TonlibBlockId::from(*transactions.block_id()))?;
        s.serialize_field("req_count", &self.req_count)?;

        let transactions = TransactionsList {
            req_count: self.req_count as usize,
            incomplete: std::cell::Cell::new(false),
            transactions: RefCell::new(transactions),
        };
        s.serialize_field("transactions", &transactions)?;
        s.serialize_field("incomplete", &transactions.incomplete.get())?;
        s.serialize_field("@extra", &TonlibExtra)?;
        s.end()
    }
}

pub struct GetBlockTransactionsExtResponse {
    pub req_count: u8,
    pub transactions: RefCell<Option<BlockTransactionsIterBuilder>>,
}

impl GetBlockTransactionsExtResponse {
    const TY: &'static str = "blocks.transactionsExt";
}

impl Serialize for GetBlockTransactionsExtResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct TransactionsList<'a> {
            req_count: usize,
            incomplete: std::cell::Cell<bool>,
            transactions: &'a RefCell<Option<BlockTransactionsIterBuilder>>,
        }

        impl Serialize for TransactionsList<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let iter = self
                    .transactions
                    .borrow_mut()
                    .take()
                    .expect("transactions response must not be serialized twise");

                let workchain = iter.block_id().shard.workchain() as i8;

                let mut seq = serializer.serialize_seq(None)?;

                let mut buffer = String::new();
                let mut n = 0usize;

                // NOTE: We use a `.map` from a separate impl thus we cannot use `.try_for_each`.
                #[allow(clippy::map_collect_result_unit)]
                let mut iter = iter.map(|_, _, item| {
                    TonlibTransaction::handle_serde::<S, _, _, _, _>(
                        item,
                        &mut buffer,
                        |_| true,
                        |tx| StdAddr::new(workchain, tx.account),
                        |tx| {
                            n += 1;
                            seq.serialize_element(&TonlibBlockTransaction {
                                tx,
                                account: tx.address.account_address,
                            })
                        },
                    )
                });

                iter.by_ref()
                    .take(self.req_count as _)
                    .collect::<Result<(), _>>()?;

                let mut iter = iter.into_ids();

                if n >= self.req_count {
                    self.incomplete.set(iter.next().is_some());
                }

                seq.end()
            }
        }

        let mut s = serializer.serialize_struct("GetBlockTransactionsExtResponse", 6)?;
        s.serialize_field("@type", Self::TY)?;
        s.serialize_field(
            "id",
            &TonlibBlockId::from(*self.transactions.borrow().as_ref().unwrap().block_id()),
        )?;
        s.serialize_field("req_count", &self.req_count)?;

        let transactions = TransactionsList {
            req_count: self.req_count as usize,
            incomplete: std::cell::Cell::new(false),
            transactions: &self.transactions,
        };
        s.serialize_field("transactions", &transactions)?;
        s.serialize_field("incomplete", &transactions.incomplete.get())?;
        s.serialize_field("@extra", &TonlibExtra)?;
        s.end()
    }
}

#[derive(Serialize)]
pub struct TonlibBlockTransactionId<'a> {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub mode: u8,
    #[serde(with = "serde_helpers::string")]
    pub account: &'a StdAddr,
    #[serde(with = "serde_helpers::string")]
    pub lt: u64,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: &'a HashBytes,
}

impl TonlibBlockTransactionId<'_> {
    pub const TY: &'static str = "blocks.shortTxId";
}

#[derive(Serialize)]
pub struct TonlibBlockTransaction<'t, 'a> {
    #[serde(flatten)]
    pub tx: &'t TonlibTransaction<'a>,
    pub account: &'a StdAddr,
}

#[derive(Serialize)]
pub struct TonlibTransaction<'a> {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub address: TonlibAddress<'a>,
    pub utime: u32,
    pub data: &'a str,
    pub transaction_id: TonlibTransactionId,
    #[serde(with = "serde_helpers::string")]
    pub fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    pub storage_fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    pub other_fee: Tokens,
    #[serde(serialize_with = "TonlibMessage::serialize_in_msg")]
    pub in_msg: Option<Cell>,
    pub out_msgs: Vec<TonlibMessage>,
}

impl TonlibTransaction<'_> {
    pub const TY: &'static str = "raw.transaction";

    fn handle_serde<S, T, F, A, P>(
        item: T,
        buffer: &mut String,
        mut filter: F,
        mut address: A,
        mut serialize: P,
    ) -> Option<Result<(), S::Error>>
    where
        S: serde::Serializer,
        T: AsRef<[u8]>,
        for<'a> A: FnMut(&'a Transaction) -> StdAddr,
        for<'a> F: FnMut(&'a Transaction) -> bool,
        for<'t, 'a> P: FnMut(&'t TonlibTransaction<'a>) -> Result<(), S::Error>,
    {
        use serde::ser::Error;

        let item = item.as_ref();
        let cell = match Boc::decode(item) {
            Ok(cell) => cell,
            Err(e) => return Some(Err(S::Error::custom(e))),
        };
        let tx = match cell.parse::<Transaction>() {
            Ok(tx) => tx,
            Err(e) => return Some(Err(S::Error::custom(e))),
        };
        if !filter(&tx) {
            return None;
        }

        let hash = *cell.repr_hash();
        drop(cell);

        Some((|| {
            let mut fee = tx.total_fees.tokens;

            let mut out_msgs = Vec::with_capacity(tx.out_msg_count.into_inner() as _);
            for item in tx.out_msgs.values() {
                let msg = item
                    .and_then(|cell| TonlibMessage::parse(&cell))
                    .map_err(Error::custom)?;

                fee = fee.saturating_add(msg.fwd_fee);
                fee = fee.saturating_add(msg.ihr_fee);
                out_msgs.push(msg);
            }

            let storage_fee = match tx.load_info().map_err(Error::custom)? {
                TxInfo::Ordinary(info) => match info.storage_phase {
                    Some(phase) => phase.storage_fees_collected,
                    None => Tokens::ZERO,
                },
                TxInfo::TickTock(info) => info.storage_phase.storage_fees_collected,
            };

            BASE64_STANDARD.encode_string(item, buffer);

            let address = address(&tx);
            let item = TonlibTransaction {
                ty: TonlibTransaction::TY,
                address: TonlibAddress::new(&address),
                utime: tx.now,
                data: &*buffer,
                transaction_id: TonlibTransactionId::new(tx.lt, hash),
                fee,
                storage_fee,
                other_fee: fee.saturating_sub(storage_fee),
                in_msg: tx.in_msg,
                out_msgs,
            };
            let res = serialize(&item);
            buffer.clear();
            res
        })())
    }
}

#[derive(Serialize)]
pub struct TonlibMessage {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
    #[serde(with = "serde_helpers::option_tonlib_address")]
    pub source: Option<StdAddr>,
    #[serde(with = "serde_helpers::option_tonlib_address")]
    pub destination: Option<StdAddr>,
    #[serde(with = "serde_helpers::string")]
    pub value: Tokens,
    pub extra_currencies: [(); 0],
    pub fwd_fee: Tokens,
    pub ihr_fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    pub created_lt: u64,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub body_hash: HashBytes,
    pub msg_data: TonlibMessageData,
}

impl TonlibMessage {
    pub const TY: &str = "raw.message";

    fn parse(cell: &Cell) -> Result<Self, Error> {
        fn to_std_addr(addr: IntAddr) -> Option<StdAddr> {
            match addr {
                IntAddr::Std(addr) => Some(addr),
                IntAddr::Var(_) => None,
            }
        }

        let hash = *cell.repr_hash();
        let msg = cell.parse::<Message<'_>>()?;

        let source;
        let destination;
        let value;
        let fwd_fee;
        let ihr_fee;
        let created_lt;
        match msg.info {
            MsgInfo::Int(info) => {
                source = to_std_addr(info.src);
                destination = to_std_addr(info.dst);
                value = info.value.tokens;
                fwd_fee = info.fwd_fee;
                ihr_fee = info.ihr_fee;
                created_lt = info.created_lt;
            }
            MsgInfo::ExtIn(info) => {
                source = None;
                destination = to_std_addr(info.dst);
                value = Tokens::ZERO;
                fwd_fee = Tokens::ZERO;
                ihr_fee = Tokens::ZERO;
                created_lt = 0;
            }
            MsgInfo::ExtOut(info) => {
                source = to_std_addr(info.src);
                destination = None;
                value = Tokens::ZERO;
                fwd_fee = Tokens::ZERO;
                ihr_fee = Tokens::ZERO;
                created_lt = info.created_lt;
            }
        }

        let body = CellBuilder::build_from(msg.body)?;
        let body_hash = *body.repr_hash();
        let init_state = msg.init.map(CellBuilder::build_from).transpose()?;

        Ok(Self {
            ty: Self::TY,
            hash,
            source,
            destination,
            value,
            extra_currencies: [],
            fwd_fee,
            ihr_fee,
            created_lt,
            body_hash,
            msg_data: TonlibMessageData {
                ty: TonlibMessageData::TY,
                body,
                init_state,
            },
        })
    }

    fn serialize_in_msg<S>(cell: &Option<Cell>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        match cell {
            Some(cell) => {
                let msg = Self::parse(cell).map_err(Error::custom)?;
                serializer.serialize_some(&msg)
            }
            None => serializer.serialize_none(),
        }
    }
}

#[derive(Serialize)]
pub struct TonlibMessageData {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "Boc")]
    pub body: Cell,
    #[serde(with = "serde_helpers::boc_or_empty")]
    pub init_state: Option<Cell>,
}

impl TonlibMessageData {
    pub const TY: &str = "msg.dataRaw";
}

#[derive(Serialize)]
pub struct TonlibAddress<'a> {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_helpers::tonlib_address")]
    pub account_address: &'a StdAddr,
}

impl<'a> TonlibAddress<'a> {
    pub const TY: &'static str = "accountAddress";

    pub fn new(address: &'a StdAddr) -> Self {
        Self {
            ty: Self::TY,
            account_address: address,
        }
    }
}

// === Message hash response ===

#[derive(Serialize)]
pub struct ExtMsgInfoResponse {
    pub ty: &'static str,
    pub hash: HashBytes,
    pub hash_norm: HashBytes,
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
}

impl ExtMsgInfoResponse {
    pub const TY: &str = "raw.extMessageInfo";
}

// === Vm Response ===

#[derive(Serialize)]
pub struct RunGetMethodResponse {
    pub ty: &'static str,
    pub exit_code: i32,
    pub gas_used: u64,
    #[serde(serialize_with = "RunGetMethodResponse::serialize_stack")]
    pub stack: Option<tycho_vm::SafeRc<tycho_vm::Stack>>,
    pub last_transaction_id: TonlibTransactionId,
    pub block_id: TonlibBlockId,
    #[serde(rename = "@extra")]
    pub extra: TonlibExtra,
}

impl RunGetMethodResponse {
    pub const TY: &str = "smc.runResult";

    pub fn set_items_limit(limit: usize) {
        STACK_ITEMS_LIMIT.set(limit);
    }

    fn serialize_stack<S>(
        value: &Option<tycho_vm::SafeRc<tycho_vm::Stack>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(stack) => {
                let mut seq = serializer.serialize_seq(Some(stack.depth()))?;
                for item in &stack.items {
                    seq.serialize_element(&TonlibOutputStackItem(item.as_ref()))?;
                }
                seq.end()
            }
            None => [(); 0].serialize(serializer),
        }
    }

    fn serialize_stack_item<S>(value: &DynStackValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        use tycho_vm::StackValueType;

        let is_limit_ok = STACK_ITEMS_LIMIT.with(|limit| {
            let current_limit = limit.get();
            if current_limit > 0 {
                limit.set(current_limit - 1);
                true
            } else {
                false
            }
        });

        if !is_limit_ok {
            return Err(Error::custom("too many stack items in response"));
        }

        struct Num<'a>(&'a BigInt);

        impl std::fmt::Display for Num<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let sign = if self.0.sign() == num_bigint::Sign::Minus {
                    "-"
                } else {
                    ""
                };
                write!(f, "{sign}0x{:x}", self.0.magnitude())
            }
        }

        impl Serialize for Num<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        struct List<'a>(&'a DynStackValue, &'a DynStackValue);

        impl Serialize for List<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(None)?;
                seq.serialize_element(&TonlibOutputStackItem(self.0))?;
                let mut next = self.1;
                while !next.is_null() {
                    let (head, tail) = next
                        .as_pair()
                        .ok_or_else(|| Error::custom("invalid list"))?;
                    seq.serialize_element(&TonlibOutputStackItem(head))?;
                    next = tail;
                }
                seq.end()
            }
        }

        struct Tuple<'a>(&'a [tycho_vm::RcStackValue]);

        impl Serialize for Tuple<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(Some(self.0.len()))?;
                for item in self.0 {
                    seq.serialize_element(&TonlibOutputStackItem(item.as_ref()))?;
                }
                seq.end()
            }
        }

        #[derive(Serialize)]
        struct CellBytes<'a> {
            #[serde(with = "Boc")]
            bytes: &'a Cell,
        }

        match value.ty() {
            StackValueType::Null => ("list", [(); 0]).serialize(serializer),
            StackValueType::Int => match value.as_int() {
                Some(int) => ("num", Num(int)).serialize(serializer),
                None => ("num", "(null)").serialize(serializer),
            },
            StackValueType::Cell => {
                let cell = value
                    .as_cell()
                    .ok_or_else(|| Error::custom("invalid cell"))?;
                ("cell", CellBytes { bytes: cell }).serialize(serializer)
            }
            StackValueType::Slice => {
                let slice = value
                    .as_cell_slice()
                    .ok_or_else(|| Error::custom("invalid slice"))?;

                let built;
                let cell = if slice.range().is_full(slice.cell().as_ref()) {
                    slice.cell()
                } else {
                    built = CellBuilder::build_from(slice.apply()).map_err(Error::custom)?;
                    &built
                };

                ("cell", CellBytes { bytes: cell }).serialize(serializer)
            }
            StackValueType::Tuple => match value.as_list() {
                Some((head, tail)) => ("list", List(head, tail)).serialize(serializer),
                None => {
                    let tuple = value
                        .as_tuple()
                        .ok_or_else(|| Error::custom("invalid list"))?;
                    ("tuple", Tuple(tuple)).serialize(serializer)
                }
            },
            _ => Err(Error::custom("unsupported stack item")),
        }
    }
}

thread_local! {
    static STACK_ITEMS_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[repr(transparent)]
struct TonlibOutputStackItem<'a>(&'a DynStackValue);

impl Serialize for TonlibOutputStackItem<'_> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        const MAX_DEPTH: usize = 16;

        thread_local! {
            static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        }

        let is_depth_ok = DEPTH.with(|depth| {
            let current_depth = depth.get();
            if current_depth < MAX_DEPTH {
                depth.set(current_depth + 1);
                true
            } else {
                false
            }
        });

        if !is_depth_ok {
            return Err(Error::custom("too deep stack item"));
        }

        scopeguard::defer! {
            DEPTH.with(|depth| {
                depth.set(depth.get() - 1);
            })
        };

        RunGetMethodResponse::serialize_stack_item(self.0, serializer)
    }
}

type DynStackValue = dyn tycho_vm::StackValue + 'static;

// === Input Stack Item ===

#[derive(Debug)]
pub enum TonlibInputStackItem {
    Num(num_bigint::BigInt),
    Cell(Cell),
    Slice(Cell),
    Builder(Cell),
}

impl TryFrom<TonlibInputStackItem> for tycho_vm::RcStackValue {
    type Error = Error;

    fn try_from(value: TonlibInputStackItem) -> Result<Self, Self::Error> {
        match value {
            TonlibInputStackItem::Num(num) => Ok(tycho_vm::RcStackValue::new_dyn_value(num)),
            TonlibInputStackItem::Cell(cell) => Ok(tycho_vm::RcStackValue::new_dyn_value(cell)),
            TonlibInputStackItem::Slice(cell) => {
                if cell.is_exotic() {
                    return Err(Error::UnexpectedExoticCell);
                }
                let slice = tycho_vm::OwnedCellSlice::new_allow_exotic(cell);
                Ok(tycho_vm::RcStackValue::new_dyn_value(slice))
            }
            TonlibInputStackItem::Builder(cell) => {
                let mut b = CellBuilder::new();
                b.store_slice(cell.as_slice_allow_exotic())?;
                Ok(tycho_vm::RcStackValue::new_dyn_value(b))
            }
        }
    }
}

impl<'de> Deserialize<'de> for TonlibInputStackItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        struct IntBounds {
            min: BigInt,
            max: BigInt,
        }

        impl IntBounds {
            fn get() -> &'static Self {
                static BOUNDS: OnceLock<IntBounds> = OnceLock::new();
                BOUNDS.get_or_init(|| Self {
                    min: BigInt::from(-1) << 256,
                    max: (BigInt::from(1) << 256) - 1,
                })
            }

            fn contains(&self, int: &BigInt) -> bool {
                *int >= self.min && *int <= self.max
            }
        }

        #[derive(Deserialize)]
        enum StackItemType {
            #[serde(rename = "num")]
            Num,
            #[serde(rename = "tvm.Cell")]
            Cell,
            #[serde(rename = "tvm.Slice")]
            Slice,
            #[serde(rename = "tvm.Builder")]
            Builder,
        }

        struct StackItemVisitor;

        impl<'de> serde::de::Visitor<'de> for StackItemVisitor {
            type Value = TonlibInputStackItem;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a tuple of two items")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                fn map_cell<T, F: FnOnce(Cell) -> T, E: Error>(
                    value: impl AsRef<str>,
                    f: F,
                ) -> Result<T, E> {
                    Boc::decode_base64(value.as_ref())
                        .map(f)
                        .map_err(Error::custom)
                }

                let Some(ty) = seq.next_element()? else {
                    return Err(Error::custom(
                        "expected the first item to be a stack item type",
                    ));
                };

                let Some(serde_helpers::BorrowedStr(value)) = seq.next_element()? else {
                    return Err(Error::custom(
                        "expected the second item to be a stack item value",
                    ));
                };

                if seq
                    .next_element::<&serde_json::value::RawValue>()?
                    .is_some()
                {
                    return Err(Error::custom("too many tuple items"));
                }

                match ty {
                    StackItemType::Num => {
                        const MAX_INT_LEN: usize = 79;

                        if value.len() > MAX_INT_LEN {
                            return Err(Error::invalid_length(
                                value.len(),
                                &"a decimal integer in range [-2^256, 2^256)",
                            ));
                        }

                        let int = BigInt::from_str(value.as_ref()).map_err(Error::custom)?;
                        if !IntBounds::get().contains(&int) {
                            return Err(Error::custom("integer out of bounds"));
                        }
                        Ok(TonlibInputStackItem::Num(int))
                    }
                    StackItemType::Cell => map_cell(value, TonlibInputStackItem::Cell),
                    StackItemType::Slice => map_cell(value, TonlibInputStackItem::Slice),
                    StackItemType::Builder => map_cell(value, TonlibInputStackItem::Builder),
                }
            }
        }

        deserializer.deserialize_seq(StackItemVisitor)
    }
}

// === Common Stuff ===

#[derive(Serialize)]
pub struct TonlibTransactionId {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_helpers::string")]
    pub lt: u64,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub hash: HashBytes,
}

impl TonlibTransactionId {
    pub const TY: &str = "internal.transactionId";

    pub fn new(lt: u64, hash: HashBytes) -> Self {
        Self {
            ty: Self::TY,
            lt,
            hash,
        }
    }
}

impl Default for TonlibTransactionId {
    fn default() -> Self {
        Self::new(0, HashBytes::ZERO)
    }
}

#[derive(Serialize)]
pub struct TonlibBlockId {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub workchain: i32,
    #[serde(with = "serde_helpers::string")]
    pub shard: i64,
    pub seqno: u32,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub root_hash: HashBytes,
    #[serde(with = "serde_helpers::tonlib_hash")]
    pub file_hash: HashBytes,
}

impl TonlibBlockId {
    pub const TY: &str = "ton.blockIdExt";
}

impl From<BlockId> for TonlibBlockId {
    fn from(value: BlockId) -> Self {
        Self {
            ty: Self::TY,
            workchain: value.shard.workchain(),
            shard: value.shard.prefix() as i64,
            seqno: value.seqno,
            root_hash: value.root_hash,
            file_hash: value.file_hash,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TonlibExtra;

impl Serialize for TonlibExtra {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl std::fmt::Display for TonlibExtra {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let rand: f32 = rand::thread_rng().gen();

        write!(f, "{now}:0:{rand}")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TonlibOk;

impl Serialize for TonlibOk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("Ok", 1)?;
        s.serialize_field("@type", "ok")?;
        s.end()
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "@type")]
pub enum ParsedAccountState {
    #[serde(rename = "raw.accountState")]
    Raw {
        #[serde(with = "serde_helpers::boc_or_empty")]
        code: Option<Cell>,
        #[serde(with = "serde_helpers::boc_or_empty")]
        data: Option<Cell>,
        #[serde(serialize_with = "serde_helpers::tonlib_hash::serialize_or_empty")]
        frozen_hash: Option<HashBytes>,
    },
    #[serde(rename = "wallet.v3.accountState")]
    WalletV3 {
        #[serde(with = "serde_helpers::string")]
        wallet_id: i64,
        seqno: i32,
    },
    #[serde(rename = "wallet.v4.accountState")]
    WalletV4 {
        #[serde(with = "serde_helpers::string")]
        wallet_id: i64,
        seqno: i32,
    },
    #[serde(rename = "wallet.highload.v1.accountState")]
    HighloadWalletV1 {
        #[serde(with = "serde_helpers::string")]
        wallet_id: i64,
        seqno: i32,
    },
    #[serde(rename = "wallet.highload.v2.accountState")]
    HighloadWalletV2 {
        #[serde(with = "serde_helpers::string")]
        wallet_id: i64,
    },
    #[serde(rename = "uninited.accountState")]
    Uninit {
        #[serde(serialize_with = "serde_helpers::tonlib_hash::serialize_or_empty")]
        frozen_hash: Option<HashBytes>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasicContractType {
    HighloadWalletV1,
    HighloadWalletV2,
    WalletV3,
    WalletV4,
}

pub struct BasicContractInfo {
    pub ty: BasicContractType,
    pub revision: i32,
}

impl BasicContractInfo {
    pub fn guess(code_hash: &HashBytes) -> Option<Self> {
        Some(match *code_hash {
            code_hash::WALLET_V3_R2 => Self {
                ty: BasicContractType::WalletV3,
                revision: 2,
            },
            code_hash::WALLET_V3_R1 => Self {
                ty: BasicContractType::WalletV3,
                revision: 1,
            },
            code_hash::WALLET_V4_R2 => Self {
                ty: BasicContractType::WalletV4,
                revision: 2,
            },
            code_hash::WALLET_V4_R1 => Self {
                ty: BasicContractType::WalletV4,
                revision: 1,
            },
            code_hash::HIGHLOAD_WALLET_V2_R2 => Self {
                ty: BasicContractType::HighloadWalletV2,
                revision: 2,
            },
            code_hash::HIGHLOAD_WALLET_V2_R1 => Self {
                ty: BasicContractType::HighloadWalletV2,
                revision: 1,
            },
            code_hash::HIGHLOAD_WALLET_V2 => Self {
                ty: BasicContractType::HighloadWalletV2,
                revision: -1,
            },
            code_hash::HIGHLOAD_WALLET_V1_R2 => Self {
                ty: BasicContractType::HighloadWalletV1,
                revision: 2,
            },
            code_hash::HIGHLOAD_WALLET_V1_R1 => Self {
                ty: BasicContractType::HighloadWalletV1,
                revision: 1,
            },
            code_hash::HIGHLOAD_WALLET_V1 => Self {
                ty: BasicContractType::HighloadWalletV1,
                revision: -1,
            },
            _ => return None,
        })
    }

    pub fn read_init_data(&self, data: &DynCell) -> Result<ParsedAccountState, Error> {
        let mut cs = data.as_slice()?;
        Ok(match self.ty {
            BasicContractType::HighloadWalletV1 => {
                let seqno = cs.load_u32()? as i32;
                let wallet_id = cs.load_u32()? as i64;
                ParsedAccountState::HighloadWalletV1 { wallet_id, seqno }
            }
            BasicContractType::HighloadWalletV2 => {
                let wallet_id = cs.load_u32()? as i64;
                ParsedAccountState::HighloadWalletV2 { wallet_id }
            }
            BasicContractType::WalletV3 => {
                let seqno = cs.load_u32()? as i32;
                let wallet_id = cs.load_u32()? as i64;
                ParsedAccountState::WalletV3 { wallet_id, seqno }
            }
            BasicContractType::WalletV4 => {
                let seqno = cs.load_u32()? as i32;
                let wallet_id = cs.load_u32()? as i64;
                ParsedAccountState::WalletV4 { wallet_id, seqno }
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum WalletType {
    #[serde(rename = "wallet v3 r1")]
    WalletV3R1,
    #[serde(rename = "wallet v3 r2")]
    WalletV3R2,
    #[serde(rename = "wallet v4 r1")]
    WalletV4R1,
    #[serde(rename = "wallet v4 r2")]
    WalletV4R2,
    #[serde(rename = "wallet v5 r1")]
    WalletV5R1,
    #[serde(rename = "nominator pool v1")]
    NominatorPoolV1,
    #[serde(rename = "highload wallet v1")]
    HighloadWalletV1,
    #[serde(rename = "highload wallet v1 r1")]
    HighloadWalletV1R1,
    #[serde(rename = "highload wallet v1 r2")]
    HighloadWalletV1R2,
    #[serde(rename = "highload wallet v2")]
    HighloadWalletV2,
    #[serde(rename = "highload wallet v2 r1")]
    HighloadWalletV2R1,
    #[serde(rename = "highload wallet v2 r2")]
    HighloadWalletV2R2,
}

#[derive(Serialize)]
pub struct WalletFields {
    pub wallet_type: WalletType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seqno: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wallet_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_signature_allowed: Option<bool>,
}

impl WalletFields {
    pub fn load_from(code: &DynCell, data: &DynCell) -> Result<Self, Error> {
        fn no_fields(ty: WalletType) -> WalletFields {
            WalletFields {
                wallet_type: ty,
                seqno: None,
                wallet_id: None,
                is_signature_allowed: None,
            }
        }

        fn only_wallet_id(ty: WalletType, data: &DynCell) -> Result<WalletFields, Error> {
            let mut fields = no_fields(ty);
            fields.wallet_id = Some(data.as_slice()?.load_u32()? as i32);
            Ok(fields)
        }

        fn seqno_and_wallet_id(ty: WalletType, data: &DynCell) -> Result<WalletFields, Error> {
            let mut fields = no_fields(ty);
            let mut data = data.as_slice()?;
            fields.seqno = Some(data.load_u32()? as i32);
            fields.wallet_id = Some(data.load_u32()? as i32);
            Ok(fields)
        }

        fn all_fields(ty: WalletType, data: &DynCell) -> Result<WalletFields, Error> {
            let mut fields = no_fields(ty);
            let mut data = data.as_slice()?;
            fields.is_signature_allowed = Some(data.load_bit()?);
            fields.seqno = Some(data.load_u32()? as i32);
            fields.wallet_id = Some(data.load_u32()? as i32);
            Ok(fields)
        }

        match *code.repr_hash() {
            code_hash::WALLET_V3_R1 => seqno_and_wallet_id(WalletType::WalletV3R1, data),
            code_hash::WALLET_V3_R2 => seqno_and_wallet_id(WalletType::WalletV3R2, data),
            code_hash::WALLET_V4_R1 => seqno_and_wallet_id(WalletType::WalletV4R1, data),
            code_hash::WALLET_V4_R2 => seqno_and_wallet_id(WalletType::WalletV4R2, data),
            code_hash::WALLET_V5_R1 => all_fields(WalletType::WalletV5R1, data),

            code_hash::HIGHLOAD_WALLET_V2 => only_wallet_id(WalletType::HighloadWalletV2, data),
            code_hash::HIGHLOAD_WALLET_V2_R1 => {
                only_wallet_id(WalletType::HighloadWalletV2R1, data)
            }
            code_hash::HIGHLOAD_WALLET_V2_R2 => {
                only_wallet_id(WalletType::HighloadWalletV2R2, data)
            }

            code_hash::HIGHLOAD_WALLET_V1 => {
                seqno_and_wallet_id(WalletType::HighloadWalletV1, data)
            }
            code_hash::HIGHLOAD_WALLET_V1_R1 => {
                seqno_and_wallet_id(WalletType::HighloadWalletV1R1, data)
            }
            code_hash::HIGHLOAD_WALLET_V1_R2 => {
                seqno_and_wallet_id(WalletType::HighloadWalletV1R2, data)
            }

            code_hash::NOMINATOR_POOL_V1 => Ok(no_fields(WalletType::NominatorPoolV1)),

            _ => Err(Error::InvalidTag),
        }
    }
}

mod code_hash {
    use super::*;

    pub const HIGHLOAD_WALLET_V1: HashBytes = HashBytes([
        0x0a, 0xb1, 0xff, 0x93, 0xa9, 0xe7, 0x9c, 0x0d, 0xef, 0xf4, 0x40, 0x5d, 0x8d, 0xad, 0x6b,
        0x5a, 0xc9, 0xf8, 0xc0, 0x1b, 0x2f, 0x1e, 0xbc, 0xb4, 0x25, 0x9f, 0xeb, 0x9e, 0x98, 0x38,
        0x00, 0x99,
    ]);

    pub const HIGHLOAD_WALLET_V1_R1: HashBytes = HashBytes([
        0xd8, 0xcd, 0xbb, 0xb7, 0x9f, 0x2c, 0x5c, 0xaa, 0x67, 0x7a, 0xc4, 0x50, 0x77, 0x0b, 0xe0,
        0x35, 0x1b, 0xe2, 0x1e, 0x12, 0x50, 0x48, 0x6d, 0xe8, 0x5c, 0xc5, 0x2a, 0xa3, 0x3d, 0xd1,
        0x64, 0x84,
    ]);

    pub const HIGHLOAD_WALLET_V1_R2: HashBytes = HashBytes([
        0x36, 0x8c, 0x03, 0x97, 0x2e, 0x5f, 0x4c, 0x24, 0x41, 0x2b, 0x81, 0x86, 0x52, 0xde, 0x1d,
        0xe2, 0xa2, 0x63, 0x0d, 0x1c, 0x01, 0x16, 0x09, 0x99, 0x9d, 0xff, 0x74, 0x5f, 0xb3, 0x68,
        0x49, 0xad,
    ]);

    pub const HIGHLOAD_WALLET_V2: HashBytes = HashBytes([
        0x94, 0x94, 0xd1, 0xcc, 0x8e, 0xdf, 0x12, 0xf0, 0x56, 0x71, 0xa1, 0xa9, 0xba, 0x09, 0x92,
        0x10, 0x96, 0xeb, 0x50, 0x81, 0x1e, 0x19, 0x24, 0xec, 0x65, 0xc3, 0xc6, 0x29, 0xfb, 0xb8,
        0x08, 0x12,
    ]);

    pub const HIGHLOAD_WALLET_V2_R1: HashBytes = HashBytes([
        0x8c, 0xeb, 0x45, 0xb3, 0xcd, 0x4b, 0x5c, 0xc6, 0x0e, 0xaa, 0xe1, 0xc1, 0x3b, 0x9c, 0x09,
        0x23, 0x92, 0x67, 0x7f, 0xe5, 0x36, 0xb2, 0xe9, 0xb2, 0xd8, 0x01, 0xb6, 0x2e, 0xff, 0x93,
        0x1f, 0xe1,
    ]);

    pub const HIGHLOAD_WALLET_V2_R2: HashBytes = HashBytes([
        0x0b, 0x3a, 0x88, 0x7a, 0xea, 0xcd, 0x2a, 0x7d, 0x40, 0xbb, 0x55, 0x50, 0xbc, 0x92, 0x53,
        0x15, 0x6a, 0x02, 0x90, 0x65, 0xae, 0xfb, 0x6d, 0x6b, 0x58, 0x37, 0x35, 0xd5, 0x8d, 0xa9,
        0xd5, 0xbe,
    ]);

    pub const WALLET_V3_R1: HashBytes = HashBytes([
        0xb6, 0x10, 0x41, 0xa5, 0x8a, 0x79, 0x80, 0xb9, 0x46, 0xe8, 0xfb, 0x9e, 0x19, 0x8e, 0x3c,
        0x90, 0x4d, 0x24, 0x79, 0x9f, 0xfa, 0x36, 0x57, 0x4e, 0xa4, 0x25, 0x1c, 0x41, 0xa5, 0x66,
        0xf5, 0x81,
    ]);

    pub const WALLET_V3_R2: HashBytes = HashBytes([
        0x84, 0xda, 0xfa, 0x44, 0x9f, 0x98, 0xa6, 0x98, 0x77, 0x89, 0xba, 0x23, 0x23, 0x58, 0x07,
        0x2b, 0xc0, 0xf7, 0x6d, 0xc4, 0x52, 0x40, 0x02, 0xa5, 0xd0, 0x91, 0x8b, 0x9a, 0x75, 0xd2,
        0xd5, 0x99,
    ]);

    pub const WALLET_V4_R1: HashBytes = HashBytes([
        0x64, 0xdd, 0x54, 0x80, 0x55, 0x22, 0xc5, 0xbe, 0x8a, 0x9d, 0xb5, 0x9c, 0xea, 0x01, 0x05,
        0xcc, 0xf0, 0xd0, 0x87, 0x86, 0xca, 0x79, 0xbe, 0xb8, 0xcb, 0x79, 0xe8, 0x80, 0xa8, 0xd7,
        0x32, 0x2d,
    ]);

    pub const WALLET_V4_R2: HashBytes = HashBytes([
        0xfe, 0xb5, 0xff, 0x68, 0x20, 0xe2, 0xff, 0x0d, 0x94, 0x83, 0xe7, 0xe0, 0xd6, 0x2c, 0x81,
        0x7d, 0x84, 0x67, 0x89, 0xfb, 0x4a, 0xe5, 0x80, 0xc8, 0x78, 0x86, 0x6d, 0x95, 0x9d, 0xab,
        0xd5, 0xc0,
    ]);

    pub const WALLET_V5_R1: HashBytes = HashBytes([
        0x20, 0x83, 0x4b, 0x7b, 0x72, 0xb1, 0x12, 0x14, 0x7e, 0x1b, 0x2f, 0xb4, 0x57, 0xb8, 0x4e,
        0x74, 0xd1, 0xa3, 0x0f, 0x04, 0xf7, 0x37, 0xd4, 0xf6, 0x2a, 0x66, 0x8e, 0x95, 0x52, 0xd2,
        0xb7, 0x2f,
    ]);

    pub const NOMINATOR_POOL_V1: HashBytes = HashBytes([
        0x9a, 0x3e, 0xc1, 0x4b, 0xc0, 0x98, 0xf6, 0xb4, 0x40, 0x64, 0xc3, 0x05, 0x22, 0x2c, 0xae,
        0xa2, 0x80, 0x0f, 0x17, 0xdd, 0xa8, 0x5e, 0xe6, 0xa8, 0x19, 0x8a, 0x70, 0x95, 0xed, 0xe1,
        0x0d, 0xcf,
    ]);
}
