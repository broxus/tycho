use std::cell::RefCell;
use std::str::FromStr;
use std::sync::OnceLock;

use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::models::{BlockId, IntAddr, Message, MsgInfo, StdAddr, Transaction, TxInfo};
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use num_bigint::BigInt;
use rand::Rng;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use tycho_block_util::message::ExtMsgRepr;
use tycho_storage::TransactionsIterBuilder;
use tycho_util::serde_helpers::{self, Base64BytesWithLimit};

// === Requests ===

#[derive(Debug, Deserialize)]
pub struct AccountParams {
    #[serde(with = "serde_tonlib_address")]
    pub address: StdAddr,
}

#[derive(Debug, Deserialize)]
pub struct TransactionsParams {
    #[serde(with = "serde_tonlib_address")]
    pub address: StdAddr,
    #[serde(default = "default_tx_limit")]
    pub limit: u8,
    #[serde(default, deserialize_with = "serde_string_or_u64::deserialize_option")]
    pub lt: Option<u64>,
    #[expect(unused)]
    #[serde(default, with = "serde_option_tonlib_hash")]
    pub hash: Option<HashBytes>,
    #[serde(default, with = "serde_string_or_u64")]
    pub to_lt: u64,
}

const fn default_tx_limit() -> u8 {
    10
}

#[derive(Debug, Deserialize)]
pub struct SendBocParams {
    #[serde(with = "Base64BytesWithLimit::<{ ExtMsgRepr::MAX_BOC_SIZE }>")]
    pub boc: bytes::Bytes,
}

#[derive(Debug, Deserialize)]
pub struct RunGetMethodParams {
    #[serde(with = "serde_tonlib_address")]
    pub address: StdAddr,
    #[serde(with = "serde_method_id")]
    pub method: i64,
    pub stack: Vec<TonlibInputStackItem>,
}

// === Responses ===

#[derive(Serialize)]
pub struct AddressInformationResponse {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_helpers::string")]
    pub balance: Tokens,
    pub extra_currencies: [(); 0],
    #[serde(with = "serde_boc_or_empty")]
    pub code: Option<Cell>,
    #[serde(with = "serde_boc_or_empty")]
    pub data: Option<Cell>,
    pub last_transaction_id: TonlibTransactionId,
    pub block_id: TonlibBlockId,
    #[serde(serialize_with = "serde_tonlib_hash::serialize_or_empty")]
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

// === Transactions Response ===

pub struct GetTransactionsResponse<'a> {
    pub address: &'a StdAddr,
    pub list: RefCell<Option<TransactionsIterBuilder<'a>>>,
    pub limit: u8,
    pub to_lt: u64,
}

impl Serialize for GetTransactionsResponse<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};

        let list = self.list.borrow_mut().take().unwrap();

        let mut seq = serializer.serialize_seq(None)?;

        let mut buffer = String::new();

        // NOTE: We use a `.map` from a separate impl thus we cannot use `.try_for_each`.
        #[allow(clippy::map_collect_result_unit)]
        list.map(|item| {
            let cell = match Boc::decode(item) {
                Ok(cell) => cell,
                Err(e) => return Some(Err(S::Error::custom(e))),
            };
            let tx = match cell.parse::<Transaction>() {
                Ok(tx) => tx,
                Err(e) => return Some(Err(S::Error::custom(e))),
            };
            if tx.lt <= self.to_lt {
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

                BASE64_STANDARD.encode_string(item, &mut buffer);
                let res = seq.serialize_element(&TonlibTransaction {
                    ty: TonlibTransaction::TY,
                    address: TonlibAddress::new(self.address),
                    utime: tx.now,
                    data: &buffer,
                    transaction_id: TonlibTransactionId::new(tx.lt, hash),
                    fee,
                    storage_fee,
                    other_fee: fee.saturating_sub(storage_fee),
                    in_msg: tx.in_msg,
                    out_msgs,
                });
                buffer.clear();
                res
            })())
        })
        .take(self.limit as _)
        .collect::<Result<(), _>>()?;

        seq.end()
    }
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
}

#[derive(Serialize)]
pub struct TonlibMessage {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_tonlib_hash")]
    pub hash: HashBytes,
    #[serde(with = "serde_option_tonlib_address")]
    pub source: Option<StdAddr>,
    #[serde(with = "serde_option_tonlib_address")]
    pub destination: Option<StdAddr>,
    #[serde(with = "serde_helpers::string")]
    pub value: Tokens,
    pub extra_currencies: [(); 0],
    pub fwd_fee: Tokens,
    pub ihr_fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    pub created_lt: u64,
    #[serde(with = "serde_tonlib_hash")]
    pub body_hash: HashBytes,
    pub msg_data: TonlibMessageData,
}

impl TonlibMessage {
    pub const TY: &str = "raw.message";

    fn parse(cell: &Cell) -> Result<Self, everscale_types::error::Error> {
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
    #[serde(with = "serde_boc_or_empty")]
    pub init_state: Option<Cell>,
}

impl TonlibMessageData {
    pub const TY: &str = "msg.dataRaw";
}

#[derive(Serialize)]
pub struct TonlibAddress<'a> {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_tonlib_address")]
    pub account_address: &'a StdAddr,
}

impl<'a> TonlibAddress<'a> {
    pub const TY: &'static str = "accountAddress";

    fn new(address: &'a StdAddr) -> Self {
        Self {
            ty: Self::TY,
            account_address: address,
        }
    }
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
                write!(f, "{sign}0x{:x}", self.0)
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
    type Error = everscale_types::error::Error;

    fn try_from(value: TonlibInputStackItem) -> Result<Self, Self::Error> {
        match value {
            TonlibInputStackItem::Num(num) => Ok(tycho_vm::RcStackValue::new_dyn_value(num)),
            TonlibInputStackItem::Cell(cell) => Ok(tycho_vm::RcStackValue::new_dyn_value(cell)),
            TonlibInputStackItem::Slice(cell) => {
                if cell.is_exotic() {
                    return Err(everscale_types::error::Error::UnexpectedExoticCell);
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
    #[serde(with = "serde_tonlib_hash")]
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
    #[serde(with = "serde_tonlib_hash")]
    pub root_hash: HashBytes,
    #[serde(with = "serde_tonlib_hash")]
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

mod serde_option_tonlib_address {
    use super::*;

    pub fn serialize<S>(value: &Option<StdAddr>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper<'a>(#[serde(with = "serde_tonlib_address")] &'a StdAddr);

        value.as_ref().map(Wrapper).serialize(serializer)
    }
}

mod serde_tonlib_address {
    use everscale_types::models::{StdAddr, StdAddrBase64Repr};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StdAddr, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StdAddrBase64Repr::<true>::deserialize(deserializer)
    }

    pub fn serialize<S>(value: &StdAddr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StdAddrBase64Repr::<true>::serialize(value, serializer)
    }
}

mod serde_option_tonlib_hash {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<HashBytes>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper(#[serde(with = "serde_tonlib_hash")] HashBytes);

        Option::<Wrapper>::deserialize(deserializer).map(|x| x.map(|Wrapper(x)| x))
    }
}

mod serde_tonlib_hash {
    use std::str::FromStr;

    use serde::de::Error;

    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashBytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serde_helpers::BorrowedStr(s) = <_>::deserialize(deserializer)?;
        HashBytes::from_str(s.as_ref()).map_err(Error::custom)
    }

    pub fn serialize<S>(value: &HashBytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut res = [0u8; 44];
        BASE64_STANDARD
            .encode_slice(value.as_array(), &mut res)
            .unwrap();
        // SAFETY: `res` is guaranteed to contain a valid ASCII base64.
        let res = unsafe { std::str::from_utf8_unchecked(&res) };

        serializer.serialize_str(res)
    }

    pub fn serialize_or_empty<S>(
        value: &Option<HashBytes>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => serialize(value, serializer),
            None => serializer.serialize_str(""),
        }
    }
}

mod serde_boc_or_empty {
    use super::*;

    pub fn serialize<S>(value: &Option<Cell>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => Boc::serialize(value, serializer),
            None => serializer.serialize_str(""),
        }
    }
}

mod serde_method_id {
    use std::borrow::Cow;

    use super::*;

    const MAX_METHOD_NAME_LEN: usize = 128;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum MethodId<'a> {
            Int(i64),
            String(#[serde(borrow)] Cow<'a, str>),
        }

        Ok(match <_>::deserialize(deserializer)? {
            MethodId::Int(int) => int,
            MethodId::String(str) => {
                let bytes = str.as_bytes();
                if bytes.len() > MAX_METHOD_NAME_LEN {
                    return Err(Error::custom("method name is too long"));
                }
                everscale_types::crc::crc_16(bytes) as i64 | 0x10000
            }
        })
    }
}

mod serde_string_or_u64 {
    use super::*;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Int(u64),
        String(#[serde(with = "serde_helpers::string")] u64),
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match <_>::deserialize(deserializer)? {
            Value::Int(x) | Value::String(x) => x,
        })
    }

    pub fn deserialize_option<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let Some(value) = <_>::deserialize(deserializer)? else {
            return Ok(None);
        };

        Ok(Some(match value {
            Value::Int(x) | Value::String(x) => x,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_or_u64() {
        #[derive(Debug, Eq, PartialEq, Deserialize)]
        struct Struct {
            #[serde(with = "serde_string_or_u64")]
            value: u64,
        }

        for (serialied, expected) in [
            (r#"{"value":0}"#, Struct { value: 0 }),
            (r#"{"value":123}"#, Struct { value: 123 }),
            (r#"{"value":"123"}"#, Struct { value: 123 }),
            (r#"{"value":18446744073709551615}"#, Struct {
                value: u64::MAX,
            }),
            (r#"{"value":"18446744073709551615"}"#, Struct {
                value: u64::MAX,
            }),
        ] {
            let parsed: Struct = serde_json::from_str(serialied).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn string_or_u64_option() {
        #[derive(Debug, Eq, PartialEq, Deserialize)]
        struct Struct {
            #[serde(deserialize_with = "serde_string_or_u64::deserialize_option")]
            value: Option<u64>,
        }

        for (serialied, expected) in [
            (r#"{"value":null}"#, Struct { value: None }),
            (r#"{"value":0}"#, Struct { value: Some(0) }),
            (r#"{"value":"0"}"#, Struct { value: Some(0) }),
            (r#"{"value":123}"#, Struct { value: Some(123) }),
            (r#"{"value":"123","value_opt":null}"#, Struct {
                value: Some(123),
            }),
            (r#"{"value":18446744073709551615}"#, Struct {
                value: Some(u64::MAX),
            }),
            (r#"{"value":"18446744073709551615"}"#, Struct {
                value: Some(u64::MAX),
            }),
        ] {
            let parsed: Struct = serde_json::from_str(serialied).unwrap();
            assert_eq!(parsed, expected);
        }
    }
}
