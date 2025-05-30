use everscale_types::models::{IntAddr, MsgInfo, OwnedMessage, StdAddr, StdAddrBase64Repr};
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use tycho_block_util::message::build_normalized_external_message;
use tycho_util::FastHashSet;

use crate::util::serde_helpers;

// === Requests ===

#[derive(Debug, Deserialize)]
pub struct AdjacentTransactionsRequest {
    pub hash: HashBytes,
    #[serde(default)]
    pub direction: Option<MessageDirection>,
}

// === Responses ===

/// === Stuff ===

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
    // TODO: Replace with state init model.
    pub init_state: Option<()>,
    #[serde(with = "serde_helpers::option_tonlib_hash")]
    pub hash_norm: Option<HashBytes>,
}

impl Message {
    pub fn from_raw(
        hash: &HashBytes,
        msg: &OwnedMessage,
    ) -> Result<Self, everscale_types::error::Error> {
        let message_content = if msg.body.0.is_full(&msg.body.1) {
            MessageContent {
                hash: *msg.body.1.repr_hash(),
                body: msg.body.1.clone(),
                // TODO: Parse content.
                decoded: None,
            }
        } else {
            let body = CellBuilder::build_from(CellSlice::apply_allow_exotic(&msg.body))?;
            MessageContent {
                hash: *body.repr_hash(),
                body,
                // TODO: Parse content.
                decoded: None,
            }
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
            init_state: None,
            hash_norm: None,
        };
        match &msg.info {
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
    pub decoded: Option<()>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageDirection {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExtraCurrenciesStub {}

pub struct AddressBook {
    pub items: FastHashSet<StdAddr>,
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
