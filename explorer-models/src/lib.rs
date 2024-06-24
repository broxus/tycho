pub use contracts::*;

pub use self::accounts::*;
pub use self::block::*;
pub use self::message::*;
pub use self::transaction::*;
pub use crate::utils::HashInsert;

mod accounts;
mod block;
mod contracts;
mod message;
pub mod schema;
mod transaction;
mod types;
mod utils;

pub type Hash = HashInsert<32>;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProcessingType {
    OnlyBlocks,
    Full,
}

#[derive(Default)]
pub struct ProcessingContext {
    pub blocks: Vec<Block>,
    pub configs: Vec<KeyBlockConfig>,
    pub transactions: Vec<Transaction>,
    pub messages: Vec<Message>,
    pub transaction_messages: Vec<TransactionMessage>,
    pub raw_transactions: Vec<RawTransaction>,
    pub account_updates: Vec<AccountUpdate>,
}

impl ProcessingContext {
    // accounts are in one block so in one shard
    pub fn known_accounts(&self) -> Option<(i8, Vec<HashInsert<32>>)> {
        let wc = self.account_updates.first()?.wc;
        let accounts = self.account_updates.iter().map(|x| x.address).collect();

        Some((wc, accounts))
    }

    pub fn clear(&mut self) {
        self.blocks.clear();
        self.configs.clear();
        self.transactions.clear();
        self.messages.clear();
        self.transaction_messages.clear();
        self.raw_transactions.clear();
        self.account_updates.clear();
    }
}

pub struct TransactionForParsing<'tx> {
    pub code_hash: HashInsert<32>,
    pub serialized_tx: &'tx [u8],
    pub workchain: i8,
    pub account_id: &'tx [u8],
}

pub trait NumBinds {
    const NUM_BINDS: usize;
}
