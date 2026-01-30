use std::num::NonZeroU32;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::oneshot;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{ReceivedSignature, ValidationSessionId};
use tycho_types::cell::{HashBytes, Lazy};
use tycho_types::models::{
    AutoSignatureContext, BlockchainConfigParams, OwnedMessage, StdAddr, Transaction,
};
use tycho_util::FastDashMap;

pub use self::stub_contract::StubSlasherContract;
use crate::util::BitSet;

mod stub_contract;

#[derive(Clone, Copy)]
pub struct EncodeBlocksBatchMessage<'a> {
    pub address: &'a StdAddr,
    pub session_id: ValidationSessionId,
    pub batch: &'a BlocksBatch,
    pub validator_idx: u16,
    pub signature_context: AutoSignatureContext,
    pub keypair: &'a ed25519::KeyPair,
    pub ttl: Duration,
}

pub trait SlasherContract: Send + Sync + 'static {
    fn default_batch_size(&self) -> NonZeroU32;

    fn find_params(&self, config: &BlockchainConfigParams) -> Result<Option<SlasherParams>>;

    fn encode_blocks_batch_message(
        &self,
        params: &EncodeBlocksBatchMessage<'_>,
    ) -> Result<SignedMessage>;

    fn decode_event(&self, tx: &Transaction) -> Result<Option<SlasherContractEvent>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlasherParams {
    /// Address in masterchain.
    pub address: HashBytes,
    /// Blocks batch size.
    pub blocks_batch_size: NonZeroU32,
}

pub struct SignedMessage {
    pub message: Lazy<OwnedMessage>,
    pub expire_at: u32,
}

pub struct ContractSubscription {
    address: StdAddr,
    pending_messages: FastDashMap<HashBytes, PendingMessage>,
}

impl ContractSubscription {
    pub fn new(address: &StdAddr) -> Self {
        Self {
            address: address.clone(),
            pending_messages: Default::default(),
        }
    }

    pub fn address(&self) -> &StdAddr {
        &self.address
    }

    pub fn track_message(
        &self,
        msg_hash: &HashBytes,
        expire_at: u32,
    ) -> Result<oneshot::Receiver<MessageDeliveryStatus>> {
        use dashmap::mapref::entry::Entry;

        let (tx, rx) = oneshot::channel();
        match self.pending_messages.entry(*msg_hash) {
            Entry::Vacant(entry) => {
                entry.insert(PendingMessage { expire_at, tx });
                Ok(rx)
            }
            Entry::Occupied(_) => anyhow::bail!("duplicate external message: {msg_hash}"),
        }
    }

    pub fn handle_account_transaction(&self, tx_hash: &HashBytes, tx: &Transaction) -> Result<()> {
        let Some(in_msg) = &tx.in_msg else {
            return Ok(());
        };
        let msg_hash = in_msg.repr_hash();

        if let Some((_, pending)) = self.pending_messages.remove(msg_hash) {
            pending
                .tx
                .send(MessageDeliveryStatus::Sent { tx_hash: *tx_hash })
                .ok();
        }
        Ok(())
    }

    pub fn cleanup_expired_messages(&self, now_sec: u32) {
        let mut dropped = 0usize;
        self.pending_messages.retain(|_, msg| {
            let retain = msg.expire_at >= now_sec;
            dropped += !retain as usize;
            retain
        });
        if dropped > 0 {
            tracing::warn!(dropped, "dropped pending messages");
        }
    }
}

struct PendingMessage {
    expire_at: u32,
    tx: oneshot::Sender<MessageDeliveryStatus>,
}

#[derive(Debug, Clone, Copy)]
pub enum MessageDeliveryStatus {
    Sent { tx_hash: HashBytes },
    Expired,
}

// TODO: Add mempool batches or votes here
#[derive(Debug, PartialEq, Eq)]
pub enum SlasherContractEvent {
    SubmitBlocksBatch(SubmitBlocksBatch),
}

// TODO: Propagate session id?
#[derive(Debug, PartialEq, Eq)]
pub struct SubmitBlocksBatch {
    pub validator_idx: u16,
    pub blocks_batch: BlocksBatch,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlocksBatch {
    pub start_seqno: u32,
    pub committed_blocks: BitSet,
    pub signatures_history: Box<[SignatureHistory]>,
}

impl BlocksBatch {
    pub fn new(start_seqno: u32, len: NonZeroU32, map_ids: &[u16]) -> Self {
        let len = len.get() as usize;

        Self {
            start_seqno,
            committed_blocks: BitSet::with_capacity(len),
            signatures_history: map_ids
                .iter()
                .map(|validator_idx| SignatureHistory {
                    validator_idx: *validator_idx,
                    bits: BitSet::with_capacity(len * 2),
                })
                .collect::<Box<[_]>>(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.committed_blocks.is_zero()
    }

    pub fn start_seqno(&self) -> u32 {
        self.start_seqno
    }

    pub fn seqno_after(&self) -> u32 {
        self.start_seqno
            .saturating_add(self.committed_blocks.len() as u32)
    }

    pub fn contains_seqno(&self, seqno: u32) -> bool {
        (self.start_seqno..self.seqno_after()).contains(&seqno)
    }

    pub fn commit_signatures(&mut self, mut seqno: u32, signatures: &[ReceivedSignature]) -> bool {
        if !self.contains_seqno(seqno) || signatures.len() != self.signatures_history.len() {
            return false;
        }

        seqno -= self.start_seqno;

        self.committed_blocks.set(seqno as usize, true);
        for (history, received) in std::iter::zip(&mut self.signatures_history, signatures) {
            let idx = (seqno as usize) * 2;
            history.bits.set(idx, received.has_invalid_signature());
            history.bits.set(idx + 1, received.has_valid_signature());
        }

        true
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SignatureHistory {
    pub validator_idx: u16,
    pub bits: BitSet,
}
