use std::num::NonZeroU32;
use std::ops::{Range, RangeInclusive};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::oneshot;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{AnchorPeerStats, AnchorStats, ReceivedSignature, ValidationSessionId};
use tycho_types::cell::{HashBytes, Lazy};
use tycho_types::models::{
    BlockchainConfigParams, OwnedMessage, SignatureContext, StdAddr, Transaction,
};
use tycho_util::FastDashMap;

pub use self::stub_contract::StubSlasherContract;
use crate::tracing_targets;
use crate::util::BitSet;

mod stub_contract;

#[derive(Clone, Copy)]
pub struct EncodeBlocksBatchMessage<'a> {
    pub address: &'a StdAddr,
    pub session_id: ValidationSessionId,
    pub batch: &'a BlocksBatch,
    pub validator_idx: u16,
    pub signature_context: SignatureContext,
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

    pub fn handle_account_transaction(
        &self,
        tx_hash: &HashBytes,
        tx: &Transaction,
    ) -> Result<bool> {
        let Some(in_msg) = &tx.in_msg else {
            return Ok(false);
        };
        let msg_hash = in_msg.repr_hash();

        if let Some((_, pending)) = self.pending_messages.remove(msg_hash) {
            pending
                .tx
                .send(MessageDeliveryStatus::Sent { tx_hash: *tx_hash })
                .ok();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn cleanup_expired_messages(&self, now_sec: u32) {
        let expired = self
            .pending_messages
            .iter()
            .filter_map(|entry| (entry.expire_at < now_sec).then_some(*entry.key()))
            .collect::<Vec<_>>();

        let dropped = expired.len();
        for msg_hash in expired {
            if let Some((_, pending)) = self.pending_messages.remove(&msg_hash) {
                pending.tx.send(MessageDeliveryStatus::Expired).ok();
            }
        }

        if dropped > 0 {
            tracing::warn!(
                target: tracing_targets::SLASHER,
                dropped,
                "dropped pending messages"
            );
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

#[derive(Debug, PartialEq, Eq)]
pub struct SubmitBlocksBatch {
    pub session_id: ValidationSessionId,
    pub validator_idx: u16,
    pub blocks_batch: BlocksBatch,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlocksBatch {
    pub start_seqno: u32,
    pub anchor_range: Option<RangeInclusive<u32>>,
    pub committed_blocks: BitSet,
    pub signatures_history: Box<[SignatureHistory]>,
    /// sorted by `validator_idx` since mempool output
    pub anchor_stats_history: Box<[AnchorStatsHistory]>,
}

impl BlocksBatch {
    pub fn new(start_seqno: u32, len: NonZeroU32, map_ids: &[u16]) -> Self {
        let len = len.get() as usize;

        let mut anchor_stats_history = map_ids
            .iter()
            .map(|validator_idx| AnchorStatsHistory {
                validator_idx: *validator_idx,
                stats: AnchorPeerStats { points_proven: 0 },
            })
            .collect::<Vec<_>>();
        anchor_stats_history.sort_unstable_by_key(|a| a.validator_idx);

        Self {
            start_seqno,
            anchor_range: None,
            committed_blocks: BitSet::with_capacity(len),
            signatures_history: map_ids
                .iter()
                .map(|validator_idx| SignatureHistory {
                    validator_idx: *validator_idx,
                    bits: BitSet::with_capacity(len * 2),
                })
                .collect::<Box<[_]>>(),
            anchor_stats_history: anchor_stats_history.into_boxed_slice(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.committed_blocks.is_zero() && self.anchor_range.is_none()
    }

    pub fn start_seqno(&self) -> u32 {
        self.start_seqno
    }

    pub fn seqno_after(&self) -> u32 {
        self.start_seqno
            .saturating_add(self.committed_blocks.len() as u32)
    }

    pub fn seqno_range(&self) -> Range<u32> {
        self.start_seqno..self.seqno_after()
    }

    pub fn contains_seqno(&self, seqno: u32) -> bool {
        self.seqno_range().contains(&seqno)
    }

    pub fn committed_block_count(&self) -> usize {
        (0..self.committed_blocks.len())
            .filter(|offset| self.committed_blocks.get(*offset))
            .count()
    }

    pub fn validator_count(&self) -> usize {
        self.signatures_history.len()
    }

    pub fn push_anchor_stats(&mut self, anchor_id: u32, anchor_stats: &AnchorStats) -> bool {
        if (self.anchor_range.as_ref()).is_some_and(|r| r.contains(&anchor_id))
            || anchor_stats.0.len() != self.anchor_stats_history.len()
        {
            return false;
        }

        match &mut self.anchor_range {
            None => self.anchor_range = Some(anchor_id..=anchor_id),
            Some(exist) => {
                *exist = *exist.start()..=*exist.end().max(&anchor_id);
            }
        };

        for (history, received) in std::iter::zip(&mut self.anchor_stats_history, &*anchor_stats.0)
        {
            history.stats.points_proven =
                (history.stats.points_proven).saturating_add(received.points_proven);
        }

        true
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

#[derive(Debug, PartialEq, Eq)]
pub struct AnchorStatsHistory {
    pub validator_idx: u16,
    pub stats: AnchorPeerStats,
}
