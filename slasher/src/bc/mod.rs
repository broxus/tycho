use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::oneshot;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{AnchorStats, ReceivedSignature, ValidationSessionId};
use tycho_types::cell::{HashBytes, Lazy};
use tycho_types::models::{
    BlockchainConfigParams, OwnedMessage, SignatureContext, StdAddr, Transaction,
};
use tycho_util::FastDashMap;

pub use self::contract::StdSlasherContract;
use crate::util::BitSet;

mod contract;

const METRIC_PENDING_MESSAGES: &str = "tycho_slasher_pending_messages";

#[derive(Clone, Copy)]
pub struct EncodeBlocksBatchMessage<'a> {
    pub address: &'a StdAddr,
    pub session_id: ValidationSessionId,
    pub batch: &'a BlocksBatch,
    pub vset_hash: &'a HashBytes,
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
    ) -> Result<oneshot::Receiver<MessageDelivered>> {
        use dashmap::mapref::entry::Entry;

        let (tx, rx) = oneshot::channel();
        match self.pending_messages.entry(*msg_hash) {
            Entry::Vacant(entry) => {
                entry.insert(PendingMessage { expire_at, tx });
                self.report_pending_messages();
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
            pending.tx.send(MessageDelivered { tx_hash: *tx_hash }).ok();
            self.report_pending_messages();
            return Ok(true);
        }
        Ok(false)
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
        self.report_pending_messages();
    }

    pub fn reset_pending_messages_metrics() {
        metrics::gauge!(METRIC_PENDING_MESSAGES).set(0);
    }

    pub fn report_pending_messages(&self) {
        metrics::gauge!(METRIC_PENDING_MESSAGES).set(self.pending_messages.len() as f64);
    }
}

struct PendingMessage {
    expire_at: u32,
    tx: oneshot::Sender<MessageDelivered>,
}

#[derive(Debug, Clone, Copy)]
pub struct MessageDelivered {
    pub tx_hash: HashBytes,
}

// TODO: Add mempool batches or votes here
#[derive(Debug, PartialEq, Eq)]
pub enum SlasherContractEvent {
    SubmitBlocksBatch(SubmitBlocksBatch),
}

#[derive(Debug, PartialEq, Eq)]
pub struct SubmitBlocksBatch {
    pub vset_hash: HashBytes,
    pub validator_idx: u16,
    pub blocks_batch: BlocksBatch,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlocksBatch {
    pub start_seqno: u32,
    pub round_range: Option<RangeInclusive<u32>>,
    pub filled_rounds: u32,
    pub committed_blocks: BitSet,
    /// sorted by `validator_idx`
    pub observed: Box<[ObservedHistory]>,
}

impl BlocksBatch {
    pub fn new(start_seqno: u32, len: NonZeroU32, map_ids: &[u16]) -> Self {
        let len = len.get() as usize;

        let mut observed = map_ids
            .iter()
            .map(|validator_idx| ObservedHistory {
                validator_idx: *validator_idx,
                points_proven: 0,
                bits: BitSet::with_capacity(len * 2),
            })
            .collect::<Vec<_>>();
        observed.sort_unstable_by_key(|a| a.validator_idx);

        Self {
            start_seqno,
            round_range: None,
            filled_rounds: 0,
            committed_blocks: BitSet::with_capacity(len),
            observed: observed.into_boxed_slice(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.committed_blocks.is_zero() && self.round_range.is_none()
    }

    pub fn start_seqno(&self) -> u32 {
        self.start_seqno
    }

    pub fn seqno_after(&self) -> u32 {
        self.start_seqno
            .saturating_add(self.committed_blocks.len() as u32)
    }

    pub fn committed_block_count(&self) -> usize {
        (0..self.committed_blocks.len())
            .filter(|offset| self.committed_blocks.get(*offset))
            .count()
    }

    pub fn validator_count(&self) -> usize {
        self.observed.len()
    }

    pub fn contains_seqno(&self, seqno: u32) -> bool {
        (self.start_seqno..self.seqno_after()).contains(&seqno)
    }

    pub fn push_anchor_stats(&mut self, seqno: u32, anchor_stats: &AnchorStats) -> bool {
        if !self.contains_seqno(seqno)
            || anchor_stats.round_range.is_empty()
            || anchor_stats.filled_rounds == 0
            || anchor_stats.data.len() != self.observed.len()
        {
            return false;
        }

        let round_span =
            (*anchor_stats.round_range.end() - *anchor_stats.round_range.start()) as u64 + 1;
        if anchor_stats.filled_rounds as u64 > round_span
            || (anchor_stats.data.iter())
                .any(|stats| stats.points_proven as u32 > anchor_stats.filled_rounds)
        {
            return false;
        }

        if (self.round_range.as_ref()).is_some_and(|r| {
            r.contains(anchor_stats.round_range.start())
                || r.contains(anchor_stats.round_range.end())
                || anchor_stats.round_range.contains(r.start())
                || anchor_stats.round_range.contains(r.end())
        }) {
            return false;
        }

        match &mut self.round_range {
            None => self.round_range = Some(anchor_stats.round_range.clone()),
            Some(exist) => {
                *exist = *exist.start().min(anchor_stats.round_range.start())
                    ..=*exist.end().max(anchor_stats.round_range.end());
            }
        };
        self.filled_rounds = (self.filled_rounds).saturating_add(anchor_stats.filled_rounds);

        for (history, received) in std::iter::zip(&mut self.observed, &*anchor_stats.data) {
            history.points_proven = (history.points_proven).saturating_add(received.points_proven);
        }

        true
    }

    pub fn commit_signatures(&mut self, mut seqno: u32, signatures: &[ReceivedSignature]) -> bool {
        if !self.contains_seqno(seqno) || signatures.len() != self.observed.len() {
            return false;
        }

        seqno -= self.start_seqno;

        self.committed_blocks.set(seqno as usize, true);
        for (history, received) in std::iter::zip(&mut self.observed, signatures) {
            let idx = (seqno as usize) * 2;
            history.bits.set(idx, received.has_invalid_signature());
            history.bits.set(idx + 1, received.has_valid_signature());
        }

        true
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ObservedHistory {
    pub validator_idx: u16,
    pub points_proven: u16,
    pub bits: BitSet,
}
