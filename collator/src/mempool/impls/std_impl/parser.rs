use std::sync::Arc;

use bytes::Bytes;
use everscale_types::boc::Boc;
use everscale_types::models::MsgInfo;
use everscale_types::prelude::Load;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tycho_block_util::message::ExtMsgRepr;
use tycho_util::metrics::HistogramGuard;

use crate::mempool::impls::std_impl::deduplicator::Deduplicator;
use crate::mempool::{ExternalMessage, MempoolAnchorId};
use crate::tracing_targets;

pub struct Parser {
    blake: Deduplicator,
    sha: Deduplicator,
}

impl Parser {
    pub fn new(round_threshold: u16) -> Self {
        Self {
            blake: Deduplicator::new(round_threshold),
            sha: Deduplicator::new(round_threshold),
        }
    }

    pub fn clean(self, anchor_id: MempoolAnchorId) -> Self {
        let mut blake = self.blake;
        let mut sha = self.sha;

        let (blake, sha) = rayon::join(
            move || {
                blake.clean(anchor_id);
                blake
            },
            move || {
                sha.clean(anchor_id);
                sha
            },
        );
        Self { blake, sha }
    }

    pub fn parse_unique(
        &mut self,
        anchor_id: MempoolAnchorId,
        chain_time: u64,
        is_executable: bool,
        payloads: Vec<Bytes>,
    ) -> Vec<Arc<ExternalMessage>> {
        let _guard = HistogramGuard::begin("tycho_mempool_adapter_parse_anchor_history_time");

        let total_messages = payloads.len();
        let total_bytes: usize = payloads.iter().fold(0, |acc, bytes| acc + bytes.len());

        let all_bytes_blake = payloads
            .into_par_iter()
            .filter_map(|bytes| {
                (bytes.len() <= ExtMsgRepr::MAX_BOC_SIZE)
                    .then(|| (<[u8; 32]>::from(blake3::hash(&bytes)), bytes))
            })
            .collect::<Vec<_>>();

        let uniq_bytes_blake = all_bytes_blake
            .into_iter()
            .filter(|(blake, _)| self.blake.check_unique(anchor_id, blake))
            .map(|(_, bytes)| bytes)
            .collect::<Vec<_>>();

        let uniq_messages_blake = uniq_bytes_blake
            .into_par_iter()
            .filter_map(|bytes| Self::parse_message_bytes(&bytes).map(|cell| (cell, bytes.len())))
            .collect::<Vec<_>>();

        let mut unique_messages_bytes = 0;
        let unique_messages = uniq_messages_blake
            .into_iter()
            .filter(|(message, _)| {
                (self.sha).check_unique(anchor_id, message.cell.repr_hash().as_array())
            })
            .map(|(message, byte_len)| {
                unique_messages_bytes += byte_len;
                message
            })
            .collect::<Vec<_>>();

        metrics::counter!("tycho_mempool_msgs_unique_count").increment(unique_messages.len() as _);
        metrics::counter!("tycho_mempool_msgs_unique_bytes").increment(unique_messages_bytes as _);

        metrics::counter!("tycho_mempool_msgs_duplicates_count")
            .increment((total_messages - unique_messages.len()) as _);
        metrics::counter!("tycho_mempool_msgs_duplicates_bytes")
            .increment((total_bytes - unique_messages_bytes) as _);

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            id = anchor_id,
            %is_executable,
            time = chain_time,
            externals_unique = unique_messages.len(),
            externals_skipped = total_messages - unique_messages.len(),
            "new anchor"
        );
        unique_messages
    }

    fn parse_message_bytes(message: &Bytes) -> Option<Arc<ExternalMessage>> {
        let cell = Boc::decode(message).ok()?;
        if cell.is_exotic() || cell.level() != 0 || cell.repr_depth() > ExtMsgRepr::MAX_REPR_DEPTH {
            return None;
        }

        let mut cs = cell.as_slice_allow_exotic();
        let MsgInfo::ExtIn(info) = MsgInfo::load_from(&mut cs).ok()? else {
            return None;
        };
        Some(Arc::new(ExternalMessage { cell, info }))
    }
}
