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

pub struct Parser {
    blake: Deduplicator,
    sha: Deduplicator,
}

pub struct ParserOutput {
    pub unique_messages: Vec<Arc<ExternalMessage>>,
    pub unique_payload_bytes: usize,
}

impl Parser {
    pub fn new(round_threshold: u16) -> Self {
        Self {
            blake: Deduplicator::new(round_threshold),
            sha: Deduplicator::new(round_threshold),
        }
    }

    pub fn clean(&mut self, anchor_id: MempoolAnchorId) {
        let Self { blake, sha } = self;
        rayon::join(|| blake.clean(anchor_id), || sha.clean(anchor_id));
    }

    pub fn parse_unique(
        &mut self,
        anchor_id: MempoolAnchorId,
        payloads: Vec<Bytes>,
    ) -> ParserOutput {
        let _guard = HistogramGuard::begin("tycho_mempool_adapter_parse_anchor_history_time");

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

        let mut unique_payload_bytes = 0;
        let unique_messages = uniq_messages_blake
            .into_iter()
            .filter(|(message, _)| {
                (self.sha).check_unique(anchor_id, message.cell.repr_hash().as_array())
            })
            .map(|(message, bytes_len)| {
                unique_payload_bytes += bytes_len;
                message
            })
            .collect::<Vec<_>>();

        ParserOutput {
            unique_messages,
            unique_payload_bytes,
        }
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
