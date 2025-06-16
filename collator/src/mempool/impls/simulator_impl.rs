use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use everscale_types::boc::Boc;
use everscale_types::cell::HashBytes;
use everscale_types::models::MsgInfo;
use everscale_types::prelude::Load;
use humantime::format_duration;
use parking_lot::{Mutex, RwLock};
use tokio::task::AbortHandle;
use tycho_block_util::message::ExtMsgRepr;
use tycho_network::PeerId;

use crate::mempool::{
    ExternalMessage, GetAnchorResult, MempoolAdapter, MempoolAnchor, MempoolAnchorId,
    StateUpdateContext,
};
use crate::tracing_targets;

#[derive(Clone)]
pub struct SimulatorMempoolAdapterImpl {
    inner: Arc<Inner>,
}

struct Inner {
    unprocessed_message_queue: Arc<Mutex<VecDeque<Arc<ExternalMessage>>>>,
    anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    last_known_anchor_id: Arc<RwLock<Option<MempoolAnchorId>>>,
    anchor_interval_ms: NonZeroU32,

    peer_id: HashBytes,
    
    
    anchors_task: Option<AbortHandle>
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        Self {
            unprocessed_message_queue: self.unprocessed_message_queue.clone(),
            anchors_cache: self.anchors_cache.clone(),
            last_known_anchor_id: self.last_known_anchor_id.clone(),
            anchor_interval_ms: self.anchor_interval_ms.clone(),
            peer_id: self.peer_id.clone(),
            
            anchors_task: None
        }
    }
}

impl Inner {
    async fn generate_anchor_background_task(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.anchor_interval_ms.get() as u64,
        ));

        loop {
            interval.tick().await;

            let mut messages = self.unprocessed_message_queue.lock();

            let mut external_messages = Vec::with_capacity(messages.len());

            while let Some(msg) = messages.pop_front() {
                external_messages.push(msg);
            }

            let mut last_anchor_id = self.last_known_anchor_id.write();
            let new_anchor_id = last_anchor_id.map(|x| x + 1).unwrap_or_default();
            *last_anchor_id = Some(new_anchor_id + 1);

            let anchor = MempoolAnchor {
                id: new_anchor_id,
                prev_id: 
                author: PeerId(self.peer_id.0),
                chain_time: 0,
                externals: vec![],
            };

            self
                .anchors_cache
                .write()
                .insert(new_anchor_id, Arc::new(anchor));
        }
    }
}

impl SimulatorMempoolAdapterImpl {
    pub fn new(anchor_interval_ms: u32) -> Self {
        let mut inner = Inner {
            unprocessed_message_queue: Arc::new(Default::default()),
            anchors_cache: Arc::new(Default::default()),
            last_known_anchor_id: AtomicU32::new(0),
            anchor_interval_ms: NonZeroU32::new(anchor_interval_ms).unwrap(),
            peer_id: Default::default(),
            anchors_task: None,
        };
        
        inner = Some(tokio::spawn(inner.clone().))
        
        let slf = Self {
            inner: Arc::new(inner),
        };
 
        
        slf
    }

   
}


#[async_trait::async_trait]
impl MempoolAdapter for SimulatorMempoolAdapterImpl {
    async fn handle_mc_state_update(&self, cx: StateUpdateContext) -> anyhow::Result<()> {
        todo!()
    }

    async fn handle_signed_mc_block(&self, mc_block_seqno: BlockSeqno) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> anyhow::Result<GetAnchorResult> {
        self.inner.
    }

    async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> anyhow::Result<GetAnchorResult> {
        let range = (
            std::ops::Bound::Excluded(prev_anchor_id),
            std::ops::Bound::Unbounded,
        );

        let mut last_attempt_at = None;
        loop {
            let res = self
                .inner
                .anchors_cache
                .read()
                .range(range)
                .next()
                .map(|(_, v)| v.clone());

            let Some(anchor) = res else {
                let last_anchor_id = self
                    .inner
                    .anchors_cache
                    .read()
                    .last_key_value()
                    .map(|(_, last_anchor)| last_anchor.id)
                    .unwrap_or_default();
                let delta = prev_anchor_id.saturating_sub(last_anchor_id);
                if delta >= 20 {
                    tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                        "sleep_between_anchors set to False because prev_anchor_id {} ahead last {} on {} >= 20",
                        prev_anchor_id, last_anchor_id, delta,
                    );
                    tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                        "STUB: mempool return None because prev_anchor_id {} ahead last {} on {} >= 20",
                        prev_anchor_id, last_anchor_id, delta,
                    );
                    return Ok(GetAnchorResult::NotExist);
                } else if delta >= 3 {
                    self.sleep_between_anchors.store(false, Ordering::Release);
                    tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                        "sleep_between_anchors set to False because prev_anchor_id {} ahead last {} on {} >= 3",
                        prev_anchor_id, last_anchor_id, delta,
                    );
                }

                if last_attempt_at.is_none() {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        "There is no next anchor in cache. \
                        STUB: Requested it from mempool. Waiting...",
                    );
                }

                last_attempt_at = Some(Instant::now());
                tokio::time::sleep(tokio::time::Duration::from_millis(1320)).await;
                continue;
            };

            if !self.sleep_between_anchors.fetch_or(true, Ordering::AcqRel) {
                tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                    "sleep_between_anchors set to True when next was returned after prev_anchor_id {}",
                    prev_anchor_id,
                );
            }

            match last_attempt_at {
                Some(last) => {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        anchor_id = anchor.id,
                        elapsed = %format_duration(last.elapsed()),
                        "STUB: Returned the next anchor from mempool",
                    );
                }
                None => {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        anchor_id = anchor.id,
                        "Requested the next anchor from the local cache",
                    );
                }
            }

            return Ok(GetAnchorResult::Exist(anchor));
        }
    }

    fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> anyhow::Result<()> {
        let mut anchors_cache = self.inner.anchors_cache.write();
        anchors_cache.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }

    fn send_external(&self, message: Bytes) {
        let mut queue = self.inner.unprocessed_message_queue.lock();
        if let Some(msg) = parse_message_bytes(message.as_ref()) {
            queue.push_back(msg);
        }
    }
}

fn parse_message_bytes(message: &[u8]) -> Option<Arc<ExternalMessage>> {
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
