use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::mempool::{GetAnchorResult, MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

#[derive(Default)]
struct CacheData {
    anchors: IndexMap<MempoolAnchorId, Arc<MempoolAnchor>, ahash::RandomState>,
    is_paused: bool,
}

#[derive(Default)]
pub struct Cache {
    data: RwLock<CacheData>,
    anchor_added: Notify,
}

impl Cache {
    pub fn reset(&self) {
        let mut data = self.data.write();
        data.anchors = Default::default();
        // let waiters wait for new data to be pushed
    }

    pub fn push(&self, anchor: Arc<MempoolAnchor>) {
        let mut data = self.data.write();
        let old = data.anchors.insert(anchor.id, anchor);
        if let Some(old) = old {
            tracing::error!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = old.id,
                is_paused = Some(data.is_paused).filter(|x| *x),
                "received same anchor more than once"
            );
        }
        self.anchor_added.notify_waiters();
    }

    pub fn set_paused(&self, is_paused: bool) {
        self.data.write().is_paused = is_paused;
        self.anchor_added.notify_waiters();
    }

    pub async fn get_anchor_by_id(&self, anchor_id: MempoolAnchorId) -> GetAnchorResult {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let data = &self.data.read();

                match data.anchors.first() {
                    None => {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %anchor_id,
                            is_paused = Some(data.is_paused).filter(|x| *x),
                            waiting = Some(data.is_paused).filter(|x| !x),
                            "Anchor cache is empty"
                        );
                        if data.is_paused {
                            return GetAnchorResult::MempoolPaused;
                        } // else: continue to wait for the first anchor
                    }
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if anchor_id < *first_id => {
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %anchor_id,
                            %first_id,
                            is_paused = Some(data.is_paused).filter(|x| *x),
                            "Requested anchor is too old"
                        );
                        return GetAnchorResult::NotExist;
                    }
                    Some(_) => {
                        if let Some(found) = data.anchors.get(&anchor_id) {
                            return Some(found.clone());
                        }
                        let (last_id, _) = data.anchors.last().expect("map is not empty");
                        if *last_id > anchor_id {
                            return None; // will not be received
                        } else {
                            tracing::warn!(
                                target: tracing_targets::MEMPOOL_ADAPTER,
                                %anchor_id,
                                "Anchor is unknown, waiting"
                            );
                        }
                    }
                }
            }
            anchor_added.await;
        }
    }

    pub async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> GetAnchorResult {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let data = &self.data.read();

                match data.anchors.first() {
                    None => {
                        // Continue to wait for the first anchor
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %prev_anchor_id,
                            is_paused = Some(data.is_paused).filter(|x| *x),
                            waiting = Some(data.is_paused).filter(|x| !x),
                            "Anchor cache is empty"
                        );
                        if data.is_paused {
                            return GetAnchorResult::MempoolPaused;
                        } // else: continue to wait for the first anchor
                    }
                    // Return the first anchor on node start
                    Some((_, first)) if prev_anchor_id == 0 => {
                        return GetAnchorResult::Exist(first.clone())
                    }
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if prev_anchor_id < *first_id => {
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %prev_anchor_id,
                            %first_id,
                            is_paused = Some(data.is_paused).filter(|x| *x),
                            "Requested anchor is too old"
                        );
                        return GetAnchorResult::NotExist;
                    }
                    Some(_) => {
                        // Find the index of the previous anchor
                        if let Some(index) = data.anchors.get_index_of(&prev_anchor_id) {
                            // Try to get the next anchor
                            if let Some((_, value)) = data.anchors.get_index(index + 1) {
                                return GetAnchorResult::Exist(value.clone());
                            } else {
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    is_paused = Some(data.is_paused).filter(|x| *x),
                                    waiting = Some(data.is_paused).filter(|x| !x),
                                    "Next anchor is unknown"
                                );
                                if data.is_paused {
                                    return GetAnchorResult::MempoolPaused;
                                } // else: continue to wait for the next anchor
                            }
                        } else {
                            let (last_id, _) = data.anchors.last().expect("map is not empty");
                            if *last_id > prev_anchor_id {
                                return None; // will not be received
                            } else {
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    is_paused = Some(data.is_paused).filter(|x| *x),
                                    "Prev anchor is unknown, waiting"
                                );
                            }
                        };
                    }
                }
            }

            anchor_added.await;
        }
    }

    pub fn clear(&self, before_anchor_id: MempoolAnchorId) {
        let data = &mut self.data.write();

        data.anchors
            .retain(|anchor_id, _| anchor_id >= &before_anchor_id);

        let len = data.anchors.len();
        if data.anchors.capacity() > len.saturating_mul(4) {
            data.anchors.shrink_to(len.saturating_mul(2));
        }

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %before_anchor_id,
            is_paused = Some(data.is_paused).filter(|x| *x),
            "anchors cache was cleared",
        );
    }
}
