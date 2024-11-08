use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

#[derive(Default)]
pub struct Cache {
    anchors: RwLock<IndexMap<MempoolAnchorId, Arc<MempoolAnchor>, ahash::RandomState>>,
    anchor_added: Notify,
}

impl Cache {
    pub fn reset(&self) {
        let mut cache = self.anchors.write();
        *cache = Default::default();
        // let waiters wait for new data to be pushed
    }

    pub fn push(&self, anchor: Arc<MempoolAnchor>) {
        let old = self.anchors.write().insert(anchor.id, anchor);
        if let Some(old) = old {
            tracing::error!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = old.id,
                "received same anchor more than once"
            );
        }
        self.anchor_added.notify_waiters();
    }

    pub async fn get_anchor_by_id(&self, anchor_id: MempoolAnchorId) -> Option<Arc<MempoolAnchor>> {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let anchors = self.anchors.read();

                match anchors.first() {
                    // Continue to wait for the first anchor
                    None => {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %anchor_id,
                            "Anchor cache is empty, waiting"
                        );
                    }
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if anchor_id < *first_id => {
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %anchor_id,
                            %first_id,
                            "Requested anchor is too old"
                        );
                        return None;
                    }
                    Some(_) => {
                        if let Some(found) = anchors.get(&anchor_id) {
                            return Some(found.clone());
                        }
                        let (last_id, _) = anchors.last().expect("map is not empty");
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

    pub async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> Option<Arc<MempoolAnchor>> {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let anchors = self.anchors.read();

                match anchors.first() {
                    // Continue to wait for the first anchor
                    None => {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %prev_anchor_id,
                            "Anchor cache is empty, waiting"
                        );
                    }
                    // Return the first anchor on node start
                    Some((_, first)) if prev_anchor_id == 0 => return Some(first.clone()),
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if prev_anchor_id < *first_id => {
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %prev_anchor_id,
                            %first_id,
                            "Requested anchor is too old"
                        );
                        return None;
                    }
                    Some(_) => {
                        // Find the index of the previous anchor
                        if let Some(index) = anchors.get_index_of(&prev_anchor_id) {
                            // Try to get the next anchor
                            if let Some((_, value)) = anchors.get_index(index + 1) {
                                return Some(value.clone());
                            } else {
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    "Next anchor is unknown, waiting"
                                );
                            }
                        } else {
                            let (last_id, _) = anchors.last().expect("map is not empty");
                            if *last_id > prev_anchor_id {
                                return None; // will not be received
                            } else {
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    "Prev anchor is unknown, waiting"
                                );
                            }
                        }
                    }
                }
            }

            anchor_added.await;
        }
    }

    pub fn clear(&self, before_anchor_id: MempoolAnchorId) {
        let mut anchors_cache_rw = self.anchors.write();

        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);

        let len = anchors_cache_rw.len();
        if anchors_cache_rw.capacity() > len.saturating_mul(4) {
            anchors_cache_rw.shrink_to(len.saturating_mul(2));
        }

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "anchors cache was cleared before anchor {}",
            before_anchor_id,
        );
    }
}
