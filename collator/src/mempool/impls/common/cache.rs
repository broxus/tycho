use std::cmp;
use std::sync::Arc;
use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::Notify;
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;
#[derive(thiserror::Error, Debug)]
pub enum CacheError {
    #[error("Mempool Adapter Cache has gap between prev and found anchors {self:?}")]
    UnexpectedGap {
        prev_anchor_id: MempoolAnchorId,
        found_prev_id: MempoolAnchorId,
        found_id: MempoolAnchorId,
        is_paused: bool,
    },
    #[error(
        "Mempool Adapter Cache cannot contain anchor between prev and found ones {self:?}"
    )]
    UnexpectedAnchor {
        prev_anchor_id: MempoolAnchorId,
        found_prev_id: MempoolAnchorId,
        found_id: MempoolAnchorId,
        is_paused: bool,
    },
    #[error(
        "Only first anchor after Genesis may be not linked to previous one {self:?}"
    )]
    NoPreviousAnchor {
        prev_anchor_id: MempoolAnchorId,
        found_id: MempoolAnchorId,
        is_paused: bool,
    },
    #[error("cache was not cleaned properly: it must contain prev anchor {self:?}")]
    FirstAnchorRemoved {
        prev_anchor_id: MempoolAnchorId,
        first_id: MempoolAnchorId,
        is_paused: bool,
    },
}
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
    }
    pub fn push(&self, anchor: Arc<MempoolAnchor>) {
        let mut data = self.data.write();
        let old = data.anchors.insert(anchor.id, anchor);
        if let Some(old) = old {
            tracing::error!(
                target : tracing_targets::MEMPOOL_ADAPTER, id = old.id, is_paused = data
                .is_paused.then_some(true), "received same anchor more than once"
            );
        }
        self.anchor_added.notify_waiters();
    }
    pub fn set_paused(&self, is_paused: bool) {
        self.data.write().is_paused = is_paused;
        self.anchor_added.notify_waiters();
    }
    pub async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Option<Arc<MempoolAnchor>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_anchor_by_id)),
            file!(),
            line!(),
        );
        let anchor_id = anchor_id;
        loop {
            let anchor_added = self.anchor_added.notified();
            {
                let data = &self.data.read();
                match data.anchors.first() {
                    None => {
                        tracing::info!(
                            target : tracing_targets::MEMPOOL_ADAPTER, % anchor_id,
                            is_paused = data.is_paused.then_some(true),
                            "Anchor cache is empty, waiting"
                        );
                    }
                    Some((first_id, _)) if anchor_id < *first_id => {
                        tracing::warn!(
                            target : tracing_targets::MEMPOOL_ADAPTER, % anchor_id, %
                            first_id, is_paused = data.is_paused.then_some(true),
                            "Requested anchor is too old"
                        );
                        return None;
                    }
                    Some(_) => {
                        if let Some(found) = data.anchors.get(&anchor_id) {
                            return Some(found.clone());
                        }
                        let (last_id, _) = data
                            .anchors
                            .last()
                            .expect("map is not empty");
                        if *last_id > anchor_id {
                            return None;
                        } else {
                            tracing::warn!(
                                target : tracing_targets::MEMPOOL_ADAPTER, % anchor_id,
                                "Anchor is unknown, waiting"
                            );
                        }
                    }
                }
            }
            {
                __guard.end_section(line!());
                let __result = anchor_added.await;
                __guard.start_section(line!());
                __result
            };
        }
    }
    pub async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>, CacheError> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_next_anchor)),
            file!(),
            line!(),
        );
        let prev_anchor_id = prev_anchor_id;
        loop {
            let anchor_added = self.anchor_added.notified();
            {
                let data = &self.data.read();
                match data.anchors.first() {
                    None => {
                        tracing::info!(
                            target : tracing_targets::MEMPOOL_ADAPTER, % prev_anchor_id,
                            is_paused = data.is_paused.then_some(true),
                            "Anchor cache is empty, waiting"
                        );
                    }
                    Some((first_id, first)) if prev_anchor_id < *first_id => {
                        return match first.prev_id {
                            None => Ok(Some(first.clone())),
                            Some(id) if id == prev_anchor_id => {
                                Err(CacheError::FirstAnchorRemoved {
                                    prev_anchor_id,
                                    first_id: first.id,
                                    is_paused: data.is_paused,
                                })
                            }
                            Some(_) => {
                                tracing::warn!(
                                    target : tracing_targets::MEMPOOL_ADAPTER, % prev_anchor_id,
                                    % first_id, first_prev_id = first.prev_id, is_paused = data
                                    .is_paused.then_some(true), "Requested anchor is too old"
                                );
                                Ok(None)
                            }
                        };
                    }
                    Some(_) => {
                        if let Some(index) = data.anchors.get_index_of(&prev_anchor_id) {
                            if let Some((_, found)) = data.anchors.get_index(index + 1) {
                                let error = if let Some(found_prev_id) = found.prev_id {
                                    match prev_anchor_id.cmp(&found_prev_id) {
                                        cmp::Ordering::Equal => return Ok(Some(found.clone())),
                                        cmp::Ordering::Less => {
                                            CacheError::UnexpectedGap {
                                                prev_anchor_id,
                                                found_prev_id,
                                                found_id: found.id,
                                                is_paused: data.is_paused,
                                            }
                                        }
                                        cmp::Ordering::Greater => {
                                            CacheError::UnexpectedAnchor {
                                                prev_anchor_id,
                                                found_prev_id,
                                                found_id: found.id,
                                                is_paused: data.is_paused,
                                            }
                                        }
                                    }
                                } else {
                                    CacheError::NoPreviousAnchor {
                                        prev_anchor_id,
                                        found_id: found.id,
                                        is_paused: data.is_paused,
                                    }
                                };
                                tracing::error!(
                                    target : tracing_targets::MEMPOOL_ADAPTER, "{error}"
                                );
                                return Err(error);
                            } else {
                                tracing::warn!(
                                    target : tracing_targets::MEMPOOL_ADAPTER, % prev_anchor_id,
                                    is_paused = data.is_paused.then_some(true),
                                    "Next anchor is unknown, waiting"
                                );
                            }
                        } else {
                            let (last_id, _) = data
                                .anchors
                                .last()
                                .expect("map is not empty");
                            if *last_id > prev_anchor_id {
                                return Ok(None);
                            } else {
                                tracing::warn!(
                                    target : tracing_targets::MEMPOOL_ADAPTER, % prev_anchor_id,
                                    is_paused = data.is_paused.then_some(true),
                                    "Prev anchor is unknown, waiting"
                                );
                            }
                        };
                    }
                }
            }
            {
                __guard.end_section(line!());
                let __result = anchor_added.await;
                __guard.start_section(line!());
                __result
            };
        }
    }
    pub fn clear(&self, before_anchor_id: MempoolAnchorId) {
        let data = &mut self.data.write();
        data.anchors.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        data.shrink();
        tracing::info!(
            target : tracing_targets::MEMPOOL_ADAPTER, % before_anchor_id, is_paused =
            data.is_paused.then_some(true), "anchors cache was cleared",
        );
    }
}
impl CacheData {
    fn shrink(&mut self) {
        let len = self.anchors.len();
        if self.anchors.capacity() > len.saturating_mul(4) {
            self.anchors.shrink_to(len.saturating_mul(2));
        }
    }
}
