use std::cmp;
use std::cmp::Ordering;
use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tycho_network::PeerId;
use tycho_util::mem::Reclaimer;

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
    #[error("Mempool Adapter Cache cannot contain anchor between prev and found ones {self:?}")]
    UnexpectedAnchor {
        prev_anchor_id: MempoolAnchorId,
        found_prev_id: MempoolAnchorId,
        found_id: MempoolAnchorId,
        is_paused: bool,
    },
    #[error("Mempool Adapter Cache contains out of order anchor: {self:?}")]
    OutOfOrderAnchor {
        lookup_id: MempoolAnchorId,
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

#[derive(thiserror::Error, Debug)]
#[error("received forked anchor: {self:?}")]
pub struct AnchorForkError {
    id: MempoolAnchorId,
    is_paused: bool,
    prev_id_diff: Option<(Option<MempoolAnchorId>, Option<MempoolAnchorId>)>,
    chain_time_diff: Option<(u64, u64)>,
    externals_count_diff: Option<(usize, usize)>,
    has_externals_diff: Option<bool>,
    author_diff: Option<(PeerId, PeerId)>,
}

#[derive(Default)]
struct CacheData {
    anchors: IndexMap<MempoolAnchorId, Arc<MempoolAnchor>, ahash::RandomState>,
    is_paused: bool,
    is_off_after_anchor: Option<MempoolAnchorId>,
}

#[derive(Default)]
pub struct Cache {
    data: RwLock<CacheData>,
    anchor_added: Notify,
}

impl Cache {
    pub fn close(&self, after_anchor_id: MempoolAnchorId) {
        let mut data = self.data.write();
        data.is_off_after_anchor = Some(after_anchor_id);

        drop(data);
        self.anchor_added.notify_waiters();
        // postpone destructive changes
    }

    /// returns anchor id at which cache was closed
    pub fn reopen(&self, drop_data: bool) -> Option<MempoolAnchorId> {
        let mut data = self.data.write();

        let after_anchor_id = data.is_off_after_anchor.take()?;

        if !drop_data {
            self.anchor_added.notify_waiters();
            return Some(after_anchor_id);
        }

        let pos = (data.anchors)
            .binary_search_keys(&(after_anchor_id + 1))
            .unwrap_or_else(std::convert::identity);

        let anchors_to_clean = (data.anchors).drain(pos..).collect::<Vec<_>>();

        drop(data);

        self.anchor_added.notify_waiters();

        Reclaimer::instance().drop(anchors_to_clean);

        Some(after_anchor_id)
    }

    pub fn push(&self, anchor: Arc<MempoolAnchor>) -> Result<(), Box<AnchorForkError>> {
        let id = anchor.id;
        let prev_id = anchor.prev_id;
        let chain_time = anchor.chain_time;
        let externals_count = anchor.externals.len();
        let author = anchor.author;

        let mut data = self.data.write();
        if let Some(old) = data.anchors.insert(id, anchor) {
            let is_paused = data.is_paused;
            // restore as it was
            let new = data.anchors.insert(id, old.clone()).expect("inserted");
            drop(data);

            let externals_count_diff = (old.externals.len() != externals_count)
                .then_some((old.externals.len(), externals_count));

            let has_externals_diff = if externals_count_diff.is_some() {
                None
            } else {
                let has_diff = (old.externals.iter())
                    .zip(&new.externals)
                    .find(|(a, b)| a.hash() != b.hash())
                    .is_some();
                Some(has_diff)
            };

            let err = AnchorForkError {
                id,
                is_paused,
                // first anchor after restart will not contain prev id
                prev_id_diff: (prev_id.is_some() && old.prev_id != prev_id)
                    .then_some((old.prev_id, prev_id)),
                chain_time_diff: (old.chain_time != chain_time)
                    .then_some((old.chain_time, chain_time)),
                externals_count_diff,
                has_externals_diff,
                author_diff: (old.author != author).then_some((old.author, author)),
            };
            if err.prev_id_diff.is_some()
                || err.chain_time_diff.is_some()
                || err.externals_count_diff.is_some()
                || err.has_externals_diff == Some(true)
                || err.author_diff.is_some()
            {
                return Err(Box::new(err));
            } else {
                tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    %id,
                    ?prev_id,
                    %chain_time,
                    %externals_count,
                    %is_paused,
                    "ignoring same anchor replacement:"
                );
            }
        } else {
            self.anchor_added.notify_waiters();
        }
        Ok(())
    }

    pub fn set_paused(&self, is_paused: bool) {
        self.data.write().is_paused = is_paused;
        self.anchor_added.notify_waiters();
    }

    pub async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>, CacheError> {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            'attempt: {
                let data = &self.data.read();

                if data.anchor_is_off(anchor_id) {
                    break 'attempt;
                }

                match data.anchors.first() {
                    // Continue to wait for the first anchor
                    None => {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %anchor_id,
                            is_paused = data.is_paused.then_some(true),
                            "Anchor cache is empty, waiting:"
                        );
                    }
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if anchor_id < *first_id => {
                        if data.anchor_is_off(*first_id) {
                            break 'attempt;
                        }
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %anchor_id,
                            %first_id,
                            is_paused = data.is_paused.then_some(true),
                            "Requested anchor is too old:"
                        );
                        return Ok(None);
                    }
                    Some(_) => {
                        let pos = (data.anchors)
                            .binary_search_keys(&anchor_id)
                            .unwrap_or_else(std::convert::identity);

                        if let Some((_, found)) = data.anchors.get_index(pos) {
                            if data.anchor_is_off(found.id) {
                                break 'attempt;
                            }
                            return match found.id.cmp(&anchor_id) {
                                Ordering::Equal => Ok(Some(found.clone())),
                                Ordering::Greater => Ok(None), // will not be received
                                Ordering::Less => Err(CacheError::OutOfOrderAnchor {
                                    lookup_id: anchor_id,
                                    found_id: found.id,
                                    is_paused: data.is_paused,
                                }),
                            };
                        } else {
                            tracing::warn!(
                                target: tracing_targets::MEMPOOL_ADAPTER,
                                %anchor_id,
                                "Anchor is unknown, waiting:"
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
    ) -> Result<Option<Arc<MempoolAnchor>>, CacheError> {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            'attempt: {
                let data = &self.data.read();

                if data.anchor_is_off(prev_anchor_id.saturating_add(1)) {
                    break 'attempt;
                }

                match data.anchors.first() {
                    None => {
                        // Continue to wait for the first anchor
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            %prev_anchor_id,
                            is_paused = data.is_paused.then_some(true),
                            "Anchor cache is empty, waiting:"
                        );
                    }
                    Some((first_id, first)) if prev_anchor_id < *first_id => {
                        if data.anchor_is_off(first.id) {
                            break 'attempt;
                        }
                        return match first.prev_id {
                            None => {
                                // Return the first anchor after genesis
                                Ok(Some(first.clone()))
                            }
                            Some(id) if id == prev_anchor_id => {
                                // First anchor in cache is exactly next to requested
                                // Ok(Some(first.clone()));
                                // interesting if we can ever get this error
                                Err(CacheError::FirstAnchorRemoved {
                                    prev_anchor_id,
                                    first_id: first.id,
                                    is_paused: data.is_paused,
                                })
                            }
                            Some(_) => {
                                // Trying to get anchor that is too old
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    %first_id,
                                    first_prev_id = first.prev_id,
                                    is_paused = data.is_paused.then_some(true),
                                    "Requested anchor is too old:"

                                );
                                Ok(None)
                            }
                        };
                    }
                    Some(_) => {
                        // Find the index of the previous anchor
                        let search_result = (data.anchors).binary_search_keys(&prev_anchor_id);

                        if let Ok(index) = search_result {
                            // Try to get the next anchor
                            if let Some((_, found)) = data.anchors.get_index(index + 1) {
                                if data.anchor_is_off(found.id) {
                                    break 'attempt;
                                }

                                let error = if let Some(found_prev_id) = found.prev_id {
                                    match prev_anchor_id.cmp(&found_prev_id) {
                                        cmp::Ordering::Equal => return Ok(Some(found.clone())),
                                        cmp::Ordering::Less => CacheError::UnexpectedGap {
                                            prev_anchor_id,
                                            found_prev_id,
                                            found_id: found.id,
                                            is_paused: data.is_paused,
                                        },
                                        cmp::Ordering::Greater => CacheError::UnexpectedAnchor {
                                            prev_anchor_id,
                                            found_prev_id,
                                            found_id: found.id,
                                            is_paused: data.is_paused,
                                        },
                                    }
                                } else {
                                    tracing::debug!(
                                        target: tracing_targets::MEMPOOL_ADAPTER,
                                        %prev_anchor_id,
                                        found_anchor_id = found.id,
                                        is_paused = data.is_paused.then_some(true),
                                        "Found first after a gep:"
                                    );
                                    return Ok(Some(found.clone()));
                                };
                                tracing::error!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    "{error}"
                                );
                                return Err(error);
                            } else {
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    is_paused = data.is_paused.then_some(true),
                                    "Next anchor is unknown, waiting:"
                                );
                            }
                        } else if let Err(next_pos) = search_result {
                            if let Some((next_id, _)) = data.anchors.get_index(next_pos) {
                                // map is not empty
                                if data.anchor_is_off(*next_id) {
                                    break 'attempt;
                                }
                                return Ok(None); // will not be received
                            } else {
                                tracing::warn!(
                                    target: tracing_targets::MEMPOOL_ADAPTER,
                                    %prev_anchor_id,
                                    is_paused = data.is_paused.then_some(true),
                                    "Prev anchor is unknown, waiting:"
                                );
                            }
                        } else {
                            unreachable!("both `Ok` and `Err` have `if` branches");
                        };
                    }
                }
            }

            anchor_added.await;
        }
    }

    pub fn clear(&self, before_anchor_id: MempoolAnchorId) {
        let mut data = self.data.write();

        let pos = (data.anchors)
            .binary_search_keys(&before_anchor_id)
            .unwrap_or_else(std::convert::identity);

        let anchors_to_clean = (data.anchors).drain(..pos).collect::<Vec<_>>();

        data.shrink();

        let is_paused = data.is_paused.then_some(true);

        // NOTE: Drop the lock as soon as possible.
        drop(data);

        Reclaimer::instance().drop(anchors_to_clean);

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %before_anchor_id,
            is_paused,
            "anchors cache was cleared:",
        );
    }
}

impl CacheData {
    // Close keeps future anchors buffered, but readers must not observe them
    // until reopen decides whether to keep or drop that tail.
    fn anchor_is_off(&self, anchor_id: MempoolAnchorId) -> bool {
        self.is_off_after_anchor
            .is_some_and(|after_anchor_id| anchor_id > after_anchor_id)
    }

    fn shrink(&mut self) {
        let len = self.anchors.len();
        if self.anchors.capacity() > len.saturating_mul(4) {
            self.anchors.shrink_to(len.saturating_mul(2));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tycho_network::PeerId;

    use super::{Cache, CacheError};
    use crate::mempool::{MempoolAnchor, MempoolAnchorId};

    const WAIT: Duration = Duration::from_millis(100);
    const DONE: Duration = Duration::from_secs(1);

    fn anchor(id: MempoolAnchorId, prev_id: Option<MempoolAnchorId>) -> Arc<MempoolAnchor> {
        Arc::new(MempoolAnchor {
            id,
            prev_id,
            author: PeerId(Default::default()),
            chain_time: id as u64,
            externals: vec![],
        })
    }

    async fn unwrap_task<T: Send, E: Debug>(task: JoinHandle<Result<T, E>>) -> T {
        let result = timeout(DONE, task).await.expect("task must not hang");
        let result = result.expect("task must not panic");
        result.expect("task must not raise error")
    }

    #[tokio::test]
    async fn get_next_first_anchor_removed() {
        let cache = Cache::default();
        cache.push(anchor(6, Some(5))).unwrap();

        let result = cache.get_next_anchor(5).await;
        let error = result.expect_err("must detect removed first anchor");
        assert!(matches!(error, CacheError::FirstAnchorRemoved {
            prev_anchor_id: 5,
            first_id: 6,
            ..
        }));
    }

    #[tokio::test]
    async fn get_next_unexpected_gap() {
        let cache = Cache::default();
        cache.push(anchor(5, None)).unwrap();
        cache.push(anchor(11, Some(10))).unwrap();

        let result = cache.get_next_anchor(5).await;
        let error = result.expect_err("must detect gap");
        assert!(matches!(error, CacheError::UnexpectedGap {
            prev_anchor_id: 5,
            found_prev_id: 10,
            found_id: 11,
            ..
        }));
    }

    #[tokio::test]
    async fn get_next_unexpected_anchor() {
        let cache = Cache::default();
        cache.push(anchor(10, None)).unwrap();
        cache.push(anchor(11, Some(5))).unwrap();

        let result = cache.get_next_anchor(10).await;
        let error = result.expect_err("must detect unexpected middle anchor");
        assert!(matches!(error, CacheError::UnexpectedAnchor {
            prev_anchor_id: 10,
            found_prev_id: 5,
            found_id: 11,
            ..
        }));
    }

    #[tokio::test]
    async fn wait_for_next_anchor() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();

        let cache_copy = cache.clone();
        let mut task: JoinHandle<Result<Option<Arc<MempoolAnchor>>, CacheError>> =
            tokio::spawn(async move { cache_copy.get_next_anchor(5).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        cache.push(anchor(6, Some(5))).unwrap();

        let next = unwrap_task(task).await.expect("anchor must exist");
        assert_eq!(next.id, 6);
    }

    #[tokio::test]
    async fn close_reopen_keeps_current() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();
        cache.close(5);

        let cache_copy = cache.clone();
        let mut task = tokio::spawn(async move { cache_copy.get_anchor_by_id(6).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        cache.push(anchor(6, Some(5))).unwrap();

        assert_eq!(cache.reopen(false), Some(5));

        let found = unwrap_task(task).await.expect("anchor must exist");
        assert_eq!(found.id, 6);
    }

    #[tokio::test]
    async fn close_reopen_serves_new_next() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();
        cache.close(5);

        let cache_copy = cache.clone();
        let mut task = tokio::spawn(async move { cache_copy.get_next_anchor(5).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        cache.push(anchor(6, Some(5))).unwrap();

        assert_eq!(cache.reopen(false), Some(5));

        let next = unwrap_task(task).await.expect("anchor must exist");
        assert_eq!(next.id, 6);
    }

    #[tokio::test]
    async fn close_reopen_serves_old_next() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();
        cache.push(anchor(6, Some(5))).unwrap();
        cache.close(5);

        let cache_copy = cache.clone();
        let mut task = tokio::spawn(async move { cache_copy.get_next_anchor(5).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        assert_eq!(cache.reopen(false), Some(5));

        let next = unwrap_task(task).await.expect("anchor must exist");
        assert_eq!(next.id, 6);
    }

    #[tokio::test]
    async fn close_hides_old_tail_above_boundary() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(15, None)).unwrap();
        cache.close(13);

        let cache_copy = cache.clone();
        let mut by_id_task = tokio::spawn(async move { cache_copy.get_anchor_by_id(12).await });

        let cache_copy = cache.clone();
        let mut next_task = tokio::spawn(async move { cache_copy.get_next_anchor(12).await });

        assert!(timeout(WAIT, &mut by_id_task).await.is_err());
        assert!(timeout(WAIT, &mut next_task).await.is_err());

        assert_eq!(cache.reopen(false), Some(13));

        let maybe_id = unwrap_task(by_id_task).await.map(|a| a.id);
        assert_eq!(maybe_id, None);

        let next = unwrap_task(next_task).await.expect("anchor must exist");
        assert_eq!(next.id, 15);
    }

    #[tokio::test]
    async fn close_reopen_drops_new_input() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();
        cache.push(anchor(6, Some(5))).unwrap();
        cache.close(5);

        let cache_copy = cache.clone();
        let mut task = tokio::spawn(async move { cache_copy.get_anchor_by_id(6).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        assert_eq!(cache.reopen(true), Some(5));
        cache.push(anchor(10, Some(5))).unwrap(); // make id=6 "too old" to unhang task

        let maybe_id = unwrap_task(task).await.map(|a| a.id);
        assert_eq!(maybe_id, None);
    }

    #[tokio::test]
    async fn close_reopen_drops_to_fork() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();
        cache.push(anchor(6, Some(5))).unwrap();
        cache.close(5);

        let cache_copy = cache.clone();
        let mut task = tokio::spawn(async move { cache_copy.get_next_anchor(5).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        assert_eq!(cache.reopen(true), Some(5));
        cache.push(anchor(10, Some(5))).unwrap(); // make id=5 "too old" to unhang task

        let next = unwrap_task(task).await.expect("anchor must exist");
        assert_eq!(next.id, 10);
    }

    #[tokio::test]
    async fn close_reopen_waits_next_anchor() {
        let cache = Arc::new(Cache::default());
        cache.push(anchor(5, None)).unwrap();
        cache.close(5);

        let cache_copy = cache.clone();
        let mut task = tokio::spawn(async move { cache_copy.get_next_anchor(5).await });

        assert!(timeout(WAIT, &mut task).await.is_err());

        cache.push(anchor(6, Some(5))).unwrap();
        assert!(cache.push(anchor(6, Some(5))).is_ok(), "duplicate push");
        assert!(cache.push(anchor(6, None)).is_ok(), "push after restart");
        assert!(cache.push(anchor(6, Some(15))).is_err(), "forked push");

        assert_eq!(cache.reopen(true), Some(5));

        assert!(timeout(WAIT, &mut task).await.is_err());

        (cache.push(anchor(6, Some(5)))).expect("reopen must remove prev version");

        let next = unwrap_task(task).await.expect("anchor must exist");
        assert_eq!(next.id, 6);
    }
}
