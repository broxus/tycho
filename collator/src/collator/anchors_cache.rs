use std::collections::VecDeque;
use std::sync::Arc;

use tycho_network::PeerId;

use crate::collator::messages_reader::state::external::ExternalKey;
use crate::mempool::{MempoolAnchor, MempoolAnchorId};

#[derive(Debug, Clone)]
pub struct AnchorInfo {
    pub id: MempoolAnchorId,
    pub ct: u64,
    #[allow(dead_code)]
    pub all_exts_count: usize,
    #[allow(dead_code)]
    pub our_exts_count: usize,
    pub author: PeerId,
}

impl AnchorInfo {
    pub fn from_anchor(anchor: &MempoolAnchor, our_exts_count: usize) -> AnchorInfo {
        Self {
            id: anchor.id,
            ct: anchor.chain_time,
            all_exts_count: anchor.externals.len(),
            our_exts_count,
            author: anchor.author,
        }
    }
}

#[derive(Clone)]
pub struct CachedAnchor {
    pub anchor: Arc<MempoolAnchor>,
    pub our_exts_count: usize,
}

#[derive(Default, Clone)]
pub struct AnchorsCache {
    cache: VecDeque<(MempoolAnchorId, CachedAnchor)>,
    imported_anchors_info_history: VecDeque<AnchorInfo>,
    has_pending_externals: bool,
}

impl AnchorsCache {
    pub fn add_imported_anchor_info(&mut self, anchor_info: AnchorInfo) {
        self.imported_anchors_info_history.push_back(anchor_info);
    }

    fn remove_imported_anchors_info_before(&mut self, ct: u64) {
        while let Some(info) = self.imported_anchors_info_history.front() {
            if info.ct < ct && self.imported_anchors_info_history.len() > 1 {
                self.imported_anchors_info_history.pop_front();
            } else {
                break;
            }
        }
    }

    fn remove_imported_anchors_info_above(&mut self, ct: u64) {
        while let Some(info) = self.imported_anchors_info_history.back() {
            if info.ct > ct && self.imported_anchors_info_history.len() > 1 {
                self.imported_anchors_info_history.pop_back();
            } else {
                break;
            }
        }
    }

    pub fn last_imported_anchor_info(&self) -> Option<&AnchorInfo> {
        self.imported_anchors_info_history.back()
    }

    pub fn get_last_imported_anchor_id_and_ct(&self) -> Option<(u32, u64)> {
        self.last_imported_anchor_info().map(|a| (a.id, a.ct))
    }

    pub fn add(&mut self, anchor: Arc<MempoolAnchor>, our_exts_count: usize) {
        self.add_imported_anchor_info(AnchorInfo::from_anchor(&anchor, our_exts_count));

        if our_exts_count > 0 {
            self.has_pending_externals = true;
            self.cache.push_back((anchor.id, CachedAnchor {
                anchor,
                our_exts_count,
            }));
        }
    }

    pub fn pop_front(&mut self) -> Option<(MempoolAnchorId, Arc<MempoolAnchor>)> {
        let removed = self.cache.pop_front();

        if let Some((_, ca)) = &removed {
            self.remove_imported_anchors_info_before(ca.anchor.chain_time);
            self.has_pending_externals = !self.cache.is_empty();
        }

        removed.map(|(id, ca)| (id, ca.anchor))
    }

    pub fn remove_last_imported_above(&mut self, ct: u64) -> Option<&AnchorInfo> {
        let mut was_removed = false;
        while let Some(last) = self.cache.back().map(|(_, ca)| ca) {
            if last.anchor.chain_time > ct {
                was_removed = true;
                self.cache.pop_back();
            } else {
                break;
            }
        }

        self.remove_imported_anchors_info_above(ct);

        if was_removed {
            self.has_pending_externals = !self.cache.is_empty();
        }

        self.last_imported_anchor_info()
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.imported_anchors_info_history.clear();
        self.has_pending_externals = false;
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&self, index: usize) -> Option<(MempoolAnchorId, Arc<MempoolAnchor>)> {
        self.cache
            .get(index)
            .map(|(id, ca)| (*id, ca.anchor.clone()))
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, (MempoolAnchorId, CachedAnchor)> {
        self.cache.iter()
    }

    #[cfg(test)]
    pub fn first_with_our_externals(&self) -> Option<&Arc<MempoolAnchor>> {
        let mut idx = 0;
        while let Some((_, ca)) = self.cache.get(idx) {
            if ca.our_exts_count > 0 {
                return Some(&ca.anchor);
            }
            idx += 1;
        }
        None
    }

    pub fn has_pending_externals(&self) -> bool {
        self.has_pending_externals
    }

    pub fn check_has_pending_externals_in_range(&self, up_to: &ExternalKey) -> bool {
        self.cache
            .iter()
            .any(|(id, ca)| id <= &up_to.anchor_id && ca.our_exts_count > 0)
    }
}

#[derive(Clone)]
#[allow(dead_code)]
enum UndoOp {
    Add {
        anchor_id: MempoolAnchorId,
        added_to_cache: bool,
    },
    PopFront {
        anchor_id: MempoolAnchorId,
        cached_anchor: CachedAnchor,
        removed_infos: Vec<AnchorInfo>,
        old_has_pending: bool,
    },
}

#[allow(dead_code)]
pub struct AnchorsCacheTransaction<'a> {
    cache: &'a mut AnchorsCache,
    undo_log: Vec<UndoOp>,
    committed: bool,
}

#[allow(dead_code)]
impl<'a> AnchorsCacheTransaction<'a> {
    pub fn new(cache: &'a mut AnchorsCache) -> Self {
        Self {
            cache,
            undo_log: Vec::new(),
            committed: false,
        }
    }

    pub fn add(&mut self, anchor: Arc<MempoolAnchor>, our_exts_count: usize) {
        let anchor_id = anchor.id;
        let added_to_cache = our_exts_count > 0;

        self.undo_log.push(UndoOp::Add {
            anchor_id,
            added_to_cache,
        });

        self.cache.add(anchor, our_exts_count);
    }

    pub fn pop_front(&mut self) -> Option<(MempoolAnchorId, Arc<MempoolAnchor>)> {
        let front = self.cache.cache.front().cloned()?;
        let (anchor_id, cached_anchor) = front;

        let old_has_pending = self.cache.has_pending_externals;

        let mut removed_infos = Vec::new();
        let chain_time = cached_anchor.anchor.chain_time;
        for info in &self.cache.imported_anchors_info_history {
            if info.ct < chain_time && self.cache.imported_anchors_info_history.len() > 1 {
                removed_infos.push(info.clone());
            } else {
                break;
            }
        }

        self.undo_log.push(UndoOp::PopFront {
            anchor_id,
            cached_anchor: cached_anchor.clone(),
            removed_infos,
            old_has_pending,
        });

        self.cache.pop_front()
    }

    pub fn commit(mut self) {
        self.committed = true;
    }

    fn rollback(&mut self) {
        while let Some(op) = self.undo_log.pop() {
            match op {
                UndoOp::Add {
                    anchor_id: _,
                    added_to_cache,
                } => {
                    self.cache.imported_anchors_info_history.pop_back();

                    if added_to_cache {
                        self.cache.cache.pop_back();
                        self.cache.has_pending_externals = !self.cache.cache.is_empty();
                    }
                }
                UndoOp::PopFront {
                    anchor_id,
                    cached_anchor,
                    removed_infos,
                    old_has_pending,
                } => {
                    self.cache.cache.push_front((anchor_id, cached_anchor));

                    for info in removed_infos.into_iter().rev() {
                        self.cache.imported_anchors_info_history.push_front(info);
                    }

                    self.cache.has_pending_externals = old_has_pending;
                }
            }
        }
    }

    pub fn last_imported_anchor_info(&self) -> Option<&AnchorInfo> {
        self.cache.last_imported_anchor_info()
    }

    pub fn get_last_imported_anchor_id_and_ct(&self) -> Option<(u32, u64)> {
        self.cache.get_last_imported_anchor_id_and_ct()
    }

    pub fn get(&self, index: usize) -> Option<(MempoolAnchorId, Arc<MempoolAnchor>)> {
        self.cache.get(index)
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, (MempoolAnchorId, CachedAnchor)> {
        self.cache.iter()
    }

    pub fn has_pending_externals(&self) -> bool {
        self.cache.has_pending_externals()
    }

    pub fn check_has_pending_externals_in_range(&self, up_to: &ExternalKey) -> bool {
        self.cache.check_has_pending_externals_in_range(up_to)
    }
}

impl Drop for AnchorsCacheTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_anchor(id: MempoolAnchorId, chain_time: u64) -> Arc<MempoolAnchor> {
        Arc::new(MempoolAnchor {
            id,
            prev_id: if id > 0 { Some(id - 1) } else { None },
            chain_time,
            author: PeerId([0; 32]),
            externals: Default::default(),
        })
    }

    #[test]
    fn test_transaction_commit() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 5);
            tx.add(make_anchor(2, 200), 3);
            tx.commit();
        }

        assert_eq!(cache.len(), 2);
        assert!(cache.has_pending_externals());
    }

    #[test]
    fn test_transaction_rollback_on_drop() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 5);
            tx.add(make_anchor(2, 200), 3);
        }

        assert_eq!(cache.len(), 0);
        assert!(!cache.has_pending_externals());
    }

    #[test]
    fn test_transaction_pop_front_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        assert_eq!(cache.len(), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            let popped = tx.pop_front();
            assert!(popped.is_some());
            assert_eq!(popped.unwrap().0, 1);
        }

        assert_eq!(cache.len(), 2);
        let (id, _) = cache.get(0).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_transaction_pop_front_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            let popped = tx.pop_front();
            assert!(popped.is_some());
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
        let (id, _) = cache.get(0).unwrap();
        assert_eq!(id, 2);
    }

    #[test]
    fn test_transaction_mixed_operations_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.add(make_anchor(2, 200), 3);
            tx.add(make_anchor(3, 300), 2);
        }

        assert_eq!(cache.len(), 1);
        let (id, _) = cache.get(0).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_transaction_add_with_zero_externals() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 0);
            tx.commit();
        }

        assert_eq!(cache.len(), 0);
        assert!(cache.last_imported_anchor_info().is_some());
    }

    #[test]
    fn test_transaction_add_with_zero_externals_rollback() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 0);
        }

        assert_eq!(cache.len(), 0);
        assert!(cache.last_imported_anchor_info().is_none());
    }
}

