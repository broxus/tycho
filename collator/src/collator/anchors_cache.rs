use std::collections::VecDeque;
use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::metrics::HistogramGuard;

use crate::collator::messages_reader::state::ext::ExternalKey;
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

    /// Returns the last imported anchor info that would remain after
    /// `remove_last_imported_above(ct)`, without actually modifying the cache.
    pub fn last_imported_anchor_info_at_ct(&self, ct: u64) -> Option<&AnchorInfo> {
        self.imported_anchors_info_history
            .iter()
            .rev()
            .find(|info| info.ct <= ct)
            .or_else(|| self.imported_anchors_info_history.front())
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
    RemoveLastImportedAbove {
        removed_from_cache: Vec<(MempoolAnchorId, CachedAnchor)>,
        removed_from_history: Vec<AnchorInfo>,
        old_has_pending: bool,
    },
}

pub struct AnchorsCacheTransaction<'a> {
    cache: &'a mut AnchorsCache,
    undo_log: Vec<UndoOp>,
    committed: bool,
}

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
        let mut remaining_len = self.cache.imported_anchors_info_history.len();

        for info in &self.cache.imported_anchors_info_history {
            if info.ct < chain_time && remaining_len > 1 {
                removed_infos.push(info.clone());
                remaining_len -= 1;
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

    pub fn remove_last_imported_above(&mut self, ct: u64) {
        let old_has_pending = self.cache.has_pending_externals;

        // Collect what will be removed from cache
        let mut removed_from_cache = Vec::new();
        while let Some((id, ca)) = self.cache.cache.back() {
            if ca.anchor.chain_time > ct {
                removed_from_cache.push((*id, ca.clone()));
                self.cache.cache.pop_back();
            } else {
                break;
            }
        }

        // Collect what will be removed from history
        let mut removed_from_history = Vec::new();
        while let Some(info) = self.cache.imported_anchors_info_history.back() {
            if info.ct > ct && self.cache.imported_anchors_info_history.len() > 1 {
                removed_from_history.push(info.clone());
                self.cache.imported_anchors_info_history.pop_back();
            } else {
                break;
            }
        }

        if !removed_from_cache.is_empty() {
            self.cache.has_pending_externals = !self.cache.cache.is_empty();
        }

        self.undo_log.push(UndoOp::RemoveLastImportedAbove {
            removed_from_cache,
            removed_from_history,
            old_has_pending,
        });
    }

    pub fn commit(&mut self) {
        let _histogram = HistogramGuard::begin("tycho_do_collate_anchors_cache_commit_time");
        self.committed = true;
    }

    pub fn rollback(&mut self) {
        let _histogram = HistogramGuard::begin("tycho_do_collate_anchors_cache_rollback_time");
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
                UndoOp::RemoveLastImportedAbove {
                    removed_from_cache,
                    removed_from_history,
                    old_has_pending,
                } => {
                    // Restore cache (in reverse order since we popped from back)
                    for (id, ca) in removed_from_cache.into_iter().rev() {
                        self.cache.cache.push_back((id, ca));
                    }
                    // Restore history
                    for info in removed_from_history.into_iter().rev() {
                        self.cache.imported_anchors_info_history.push_back(info);
                    }
                    self.cache.has_pending_externals = old_has_pending;
                }
            }
        }
    }

    pub fn last_imported_anchor_info(&self) -> Option<&AnchorInfo> {
        self.cache.last_imported_anchor_info()
    }

    pub fn last_imported_anchor_info_at_ct(&self, ct: u64) -> Option<&AnchorInfo> {
        self.cache.last_imported_anchor_info_at_ct(ct)
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

    // ==================== BASIC TESTS ====================

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
        assert_eq!(cache.imported_anchors_info_history.len(), 2);
    }

    #[test]
    fn test_transaction_rollback_on_drop() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 5);
            tx.add(make_anchor(2, 200), 3);
            // no commit
        }

        assert_eq!(cache.len(), 0);
        assert!(!cache.has_pending_externals());
        assert_eq!(cache.imported_anchors_info_history.len(), 0);
    }

    // ==================== ADD WITH ZERO EXTERNALS ====================

    #[test]
    fn test_add_zero_externals_commit() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 0);
            tx.commit();
        }

        assert_eq!(cache.len(), 0); // not added to cache
        assert!(!cache.has_pending_externals());
        assert_eq!(cache.imported_anchors_info_history.len(), 1); // but added to history
    }

    #[test]
    fn test_add_zero_externals_rollback() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 0);
        }

        assert_eq!(cache.len(), 0);
        assert!(cache.imported_anchors_info_history.is_empty());
    }

    #[test]
    fn test_add_mixed_externals_rollback() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 5); // will be added to cache
            tx.add(make_anchor(2, 200), 0); // won't be added to cache
            tx.add(make_anchor(3, 300), 3); // will be added to cache
        }

        assert_eq!(cache.len(), 0);
        assert!(cache.imported_anchors_info_history.is_empty());
        assert!(!cache.has_pending_externals());
    }

    // ==================== POP_FRONT ====================

    #[test]
    fn test_pop_front_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            let popped = tx.pop_front();
            assert_eq!(popped.unwrap().0, 1);
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 2);
    }

    #[test]
    fn test_pop_front_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        let original_history_len = cache.imported_anchors_info_history.len();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            let popped = tx.pop_front();
            assert!(popped.is_some());
        }

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(
            cache.imported_anchors_info_history.len(),
            original_history_len
        );
    }

    #[test]
    fn test_pop_front_empty_cache() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            let popped = tx.pop_front();
            assert!(popped.is_none());
            tx.commit();
        }

        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_multiple_pop_front_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            assert_eq!(tx.pop_front().unwrap().0, 1);
            assert_eq!(tx.pop_front().unwrap().0, 2);
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 3);
    }

    #[test]
    fn test_multiple_pop_front_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.pop_front();
        }

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(cache.get(1).unwrap().0, 2);
        assert_eq!(cache.get(2).unwrap().0, 3);
    }

    // ==================== IMPORTED_ANCHORS_INFO REMOVAL ====================

    #[test]
    fn test_pop_front_removes_old_infos() {
        let mut cache = AnchorsCache::default();
        // Add anchors with different chain_time
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        assert_eq!(cache.imported_anchors_info_history.len(), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            // pop anchor with ct=100, should remove info with ct < 100 (none)
            tx.pop_front();
            tx.commit();
        }

        // After pop(ct=100): info where ct < 100 are removed, but len > 1
        // We have [100, 200, 300] -> nothing will be removed (100 is not < 100)
        assert_eq!(cache.imported_anchors_info_history.len(), 3);
    }

    #[test]
    fn test_pop_front_removes_multiple_old_infos() {
        let mut cache = AnchorsCache::default();

        // Add anchor without externals (only to history)
        cache.add(make_anchor(1, 50), 0);
        cache.add(make_anchor(2, 80), 0);
        // Add anchor with externals
        cache.add(make_anchor(3, 100), 5);
        cache.add(make_anchor(4, 200), 3);

        assert_eq!(cache.imported_anchors_info_history.len(), 4);
        assert_eq!(cache.len(), 2); // only 2 in cache

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            // pop anchor with ct=100, should remove info with ct < 100
            tx.pop_front();
            tx.commit();
        }

        // [50, 80, 100, 200] -> pop(ct=100) -> remove 50, then 80 (len is still > 1)
        // Remains [100, 200]
        assert_eq!(cache.imported_anchors_info_history.len(), 2);
        assert_eq!(cache.imported_anchors_info_history.front().unwrap().ct, 100);
    }

    #[test]
    fn test_pop_front_info_removal_rollback() {
        let mut cache = AnchorsCache::default();

        cache.add(make_anchor(1, 50), 0);
        cache.add(make_anchor(2, 80), 0);
        cache.add(make_anchor(3, 100), 5);
        cache.add(make_anchor(4, 200), 3);

        let original_history: Vec<_> = cache
            .imported_anchors_info_history
            .iter()
            .map(|i| i.ct)
            .collect();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            // no commit - rollback
        }

        let restored_history: Vec<_> = cache
            .imported_anchors_info_history
            .iter()
            .map(|i| i.ct)
            .collect();
        assert_eq!(original_history, restored_history);
    }

    #[test]
    fn test_pop_front_keeps_at_least_one_info() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.commit();
        }

        // Even after removal, at least 1 info should remain (condition len > 1)
        assert_eq!(cache.imported_anchors_info_history.len(), 1);
    }

    // ==================== MIXED OPERATIONS ====================

    #[test]
    fn test_mixed_operations_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.add(make_anchor(2, 200), 3);
            tx.add(make_anchor(3, 300), 2);
            tx.commit();
        }

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(0).unwrap().0, 2);
        assert_eq!(cache.get(1).unwrap().0, 3);
    }

    #[test]
    fn test_mixed_operations_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.add(make_anchor(2, 200), 3);
            tx.add(make_anchor(3, 300), 2);
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 1);
    }

    #[test]
    fn test_add_pop_add_rollback() {
        let mut cache = AnchorsCache::default();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(1, 100), 5);
            tx.pop_front();
            tx.add(make_anchor(2, 200), 3);
        }

        assert_eq!(cache.len(), 0);
        assert!(cache.imported_anchors_info_history.is_empty());
    }

    // ==================== HAS_PENDING_EXTERNALS ====================

    #[test]
    fn test_has_pending_externals_after_pop_all() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            assert!(!tx.has_pending_externals());
            tx.commit();
        }

        assert!(!cache.has_pending_externals());
    }

    #[test]
    fn test_has_pending_externals_restored_on_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        assert!(cache.has_pending_externals());

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            assert!(!tx.has_pending_externals());
        }

        assert!(cache.has_pending_externals());
    }

    // ==================== EDGE CASES ====================

    #[test]
    fn test_empty_transaction_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_empty_transaction_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let _tx = AnchorsCacheTransaction::new(&mut cache);
        }

        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_pop_until_empty_then_add() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.pop_front();
            assert!(tx.pop_front().is_none());
            tx.add(make_anchor(3, 300), 2);
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 3);
    }

    // ==================== CHECKING CORRECTNESS OF remaining_len LOGIC ====================

    #[test]
    fn test_remaining_len_logic_boundary() {
        let mut cache = AnchorsCache::default();

        // Create a situation: [ct=50, ct=80] in history, pop anchor with ct=100
        // Original: will only remove ct=50 (after that len=1, condition > 1 is false)
        cache.add(make_anchor(1, 50), 0);
        cache.add(make_anchor(2, 80), 0);
        cache.add(make_anchor(3, 100), 5);

        assert_eq!(cache.imported_anchors_info_history.len(), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.commit();
        }

        // After pop: [50, 80, 100] -> remove where ct < 100 and len > 1
        // Step 1: 50 < 100 && 3 > 1 -> remove, len=2
        // Step 2: 80 < 100 && 2 > 1 -> remove, len=1
        // Step 3: 100 < 100 -> false, stop
        // Expected: only [100] remains
        assert_eq!(cache.imported_anchors_info_history.len(), 1);
        assert_eq!(cache.imported_anchors_info_history.front().unwrap().ct, 100);
    }

    #[test]
    fn test_remaining_len_logic_boundary_rollback() {
        let mut cache = AnchorsCache::default();

        cache.add(make_anchor(1, 50), 0);
        cache.add(make_anchor(2, 80), 0);
        cache.add(make_anchor(3, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            // rollback
        }

        // Should restore exactly 3 info: [50, 80, 100]
        assert_eq!(cache.imported_anchors_info_history.len(), 3);
        let cts: Vec<_> = cache
            .imported_anchors_info_history
            .iter()
            .map(|i| i.ct)
            .collect();
        assert_eq!(cts, vec![50, 80, 100]);
    }

    // ==================== ACCESSOR METHODS IN TRANSACTION ====================

    #[test]
    fn test_transaction_accessors() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);

            assert_eq!(tx.get(0).unwrap().0, 1);
            assert_eq!(tx.get(1).unwrap().0, 2);
            assert!(tx.get(2).is_none());

            assert!(tx.has_pending_externals());

            let (id, ct) = tx.get_last_imported_anchor_id_and_ct().unwrap();
            assert_eq!(id, 2);
            assert_eq!(ct, 200);

            tx.pop_front();

            assert_eq!(tx.get(0).unwrap().0, 2);
            assert!(tx.get(1).is_none());

            tx.commit();
        }
    }

    #[test]
    fn test_transaction_iter() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            let ids: Vec<_> = tx.iter().map(|(id, _)| *id).collect();
            assert_eq!(ids, vec![1, 2]);
            tx.commit();
        }
    }

    // ==================== REMOVE_LAST_IMPORTED_ABOVE ====================

    #[test]
    fn test_remove_last_imported_above_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(150);
            tx.commit();
        }

        // Should remove anchors with ct > 150 (i.e., 200 and 300)
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(cache.imported_anchors_info_history.len(), 1);
        assert_eq!(cache.imported_anchors_info_history.front().unwrap().ct, 100);
    }

    #[test]
    fn test_remove_last_imported_above_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        let original_len = cache.len();
        let original_history_len = cache.imported_anchors_info_history.len();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(150);
            // no commit - rollback
        }

        assert_eq!(cache.len(), original_len);
        assert_eq!(
            cache.imported_anchors_info_history.len(),
            original_history_len
        );
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(cache.get(1).unwrap().0, 2);
        assert_eq!(cache.get(2).unwrap().0, 3);
    }

    #[test]
    fn test_remove_last_imported_above_nothing_to_remove() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(300); // ct > 300 -> nothing
            tx.commit();
        }

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.imported_anchors_info_history.len(), 2);
    }

    #[test]
    fn test_remove_last_imported_above_all() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(50); // all ct > 50
            tx.commit();
        }

        assert_eq!(cache.len(), 0);
        assert!(!cache.has_pending_externals());
        // History should keep at least 1 entry due to len > 1 check
        assert_eq!(cache.imported_anchors_info_history.len(), 1);
    }

    #[test]
    fn test_remove_last_imported_above_has_pending_restored_on_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);

        assert!(cache.has_pending_externals());

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(50); // removes all
            assert!(!tx.has_pending_externals());
            // no commit
        }

        assert!(cache.has_pending_externals());
    }

    #[test]
    fn test_remove_last_imported_above_with_zero_externals() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 0); // only in history
        cache.add(make_anchor(3, 300), 3);
        cache.add(make_anchor(4, 400), 0); // only in history

        assert_eq!(cache.len(), 2); // only 1 and 3 in cache
        assert_eq!(cache.imported_anchors_info_history.len(), 4);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(250);
            tx.commit();
        }

        // Removes from cache: anchor 3 (ct=300)
        // Removes from history: ct=400, ct=300 (but keeps at least 1)
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 1);
        // History: [100, 200, 300, 400] -> remove 400, remove 300 -> [100, 200]
        assert_eq!(cache.imported_anchors_info_history.len(), 2);
    }

    #[test]
    fn test_remove_last_imported_above_with_zero_externals_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 0);
        cache.add(make_anchor(3, 300), 3);
        cache.add(make_anchor(4, 400), 0);

        let original_cache_ids: Vec<_> = cache.iter().map(|(id, _)| *id).collect();
        let original_history_cts: Vec<_> = cache
            .imported_anchors_info_history
            .iter()
            .map(|i| i.ct)
            .collect();

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(250);
            // no commit
        }

        let restored_cache_ids: Vec<_> = cache.iter().map(|(id, _)| *id).collect();
        let restored_history_cts: Vec<_> = cache
            .imported_anchors_info_history
            .iter()
            .map(|i| i.ct)
            .collect();

        assert_eq!(original_cache_ids, restored_cache_ids);
        assert_eq!(original_history_cts, restored_history_cts);
    }

    // ==================== MIXED OPERATIONS WITH REMOVE_LAST_IMPORTED_ABOVE ====================

    #[test]
    fn test_add_then_remove_last_imported_above_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(2, 200), 3);
            tx.add(make_anchor(3, 300), 2);
            tx.remove_last_imported_above(150);
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 1);
    }

    #[test]
    fn test_add_then_remove_last_imported_above_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.add(make_anchor(2, 200), 3);
            tx.add(make_anchor(3, 300), 2);
            tx.remove_last_imported_above(150);
            // no commit
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 1);
    }

    #[test]
    fn test_pop_front_then_remove_last_imported_above_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);
        cache.add(make_anchor(4, 400), 1);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front(); // removes 1
            tx.remove_last_imported_above(250); // removes 3, 4
            tx.commit();
        }

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0).unwrap().0, 2);
    }

    #[test]
    fn test_pop_front_then_remove_last_imported_above_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);
        cache.add(make_anchor(4, 400), 1);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.pop_front();
            tx.remove_last_imported_above(250);
            // no commit
        }

        assert_eq!(cache.len(), 4);
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(cache.get(1).unwrap().0, 2);
        assert_eq!(cache.get(2).unwrap().0, 3);
        assert_eq!(cache.get(3).unwrap().0, 4);
    }

    #[test]
    fn test_remove_last_imported_above_then_add_commit() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(150); // removes 2, 3
            tx.add(make_anchor(4, 250), 4);
            tx.commit();
        }

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(cache.get(1).unwrap().0, 4);
    }

    #[test]
    fn test_remove_last_imported_above_then_add_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(150);
            tx.add(make_anchor(4, 250), 4);
            // no commit
        }

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(0).unwrap().0, 1);
        assert_eq!(cache.get(1).unwrap().0, 2);
        assert_eq!(cache.get(2).unwrap().0, 3);
    }

    #[test]
    fn test_multiple_remove_last_imported_above_rollback() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);
        cache.add(make_anchor(4, 400), 1);
        cache.add(make_anchor(5, 500), 1);

        {
            let mut tx = AnchorsCacheTransaction::new(&mut cache);
            tx.remove_last_imported_above(350); // removes 4, 5
            tx.remove_last_imported_above(150); // removes 2, 3
            // no commit
        }

        assert_eq!(cache.len(), 5);
        let ids: Vec<_> = cache.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    // ==================== LAST_IMPORTED_ANCHOR_INFO_AT_CT ====================

    #[test]
    fn test_last_imported_anchor_info_at_ct_in_transaction() {
        let mut cache = AnchorsCache::default();
        cache.add(make_anchor(1, 100), 5);
        cache.add(make_anchor(2, 200), 3);
        cache.add(make_anchor(3, 300), 2);

        {
            let tx = AnchorsCacheTransaction::new(&mut cache);

            // ct=250 -> should return anchor 2 (ct=200)
            let info = tx.last_imported_anchor_info_at_ct(250).unwrap();
            assert_eq!(info.id, 2);
            assert_eq!(info.ct, 200);

            // ct=100 -> should return anchor 1 (ct=100)
            let info = tx.last_imported_anchor_info_at_ct(100).unwrap();
            assert_eq!(info.id, 1);
            assert_eq!(info.ct, 100);

            // ct=50 -> should return first anchor (ct=100) as fallback
            let info = tx.last_imported_anchor_info_at_ct(50).unwrap();
            assert_eq!(info.id, 1);
            assert_eq!(info.ct, 100);
        }
    }
}
