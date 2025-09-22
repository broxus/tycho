use std::collections::hash_map;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tycho_types::models::ShardStateUnsplit;
use tycho_util::FastHashMap;

#[derive(Clone, Default)]
#[repr(transparent)]
pub struct MinRefMcStateTracker {
    inner: Arc<Inner>,
}

impl MinRefMcStateTracker {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    pub fn seqno(&self) -> Option<u32> {
        self.inner.counters.read().min_seqno
    }

    pub fn insert(&self, state: &ShardStateUnsplit) -> RefMcStateHandle {
        if state.seqno == 0 {
            // Insert zerostates as untracked states to prevent their cache
            // to hold back the global archives GC. This handle will still
            // point to a shared tracker, but will have not touch any ref.
            self.insert_untracked()
        } else {
            self.insert_seqno(state.min_ref_mc_seqno)
        }
    }

    pub fn insert_seqno(&self, mc_seqno: u32) -> RefMcStateHandle {
        self.inner.insert(mc_seqno)
    }

    pub fn insert_untracked(&self) -> RefMcStateHandle {
        RefMcStateHandle(Arc::new(HandleInner {
            min_ref_mc_state: self.inner.clone(),
            mc_seqno: None,
        }))
    }

    #[inline]
    fn wrap(inner: &Arc<Inner>) -> &Self {
        // SAFETY: `MinRefMcStateTracker` has the same memory layout as `Arc<Inner>`.
        unsafe { &*(inner as *const Arc<Inner>).cast::<Self>() }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct RefMcStateHandle(Arc<HandleInner>);

impl RefMcStateHandle {
    pub fn min_safe<'a>(&'a self, other: &'a Self) -> &'a Self {
        match (self.0.mc_seqno, other.0.mc_seqno) {
            // Tracked seqno is safer.
            (_, None) => self,
            (None, Some(_)) => other,
            // Lower seqno is safer.
            (Some(this_seqno), Some(other_seqno)) => {
                if other_seqno < this_seqno {
                    other
                } else {
                    self
                }
            }
        }
    }

    pub fn tracker(&self) -> &MinRefMcStateTracker {
        MinRefMcStateTracker::wrap(&self.0.min_ref_mc_state)
    }
}

impl std::fmt::Debug for RefMcStateHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefMcStateHandle")
            .field("mc_seqno", &self.0.mc_seqno)
            .finish()
    }
}

#[derive(Default)]
struct Inner {
    counters: parking_lot::RwLock<StateIds>,
}

impl Inner {
    fn insert(self: &Arc<Self>, mc_seqno: u32) -> RefMcStateHandle {
        // Fast path, just increase existing counter
        let counters = self.counters.read();
        if let Some(counter) = counters.refs.get(&mc_seqno) {
            counter.fetch_add(1, Ordering::Release);
            return RefMcStateHandle(Arc::new(HandleInner {
                min_ref_mc_state: self.clone(),
                mc_seqno: Some(mc_seqno),
            }));
        }
        drop(counters);

        // Fallback to exclusive write
        let mut counters = self.counters.write();
        match counters.refs.entry(mc_seqno) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(AtomicU32::new(1));

                match &mut counters.min_seqno {
                    Some(seqno) if mc_seqno < *seqno => *seqno = mc_seqno,
                    None => counters.min_seqno = Some(mc_seqno),
                    _ => {}
                }
            }
            hash_map::Entry::Occupied(entry) => {
                entry.get().fetch_add(1, Ordering::Release);
            }
        }

        RefMcStateHandle(Arc::new(HandleInner {
            min_ref_mc_state: self.clone(),
            mc_seqno: Some(mc_seqno),
        }))
    }

    fn remove(&self, mc_seqno: u32) {
        // Fast path, just decrease existing counter
        let counters = self.counters.read();
        if let Some(counter) = counters.refs.get(&mc_seqno) {
            if counter.fetch_sub(1, Ordering::AcqRel) > 1 {
                return;
            }
        } else {
            return;
        }
        drop(counters);

        // Fallback to exclusive write to update current min
        let mut counters = self.counters.write();
        match counters.refs.entry(mc_seqno) {
            hash_map::Entry::Occupied(entry) if entry.get().load(Ordering::Acquire) == 0 => {
                entry.remove();
                if matches!(counters.min_seqno, Some(seqno) if seqno == mc_seqno) {
                    counters.min_seqno = counters.refs.keys().min().copied();
                }
            }
            _ => {}
        }
    }
}

struct HandleInner {
    min_ref_mc_state: Arc<Inner>,
    mc_seqno: Option<u32>,
}

impl Drop for HandleInner {
    fn drop(&mut self) {
        if let Some(mc_seqno) = self.mc_seqno {
            self.min_ref_mc_state.remove(mc_seqno);
        }
    }
}

#[derive(Default)]
struct StateIds {
    min_seqno: Option<u32>,
    refs: FastHashMap<u32, AtomicU32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_ref_mc_state() {
        let state = MinRefMcStateTracker::new();

        {
            let _handle = state.insert_seqno(10);
            assert_eq!(state.seqno(), Some(10));
        }
        assert_eq!(state.seqno(), None);

        {
            let handle1 = state.insert_seqno(10);
            assert_eq!(state.seqno(), Some(10));
            let _handle2 = state.insert_seqno(15);
            assert_eq!(state.seqno(), Some(10));
            let handle3 = state.insert_seqno(10);
            assert_eq!(state.seqno(), Some(10));
            drop(handle3);
            assert_eq!(state.seqno(), Some(10));
            drop(handle1);
            assert_eq!(state.seqno(), Some(15));
        }
        assert_eq!(state.seqno(), None);
    }
}
