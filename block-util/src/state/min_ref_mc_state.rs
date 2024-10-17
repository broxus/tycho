use std::collections::hash_map;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tycho_util::FastHashMap;

#[derive(Clone, Default)]
#[repr(transparent)]
pub struct MinRefMcStateTracker {
    inner: Arc<Inner>,
}

impl MinRefMcStateTracker {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seqno(&self) -> Option<u32> {
        self.inner.counters.read().min_seqno
    }

    pub(crate) fn insert(&self, mc_seqno: u32) -> RefMcStateHandle {
        self.inner.insert(mc_seqno)
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
    #[cfg(any(test, feature = "test"))]
    pub fn new_untracked(mc_seqno: u32) -> Self {
        Self(Arc::new(HandleInner {
            min_ref_mc_state: Arc::new(Inner::default()),
            mc_seqno,
        }))
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
                mc_seqno,
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
            mc_seqno,
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
    mc_seqno: u32,
}

impl Drop for HandleInner {
    fn drop(&mut self) {
        self.min_ref_mc_state.remove(self.mc_seqno);
    }
}

#[derive(Default)]
struct StateIds {
    min_seqno: Option<u32>,
    refs: FastHashMap<u32, AtomicU32>,
}
