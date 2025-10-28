use tycho_util::FastHashSet;

use crate::ClientId;
use crate::index::forward::small::SmallForwardSet;

mod small;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum ForwardSet {
    Small(SmallForwardSet),
    Naive(ForwardSetHash),
}

const ELEMENT_WITH_CONTROL: usize = size_of::<ClientId>() + 1; // 1 for control byte

impl ForwardSet {
    pub fn new() -> Self {
        ForwardSet::Small(SmallForwardSet::new())
    }

    pub fn heap_bytes(&self) -> usize {
        match self {
            ForwardSet::Small(_) => 0,
            ForwardSet::Naive(naive) => naive.heap_bytes(),
        }
    }

    /// Returns memory usage diff
    pub fn add_with_delta(&mut self, id: ClientId) -> usize {
        match self {
            ForwardSet::Small(small) => {
                // Try to do it inline; if we overflow, promote.
                if small.add(id) {
                    0
                } else {
                    let naive = ForwardSetHash::from_small(small, id);
                    let delta = naive.heap_bytes();
                    *self = ForwardSet::Naive(naive);
                    delta
                }
            }
            ForwardSet::Naive(naive) => {
                let before = naive.ids.capacity();
                let _ = naive.ids.insert(id);
                let after = naive.ids.capacity();
                (after.saturating_sub(before)) * ELEMENT_WITH_CONTROL
            }
        }
    }

    pub fn remove(&mut self, id: ClientId) -> bool {
        match self {
            ForwardSet::Small(small) => small.remove(id),
            ForwardSet::Naive(naive) => naive.ids.remove(&id),
        }
    }

    /// Extends `out` with the current subscribers. Order is not guaranteed.
    pub fn extend_into(&self, out: &mut Vec<ClientId>) {
        match self {
            ForwardSet::Small(small) => small.extend_into(out),
            ForwardSet::Naive(naive) => out.extend(naive.ids.iter().copied()),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            ForwardSet::Small(small) => small.is_empty(),
            ForwardSet::Naive(naive) => naive.ids.is_empty(),
        }
    }

    #[cfg(test)]
    pub(super) fn is_small(&self) -> bool {
        matches!(self, ForwardSet::Small(_))
    }

    #[cfg(test)]
    pub fn iter_vec(&self) -> Vec<ClientId> {
        let mut out = Vec::new();
        self.extend_into(&mut out);
        out
    }

    #[cfg(test)]
    pub fn contains(&self, id: ClientId) -> bool {
        match self {
            ForwardSet::Small(small) => small.contains(id),
            ForwardSet::Naive(naive) => naive.ids.contains(&id),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ForwardSetHash {
    ids: FastHashSet<ClientId>,
}

impl ForwardSetHash {
    pub(super) fn from_small(small: &SmallForwardSet, extra: ClientId) -> Self {
        let mut ids = FastHashSet::with_capacity_and_hasher(small.len() + 1, Default::default());
        ids.extend(small.ids[..small.len()].iter().copied());
        ids.insert(extra);
        Self { ids }
    }

    pub fn heap_bytes(&self) -> usize {
        self.ids.capacity() * ELEMENT_WITH_CONTROL
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.ids.capacity()
    }
}
