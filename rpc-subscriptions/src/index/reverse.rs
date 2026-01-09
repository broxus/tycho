use smallvec::SmallVec;

use crate::api::MAX_ADDRS_PER_CLIENT;
use crate::types::InternedAddrId;

/// Reverse index: up to 255 addresses per client (sorted unique).
// TODO: use fixed array?
#[derive(Debug, Clone)]
pub struct IntAddrs {
    addrs: SmallVec<[InternedAddrId; 8]>,
}

pub(crate) enum AddResult {
    Inserted,
    AlreadyPresent,
    AtCapacity,
}

impl IntAddrs {
    pub(crate) fn new() -> Self {
        Self {
            addrs: SmallVec::new(),
        }
    }

    pub(crate) fn add_with_delta(&mut self, a: InternedAddrId) -> (AddResult, usize) {
        let before_bytes = self.heap_bytes();

        let res = match self.addrs.binary_search(&a) {
            Ok(_) => AddResult::AlreadyPresent,
            Err(i) => {
                if self.addrs.len() >= MAX_ADDRS_PER_CLIENT as usize {
                    AddResult::AtCapacity
                } else {
                    // insert into the position found by binary search to keep the order
                    self.addrs.insert(i, a);
                    AddResult::Inserted
                }
            }
        };

        let after_bytes = self.heap_bytes();

        (res, after_bytes - before_bytes)
    }

    /// Returns true if removed.
    pub(crate) fn remove(&mut self, a: InternedAddrId) -> bool {
        if let Ok(i) = self.addrs.binary_search(&a) {
            self.addrs.remove(i);
            true
        } else {
            false
        }
    }

    pub fn heap_bytes(&self) -> usize {
        if self.addrs.spilled() {
            self.addrs.capacity() * size_of::<InternedAddrId>()
        } else {
            0
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.addrs.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.addrs.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = InternedAddrId> + '_ {
        self.addrs.iter().copied()
    }
}
