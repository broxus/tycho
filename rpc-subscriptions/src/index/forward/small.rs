use crate::ClientId;
use crate::index::forward::ForwardSetHash;

pub(crate) const SMALL_CAP: usize =
    const { (size_of::<ForwardSetHash>() - size_of::<u8>()) / size_of::<ClientId>() };

const _: () = {
    assert!(size_of::<SmallForwardSet>() <= size_of::<ForwardSetHash>());
    assert!(SMALL_CAP <= u8::MAX as usize);
};

#[derive(Clone, Copy, Debug)]
pub struct SmallForwardSet {
    pub(super) len: u8,
    pub(super) ids: [ClientId; SMALL_CAP],
}

impl SmallForwardSet {
    pub(super) fn new() -> Self {
        Self {
            len: 0,
            ids: [ClientId::new(0); SMALL_CAP],
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub(super) fn extend_into(&self, out: &mut Vec<ClientId>) {
        out.extend_from_slice(self.as_slice());
    }

    #[inline]
    pub(super) fn contains(&self, id: ClientId) -> bool {
        self.as_slice().contains(&id)
    }

    /// Returns true if we stayed small, false if caller should promote.
    pub(super) fn add(&mut self, id: ClientId) -> bool {
        if self.contains(id) {
            return true;
        }
        let len = self.len();
        if len == SMALL_CAP {
            return false;
        }
        self.ids[len] = id;
        self.len += 1;
        true
    }

    pub(super) fn remove(&mut self, id: ClientId) -> bool {
        let len = self.len();
        for i in 0..len {
            if self.ids[i] == id {
                self.ids[i] = self.ids[len - 1];
                self.len -= 1;
                return true;
            }
        }
        false
    }

    fn as_slice(&self) -> &[ClientId] {
        &self.ids[..self.len()]
    }
}
