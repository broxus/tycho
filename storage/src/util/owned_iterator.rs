use std::sync::Arc;

use weedb::rocksdb;
use weedb::rocksdb::DBRawIterator;

use crate::util::StoredValue;

pub struct OwnedIterator {
    inner: DBRawIterator<'static>,
    _db: Arc<rocksdb::DB>,
}

impl OwnedIterator {
    pub fn new(iter: DBRawIterator<'_>, db: Arc<rocksdb::DB>) -> Self {
        unsafe fn extend_lifetime<'a>(r: DBRawIterator<'a>) -> DBRawIterator<'static> {
            std::mem::transmute::<DBRawIterator<'a>, DBRawIterator<'static>>(r)
        }

        let inner = unsafe { extend_lifetime(iter) };
        Self { inner, _db: db }
    }

    pub fn next(&mut self) {
        self.inner.next();
    }

    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
    }

    pub fn seek<T: StoredValue>(&mut self, key: T) {
        self.inner.seek(key.to_vec().as_slice());
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
