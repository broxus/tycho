use std::collections::HashMap;
use std::sync::Arc;
use everscale_types::boc::Boc;
use everscale_types::cell::Cell;

use everscale_types::models::ShardIdent;
use weedb::rocksdb::{
    DBCommon, DBRawIteratorWithThreadMode, IteratorMode, MultiThreaded, ReadOptions,
    SnapshotWithThreadMode, DB,
};
use weedb::OwnedSnapshot;

use crate::internal_queue::snapshot::{MessageWithSource, ShardRange, StateSnapshot};
use anyhow::Result;
pub struct PersistentStateSnapshot {
    iter: weedb::rocksdb::DBRawIterator<'static>,
}

impl PersistentStateSnapshot {
    pub fn new(iter: weedb::rocksdb::DBRawIterator<'static>) -> Self {

        iter.seek_to_first();



        // let iter = snapshot.iterator_cf_opt(cf1_handle, read_opts, IteratorMode::From(start_key, rocksdb::Direction::Forward));

        Self { iter }
    }
}

impl StateSnapshot for PersistentStateSnapshot {
    fn next(&mut self) -> Result<Option<Cell>> {
        while self.iter.valid() {
            if let (Some(key), Some(value)) = (self.iter.key(), self.iter.value()) {
                let cell_data = value;
                let cell = Boc::decode(cell_data)?;
                let key =
                InternalMessageKey::deserialize(&key);
                return Some(value)
                // println!("Key: {:?}, Value: {:?}", key, value);
            }
            self.iter.next(); // Move to the next key-value pair
        }
    }

    fn peek(&self) -> Option<Arc<MessageWithSource>> {
        None
    }
}
