// use std::sync::Arc;

// use tycho_storage::rocksdb::{SnapshotWithThreadMode, DB};
//
// use crate::internal_queue::snapshot::{MessageWithSource, StateSnapshot};
//
// pub struct PersistentStateSnapshot {
//     snapshot: SnapshotWithThreadMode<'static, DB>,
// }
//
// impl PersistentStateSnapshot {
//     pub fn new(snapshot: SnapshotWithThreadMode<'static, DB>) -> Self {
//         Self { snapshot }
//     }
// }
//
// impl StateSnapshot for PersistentStateSnapshot {
//     fn next(&mut self) -> Option<Arc<MessageWithSource>> {
//         None
//     }
//
//     fn peek(&self) -> Option<Arc<MessageWithSource>> {
//         None
//     }
// }
