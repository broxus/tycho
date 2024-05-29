use std::sync::Arc;

use crate::internal_queue::snapshot::{MessageWithSource, StateSnapshot};

pub struct SnapshotManager {
    snapshots: Vec<Box<dyn StateSnapshot>>,
    current_snapshot: usize,
}

impl SnapshotManager {
    pub fn new(snapshots: Vec<Box<dyn StateSnapshot>>) -> Self {
        SnapshotManager {
            snapshots,
            current_snapshot: 0,
        }
    }

    /// if first snapshot doesn't have any messages, it will try to get messages from the next snapshot and mark it as the current one
    pub fn next(&mut self) -> Option<Arc<MessageWithSource>> {
        while self.current_snapshot < self.snapshots.len() {
            if let Some(message) = self.snapshots[self.current_snapshot].next() {
                return Some(message);
            }
            self.current_snapshot += 1;
        }
        None
    }

    /// peek the next message without moving the cursor
    pub fn peek(&self) -> Option<Arc<MessageWithSource>> {
        let mut current_snapshot = self.current_snapshot;
        while current_snapshot < self.snapshots.len() {
            if let Some(message) = self.snapshots[current_snapshot].peek() {
                return Some(message);
            }
            current_snapshot += 1;
        }
        None
    }
}
