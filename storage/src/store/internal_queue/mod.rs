//! Internal message queue storage implementation.
//!
//! This module provides functionality for storing and working with internal messages
//! in the queue, including transactions, snapshots, and iterators.

pub mod iterator;
pub mod model;
pub mod snapshot;
pub mod storage;
pub mod transaction;

// Constants
pub const INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY: &[u8] = b"last_committed_mc_block_id";
