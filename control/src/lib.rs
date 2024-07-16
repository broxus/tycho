use everscale_types::models::BlockId;

use crate::models::BlockFull;

mod client;
mod models;
mod server;

pub use client::*;
pub use server::{ControlServerImpl, ControlServerListener};
use tycho_core::block_strider::ManualGcTrigger;

#[tarpc::service]
pub trait ControlServer {
    /// Returns a pong response.
    async fn ping(i: u32) -> u32;

    /// Triggers GC for specified mc_block_id
    async fn trigger_gc(trigger: ManualGcTrigger);

    /// Sets profiler state to targeted value. Return bool result indicates if state was changed
    async fn trigger_memory_profiler(set: bool) -> bool;

    /// Get next key
    async fn get_next_key_blocks_ids(block_id: BlockId, max_size: usize) -> Option<Vec<BlockId>>;

    /// Get and print block info for specified block id
    async fn get_block_full(block_id: BlockId) -> Option<BlockFull>;

    async fn get_archive_info(mc_seqno: u32) -> Option<u32>;

    async fn get_archive_slice(id: u32, limit: u32, offset: u64) -> Option<Vec<u8>>;
}
