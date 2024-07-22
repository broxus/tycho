use everscale_types::models::BlockId;

mod client;
mod server;

pub use client::*;
pub use server::{ControlServerImpl, ControlServerListener};

#[tarpc::service]
pub trait ControlServer {
    /// Ping a node. Should return an i + 1 response.
    async fn ping(i: u32) -> u32;

    /// Triggers GC for specified mc_block_id
    async fn trigger_gc(trigger: ManualTriggerValue, seqno: Option<u32>, distance: Option<u32>);

    /// Sets profiler state to targeted value. Return bool result indicates if state was changed
    async fn trigger_memory_profiler(set: bool) -> bool;

    /// Get block bytes
    async fn get_block(block_id: BlockId) -> Option<Vec<u8>>;

    /// Get proof bytes
    async fn get_block_proof(block_id: BlockId) -> Option<(Vec<u8>, bool)>;

    /// Get archive id
    async fn get_archive_info(mc_seqno: u32) -> Option<u32>;

    /// Download archive into file
    async fn get_archive_slice(id: u32, limit: u32, offset: u64) -> Option<Vec<u8>>;
}
