use everscale_types::models::BlockId;

mod client;
mod models;
mod server;

pub use client::*;
pub use server::ControlServerImpl;

use crate::models::BlockFull;

#[tarpc::service]
pub trait ControlServer {
    /// Returns a pong response.
    async fn ping(i: u32) -> u32;
    async fn trigger_gc(mc_block_id: BlockId, last_key_block_seqno: u32);
    async fn get_next_key_blocks_ids(block_id: BlockId, max_size: usize) -> Option<Vec<BlockId>>;
    async fn get_block_full(block_id: BlockId) -> Option<BlockFull>;
}
