use everscale_types::models::BlockId;

mod server;
mod client;

pub use server::ControlServerImpl;
pub use client::get_client;


#[tarpc::service]
trait ControlServer {
    /// Returns a pong response.
    async fn ping(i: u32) -> u32;


    async fn get_next_key_blocks_ids(block_id: BlockId, max_size: usize) -> Option<Vec<BlockId>>;
}