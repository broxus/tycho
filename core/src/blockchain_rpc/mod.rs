pub use self::client::{BlockchainRpcClient, BlockchainRpcClientConfig};
pub use self::service::{BlockchainRpcService, BlockchainRpcServiceConfig};

mod client;
mod service;

pub const INTERNAL_ERROR_CODE: u32 = 1;
