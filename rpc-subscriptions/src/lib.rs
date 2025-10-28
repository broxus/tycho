pub use api::{MAX_ADDRS_PER_CLIENT, SubscribeError, SubscriberManagerConfig, UnsubscribeError};
pub use manager::SubscriberManager;
pub use memory::MemorySnapshot;
pub use types::{ClientId, ClientStats};

mod api;
mod types;

pub(crate) mod clients;
pub(crate) mod index;
pub mod manager;
mod memory;

#[cfg(test)]
mod perf_tests;
#[cfg(test)]
pub(crate) mod test_utils;
