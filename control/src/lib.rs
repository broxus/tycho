#[cfg(feature = "client")]
pub use self::client::ControlClient;
#[cfg(feature = "client")]
pub use self::error::{ClientError, ClientResult};
pub use self::error::{ServerError, ServerResult};
#[cfg(feature = "server")]
pub use self::profiler::{MemoryProfiler, StubMemoryProfiler};
#[cfg(feature = "server")]
pub use self::server::{ControlEndpoint, ControlServer, ControlServerBuilder, ControlServerConfig};

mod error;
#[cfg(feature = "server")]
mod profiler;
pub mod proto;

#[cfg(feature = "client")]
mod client;

#[cfg(feature = "server")]
mod server;

// TODO: Change the path to a more general setup.
pub const DEFAULT_SOCKET_PATH: &str = "/var/venom/data/tycho.sock";
