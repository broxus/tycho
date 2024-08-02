pub use self::client::ControlClient;
pub use self::error::{ClientError, ServerResult};
pub use self::profiler::{MemoryProfiler, StubMemoryProfiler};
pub use self::server::{ControlEndpoint, ControlServer, ControlServerBuilder, ControlServerConfig};

mod client;
mod error;
mod profiler;
mod proto;
mod server;

// TODO: Change the path to a more general setup.
pub const DEFAULT_SOCKET_PATH: &str = "/var/venom/data/tycho.sock";
