#[cfg(feature = "test")]
pub use anchor_consumer::*;
#[cfg(feature = "test")]
pub use bootstrap::*;
// TODO hide the whole mod under feature flag after configs are implemented
pub(crate) use genesis::*;

#[cfg(feature = "test")]
mod anchor_consumer;
#[cfg(feature = "test")]
mod bootstrap;
mod genesis;
