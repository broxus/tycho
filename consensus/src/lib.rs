mod dag;
mod effects;
mod engine;
mod intercom;
#[cfg(feature = "mock-feedback")]
pub mod mock_feedback;
mod models;
// TODO: Move into submodules, e.g. merge with `MempoolAdapterStore`.
mod storage;
#[cfg(any(feature = "test", test))]
pub mod test_utils;

pub mod prelude {
    pub use crate::engine::lifecycle::{EngineBinding, EngineNetworkArgs, EngineSession};
    pub use crate::engine::round_watch::{RoundWatch, TopKnownAnchor};
    pub use crate::engine::{
        ConsensusConfigExt, InputBuffer, MempoolConfigBuilder, MempoolMergedConfig,
        MempoolNodeConfig,
    };
    pub use crate::intercom::InitPeers;
    pub use crate::models::{
        AnchorData, Digest, MempoolOutput, Point, PointInfo, PointRestore, PointRestoreSelect,
        Round,
    };
    pub use crate::storage::{MempoolAdapterStore, MempoolDb};
}
