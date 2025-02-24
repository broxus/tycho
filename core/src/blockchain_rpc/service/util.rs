use governor::state::keyed::DefaultKeyedStateStore;
use tycho_network::PeerId;

use crate::proto::blockchain::*;
use crate::proto::overlay;

macro_rules! constructor_to_string {
    ($($ty:path as $name:ident),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Constructor {
            $($name),*
        }

        impl Constructor {
            pub fn from_tl_id(id: u32) -> Option<Self> {
                match id {
                    $(<$ty>::TL_ID => Some(Self::$name)),*,
                    _ => None
                }
            }

            pub fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$name => stringify!($name)),*
                }
            }

        }
    };
}

// update list in `def core_blockchain_rpc_per_method_stats() -> RowPanel:` after changing this
constructor_to_string! {
    overlay::Ping as Ping,
    rpc::GetNextKeyBlockIds as GetNextKeyBlockIds,
    rpc::GetBlockFull as GetBlockFull,
    rpc::GetNextBlockFull as GetNextBlockFull,
    rpc::GetBlockDataChunk as GetBlockDataChunk,
    rpc::GetKeyBlockProof as GetKeyBlockProof,
    rpc::GetPersistentShardStateInfo as GetPersistentShardStateInfo,
    rpc::GetPersistentQueueStateInfo as GetPersistentQueueStateInfo,
    rpc::GetPersistentShardStateChunk as GetPersistentShardStateChunk,
    rpc::GetPersistentQueueStateChunk as GetPersistentQueueStateChunk,
    rpc::GetArchiveInfo as GetArchiveInfo,
    rpc::GetArchiveChunk as GetArchiveChunk
}

pub type RateLimiter = governor::RateLimiter<
    PeerId,
    DefaultKeyedStateStore<PeerId, ahash::RandomState>,
    governor::clock::DefaultClock,
>;
