use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tycho_network::{OverlayId, PeerInfo};
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, ConsensusConfig, GenesisInfo, ShardIdent};

use crate::proto::blockchain::OverlayIdData;

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub bootstrap_peers: Vec<PeerInfo>,
    pub zerostate: ZerostateId,
    #[serde(default)]
    pub mempool: Option<MempoolGlobalConfig>,
}

impl GlobalConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub fn validate(&self, now: u32) -> Result<()> {
        for peer in &self.bootstrap_peers {
            anyhow::ensure!(peer.verify(now), "invalid peer info for {}", peer.id);
        }
        Ok(())
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ZerostateId {
    pub root_hash: HashBytes,
    pub file_hash: HashBytes,
}

impl ZerostateId {
    pub fn as_block_id(&self) -> BlockId {
        BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: 0,
            root_hash: self.root_hash,
            file_hash: self.file_hash,
        }
    }

    pub fn compute_public_overlay_id(&self) -> OverlayId {
        OverlayId(tl_proto::hash(OverlayIdData {
            zerostate_root_hash: self.root_hash.0,
            zerostate_file_hash: self.file_hash.0,
        }))
    }
}

/// Default zeros for start round and genesis time are the same in
/// [`ConsensusInfo`](tycho_types::models::ConsensusInfo).
///
/// This will replace genesis from master chain data only if this has time greater than in mc.
/// Also, this genesis must have round not less than in last applied mc block.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MempoolGlobalConfig {
    /// start round must be set to the `processed_to_anchor_id` from last applied master chain block
    /// actual genesis round may be greater (or still equal) after alignment;
    /// millis must be set not less than last applied mc block time, or better use actual time
    #[serde(flatten)]
    pub genesis_info: GenesisInfo,
    /// To restart a stale consensus with a new config, the majority of nodes has to provide
    /// new common genesis and settings in a global config file.
    /// Then an external message must be sent to update blockchain config contract ASAP in order to
    /// issue a key block with updated consensus config and persist it in blockchain state.
    /// Until then "mempool" part of the global config file must contain all the same data.
    ///
    /// It's because a block can include any settings only when config contract is called,
    /// otherwise contract state will not match.
    /// Quite the opposite is a recovery genesis info: it is immediately included
    /// into the first master chain block as an "extra" part, and makes it a key block.
    #[serde(flatten)]
    pub consensus_config: Option<ConsensusConfig>,
}
