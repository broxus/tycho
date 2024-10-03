use std::path::Path;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardIdent};
use serde::{Deserialize, Serialize};
use tycho_network::{OverlayId, PeerInfo};

use crate::proto::blockchain::OverlayIdData;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub bootstrap_peers: Vec<PeerInfo>,
    pub zerostate: ZerostateId,
    pub mempool: MempoolGlobalConfig,
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

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MempoolGlobalConfig {
    pub clock_skew: u64,
    pub commit_depth: u8,
    pub genesis_round: u32,
    pub payload_batch_size: u64,
    pub deduplicate_rounds: u16,
    pub max_anchor_distance: u16,
}
