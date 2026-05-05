use std::net::IpAddr;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use tycho_network::{
    OverlayIngressPolicyDecision, PublicOverlayRateLimitPolicy, PublicOverlayRateLimiter,
    ServiceRequest, try_handle_prefix,
};
use tycho_util::FastHashSet;
use tycho_util::rate_limit::{RateLimitConfig, RateLimitPolicy, TokenBucketConfig};

use crate::proto::blockchain::rpc;
use crate::proto::overlay;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BlockchainRpcRateLimitsConfig {
    limiter: RateLimitConfig,
    whitelist: Vec<IpAddr>,
    traffic: BlockchainRpcTrafficLimits,
}

impl From<BlockchainRpcRateLimitsConfig> for PublicOverlayRateLimiter {
    fn from(config: BlockchainRpcRateLimitsConfig) -> Self {
        PublicOverlayRateLimiter::new(config.limiter, BlockchainRpcRateLimitPolicy {
            traffic: config.traffic,
            whitelist: config.whitelist.into_iter().collect(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BlockchainRpcTrafficLimits {
    light_queries: TokenBucketConfig,
    heavy_queries: TokenBucketConfig,
    broadcasts: TokenBucketConfig,
}

impl Default for BlockchainRpcTrafficLimits {
    fn default() -> Self {
        Self {
            light_queries: TokenBucketConfig::new(
                NonZeroU32::new(20).unwrap(),
                NonZeroU32::new(20).unwrap(),
            ),
            heavy_queries: TokenBucketConfig::new(
                NonZeroU32::new(10).unwrap(),
                NonZeroU32::new(10).unwrap(),
            ),
            broadcasts: TokenBucketConfig::new(
                NonZeroU32::new(5).unwrap(),
                NonZeroU32::new(5).unwrap(),
            ),
        }
    }
}

impl BlockchainRpcTrafficLimits {
    fn policy(
        &self,
        class: BlockchainRpcTrafficClass,
    ) -> RateLimitPolicy<BlockchainRpcTrafficClass> {
        let bucket = match class {
            BlockchainRpcTrafficClass::LightQuery => self.light_queries,
            BlockchainRpcTrafficClass::HeavyQuery => self.heavy_queries,
            BlockchainRpcTrafficClass::Broadcast => self.broadcasts,
        };

        RateLimitPolicy { class, bucket }
    }
}

struct BlockchainRpcRateLimitPolicy {
    traffic: BlockchainRpcTrafficLimits,
    whitelist: FastHashSet<IpAddr>,
}

impl BlockchainRpcRateLimitPolicy {
    fn classify(constructor: u32) -> BlockchainRpcTrafficClass {
        match constructor {
            overlay::Ping::TL_ID
            | rpc::GetArchiveInfo::TL_ID
            | rpc::GetPersistentShardStateInfo::TL_ID
            | rpc::GetPersistentQueueStateInfo::TL_ID
            | rpc::GetArchiveChunk::TL_ID
            | rpc::GetBlockDataChunk::TL_ID
            | rpc::GetPersistentQueueStateChunk::TL_ID
            | rpc::GetPersistentShardStateChunk::TL_ID => BlockchainRpcTrafficClass::LightQuery,
            _ => BlockchainRpcTrafficClass::HeavyQuery,
        }
    }
}

impl PublicOverlayRateLimitPolicy for BlockchainRpcRateLimitPolicy {
    type Class = BlockchainRpcTrafficClass;

    fn classify_query(&self, req: &ServiceRequest) -> OverlayIngressPolicyDecision<Self::Class> {
        if self.whitelist.contains(&req.metadata.remote_address.ip()) {
            return OverlayIngressPolicyDecision::Bypass;
        }

        let constructor = match try_handle_prefix(req) {
            Ok((constructor, _)) => constructor,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                return OverlayIngressPolicyDecision::Drop;
            }
        };

        let class = BlockchainRpcRateLimitPolicy::classify(constructor);
        OverlayIngressPolicyDecision::Allow(self.traffic.policy(class))
    }

    fn classify_message(&self, req: &ServiceRequest) -> OverlayIngressPolicyDecision<Self::Class> {
        if self.whitelist.contains(&req.metadata.remote_address.ip()) {
            OverlayIngressPolicyDecision::Bypass
        } else {
            OverlayIngressPolicyDecision::Allow(
                self.traffic.policy(BlockchainRpcTrafficClass::Broadcast),
            )
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BlockchainRpcTrafficClass {
    LightQuery,
    HeavyQuery,
    Broadcast,
}
