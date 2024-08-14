use std::sync::Arc;

use everscale_types::models::*;
use tl_proto::{TlRead, TlWrite};

/// Data for computing a private overlay id.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "validator.overlayIdData", scheme = "proto.tl")]
pub struct OverlayIdData {
    pub zerostate_root_hash: [u8; 32],
    pub zerostate_file_hash: [u8; 32],
    #[tl(with = "tycho_block_util::tl::shard_ident")]
    pub shard_ident: ShardIdent,
    /// Timestamp of the corresponding validator set.
    pub session_id: u32,
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Exchange {
    #[tl(id = "validator.exchange.complete")]
    Complete(#[tl(with = "tycho_util::tl::signature_arc")] Arc<[u8; 64]>),
    #[tl(id = "validator.exchange.cached")]
    Cached,
}

pub mod rpc {
    use super::*;

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "validator.exchangeSignatures", scheme = "proto.tl")]
    pub struct ExchangeSignaturesOwned {
        pub block_seqno: u32,
        #[tl(with = "tycho_util::tl::signature_arc")]
        pub signature: Arc<[u8; 64]>,
    }

    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "validator.exchangeSignatures", scheme = "proto.tl")]
    pub struct ExchangeSignaturesRef<'tl> {
        pub block_seqno: u32,
        #[tl(with = "tycho_util::tl::signature_ref")]
        pub signature: &'tl [u8; 64],
    }
}
