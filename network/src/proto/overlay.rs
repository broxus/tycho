use std::sync::Arc;

use tl_proto::{TlRead, TlWrite};

use crate::types::PeerId;
use crate::util::tl;

/// A data to sign for [`PublicEntry`].
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.publicEntryToSign", scheme = "proto.tl")]
pub struct PublicEntryToSign<'tl> {
    /// Public overlay id.
    pub overlay_id: &'tl [u8; 32],
    /// Node public key.
    pub peer_id: &'tl PeerId,
    /// Unix timestamp when the info was generated.
    pub created_at: u32,
}

/// A public overlay entry.
#[derive(Debug, Clone, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.publicEntry", scheme = "proto.tl")]
pub struct PublicEntry {
    /// Node public key.
    pub peer_id: PeerId,
    /// Unix timestamp when the info was generated.
    pub created_at: u32,
    /// A signature of the [`PublicEntryToSign`] (as boxed).
    #[tl(signature, with = "tl::signature_owned")]
    pub signature: Box<[u8; 64]>,
}

/// A list of public overlay entries.
#[derive(Debug, Clone, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PublicEntriesResponse {
    #[tl(id = "overlay.publicEntries")]
    PublicEntries(#[tl(with = "tl::VecWithMaxLen::<20>")] Vec<Arc<PublicEntry>>),
    #[tl(id = "overlay.overlayNotFound")]
    OverlayNotFound,
}

/// Overlay RPC models.
pub mod rpc {
    use super::*;

    /// Exchanges random entries of the specified public overlay.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "overlay.exchangeRandomPublicEntries", scheme = "proto.tl")]
    pub struct ExchangeRandomPublicEntries {
        /// Public overlay id.
        pub overlay_id: [u8; 32],
        /// A list of public overlay entries.
        #[tl(with = "tl::VecWithMaxLen::<20>")]
        pub entries: Vec<Arc<PublicEntry>>,
    }

    /// Overlay query/message prefix with an overlay id.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "overlay.prefix", scheme = "proto.tl")]
    pub struct Prefix<'tl> {
        pub overlay_id: &'tl [u8; 32],
    }
}
