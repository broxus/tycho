use std::sync::Arc;

use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};

use crate::types::{PeerId, PeerInfo};
use crate::util::{check_peer_signature, tl};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PeerValueKeyName {
    #[tl(id = "dht.peerValueKeyName.nodeInfo")]
    NodeInfo,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum OverlayValueKeyName {
    #[tl(id = "dht.overlayValueKeyName.peersList")]
    PeersList,
}

/// Key for values that can only be updated by the owner.
///
/// See [`SignedValueKeyRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.peerValueKey", scheme = "proto.tl")]
pub struct PeerValueKey {
    /// Key name.
    pub name: PeerValueKeyName,
    /// Public key of the owner.
    pub peer_id: PeerId,
}

/// Key for values that can only be updated by the owner.
///
/// See [`SignedValueKey`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.peerValueKey", scheme = "proto.tl")]
pub struct PeerValueKeyRef<'tl> {
    /// Key name.
    pub name: PeerValueKeyName,
    /// Public key of the owner.
    pub peer_id: &'tl PeerId,
}

impl PeerValueKeyRef<'_> {
    pub fn as_owned(&self) -> PeerValueKey {
        PeerValueKey {
            name: self.name,
            peer_id: *self.peer_id,
        }
    }
}

/// Key for overlay-managed values.
///
/// See [`OverlayValueKeyRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.overlayValueKey", scheme = "proto.tl")]
pub struct OverlayValueKey {
    /// Key name.
    pub name: OverlayValueKeyName,
    /// Overlay id.
    pub overlay_id: [u8; 32],
}

/// Key for overlay-managed values.
///
/// See [`OverlayValueKey`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.overlayValueKey", scheme = "proto.tl")]
pub struct OverlayValueKeyRef<'tl> {
    /// Key name.
    pub name: OverlayValueKeyName,
    /// Overlay id.
    pub overlay_id: &'tl [u8; 32],
}

impl OverlayValueKeyRef<'_> {
    pub fn as_owned(&self) -> OverlayValueKey {
        OverlayValueKey {
            name: self.name,
            overlay_id: *self.overlay_id,
        }
    }
}

/// Value with a known owner.
///
/// See [`PeerValueRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.peerValue", scheme = "proto.tl")]
pub struct PeerValue {
    /// Peer value key.
    pub key: PeerValueKey,
    /// Any data.
    pub data: Box<[u8]>,
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
    /// A `ed25519` signature of this entry.
    #[tl(signature, with = "tl::signature_owned")]
    pub signature: Box<[u8; 64]>,
}

/// Value with a known owner.
///
/// See [`PeerValue`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.peerValue", scheme = "proto.tl")]
pub struct PeerValueRef<'tl> {
    /// Peer value key.
    pub key: PeerValueKeyRef<'tl>,
    /// Any data.
    pub data: &'tl [u8],
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
    /// A `ed25519` signature of this entry.
    #[tl(signature, with = "tl::signature_ref")]
    pub signature: &'tl [u8; 64],
}

impl PeerValueRef<'_> {
    pub fn as_owned(&self) -> PeerValue {
        PeerValue {
            key: self.key.as_owned(),
            data: Box::from(self.data),
            expires_at: self.expires_at,
            signature: Box::new(*self.signature),
        }
    }
}

/// Overlay-managed value.
///
/// See [`OverlayValueRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.overlayValue", scheme = "proto.tl")]
pub struct OverlayValue {
    /// Overlay key.
    pub key: OverlayValueKey,
    /// Any data.
    pub data: Box<[u8]>,
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
}

/// Overlay-managed value.
///
/// See [`OverlayValue`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.overlayValue", scheme = "proto.tl")]
pub struct OverlayValueRef<'tl> {
    /// Overlay key.
    pub key: OverlayValueKeyRef<'tl>,
    /// Any data.
    pub data: &'tl [u8],
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
}

impl OverlayValueRef<'_> {
    pub fn as_owned(&self) -> OverlayValue {
        OverlayValue {
            key: self.key.as_owned(),
            data: Box::from(self.data),
            expires_at: self.expires_at,
        }
    }
}

/// Stored value.
///
/// See [`ValueRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Value with a known owner.
    Peer(PeerValue),
    /// Overlay-managed value.
    Overlay(OverlayValue),
}

impl Value {
    pub fn is_valid(&self, at: u32, key_hash: &[u8; 32]) -> bool {
        match self {
            Self::Peer(value) => {
                value.expires_at >= at
                    && key_hash == &tl_proto::hash(&value.key)
                    && check_peer_signature(&value.key.peer_id, &value.signature, value)
            }
            Self::Overlay(value) => {
                value.expires_at >= at && key_hash == &tl_proto::hash(&value.key)
            }
        }
    }

    pub const fn expires_at(&self) -> u32 {
        match self {
            Self::Peer(value) => value.expires_at,
            Self::Overlay(value) => value.expires_at,
        }
    }
}

impl TlWrite for Value {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        match self {
            Self::Peer(value) => value.max_size_hint(),
            Self::Overlay(value) => value.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match self {
            Self::Peer(value) => value.write_to(packet),
            Self::Overlay(value) => value.write_to(packet),
        }
    }
}

impl<'a> TlRead<'a> for Value {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        let id = u32::read_from(packet, offset)?;
        *offset -= 4;
        match id {
            PeerValue::TL_ID => PeerValue::read_from(packet, offset).map(Self::Peer),
            OverlayValue::TL_ID => OverlayValue::read_from(packet, offset).map(Self::Overlay),
            _ => Err(tl_proto::TlError::UnknownConstructor),
        }
    }
}

/// Stored value.
///
/// See [`Value`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef<'tl> {
    /// Value with a known owner.
    Peer(PeerValueRef<'tl>),
    /// Overlay-managed value.
    Overlay(OverlayValueRef<'tl>),
}

impl ValueRef<'_> {
    pub fn is_valid(&self, at: u32, key_hash: &[u8; 32]) -> bool {
        match self {
            Self::Peer(value) => {
                value.expires_at >= at
                    && key_hash == &tl_proto::hash(&value.key)
                    && check_peer_signature(value.key.peer_id, value.signature, value)
            }
            Self::Overlay(value) => {
                value.expires_at >= at && key_hash == &tl_proto::hash(&value.key)
            }
        }
    }

    pub const fn expires_at(&self) -> u32 {
        match self {
            Self::Peer(value) => value.expires_at,
            Self::Overlay(value) => value.expires_at,
        }
    }
}

impl TlWrite for ValueRef<'_> {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        match self {
            Self::Peer(value) => value.max_size_hint(),
            Self::Overlay(value) => value.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match self {
            Self::Peer(value) => value.write_to(packet),
            Self::Overlay(value) => value.write_to(packet),
        }
    }
}

impl<'a> TlRead<'a> for ValueRef<'a> {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        let id = u32::read_from(packet, offset)?;
        *offset -= 4;
        match id {
            PeerValue::TL_ID => PeerValueRef::read_from(packet, offset).map(Self::Peer),
            OverlayValue::TL_ID => OverlayValueRef::read_from(packet, offset).map(Self::Overlay),
            _ => Err(tl_proto::TlError::UnknownConstructor),
        }
    }
}

/// A response for the [`rpc::FindNode`] query.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.nodesFound", scheme = "proto.tl")]
pub struct NodeResponse {
    /// List of nodes closest to the key.
    #[tl(with = "tl::VecWithMaxLen::<20>")]
    pub nodes: Vec<Arc<PeerInfo>>,
}

/// A response for the [`rpc::FindValue`] query.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ValueResponse {
    /// An existing value for the specified key.
    #[tl(id = "dht.valueFound")]
    Found(Box<Value>),
    /// List of nodes closest to the key.
    #[tl(id = "dht.valueNotFound")]
    NotFound(#[tl(with = "tl::VecWithMaxLen::<20>")] Vec<Arc<PeerInfo>>),
}

/// A response for the [`rpc::FindValue`] query.
#[derive(Debug, Clone)]
pub enum ValueResponseRaw {
    Found(Bytes),
    NotFound(Vec<Arc<PeerInfo>>),
}

impl TlWrite for ValueResponseRaw {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + match self {
            Self::Found(value) => value.max_size_hint(),
            Self::NotFound(nodes) => nodes.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        const FOUND_TL_ID: u32 = tl_proto::id!("dht.valueFound", scheme = "proto.tl");
        const NOT_FOUND_TL_ID: u32 = tl_proto::id!("dht.valueNotFound", scheme = "proto.tl");

        match self {
            Self::Found(value) => {
                packet.write_u32(FOUND_TL_ID);
                packet.write_raw_slice(value);
            }
            Self::NotFound(nodes) => {
                packet.write_u32(NOT_FOUND_TL_ID);
                nodes.write_to(packet);
            }
        }
    }
}

/// A response for the [`rpc::GetNodeInfo`] query.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.nodeInfoFound", scheme = "proto.tl")]
pub struct NodeInfoResponse {
    /// Signed node info.
    pub info: PeerInfo,
}

/// DHT RPC models.
pub mod rpc {
    use super::*;

    /// Query wrapper with an announced peer info.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.withPeerInfo", scheme = "proto.tl")]
    pub struct WithPeerInfo {
        /// A signed info of the sender.
        pub peer_info: Arc<PeerInfo>,
    }

    /// Query wrapper with an announced peer info.
    #[derive(Debug, Clone, TlWrite)]
    #[tl(boxed, id = "dht.withPeerInfo", scheme = "proto.tl")]
    pub struct WithPeerInfoRef<'tl> {
        /// A signed info of the sender.
        pub peer_info: &'tl PeerInfo,
    }

    /// Suggest a node to store that value.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.store", scheme = "proto.tl")]
    pub struct Store {
        /// A value to store.
        pub value: Value,
    }

    /// Suggest a node to store that value.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.store", scheme = "proto.tl")]
    pub struct StoreRef<'tl> {
        /// A value to store.
        pub value: ValueRef<'tl>,
    }

    /// Search for `k` closest nodes.
    ///
    /// See [`NodeResponse`].
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.findNode", scheme = "proto.tl")]
    pub struct FindNode {
        /// Key hash.
        pub key: [u8; 32],
        /// Maximum number of nodes to return.
        pub k: u32,
    }

    /// Search for a value if stored or `k` closest nodes.
    ///
    /// See [`ValueResponse`].
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.findValue", scheme = "proto.tl")]
    pub struct FindValue {
        /// Key hash.
        pub key: [u8; 32],
        /// Maximum number of nodes to return.
        pub k: u32,
    }

    /// Requests a signed address list from the node.
    ///
    /// See [`NodeInfoResponse`].
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.getNodeInfo", scheme = "proto.tl")]
    pub struct GetNodeInfo;
}
