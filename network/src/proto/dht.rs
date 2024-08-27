use std::sync::Arc;

use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};
use tycho_util::tl;

use crate::types::{PeerId, PeerInfo};
use crate::util::check_peer_signature;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PeerValueKeyName {
    #[tl(id = "dht.peerValueKeyName.nodeInfo")]
    NodeInfo,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum MergedValueKeyName {
    #[tl(id = "dht.mergedValueKeyName.publicOverlayEntries")]
    PublicOverlayEntries,
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

/// Key for group-managed values.
///
/// See [`MergedValueKeyRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.mergedValueKey", scheme = "proto.tl")]
pub struct MergedValueKey {
    /// Key name.
    pub name: MergedValueKeyName,
    /// Group id.
    pub group_id: [u8; 32],
}

/// Key for group-managed values.
///
/// See [`MergedValueKey`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.mergedValueKey", scheme = "proto.tl")]
pub struct MergedValueKeyRef<'tl> {
    /// Key name.
    pub name: MergedValueKeyName,
    /// Group id.
    pub group_id: &'tl [u8; 32],
}

impl MergedValueKeyRef<'_> {
    pub fn as_owned(&self) -> MergedValueKey {
        MergedValueKey {
            name: self.name,
            group_id: *self.group_id,
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

/// Group-managed value.
///
/// See [`MergedValueRef`] for the non-owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.mergedValue", scheme = "proto.tl")]
pub struct MergedValue {
    /// Key info.
    pub key: MergedValueKey,
    /// Any data.
    pub data: Box<[u8]>,
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
}

/// Group-managed value.
///
/// See [`MergedValue`] for the owned version of the struct.
#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "dht.mergedValue", scheme = "proto.tl")]
pub struct MergedValueRef<'tl> {
    /// Key info.
    pub key: MergedValueKeyRef<'tl>,
    /// Any data.
    pub data: &'tl [u8],
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
}

impl MergedValueRef<'_> {
    pub fn as_owned(&self) -> MergedValue {
        MergedValue {
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
    /// Group-managed value.
    Merged(MergedValue),
}

impl Value {
    /// Fully verifies the value.
    ///
    /// NOTE: Might be expensive since it requires signature verification.
    pub fn verify(&self, at: u32, key_hash: &[u8; 32]) -> bool {
        self.verify_ext(at, key_hash, &mut false)
    }

    /// Fully verifies the value.
    ///
    /// NOTE: Might be expensive since it requires signature verification.
    pub fn verify_ext(&self, at: u32, key_hash: &[u8; 32], signature_checked: &mut bool) -> bool {
        match self {
            Self::Peer(value) => {
                let timings_ok = value.expires_at >= at && key_hash == &tl_proto::hash(&value.key);
                if !timings_ok {
                    return false;
                }

                *signature_checked = true;
                check_peer_signature(&value.key.peer_id, &value.signature, value)
            }
            Self::Merged(value) => {
                value.expires_at >= at && key_hash == &tl_proto::hash(&value.key)
            }
        }
    }

    pub const fn expires_at(&self) -> u32 {
        match self {
            Self::Peer(value) => value.expires_at,
            Self::Merged(value) => value.expires_at,
        }
    }
}

impl TlWrite for Value {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        match self {
            Self::Peer(value) => value.max_size_hint(),
            Self::Merged(value) => value.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match self {
            Self::Peer(value) => value.write_to(packet),
            Self::Merged(value) => value.write_to(packet),
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
            MergedValue::TL_ID => MergedValue::read_from(packet, offset).map(Self::Merged),
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
    /// Group-managed value.
    Merged(MergedValueRef<'tl>),
}

impl ValueRef<'_> {
    pub const fn expires_at(&self) -> u32 {
        match self {
            Self::Peer(value) => value.expires_at,
            Self::Merged(value) => value.expires_at,
        }
    }
}

impl TlWrite for ValueRef<'_> {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        match self {
            Self::Peer(value) => value.max_size_hint(),
            Self::Merged(value) => value.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match self {
            Self::Peer(value) => value.write_to(packet),
            Self::Merged(value) => value.write_to(packet),
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
            MergedValue::TL_ID => MergedValueRef::read_from(packet, offset).map(Self::Merged),
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
    #[repr(transparent)]
    pub struct WithPeerInfo {
        /// A signed info of the sender.
        pub peer_info: PeerInfo,
    }

    impl WithPeerInfo {
        pub fn wrap(value: &'_ PeerInfo) -> &'_ Self {
            // SAFETY: `rpc::WithPeerInfo` has the same memory layout as `PeerInfo`.
            unsafe { &*(value as *const PeerInfo).cast() }
        }
    }

    /// Suggest a node to store that value.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.store", scheme = "proto.tl")]
    #[repr(transparent)]
    pub struct Store {
        /// A value to store.
        pub value: Value,
    }

    /// Suggest a node to store that value.
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.store", scheme = "proto.tl")]
    #[repr(transparent)]
    pub struct StoreRef<'tl> {
        /// A value to store.
        pub value: ValueRef<'tl>,
    }

    impl<'tl> StoreRef<'tl> {
        pub fn wrap<'a>(value: &'a ValueRef<'tl>) -> &'a Self {
            // SAFETY: `rpc::StoreRef` has the same memory layout as `ValueRef`.
            unsafe { &*(value as *const ValueRef<'tl>).cast() }
        }
    }

    /// Search for `k` the closest nodes.
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

    /// Search for a value if stored or `k` the closest nodes.
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
