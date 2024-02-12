use bytes::Bytes;
use tl_proto::{TlRead, TlWrite};

use crate::types::{AddressList, PeerId};

/// A signed DHT node info.
#[derive(Debug, Clone, TlRead, TlWrite)]
pub struct NodeInfo {
    /// Node public key.
    pub id: PeerId,
    /// A list of possible peer addresses.
    pub address_list: AddressList,
    /// Unix timestamp when the entry was generated.
    pub created_at: u32,
    /// A `ed25519` signature of this entry.
    #[tl(signature)]
    pub signature: Bytes,
}

pub trait WithValue:
    TlWrite<Repr = tl_proto::Boxed> + for<'a> TlRead<'a, Repr = tl_proto::Boxed>
{
    type Value<'a>: TlWrite<Repr = tl_proto::Boxed> + TlRead<'a, Repr = tl_proto::Boxed>;

    fn parse_value(value: Box<Value>) -> tl_proto::TlResult<Self::Value<'static>>;
}

/// Key for values that can only be updated by the owner.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.signedKey", scheme = "proto.tl")]
pub struct SignedKey {
    /// Key name.
    pub name: Bytes,
    /// Key index (version).
    pub idx: u32,
    /// Public key of the owner.
    pub peer_id: PeerId,
}

impl WithValue for SignedKey {
    type Value<'a> = SignedValue;

    fn parse_value(value: Box<Value>) -> tl_proto::TlResult<Self::Value<'static>> {
        match *value {
            Value::Signed(value) => Ok(value),
            Value::Overlay(_) => Err(tl_proto::TlError::UnknownConstructor),
        }
    }
}

/// Key for overlay-managed values.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.overlayKey", scheme = "proto.tl")]
pub struct OverlayKey {
    /// Overlay id.
    pub id: [u8; 32],
    /// Key name.
    pub name: Bytes,
    /// Key index (version).
    pub idx: u32,
}

impl WithValue for OverlayKey {
    type Value<'a> = OverlayValue;

    fn parse_value(value: Box<Value>) -> tl_proto::TlResult<Self::Value<'static>> {
        match *value {
            Value::Signed(_) => Err(tl_proto::TlError::UnknownConstructor),
            Value::Overlay(value) => Ok(value),
        }
    }
}

/// Value with a known owner.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.signedValue", scheme = "proto.tl")]
pub struct SignedValue {
    /// Signed key.
    pub key: SignedKey,
    /// Any data.
    pub data: Bytes,
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
    /// A `ed25519` signature of this entry.
    #[tl(signature)]
    pub signature: Bytes,
}

/// Overlay-managed value.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.overlayValue", scheme = "proto.tl")]
pub struct OverlayValue {
    /// Overlay key.
    pub key: OverlayKey,
    /// Any data.
    pub data: Bytes,
    /// Unix timestamp up to which this value is valid.
    pub expires_at: u32,
}

/// Stored value.
#[derive(Debug, Clone)]
pub enum Value {
    /// Value with a known owner.
    Signed(SignedValue),
    /// Overlay-managed value.
    Overlay(OverlayValue),
}

impl Value {
    pub fn key_name(&self) -> &[u8] {
        match self {
            Self::Signed(value) => value.key.name.as_ref(),
            Self::Overlay(value) => value.key.name.as_ref(),
        }
    }

    pub const fn key_index(&self) -> u32 {
        match self {
            Self::Signed(value) => value.key.idx,
            Self::Overlay(value) => value.key.idx,
        }
    }

    pub const fn expires_at(&self) -> u32 {
        match self {
            Self::Signed(value) => value.expires_at,
            Self::Overlay(value) => value.expires_at,
        }
    }
}

impl TlWrite for Value {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        match self {
            Self::Signed(value) => value.max_size_hint(),
            Self::Overlay(value) => value.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match self {
            Self::Signed(value) => value.write_to(packet),
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
            SignedValue::TL_ID => SignedValue::read_from(packet, offset).map(Self::Signed),
            OverlayValue::TL_ID => OverlayValue::read_from(packet, offset).map(Self::Overlay),
            _ => Err(tl_proto::TlError::UnknownConstructor),
        }
    }
}

/// A response for the [`rpc::FindNode`] query.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.nodesFound", scheme = "proto.tl")]
pub struct NodeResponse {
    /// List of nodes closest to the key.
    pub nodes: Vec<NodeInfo>,
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
    NotFound(Vec<NodeInfo>),
}

/// A response for the [`rpc::GetNodeInfo`] query.
#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "dht.nodeInfoFound", scheme = "proto.tl")]
pub struct NodeInfoResponse {
    /// Signed node info.
    pub info: NodeInfo,
}

/// DHT RPC models.
pub mod rpc {
    use crate::types::RpcQuery;

    use super::*;

    /// Suggest a node to store that value.
    ///
    /// See [`Stored`].
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.store", scheme = "proto.tl")]
    pub struct Store {
        /// A value to store.
        pub value: Value,
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

    impl RpcQuery for FindNode {
        type Response = NodeResponse;
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

    impl RpcQuery for FindValue {
        type Response = ValueResponse;
    }

    /// Requests a signed address list from the node.
    ///
    /// See [`NodeInfoResponse`].
    #[derive(Debug, Clone, TlRead, TlWrite)]
    #[tl(boxed, id = "dht.getNodeInfo", scheme = "proto.tl")]
    pub struct GetNodeInfo;

    impl RpcQuery for GetNodeInfo {
        type Response = NodeInfoResponse;
    }
}
