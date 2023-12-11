use everscale_crypto::ed25519;
use tl_proto::{TlRead, TlWrite};

#[derive(Clone, Copy, TlRead, TlWrite, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[tl(boxed, id = "transport.peerId", scheme = "proto.tl")]
#[repr(transparent)]
pub struct PeerId(pub [u8; 32]);

impl PeerId {
    pub fn wrap(bytes: &[u8; 32]) -> &Self {
        // SAFETY: `[u8; 32]` has the same layout as `PeerId`.
        unsafe { &*(bytes as *const [u8; 32]).cast::<Self>() }
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn as_public_key(&self) -> Option<ed25519::PublicKey> {
        ed25519::PublicKey::from_bytes(self.0)
    }
}

impl<'a> TlRead<'a> for &'a PeerId {
    type Repr = tl_proto::Boxed;

    #[inline]
    fn read_from(packet: &'a [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        if u32::read_from(packet, offset)? != PeerId::TL_ID {
            return Err(tl_proto::TlError::UnknownConstructor);
        }
        <_>::read_from(packet, offset).map(PeerId::wrap)
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(32);
        for byte in self.0.iter().take(len) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PeerId({self})")
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        })
    }
}
