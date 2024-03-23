use std::str::FromStr;

use everscale_crypto::ed25519;
use rand::Rng;
use tl_proto::{TlRead, TlWrite};

#[derive(Clone, Copy, TlRead, TlWrite, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[tl(boxed, id = "transport.peerId", scheme = "proto.tl")]
#[repr(transparent)]
pub struct PeerId(pub [u8; 32]);

impl PeerId {
    pub const fn wrap(bytes: &[u8; 32]) -> &Self {
        // SAFETY: `[u8; 32]` has the same layout as `PeerId`.
        unsafe { &*(bytes as *const [u8; 32]).cast::<Self>() }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[inline]
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
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

impl FromStr for PeerId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut peer_id = PeerId([0; 32]);
        hex::decode_to_slice(s, &mut peer_id.0).map(|_| peer_id)
    }
}

impl serde::Serialize for PeerId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> serde::Deserialize<'de> for PeerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(tycho_util::serde_helpers::StrVisitor::new())
        } else {
            <[u8; 32]>::deserialize(deserializer).map(Self)
        }
    }
}

impl rand::distributions::Distribution<PeerId> for rand::distributions::Standard {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> PeerId {
        PeerId(rand::distributions::Standard.sample(rng))
    }
}

impl From<ed25519::PublicKey> for PeerId {
    #[inline]
    fn from(public_key: ed25519::PublicKey) -> Self {
        Self(public_key.to_bytes())
    }
}

impl PartialEq<&PeerId> for PeerId {
    #[inline]
    fn eq(&self, other: &&PeerId) -> bool {
        self == *other
    }
}

impl PartialEq<PeerId> for &PeerId {
    #[inline]
    fn eq(&self, other: &PeerId) -> bool {
        *self == other
    }
}

impl std::ops::BitXor for PeerId {
    type Output = PeerId;

    #[inline]
    fn bitxor(mut self, rhs: PeerId) -> Self::Output {
        self ^= rhs;
        self
    }
}

impl std::ops::BitXor<&PeerId> for PeerId {
    type Output = PeerId;

    #[inline]
    fn bitxor(mut self, rhs: &PeerId) -> Self::Output {
        self ^= rhs;
        self
    }
}

impl std::ops::BitXor<&PeerId> for &PeerId {
    type Output = PeerId;

    #[inline]
    fn bitxor(self, rhs: &PeerId) -> Self::Output {
        *self ^ rhs
    }
}

impl std::ops::BitXorAssign for PeerId {
    #[inline]
    fn bitxor_assign(&mut self, rhs: PeerId) {
        std::ops::BitXorAssign::bitxor_assign(self, &rhs);
    }
}

impl std::ops::BitXorAssign<&PeerId> for PeerId {
    #[inline]
    fn bitxor_assign(&mut self, rhs: &PeerId) {
        for (left, right) in self.0.iter_mut().zip(&rhs.0) {
            *left ^= right;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        const SOME_ID: &str = "5d09fe251943525a30f471791d5b4fea1298613f52ad2ad6d985fed05eb00533";

        let from_json: PeerId = serde_json::from_str(&format!("\"{SOME_ID}\"")).unwrap();
        let from_str = PeerId::from_str(SOME_ID).unwrap();
        assert_eq!(from_json, from_str);

        let to_json = serde_json::to_string(&from_json).unwrap();
        let from_json: PeerId = serde_json::from_str(&to_json).unwrap();
        assert_eq!(from_json, from_str);
    }
}
