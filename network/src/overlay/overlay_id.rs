use std::borrow::Borrow;
use std::str::FromStr;

use rand::Rng;
use tl_proto::{TlRead, TlWrite};

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, TlRead, TlWrite)]
#[repr(transparent)]
pub struct OverlayId(pub [u8; 32]);

impl OverlayId {
    pub const fn wrap(bytes: &[u8; 32]) -> &Self {
        // SAFETY: `[u8; 32]` has the same layout as `OverlayId`.
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
}

impl Borrow<[u8; 32]> for OverlayId {
    #[inline]
    fn borrow(&self) -> &[u8; 32] {
        &self.0
    }
}

impl<'a> TlRead<'a> for &'a OverlayId {
    type Repr = tl_proto::Boxed;

    #[inline]
    fn read_from(packet: &mut &'a [u8]) -> tl_proto::TlResult<Self> {
        <_>::read_from(packet).map(OverlayId::wrap)
    }
}

impl rand::distr::Distribution<OverlayId> for rand::distr::StandardUniform {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> OverlayId {
        OverlayId(rand::distr::StandardUniform.sample(rng))
    }
}

impl std::fmt::Display for OverlayId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(32);
        for byte in self.0.iter().take(len) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for OverlayId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OverlayId({self})")
    }
}

impl FromStr for OverlayId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut overlay_id = OverlayId([0; 32]);
        hex::decode_to_slice(s, &mut overlay_id.0).map(|_| overlay_id)
    }
}

impl serde::Serialize for OverlayId {
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

impl<'de> serde::Deserialize<'de> for OverlayId {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        const SOME_ID: &str = "5d09fe251943525a30f471791d5b4fea1298613f52ad2ad6d985fed05eb00533";

        let from_json: OverlayId = serde_json::from_str(&format!("\"{SOME_ID}\"")).unwrap();
        let from_str = OverlayId::from_str(SOME_ID).unwrap();
        assert_eq!(from_json, from_str);

        let to_json = serde_json::to_string(&from_json).unwrap();
        let from_json: OverlayId = serde_json::from_str(&to_json).unwrap();
        assert_eq!(from_json, from_str);
    }
}
