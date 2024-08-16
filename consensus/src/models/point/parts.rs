use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Sub};

use everscale_crypto::ed25519::KeyPair;
use serde::{Deserialize, Serialize};
use tycho_network::PeerId;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Display for Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(32);
        for byte in self.0.iter().take(len) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Debug for Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Digest(")?;
        Display::fmt(self, f)?;
        f.write_str(")")
    }
}

impl Digest {
    pub(super) fn new(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).into())
    }
    pub fn inner(&self) -> &'_ [u8; 32] {
        &self.0
    }
}

#[derive(Clone, PartialEq)]
pub struct Signature([u8; 64]);

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <&[u8]>::deserialize(deserializer)?;
        if bytes.len() != 64 {
            Err(serde::de::Error::invalid_length(bytes.len(), &"64"))
        } else {
            let mut target = [0_u8; 64];
            target.copy_from_slice(bytes);
            Ok(Signature(target))
        }
    }
}

impl Display for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(64);
        for byte in self.0.iter().take(len) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl Debug for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Signature(")?;
        Display::fmt(self, f)?;
        f.write_str(")")
    }
}

impl Signature {
    pub fn new(local_keypair: &KeyPair, digest: &Digest) -> Self {
        Self(local_keypair.sign_raw(digest.0.as_slice()))
    }

    pub fn verifies(&self, signer: &PeerId, digest: &Digest) -> bool {
        match signer.as_public_key() {
            Some(pub_key) => pub_key.verify_raw(digest.0.as_slice(), &self.0),
            None => false,
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Round(pub u32);

impl Round {
    /// stub that cannot be used even by genesis round
    pub const BOTTOM: Self = Self(0);
    pub fn prev(&self) -> Self {
        self.0
            .checked_sub(1)
            .map(Round)
            .expect("DAG round number underflow, fix dag initial configuration")
    }
    pub fn next(&self) -> Self {
        self.0
            .checked_add(1)
            .map(Round)
            .expect("DAG round number overflow, inner type exhausted")
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct UnixTime(u64);

impl UnixTime {
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }
    pub fn now() -> Self {
        Self(
            u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("current time since unix epoch")
                    .as_millis(),
            )
            .expect("current Unix time in millis as u64"),
        )
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Add for UnixTime {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl Sub for UnixTime {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl Display for UnixTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
