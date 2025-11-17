use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Sub};

use serde::Serialize;
use tl_proto::{TlRead, TlWrite};
use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::serde_helpers;

#[derive(Clone, Copy, TlWrite, TlRead, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
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

impl serde::Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde_helpers::Base64BytesWithLimit::<32>::serialize(&self.0, serializer)
    }
}

impl Digest {
    pub const MAX_TL_BYTES: usize = 32;

    pub(super) const ZERO: Self = Self([0; 32]);

    pub(super) fn new(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).into())
    }

    pub fn wrap(bytes: &[u8; 32]) -> &Self {
        // SAFETY: `[u8; 32]` has the same layout as `Digest`
        unsafe { &*(bytes as *const [u8; 32]).cast::<Self>() }
    }

    pub fn inner(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, PartialEq, TlRead, TlWrite, Serialize)]
#[repr(transparent)]
pub struct Signature(#[serde(with = "serde_helpers::signature")] [u8; 64]);

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
    pub const MAX_TL_BYTES: usize = 64;

    pub(super) const ZERO: Self = Self([0; 64]);

    pub(super) fn inner(&self) -> &[u8; 64] {
        &self.0
    }

    pub fn new(local_keypair: &KeyPair, digest: &Digest) -> Self {
        Self(local_keypair.sign_raw(digest.0.as_slice()))
    }

    pub fn verifies(&self, signer: &PeerId, digest: &Digest) -> bool {
        match signer.as_public_key() {
            Some(pub_key) => pub_key.verify_raw(digest.0.as_slice(), &self.0),
            None => false,
        }
    }

    fn wrap(bytes: &[u8; 64]) -> &Self {
        // SAFETY: `[u8; 64]` has the same layout as `Signature`
        unsafe { &*(bytes as *const [u8; 64]).cast::<Self>() }
    }
}

macro_rules! impl_tl_read_bare_ref {
    ($($ty:ty),+ $(,)?) => {$(
        impl<'a> TlRead<'a> for &'a $ty {
            type Repr = tl_proto::Bare;

            #[inline]
            fn read_from(packet: &mut &'a [u8]) -> tl_proto::TlResult<Self> {
                <_>::read_from(packet).map(<$ty>::wrap)
            }
        }
    )+};
}

impl_tl_read_bare_ref! { Digest, Signature }

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, TlRead, TlWrite)]
// new block for rust-fmt
#[derive(Serialize)]
pub struct Round(pub u32);

impl Round {
    pub const MAX_TL_SIZE: usize = 4;
}

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

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash, TlRead, TlWrite)]
// new block for rust-fmt
#[derive(Serialize)]
pub struct UnixTime(u64);

impl UnixTime {
    pub const MAX_TL_BYTES: usize = 8;
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }
    pub fn now() -> Self {
        Self(tycho_util::time::MonotonicClock::now_millis())
    }

    pub fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn millis(&self) -> u64 {
        self.0
    }
}

impl Display for UnixTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

macro_rules! impl_add_sub {
    ($ty:ty, $($rhs:ty => $cast:ident),+ $(,)?) => {$(
        impl Add<$rhs> for $ty {
            type Output = Self;
            #[inline]
            fn add(self, rhs: $rhs) -> Self {
                Self(self.0.saturating_add(rhs.$cast()))
            }
        }
        impl Sub<$rhs> for $ty {
            type Output = Self;
            #[inline]
            fn sub(self, rhs: $rhs) -> Self {
                Self(self.0.saturating_sub(rhs.$cast()))
            }
        }
    )+};
}

macro_rules! impl_diff_64 {
    ($($ty:ty),+ $(,)?) => {$(
        impl $ty {
            /// For metrics
            pub fn diff_f64(self, rhs: Self) -> f64 {
                if self >= rhs {
                    u32::try_from(self.0 - rhs.0).map_or(f64::INFINITY, f64::from)
                } else {
                    -u32::try_from(rhs.0 - self.0).map_or(f64::INFINITY, f64::from)
                }
            }
        }
    )+};
}

// Impls for types of config values. One also may unwrap `u32` from `Round` and use it as amount.
impl_add_sub! { Round, u8 => into, u16 => into, u32 => into, }

impl_add_sub! { UnixTime, UnixTime => millis, }

impl_diff_64! { Round, UnixTime }
