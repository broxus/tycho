use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Sub};

use everscale_crypto::ed25519::KeyPair;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

#[derive(Clone, Copy, TlWrite, TlRead, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);
impl Digest {
    pub const MAX_TL_BYTES: usize = 32;
}

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
    // TODO encode DB key with TL and remove this method
    pub fn wrap(value: [u8; 32]) -> Self {
        Self(value)
    }
    pub fn inner(&self) -> &'_ [u8; 32] {
        &self.0
    }
}

#[derive(Clone, TlWrite, TlRead, PartialEq)]
pub struct Signature([u8; 64]);

impl Signature {
    pub const MAX_TL_BYTES: usize = 64;
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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, TlRead, TlWrite)]
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

    // For metrics. Handle other subtraction cases individually. Addition is meaningless.
    pub fn diff_f64(self, rhs: Self) -> f64 {
        diff_f64(self.0, rhs.0)
    }
}

fn diff_f64<T>(lhs: T, rhs: T) -> f64
where
    T: Sub<Output = T> + Ord,
    u32: TryFrom<T>,
{
    if lhs >= rhs {
        u32::try_from(lhs - rhs).map_or(f64::INFINITY, f64::from)
    } else {
        -u32::try_from(rhs - lhs).map_or(f64::INFINITY, f64::from)
    }
}

// Impls for types of config values. One also may unwrap `u32` from `Round` and use it as amount.
macro_rules! impl_round_add_sub {
    ($($ty:ty),*$(,)?) => {
        $(impl Add<$ty> for Round {
            type Output = Self;
            #[inline]
            fn add(self, rhs: $ty) -> Self {
                Self(self.0.saturating_add(rhs as _))
            }
        }
        impl Sub<$ty> for Round {
            type Output = Self;
            #[inline]
            fn sub(self, rhs: $ty) -> Self {
                Self(self.0.saturating_sub(rhs as _))
            }
        })*
    };
}
impl_round_add_sub! { u8, u16, u32 }

#[derive(Copy, Clone, TlRead, TlWrite, PartialEq, Eq, PartialOrd, Ord, Debug)]
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

    pub fn diff_f64(self, rhs: Self) -> f64 {
        diff_f64(self.0, rhs.0)
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
