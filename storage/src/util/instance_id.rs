#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct InstanceId(pub [u8; 16]);

impl InstanceId {
    #[inline]
    pub fn from_slice(slice: &[u8]) -> Self {
        Self(slice.try_into().expect("slice with incorrect length"))
    }
}

impl rand::distributions::Distribution<InstanceId> for rand::distributions::Standard {
    #[inline]
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> InstanceId {
        InstanceId(rng.gen())
    }
}

impl AsRef<[u8]> for InstanceId {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
