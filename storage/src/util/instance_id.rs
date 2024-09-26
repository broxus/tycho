use rand::RngCore;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct InstanceId(pub [u8; 16]);

impl InstanceId {
    pub fn new() -> Self {
        let mut bytes = [0u8; 16];

        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut bytes);

        Self(bytes)
    }

    #[inline]
    pub fn from_slice(slice: &[u8]) -> Self {
        Self(slice.try_into().expect("slice with incorrect length"))
    }
}

impl Default for InstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[u8]> for InstanceId {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub const INSTANCE_ID: &[u8] = b"instance_id";
