use std::fmt;

use tycho_types::models::StdAddr;
use tycho_types::prelude::HashBytes;

macro_rules! impl_id_type {
    ($vis:vis $name:ident $( { $($method:item)* } )?) => {
        #[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Copy, Clone, Debug, Default)]
        #[repr(transparent)]
        $vis struct $name(pub u32);

        impl $name {
            $( $($method)* )?
        }

        impl From<u32> for $name {
            fn from(value: u32) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<$name> for usize {
            fn from(value: $name) -> Self {
                value.0 as usize
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

impl_id_type!(pub ClientId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
});
impl_id_type!(pub(crate) InternedAddrId);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ClientStats {
    pub client_id: ClientId,
    pub subscription_count: usize,
}

#[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Copy, Clone, Debug)]
pub(crate) struct Addr {
    pub(crate) wc: i8,
    pub(crate) address: HashBytes,
}

impl From<StdAddr> for Addr {
    fn from(value: StdAddr) -> Self {
        Self {
            wc: value.workchain,
            address: value.address,
        }
    }
}
