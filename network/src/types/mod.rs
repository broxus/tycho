pub use self::peer_id::*;

mod peer_id;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum Version {
    #[default]
    V1 = 1,
}

impl TryFrom<u16> for Version {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            _ => Err(anyhow::anyhow!("invalid version: {value}")),
        }
    }
}

impl Version {
    pub fn to_u16(self) -> u16 {
        self as u16
    }
}

pub struct Request<T> {
    pub version: Version,
    pub body: T,
}

pub struct Response<T> {
    pub version: Version,
    pub body: T,
}
