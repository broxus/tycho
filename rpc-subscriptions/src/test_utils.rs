use tycho_types::models::StdAddr;
use uuid::Uuid;

pub(crate) fn make_addr(i: u32) -> StdAddr {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&i.to_be_bytes());
    StdAddr {
        anycast: None,
        workchain: 0,
        address: bytes.into(),
    }
}

pub(crate) fn make_uuid(i: u8) -> Uuid {
    Uuid::from_bytes([i; 16])
}
