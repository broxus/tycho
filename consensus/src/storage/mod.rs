pub use adapter_store::*;
pub use db::*;
pub use db_cleaner::*;
pub use status_flags::*;
pub use store::*;

mod adapter_store;
mod db;
mod db_cleaner;
mod status_flags;
mod store;
mod tables;

use crate::effects::AltFormat;
use crate::models::{Digest, Round};

const POINT_KEY_LEN: usize = Round::MAX_TL_SIZE + Digest::MAX_TL_BYTES;

fn fill_point_key(round: u32, digest: &[u8; 32], key: &mut [u8; POINT_KEY_LEN]) {
    fill_point_prefix(round, key);
    key[4..].copy_from_slice(&digest[..]);
}

fn fill_point_prefix(round: u32, key: &mut [u8; POINT_KEY_LEN]) {
    key[..4].copy_from_slice(&round.to_be_bytes()[..]);
}

/// function of limited usage: zero round does not exist by application logic
/// and 4 zero bytes usually represents empty value in storage;
/// here None represents value less than 4 bytes
fn parse_round(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 4 {
        None
    } else {
        let mut round_bytes = [0_u8; 4];
        round_bytes.copy_from_slice(&bytes[..4]);
        Some(u32::from_be_bytes(round_bytes))
    }
}

fn format_point_key(bytes: &[u8]) -> String {
    if let Some(round) = parse_round(bytes) {
        if bytes.len() == POINT_KEY_LEN {
            format!("round {round} digest {}", (&bytes[4..]).alt())
        } else {
            format!("unknown {} bytes: {:.12}", bytes.len(), bytes.alt())
        }
    } else {
        format!("unknown short {} bytes {}", bytes.len(), bytes.alt())
    }
}
