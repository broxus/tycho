use serde::{Deserialize, Serialize};
use tl_proto::{TlRead, TlWrite};
use tycho_util::{serde_helpers, tl};

use crate::types::{Address, PeerId};
use crate::util::check_peer_signature;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PeerAffinity {
    High,
    Allowed,
    Never,
}

/// A signed node info.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TlRead, TlWrite)]
pub struct PeerInfo {
    /// Node public key.
    pub id: PeerId,
    /// Multiple possible addresses for the same peer.
    #[tl(with = "tl_address_list")]
    pub address_list: Box<[Address]>,
    /// Unix timestamp when the info was generated.
    pub created_at: u32,
    /// Unix timestamp up to which the info is valid.
    pub expires_at: u32,
    /// A `ed25519` signature of the info.
    #[serde(with = "serde_helpers::signature")]
    #[tl(signature, with = "tl::signature_owned")]
    pub signature: Box<[u8; 64]>,
}

impl PeerInfo {
    pub const MAX_ADDRESSES: usize = 4;

    /// Fully verifies the peer info.
    ///
    /// NOTE: Might be expensive since it requires signature verification.
    pub fn verify(&self, at: u32) -> bool {
        self.verify_ext(at, &mut false)
    }

    /// Fully verifies the peer info.
    ///
    /// NOTE: Might be expensive since it requires signature verification.
    pub fn verify_ext(&self, at: u32, signature_checked: &mut bool) -> bool {
        const CLOCK_THRESHOLD: u32 = 1;

        let timings_ok = self.created_at <= at + CLOCK_THRESHOLD
            && self.expires_at >= at
            && !self.address_list.is_empty();

        if !timings_ok {
            return false;
        }

        *signature_checked = true;
        check_peer_signature(&self.id, &self.signature, self)
    }

    pub fn is_expired(&self, at: u32) -> bool {
        self.expires_at < at
    }

    pub fn iter_addresses(&self) -> std::slice::Iter<'_, Address> {
        self.address_list.iter()
    }
}

mod tl_address_list {
    use super::*;

    pub fn size_hint(address_list: &[Address]) -> usize {
        4 + address_list
            .iter()
            .map(Address::max_size_hint)
            .sum::<usize>()
    }

    pub fn write<P: tl_proto::TlPacket>(address_list: &[Address], packet: &mut P) {
        address_list.write_to(packet);
    }

    pub fn read(packet: &mut &[u8]) -> tl_proto::TlResult<Box<[Address]>> {
        use tl_proto::TlError;

        let len = u32::read_from(packet)? as usize;
        if len == 0 || len > PeerInfo::MAX_ADDRESSES {
            return Err(TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(Address::read_from(packet)?);
        }

        Ok(items.into_boxed_slice())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn serde() {
        let target_peer_info = PeerInfo {
            id: PeerId::from_str(
                "40ed1f0e3730d9086156e706b0706b21805db8a30a2b7c73a837403e553124ee",
            )
            .unwrap(),
            address_list: Box::new([Address::from_str("101.102.103.104:12345").unwrap()]),
            created_at: 1700000000,
            expires_at: 1710000000,
            signature: Box::new([
                0xe4, 0x3b, 0xc4, 0x50, 0x73, 0xe6, 0xe2, 0x5e, 0xfa, 0xb0, 0x74, 0xc8, 0xef, 0x33,
                0xdb, 0x61, 0xf3, 0x4c, 0x68, 0xec, 0x56, 0xae, 0x38, 0x88, 0xfb, 0xc0, 0x2b, 0x1b,
                0x44, 0x6b, 0xe1, 0xc3, 0xb1, 0xdb, 0x4d, 0x34, 0xeb, 0x37, 0x03, 0x96, 0xc2, 0x9d,
                0xb2, 0xd8, 0xc0, 0x41, 0x2b, 0x9f, 0x70, 0x9a, 0x8f, 0x3c, 0x1d, 0xe6, 0x8e, 0x28,
                0x44, 0x1d, 0x7a, 0x4f, 0x39, 0xc5, 0xe1, 0x3d,
            ]),
        };

        let target_peer_info_str = r#"{
  "id": "40ed1f0e3730d9086156e706b0706b21805db8a30a2b7c73a837403e553124ee",
  "address_list": [
    "101.102.103.104:12345"
  ],
  "created_at": 1700000000,
  "expires_at": 1710000000,
  "signature": "5DvEUHPm4l76sHTI7zPbYfNMaOxWrjiI+8ArG0Rr4cOx20006zcDlsKdstjAQSufcJqPPB3mjihEHXpPOcXhPQ=="
}"#;
        assert_eq!(
            serde_json::to_string_pretty(&target_peer_info).unwrap(),
            target_peer_info_str
        );

        let from_json: PeerInfo = serde_json::from_str(target_peer_info_str).unwrap();
        assert_eq!(from_json, target_peer_info);
    }
}
