use std::sync::Arc;

use crate::types::{PeerId, PeerInfo};

pub fn make_peer_info_stub(id: PeerId) -> Arc<PeerInfo> {
    Arc::new(PeerInfo {
        id,
        address_list: Default::default(),
        created_at: 0,
        expires_at: u32::MAX,
        signature: Box::new([0; 64]),
    })
}
