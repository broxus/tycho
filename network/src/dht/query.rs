use std::time::Duration;

use bytes::Bytes;
use tycho_util::FastHashSet;

use crate::network::Network;
use crate::types::{PeerId, Request};
use crate::util::NetworkExt;

pub struct Query {
    network: Network,
    request_body: Bytes,
    visited: FastHashSet<PeerId>,
    timeout: Duration,
}

// impl Query {
//     fn visit(&mut self, peer: &PeerId) {
//         tokio::time::timeout(
//             self.timeout,
//             self.network.query(
//                 peer_id,
//                 Request {
//                     version: Default::default(),
//                     body: self.request_body,
//                 },
//             ),
//         )
//     }
// }
