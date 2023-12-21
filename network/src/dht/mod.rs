use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;

use self::routing::RoutingTable;
use self::storage::Storage;
use crate::network::WeakNetwork;
use crate::proto;
use crate::types::PeerId;

mod routing;
mod storage;

pub struct Dht(Arc<DhtInner>);

impl Dht {
    pub async fn find_peers(&self, key: &PeerId) -> Result<Vec<PeerId>> {
        todo!()
    }

    pub async fn find_value<T>(&self, key: T) -> Result<T::Value<'static>>
    where
        T: proto::dht::WithValue,
    {
        todo!()
    }
}

struct DhtInner {
    local_id: PeerId,
    routing_table: Mutex<RoutingTable>,
    last_table_refersh: Instant,
    storage: Storage,
    network: WeakNetwork,
}
